package bigcache

import (
	"errors"
	"sync"

	"github.com/allegro/bigcache/v3/queue"
)

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)

// Metadata contains information of a specific entry
type Metadata struct {
	RequestCount uint32
}

type cacheShard struct {
	hashmap     map[uint64]uint64
	entries     queue.BytesQueue
	lock        sync.RWMutex
	entryBuffer []byte
	onRemove    onRemoveCallback

	isVerbose    bool
	statsEnabled bool
	logger       Logger
	clock        clock
	lifeWindow   uint64

	hashmapStats map[uint64]uint32
	stats        shardStats
	cleanEnabled bool
	// expiryBuckets counts queued entries grouped by their insertion second.
	// Entries are FIFO-ordered, so the buckets stay ordered as well and the number
	// of expired entries still awaiting the cleaner is the sum of head buckets whose
	// second is older than the life window. All access happens while holding lock.
	expiryBuckets []expiryBucket
}

type expiryBucket struct {
	second int64
	count  int64
}

func (s *cacheShard) getWithInfo(key string, hashedKey uint64) (entry []byte, resp Response, err error) {
	currentTime := uint64(s.clock.Epoch())
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, resp, err
	}
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, resp, ErrEntryNotFound
	}

	entry = readEntry(wrappedEntry)
	if s.isExpired(wrappedEntry, currentTime) {
		resp.EntryStatus = Expired
	}
	s.lock.RUnlock()
	s.hit(hashedKey)
	return entry, resp, nil
}

func (s *cacheShard) get(key string, hashedKey uint64) ([]byte, error) {
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, err
	}
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, ErrEntryNotFound
	}
	entry := readEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)

	return entry, nil
}

func (s *cacheShard) getWrappedEntry(hashedKey uint64) ([]byte, error) {
	itemIndex := s.hashmap[hashedKey]

	if itemIndex == 0 {
		s.miss()
		return nil, ErrEntryNotFound
	}

	wrappedEntry, err := s.entries.Get(int(itemIndex))
	if err != nil {
		s.miss()
		return nil, err
	}

	return wrappedEntry, err
}

func (s *cacheShard) getValidWrapEntry(key string, hashedKey uint64) ([]byte, error) {
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		return nil, err
	}

	if !compareKeyFromEntry(wrappedEntry, key) {
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, readKeyFromEntry(wrappedEntry), hashedKey)
		}

		return nil, ErrEntryNotFound
	}
	s.hitWithoutLock(hashedKey)

	return wrappedEntry, nil
}

func (s *cacheShard) set(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	s.lock.Lock()

	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetHashFromEntry(previousEntry)
			//remove hashkey
			delete(s.hashmap, hashedKey)
		}
	}

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.trackPush(int64(currentTimestamp))
			s.hashmap[hashedKey] = uint64(index)
			s.lock.Unlock()
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			s.lock.Unlock()
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) addNewWithoutLock(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.trackPush(int64(currentTimestamp))
			s.hashmap[hashedKey] = uint64(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) setWrappedEntryWithoutLock(currentTimestamp uint64, w []byte, hashedKey uint64) error {
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetHashFromEntry(previousEntry)
		}
	}

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.trackPush(int64(currentTimestamp))
			s.hashmap[hashedKey] = uint64(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) append(key string, hashedKey uint64, entry []byte) error {
	s.lock.Lock()
	wrappedEntry, err := s.getValidWrapEntry(key, hashedKey)

	if err == ErrEntryNotFound {
		err = s.addNewWithoutLock(key, hashedKey, entry)
		s.lock.Unlock()
		return err
	}
	if err != nil {
		s.lock.Unlock()
		return err
	}

	currentTimestamp := uint64(s.clock.Epoch())

	w := appendToWrappedEntry(currentTimestamp, wrappedEntry, entry, &s.entryBuffer)

	err = s.setWrappedEntryWithoutLock(currentTimestamp, w, hashedKey)
	s.lock.Unlock()

	return err
}

func (s *cacheShard) del(hashedKey uint64) error {
	// Optimistic pre-check using only readlock
	s.lock.RLock()
	{
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.RUnlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		if err := s.entries.CheckGet(int(itemIndex)); err != nil {
			s.lock.RUnlock()
			s.delmiss()
			return err
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	{
		// After obtaining the writelock, we need to read the same again,
		// since the data delivered earlier may be stale now
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.Unlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		wrappedEntry, err := s.entries.Get(int(itemIndex))
		if err != nil {
			s.lock.Unlock()
			s.delmiss()
			return err
		}

		delete(s.hashmap, hashedKey)
		s.onRemove(wrappedEntry, Deleted)
		if s.statsEnabled {
			delete(s.hashmapStats, hashedKey)
		}
		resetHashFromEntry(wrappedEntry)
	}
	s.lock.Unlock()

	s.delhit()
	return nil
}

func (s *cacheShard) onEvict(oldestEntry []byte, currentTimestamp uint64, evict func(reason RemoveReason) error) bool {
	if s.isExpired(oldestEntry, currentTimestamp) {
		evict(Expired)
		return true
	}
	return false
}

func (s *cacheShard) isExpired(oldestEntry []byte, currentTimestamp uint64) bool {
	oldestTimestamp := readTimestampFromEntry(oldestEntry)
	if currentTimestamp <= oldestTimestamp { // if currentTimestamp < oldestTimestamp, the result will out of uint64 limits;
		return false
	}
	return currentTimestamp-oldestTimestamp >= s.lifeWindow
}

func (s *cacheShard) cleanUp(currentTimestamp uint64) {
	s.lock.Lock()
	for {
		if oldestEntry, err := s.entries.Peek(); err != nil {
			break
		} else if evicted := s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry); !evicted {
			break
		}
	}
	s.lock.Unlock()
}

// cleanUpBounded performs a single incremental cleaning pass. It holds the shard lock
// only long enough to evict at most maxEntries expired entries, then returns so the
// cleaner can move on to the next shard instead of blocking writers on one hot shard.
// A maxEntries value < 0 means no per-pass limit (the legacy full sweep).
// It returns the number of entries reclaimed during this pass.
func (s *cacheShard) cleanUpBounded(currentTimestamp uint64, maxEntries int) int {
	// Fast path: under a read lock only, check whether the head entry has expired.
	// Most sweeps happen while nothing is due yet; avoiding the write lock in that
	// case keeps frequent write-driven wake-ups from stalling concurrent writers.
	s.lock.RLock()
	oldestEntry, err := s.entries.Peek()
	if err != nil || !s.isExpired(oldestEntry, currentTimestamp) {
		s.lock.RUnlock()
		return 0
	}
	s.lock.RUnlock()

	s.lock.Lock()
	evicted := 0
	for maxEntries < 0 || evicted < maxEntries {
		oldestEntry, err := s.entries.Peek()
		if err != nil {
			break
		}
		if !s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry) {
			break
		}
		evicted++
	}
	s.lock.Unlock()
	return evicted
}

// cleanBacklog reports how many queued entries are not reachable from the live key
// index. Besides expired entries it also covers overwritten or explicitly deleted
// entries whose bytes remain in the FIFO queue until the head catches up with them.
func (s *cacheShard) cleanBacklog() int {
	s.lock.RLock()
	backlog := s.entries.Len() - len(s.hashmap)
	s.lock.RUnlock()
	return backlog
}

// expiredBacklog counts queued entries whose insertion time is older than the life
// window at currentTime. Such entries are still occupying queue memory and may also
// remain reachable through the key index until the head of the FIFO reaches them,
// which is exactly the accumulation that drives memory growth under a slow cleaner.
// Caller must hold at least a read lock.
func (s *cacheShard) expiredBacklogLocked(currentTime uint64) int64 {
	threshold := int64(currentTime) - int64(s.lifeWindow)
	var expired int64
	for i := range s.expiryBuckets {
		if s.expiryBuckets[i].second <= threshold {
			expired += s.expiryBuckets[i].count
			continue
		}
		break
	}
	return expired
}

// expiredBacklog returns a lock-guarded snapshot of entries already past their life
// window that have not yet been reclaimed by the cleaner.
func (s *cacheShard) expiredBacklog(currentTime uint64) int64 {
	s.lock.RLock()
	expired := s.expiredBacklogLocked(currentTime)
	s.lock.RUnlock()
	return expired
}

// trackPush records one queued entry under its insertion second. Entries leave the
// queue in insertion order, so equal-second entries are merged into the tail bucket.
// Caller must hold the write lock.
func (s *cacheShard) trackPush(second int64) {
	if n := len(s.expiryBuckets); n > 0 && s.expiryBuckets[n-1].second == second {
		s.expiryBuckets[n-1].count++
		return
	}
	// Clamping protects against non-monotonic test clocks: never insert a bucket
	// behind the current tail, which would break the FIFO bucket ordering.
	if n := len(s.expiryBuckets); n > 0 && second < s.expiryBuckets[n-1].second {
		second = s.expiryBuckets[n-1].second
	}
	s.expiryBuckets = append(s.expiryBuckets, expiryBucket{second: second, count: 1})
}

// trackPop removes one entry from the oldest non-empty bucket and drops the bucket
// once drained, keeping the slice proportional to the entry age spread.
// Caller must hold the write lock.
func (s *cacheShard) trackPop() {
	for i := range s.expiryBuckets {
		if s.expiryBuckets[i].count > 0 {
			s.expiryBuckets[i].count--
			if s.expiryBuckets[i].count == 0 {
				s.expiryBuckets = append(s.expiryBuckets[:i], s.expiryBuckets[i+1:]...)
			}
			return
		}
	}
}

func (s *cacheShard) getEntry(hashedKey uint64) ([]byte, error) {
	s.lock.RLock()

	entry, err := s.getWrappedEntry(hashedKey)
	// copy entry
	newEntry := make([]byte, len(entry))
	copy(newEntry, entry)

	s.lock.RUnlock()

	return newEntry, err
}

func (s *cacheShard) copyHashedKeys() (keys []uint64, next int) {
	s.lock.RLock()
	keys = make([]uint64, len(s.hashmap))

	for key := range s.hashmap {
		keys[next] = key
		next++
	}

	s.lock.RUnlock()
	return keys, next
}

func (s *cacheShard) removeOldestEntry(reason RemoveReason) error {
	oldest, err := s.entries.Pop()
	if err == nil {
		s.trackPop()
		hash := readHashFromEntry(oldest)
		if hash == 0 {
			// entry has been explicitly deleted with resetHashFromEntry, ignore
			return nil
		}
		delete(s.hashmap, hash)
		s.onRemove(oldest, reason)
		if s.statsEnabled {
			delete(s.hashmapStats, hash)
		}
		return nil
	}
	return err
}

func (s *cacheShard) reset(config Config) {
	var hashmapStatsCapacity int
	if config.StatsEnabled {
		hashmapStatsCapacity = config.initialShardSize()
	}
	s.lock.Lock()
	s.hashmap = make(map[uint64]uint64, config.initialShardSize())
	s.hashmapStats = make(map[uint64]uint32, hashmapStatsCapacity)
	s.entryBuffer = make([]byte, config.MaxEntrySize+headersSizeInBytes)
	s.entries.Reset()
	s.expiryBuckets = nil
	s.lock.Unlock()
}

func (s *cacheShard) resetStats() {
	s.stats.hits.Store(0)
	s.stats.misses.Store(0)
	s.stats.delHits.Store(0)
	s.stats.delMisses.Store(0)
	s.stats.collisions.Store(0)
}

func (s *cacheShard) len() int {
	s.lock.RLock()
	res := len(s.hashmap)
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) capacity() int {
	s.lock.RLock()
	res := s.entries.Capacity()
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:       s.stats.hits.Load(),
		Misses:     s.stats.misses.Load(),
		DelHits:    s.stats.delHits.Load(),
		DelMisses:  s.stats.delMisses.Load(),
		Collisions: s.stats.collisions.Load(),
	}
	return stats
}

func (s *cacheShard) getKeyMetadataWithLock(key uint64) Metadata {
	s.lock.RLock()
	c := s.hashmapStats[key]
	s.lock.RUnlock()
	return Metadata{
		RequestCount: c,
	}
}

func (s *cacheShard) getKeyMetadata(key uint64) Metadata {
	return Metadata{
		RequestCount: s.hashmapStats[key],
	}
}

func (s *cacheShard) hit(key uint64) {
	s.stats.hits.Add(1)
	if s.statsEnabled {
		s.lock.Lock()
		s.hashmapStats[key]++
		s.lock.Unlock()
	}
}

func (s *cacheShard) hitWithoutLock(key uint64) {
	s.stats.hits.Add(1)
	if s.statsEnabled {
		s.hashmapStats[key]++
	}
}

func (s *cacheShard) miss() {
	s.stats.misses.Add(1)
}

func (s *cacheShard) delhit() {
	s.stats.delHits.Add(1)
}

func (s *cacheShard) delmiss() {
	s.stats.delMisses.Add(1)
}

func (s *cacheShard) collision() {
	s.stats.collisions.Add(1)
}

func initNewShard(config Config, callback onRemoveCallback, clock clock) *cacheShard {
	bytesQueueInitialCapacity := config.initialShardSize() * config.MaxEntrySize
	maximumShardSizeInBytes := config.maximumShardSizeInBytes()
	if maximumShardSizeInBytes > 0 && bytesQueueInitialCapacity > maximumShardSizeInBytes {
		bytesQueueInitialCapacity = maximumShardSizeInBytes
	}
	var hashmapStatsCapacity int
	if config.StatsEnabled {
		hashmapStatsCapacity = config.initialShardSize()
	}
	return &cacheShard{
		hashmap:      make(map[uint64]uint64, config.initialShardSize()),
		hashmapStats: make(map[uint64]uint32, hashmapStatsCapacity),
		entries:      *queue.NewBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Verbose),
		entryBuffer:  make([]byte, config.MaxEntrySize+headersSizeInBytes),
		onRemove:     callback,

		isVerbose:    config.Verbose,
		logger:       newLogger(config.Logger),
		clock:        clock,
		lifeWindow:   uint64(config.LifeWindow.Seconds()),
		statsEnabled: config.StatsEnabled,
		cleanEnabled: config.CleanWindow > 0,
	}
}
