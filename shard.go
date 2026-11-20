package bigcache

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/allegro/bigcache/v3/queue"
)

// bigEntryShardFraction determines when an entry is considered big compared to
// the maximum shard size. Entries bigger than maximumShardSizeInBytes/bigEntryShardFraction
// are stored outside of the entries queue so they cannot evict smaller entries.
const bigEntryShardFraction = 4

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)

// Metadata contains information of a specific entry
type Metadata struct {
	RequestCount uint32
}

type cacheShard struct {
	hashmap     map[uint64]uint64
	entries     queue.BytesQueue
	bigEntries  map[uint64][]byte
	lock        sync.RWMutex
	entryBuffer []byte
	onRemove    onRemoveCallback

	isVerbose           bool
	statsEnabled        bool
	logger              Logger
	clock               clock
	lifeWindow          uint64
	maxEntrySizeInQueue int

	hashmapStats map[uint64]uint32
	stats        shardStats
	cleanEnabled bool
}

// readEntry unwraps the entry and, when the entry payload was stored outside
// of the queue, resolves it from bigEntries. Must be called under lock.
func (s *cacheShard) readEntry(wrappedEntry []byte, hashedKey uint64) []byte {
	entry := readEntry(wrappedEntry)
	if len(entry) == 0 && s.bigEntries != nil {
		if payload, ok := s.bigEntries[hashedKey]; ok {
			entry = make([]byte, len(payload))
			copy(entry, payload)
		}
	}
	return entry
}

// detachBigEntry removes the offloaded payload for hash (if any) and returns
// the full wrapped entry with the payload appended back. Must be called under lock.
func (s *cacheShard) detachBigEntry(wrappedEntry []byte, hash uint64) []byte {
	if s.bigEntries == nil {
		return wrappedEntry
	}
	payload, ok := s.bigEntries[hash]
	if !ok {
		return wrappedEntry
	}
	delete(s.bigEntries, hash)
	full := make([]byte, len(wrappedEntry)+len(payload))
	copy(full, wrappedEntry)
	copy(full[len(wrappedEntry):], payload)
	return full
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

	entry = s.readEntry(wrappedEntry, hashedKey)
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
	entry := s.readEntry(wrappedEntry, hashedKey)
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
			if s.bigEntries != nil {
				delete(s.bigEntries, hashedKey)
			}
		}
	}

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	err := s.pushWrappedEntry(w, hashedKey)
	s.lock.Unlock()
	return err
}

func (s *cacheShard) addNewWithoutLock(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	return s.pushWrappedEntry(w, hashedKey)
}

func (s *cacheShard) setWrappedEntryWithoutLock(currentTimestamp uint64, w []byte, hashedKey uint64) error {
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetHashFromEntry(previousEntry)
			if s.bigEntries != nil {
				delete(s.bigEntries, hashedKey)
			}
		}
	}

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	return s.pushWrappedEntry(w, hashedKey)
}

// pushWrappedEntry stores the wrapped entry in the shard. Entries bigger than
// maxEntrySizeInQueue are split: only headers and key are kept in the entries
// queue (preserving FIFO order, expiration and collision detection), while the
// payload is placed in bigEntries so it cannot evict smaller entries.
func (s *cacheShard) pushWrappedEntry(w []byte, hashedKey uint64) error {
	if s.maxEntrySizeInQueue > 0 && len(w) > s.maxEntrySizeInQueue {
		return s.pushBigEntry(w, hashedKey)
	}
	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint64(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) pushBigEntry(w []byte, hashedKey uint64) error {
	keyLength := int(binary.LittleEndian.Uint16(w[timestampSizeInBytes+hashSizeInBytes:]))
	payloadOffset := headersSizeInBytes + keyLength
	// w reuses the shared entry buffer, the payload must be copied out
	payload := make([]byte, len(w)-payloadOffset)
	copy(payload, w[payloadOffset:])
	placeholder := w[:payloadOffset]

	for {
		if index, err := s.entries.Push(placeholder); err == nil {
			s.hashmap[hashedKey] = uint64(index)
			if s.bigEntries == nil {
				s.bigEntries = make(map[uint64][]byte)
			}
			s.bigEntries[hashedKey] = payload
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

	if s.bigEntries != nil {
		if payload, ok := s.bigEntries[hashedKey]; ok {
			// reattach the offloaded payload so the appended entry is complete
			full := make([]byte, len(wrappedEntry)+len(payload))
			copy(full, wrappedEntry)
			copy(full[len(wrappedEntry):], payload)
			wrappedEntry = full
		}
	}

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
		s.onRemove(s.detachBigEntry(wrappedEntry, hashedKey), Deleted)
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

func (s *cacheShard) getEntry(hashedKey uint64) ([]byte, error) {
	s.lock.RLock()

	entry, err := s.getWrappedEntry(hashedKey)
	// copy entry
	newEntry := make([]byte, len(entry))
	copy(newEntry, entry)
	if err == nil && s.bigEntries != nil {
		if payload, ok := s.bigEntries[hashedKey]; ok {
			full := make([]byte, len(newEntry)+len(payload))
			copy(full, newEntry)
			copy(full[len(newEntry):], payload)
			newEntry = full
		}
	}

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
		hash := readHashFromEntry(oldest)
		if hash == 0 {
			// entry has been explicitly deleted with resetHashFromEntry, ignore
			return nil
		}
		delete(s.hashmap, hash)
		s.onRemove(s.detachBigEntry(oldest, hash), reason)
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
	s.bigEntries = nil
	s.entries.Reset()
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
	maxEntrySizeInQueue := 0
	if maximumShardSizeInBytes > 0 {
		maxEntrySizeInQueue = maximumShardSizeInBytes / bigEntryShardFraction
	}
	return &cacheShard{
		hashmap:      make(map[uint64]uint64, config.initialShardSize()),
		hashmapStats: make(map[uint64]uint32, hashmapStatsCapacity),
		entries:      *queue.NewBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Verbose),
		entryBuffer:  make([]byte, config.MaxEntrySize+headersSizeInBytes),
		onRemove:     callback,

		isVerbose:           config.Verbose,
		logger:              newLogger(config.Logger),
		clock:               clock,
		lifeWindow:          uint64(config.LifeWindow.Seconds()),
		statsEnabled:        config.StatsEnabled,
		cleanEnabled:        config.CleanWindow > 0,
		maxEntrySizeInQueue: maxEntrySizeInQueue,
	}
}
