package bigcache

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minimumEntriesInShard = 10 // Minimum number of entries in single shard
)

// BigCache is fast, concurrent, evicting cache created to keep big number of entries without impact on performance.
// It keeps entries on heap but omits GC for them. To achieve that, operations take place on byte arrays,
// therefore entries (de)serialization in front of the cache will be needed in most use cases.
type BigCache struct {
	shards          []*cacheShard
	lifeWindow      uint64
	clock           clock
	hash            Hasher
	config          Config
	shardMask       uint64
	close           chan struct{}
	closeOnce       sync.Once
	cleaner         cleanerStats
	writes          atomic.Uint64
	cleanNotify     chan struct{}
	lastClean       time.Time
	sweepOffset     int
	lastSweepWrites uint64
	// disableWriteDrivenCleaning is a test-only switch: when true the cleaner only
	// wakes up on the CleanWindow ticker, emulating the legacy fixed-period sweeper
	// for before/after load comparisons.
	disableWriteDrivenCleaning bool
}

// Response will contain metadata about the entry for which GetWithInfo(key) was called
type Response struct {
	EntryStatus RemoveReason
}

// RemoveReason is a value used to signal to the user why a particular key was removed in the OnRemove callback.
type RemoveReason uint32

const (
	_ RemoveReason = iota
	// Expired means the key is past its LifeWindow.
	Expired
	// NoSpace means the key is the oldest and the cache size was at its maximum when Set was called, or the
	// entry exceeded the maximum shard size.
	NoSpace
	// Deleted means Delete was called and this key was removed as a result.
	Deleted
)

// New initialize new instance of BigCache
func New(ctx context.Context, config Config) (*BigCache, error) {
	return newBigCache(ctx, config, &systemClock{})
}

// NewBigCache initialize new instance of BigCache
//
// Deprecated: NewBigCache is deprecated, please use New(ctx, config) instead,
// New takes in context and can gracefully
// shutdown with context cancellations
func NewBigCache(config Config) (*BigCache, error) {
	return newBigCache(context.Background(), config, &systemClock{})
}

func newBigCache(ctx context.Context, config Config, clock clock) (*BigCache, error) {
	if !isPowerOfTwo(config.Shards) {
		return nil, errors.New("Shards number must be power of two") //nolint:staticcheck // keep for backward compatibility
	}
	if config.MaxEntrySize < 0 {
		return nil, errors.New("MaxEntrySize must be >= 0")
	}
	if config.MaxEntriesInWindow < 0 {
		return nil, errors.New("MaxEntriesInWindow must be >= 0")
	}
	if config.HardMaxCacheSize < 0 {
		return nil, errors.New("HardMaxCacheSize must be >= 0")
	}
	if config.CleanBatchSize < cleanUnlimited {
		return nil, errors.New("CleanBatchSize must be >= -1")
	}

	lifeWindowSeconds := uint64(config.LifeWindow.Seconds())
	if config.CleanWindow > 0 && lifeWindowSeconds == 0 {
		return nil, errors.New("LifeWindow must be >= 1s when CleanWindow is set")
	}

	if config.Hasher == nil {
		config.Hasher = newDefaultHasher()
	}

	cache := &BigCache{
		shards:     make([]*cacheShard, config.Shards),
		lifeWindow: lifeWindowSeconds,
		clock:      clock,
		hash:       config.Hasher,
		config:     config,
		shardMask:  uint64(config.Shards - 1),
		close:      make(chan struct{}),
	}

	var onRemove func(wrappedEntry []byte, reason RemoveReason)
	if config.OnRemoveWithMetadata != nil {
		onRemove = cache.providedOnRemoveWithMetadata
	} else if config.OnRemove != nil {
		onRemove = cache.providedOnRemove
	} else if config.OnRemoveWithReason != nil {
		onRemove = cache.providedOnRemoveWithReason
	} else {
		onRemove = cache.notProvidedOnRemove
	}

	for i := 0; i < config.Shards; i++ {
		cache.shards[i] = initNewShard(config, onRemove, clock)
	}

	if config.CleanWindow > 0 {
		cache.cleanNotify = make(chan struct{}, 1)
		go func() {
			ticker := time.NewTicker(config.CleanWindow)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-ticker.C:
					cache.lastClean = t
					if cache.cleaningPass(uint64(t.Unix())) {
						cache.rearmCleaner()
					}
				case <-cache.cleanNotify:
					cache.cleanOnDemand()
				case <-cache.close:
					return
				}
			}
		}()
	}

	return cache, nil
}

// Close is used to signal a shutdown of the cache when you are done with it.
// This allows the cleaning goroutines to exit and ensures references are not
// kept to the cache preventing GC of the entire cache.
// Close is idempotent and safe to call concurrently.
func (c *BigCache) Close() error {
	c.closeOnce.Do(func() {
		close(c.close)
	})
	return nil
}

// Get reads entry for the key.
// It returns an ErrEntryNotFound when
// no entry exists for the given key.
func (c *BigCache) Get(key string) ([]byte, error) {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.get(key, hashedKey)
}

// GetWithInfo reads entry for the key with Response info.
// It returns an ErrEntryNotFound when
// no entry exists for the given key.
func (c *BigCache) GetWithInfo(key string) ([]byte, Response, error) {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.getWithInfo(key, hashedKey)
}

// Set saves entry under the key
func (c *BigCache) Set(key string, entry []byte) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	err := shard.set(key, hashedKey, entry)
	if err == nil {
		c.notifyCleaner()
	}
	return err
}

// Append appends entry under the key if key exists, otherwise
// it will set the key (same behaviour as Set()). With Append() you can
// concatenate multiple entries under the same key in a lock-optimized way.
func (c *BigCache) Append(key string, entry []byte) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	err := shard.append(key, hashedKey, entry)
	if err == nil {
		c.notifyCleaner()
	}
	return err
}

// Delete removes the key
func (c *BigCache) Delete(key string) error {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.del(hashedKey)
}

// Reset empties all cache shards
func (c *BigCache) Reset() error {
	for _, shard := range c.shards {
		shard.reset(c.config)
	}
	return nil
}

// ResetStats resets cache stats
func (c *BigCache) ResetStats() error {
	for _, shard := range c.shards {
		shard.resetStats()
	}
	c.cleaner.runs.Store(0)
	c.cleaner.cleanedEntries.Store(0)
	return nil
}

// Len computes the number of entries in the cache.
func (c *BigCache) Len() int {
	var len int
	for _, shard := range c.shards {
		len += shard.len()
	}
	return len
}

// Capacity returns the amount of bytes stored in the cache.
func (c *BigCache) Capacity() int {
	var len int
	for _, shard := range c.shards {
		len += shard.capacity()
	}
	return len
}

// Stats returns cache's statistics
func (c *BigCache) Stats() Stats {
	var s Stats
	now := uint64(c.clock.Epoch())
	for _, shard := range c.shards {
		tmp := shard.getStats()
		s.Hits += tmp.Hits
		s.Misses += tmp.Misses
		s.DelHits += tmp.DelHits
		s.DelMisses += tmp.DelMisses
		s.Collisions += tmp.Collisions
		s.CleanBacklog += int64(shard.cleanBacklog())
		s.ExpiredBacklog += shard.expiredBacklog(now)
	}
	s.CleanRuns = c.cleaner.runs.Load()
	s.CleanedEntries = c.cleaner.cleanedEntries.Load()
	return s
}

// KeyMetadata returns number of times a cached resource was requested.
func (c *BigCache) KeyMetadata(key string) Metadata {
	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	return shard.getKeyMetadataWithLock(hashedKey)
}

// Iterator returns iterator function to iterate over EntryInfo's from whole cache.
func (c *BigCache) Iterator() *EntryInfoIterator {
	return newIterator(c)
}

func (c *BigCache) onEvict(oldestEntry []byte, currentTimestamp uint64, evict func(reason RemoveReason) error) bool {
	oldestTimestamp := readTimestampFromEntry(oldestEntry)
	if currentTimestamp < oldestTimestamp {
		return false
	}
	if currentTimestamp-oldestTimestamp > c.lifeWindow {
		evict(Expired)
		return true
	}
	return false
}

func (c *BigCache) cleanUp(currentTimestamp uint64) {
	for _, shard := range c.shards {
		shard.cleanUp(currentTimestamp)
	}
}

// notifyCleaner nudges the background cleaner after write traffic. The signal is
// coalesced through a capacity-one channel: bursts of writes produce at most one
// pending wake-up, so the write path only pays for an atomic add plus a
// non-blocking channel send.
func (c *BigCache) notifyCleaner() {
	if c.cleanNotify == nil || c.disableWriteDrivenCleaning || c.writes.Add(1)%cleanWakeupWrites != 0 {
		return
	}
	select {
	case c.cleanNotify <- struct{}{}:
	default:
	}
}

// cleanOnDemand runs one write-driven incremental sweep. minCleanInterval throttles
// back-to-back wake-ups so that a saturated write path cannot turn cleaning into a
// busy loop competing for CPU; the CleanWindow ticker remains a safety net.
func (c *BigCache) cleanOnDemand() {
	now := time.Now()
	if now.Sub(c.lastClean) < minCleanInterval {
		// Throttled: the CleanWindow ticker remains a safety net for pending work.
		return
	}
	c.lastClean = now
	if c.cleaningPass(uint64(now.Unix())) {
		c.rearmCleaner()
	}
}

// cleaningPass visits every shard once, evicting at most CleanBatchSize expired
// entries per shard. Shards are visited from a rotating offset and the goroutine
// yields between batches, spreading the short lock acquisitions away from writers
// instead of sweeping one shard for a long time. It returns true when at least one
// shard hit its batch limit, i.e. expired entries remain and another pass is due.
func (c *BigCache) cleaningPass(currentTimestamp uint64) bool {
	configuredBatch := c.config.effectiveCleanBatchSize()
	shardCount := len(c.shards)
	c.sweepOffset = (c.sweepOffset + 1) % shardCount

	// Scale the per-shard budget to the observed write rate: entries written since
	// the previous sweep are the upper bound of what can expire in between sweeps,
	// so reclaiming at least that many per shard keeps cleaning capacity ahead of
	// expiration instead of letting backlog grow. A fixed minimum preserves the
	// configured bound during quiet periods, and a hard cap keeps each lock
	// acquisition short regardless of burst size.
	batchSize := configuredBatch
	if batchSize != cleanUnlimited {
		currentWrites := c.writes.Load()
		writesPerShard := int((currentWrites - c.lastSweepWrites) / uint64(shardCount))
		c.lastSweepWrites = currentWrites
		batchSize = max(configuredBatch, writesPerShard*2)
		if batchSize > maxCleanBatchSize {
			batchSize = maxCleanBatchSize
		}
	}

	saturated := false
	for i := range c.shards {
		shard := c.shards[(i+c.sweepOffset)%shardCount]
		evicted := shard.cleanUpBounded(currentTimestamp, batchSize)
		// Keep self-arming only while a meaningful backlog survives the pass.
		// Re-arming on every saturated shard would spin the cleaner during ordinary
		// operation; the write-driven wake-ups and ticker resume sweeping otherwise.
		if batchSize != cleanUnlimited && shard.expiredBacklog(currentTimestamp) > int64(batchSize) {
			saturated = true
		}
		c.cleaner.cleanedEntries.Add(int64(evicted))
		c.cleaner.runs.Add(1)
		if (i+1)%runtime.GOMAXPROCS(0) == 0 {
			runtime.Gosched()
		}
	}
	return saturated
}

// rearmCleaner schedules another sweep. Combined with the minCleanInterval throttle
// in cleanOnDemand this lets the cleaner drain a large expired backlog at a steady,
// bounded pace even after the write burst that produced it has stopped.
func (c *BigCache) rearmCleaner() {
	if c.cleanNotify == nil {
		return
	}
	select {
	case c.cleanNotify <- struct{}{}:
	default:
	}
}

func (c *BigCache) getShard(hashedKey uint64) (shard *cacheShard) {
	return c.shards[hashedKey&c.shardMask]
}

func (c *BigCache) providedOnRemove(wrappedEntry []byte, reason RemoveReason) {
	c.config.OnRemove(readKeyFromEntry(wrappedEntry), readEntry(wrappedEntry))
}

func (c *BigCache) providedOnRemoveWithReason(wrappedEntry []byte, reason RemoveReason) {
	if c.config.onRemoveFilter == 0 || (1<<uint(reason))&c.config.onRemoveFilter > 0 {
		c.config.OnRemoveWithReason(readKeyFromEntry(wrappedEntry), readEntry(wrappedEntry), reason)
	}
}

func (c *BigCache) notProvidedOnRemove(wrappedEntry []byte, reason RemoveReason) {
}

func (c *BigCache) providedOnRemoveWithMetadata(wrappedEntry []byte, reason RemoveReason) {
	key := readKeyFromEntry(wrappedEntry)

	hashedKey := c.hash.Sum64(key)
	shard := c.getShard(hashedKey)
	c.config.OnRemoveWithMetadata(key, readEntry(wrappedEntry), shard.getKeyMetadata(hashedKey))
}
