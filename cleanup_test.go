package bigcache

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestCleanUpBoundedEvictsAtMostBatchSize(t *testing.T) {
	t.Parallel()

	clock := mockedClock{value: 0}
	cache, err := newBigCache(context.Background(), Config{
		Shards:             1,
		LifeWindow:         time.Second,
		CleanWindow:        time.Minute,
		CleanBatchSize:     3,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
	}, &clock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := cache.Set(keyFor(i), []byte("value")); err != nil {
			t.Fatalf("unexpected set error: %v", err)
		}
	}
	clock.set(10)

	shard := cache.shards[0]
	evicted := shard.cleanUpBounded(uint64(clock.Epoch()), 3)
	if evicted != 3 {
		t.Fatalf("expected 3 evictions per pass, got %d", evicted)
	}
	if len := cache.Len(); len != 7 {
		t.Fatalf("expected 7 live entries after bounded pass, got %d", len)
	}

	// Subsequent passes drain the remaining expired entries.
	if evicted := shard.cleanUpBounded(uint64(clock.Epoch()), 3); evicted != 3 {
		t.Fatalf("expected another 3 evictions, got %d", evicted)
	}
	if evicted := shard.cleanUpBounded(uint64(clock.Epoch()), 3); evicted != 3 {
		t.Fatalf("expected the third batch of 3 evictions, got %d", evicted)
	}
	if evicted := shard.cleanUpBounded(uint64(clock.Epoch()), 3); evicted != 1 {
		t.Fatalf("expected the final single eviction, got %d", evicted)
	}
	if len := cache.Len(); len != 0 {
		t.Fatalf("expected empty cache, got %d entries", len)
	}
}

func TestCleanUpBoundedUnlimitedKeepsLegacySemantics(t *testing.T) {
	t.Parallel()

	clock := mockedClock{value: 0}
	cache, _ := newBigCache(context.Background(), Config{
		Shards:             1,
		LifeWindow:         time.Second,
		CleanWindow:        time.Minute,
		CleanBatchSize:     cleanUnlimited,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
	}, &clock)

	for i := 0; i < 10; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}
	clock.set(5)

	if evicted := cache.shards[0].cleanUpBounded(uint64(clock.Epoch()), cleanUnlimited); evicted != 10 {
		t.Fatalf("expected unlimited pass to evict all 10 entries, got %d", evicted)
	}
	if cache.Len() != 0 {
		t.Fatalf("expected empty cache, got %d", cache.Len())
	}
}

func TestCleanBacklogTracksStaleQueueEntries(t *testing.T) {
	t.Parallel()

	cache, _ := New(context.Background(), Config{
		Shards:             1,
		LifeWindow:         time.Minute,
		CleanWindow:        0,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
	})

	for i := 0; i < 5; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}
	if backlog := cache.Stats().CleanBacklog; backlog != 0 {
		t.Fatalf("expected zero backlog for fresh keys, got %d", backlog)
	}

	// Overwriting a key orphans the previous queue entry until cleaning reclaims it.
	for i := 0; i < 4; i++ {
		_ = cache.Set(keyFor(0), []byte("value2"))
	}
	if backlog := cache.Stats().CleanBacklog; backlog != 4 {
		t.Fatalf("expected backlog of 4 orphaned entries, got %d", backlog)
	}
}

func TestExpiredBacklogCountsEntriesAwaitingCleaning(t *testing.T) {
	t.Parallel()

	clock := mockedClock{value: 0}
	cache, _ := newBigCache(context.Background(), Config{
		Shards:             1,
		LifeWindow:         2 * time.Second,
		CleanWindow:        0,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
	}, &clock)

	for i := 0; i < 10; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}
	if expired := cache.Stats().ExpiredBacklog; expired != 0 {
		t.Fatalf("expected no expired entries inside life window, got %d", expired)
	}

	clock.set(3)
	if expired := cache.Stats().ExpiredBacklog; expired != 10 {
		t.Fatalf("expected all 10 entries to count as expired backlog, got %d", expired)
	}

	// Reclaiming head entries shrinks the expired backlog one-for-one, including
	// overwritten entries whose hash was reset but whose bytes still sit in the queue.
	_ = cache.Set(keyFor(0), []byte("new-value"))
	cache.cleanUp(uint64(clock.Epoch()))
	if expired := cache.Stats().ExpiredBacklog; expired != 0 {
		t.Fatalf("expected zero expired backlog after cleaning, got %d", expired)
	}

	if err := cache.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if expired := cache.Stats().ExpiredBacklog; expired != 0 {
		t.Fatalf("expected reset to clear expiry tracking, got %d", expired)
	}
}

func TestWriteDrivenCleaningKeepsUpWithExpiration(t *testing.T) {
	t.Parallel()

	clock := mockedClock{value: 0}
	cache, err := newBigCache(context.Background(), Config{
		Shards:             4,
		LifeWindow:         time.Second,
		CleanWindow:        time.Hour,
		CleanBatchSize:     256,
		MaxEntriesInWindow: 10000,
		MaxEntrySize:       256,
	}, &clock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	for i := 0; i < 200; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}
	clock.set(5)

	// The safety-net ticker is an hour away; write traffic alone must wake the cleaner.
	// More than cleanWakeupWrites writes guarantee at least one write-driven sweep.
	for i := 200; i < 200+cleanWakeupWrites+100; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}

	// The 200 pre-expiration entries must be reclaimed by write-driven sweeps even
	// though the periodic ticker is an hour away.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cache.Stats().CleanedEntries >= 200 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	stats := cache.Stats()
	if stats.CleanRuns == 0 {
		t.Fatal("expected write traffic to trigger at least one cleaning pass")
	}
	if stats.CleanedEntries < 200 {
		t.Fatalf("expected at least the 200 expired entries to be reclaimed, got %d", stats.CleanedEntries)
	}
}

func TestCleaningStatsAggregateAcrossShards(t *testing.T) {
	t.Parallel()

	clock := mockedClock{value: 0}
	config := Config{
		Shards:             8,
		LifeWindow:         time.Second,
		CleanWindow:        time.Minute,
		CleanBatchSize:     16,
		MaxEntriesInWindow: 1000,
		MaxEntrySize:       256,
	}
	cache, _ := newBigCache(context.Background(), config, &clock)

	for i := 0; i < 80; i++ {
		_ = cache.Set(keyFor(i), []byte("value"))
	}
	clock.set(5)

	cache.cleaningPass(uint64(clock.Epoch()))
	stats := cache.Stats()
	if stats.CleanRuns != int64(config.Shards) {
		t.Fatalf("expected one run counter per shard, got %d", stats.CleanRuns)
	}
	if stats.CleanedEntries != 80 {
		t.Fatalf("expected all 80 expired entries to be cleaned, got %d", stats.CleanedEntries)
	}
	if stats.CleanBacklog != 0 {
		t.Fatalf("expected empty backlog after sweep, got %d", stats.CleanBacklog)
	}

	cache.ResetStats()
	if reset := cache.Stats(); reset.CleanRuns != 0 || reset.CleanedEntries != 0 {
		t.Fatalf("expected cleaner counters to be reset, got %+v", reset)
	}
}

func TestInvalidCleanBatchSize(t *testing.T) {
	t.Parallel()

	_, err := New(context.Background(), Config{
		Shards:             1,
		LifeWindow:         time.Second,
		CleanWindow:        time.Second,
		CleanBatchSize:     -2,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
	})
	if err == nil {
		t.Fatal("expected error for CleanBatchSize below -1")
	}
}

func keyFor(i int) string {
	return "key-" + strconv.Itoa(i)
}
