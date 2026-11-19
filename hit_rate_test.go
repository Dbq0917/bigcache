package bigcache

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

// keysLandingOnShards returns n distinct keys whose hashes map to the given
// shard numbers, used to concentrate a hot set on few shards.
func keysLandingOnShards(n int, shards map[uint64]bool, shardMask uint64) []string {
	hasher := newDefaultHasher()
	keys := make([]string, 0, n)
	for i := 0; len(keys) < n; i++ {
		key := fmt.Sprintf("hot-key-%d", i)
		if shards[hasher.Sum64(key)&shardMask] {
			keys = append(keys, key)
		}
	}
	return keys
}

// TestHitRateCurveAtFullCapacity fills the cache to its hard limit and then
// keeps reading a hot key set concentrated on 2 of 16 shards while cold
// traffic churns in the background. It logs the per-epoch hit rate so the
// eviction scope tradeoff (shard-local vs global) can be observed as a curve.
func TestHitRateCurveAtFullCapacity(t *testing.T) {
	t.Parallel()

	const (
		shardsCount = 16
		hotKeysN    = 600
		epochs      = 60
		hotOps      = 600
		coldOps     = 200
	)

	type result struct {
		name       string
		hitRates   []float64
		tailAvgHit float64
	}
	results := make([]result, 2)

	for i, tc := range []struct {
		name   string
		global bool
	}{
		{"ShardLocalEviction", false},
		{"GlobalEviction", true},
	} {
		cache, err := New(context.Background(), Config{
			Shards:             shardsCount,
			LifeWindow:         10 * time.Minute,
			MaxEntriesInWindow: 100,
			MaxEntrySize:       1024,
			HardMaxCacheSize:   1, // 1MB in total, 64KB per shard in local mode
			GlobalEviction:     tc.global,
		})
		noError(t, err)

		hotKeys := keysLandingOnShards(hotKeysN, map[uint64]bool{0: true, 1: true}, uint64(shardsCount-1))
		value := blob('a', 400)
		rng := rand.New(rand.NewSource(42))

		hitRates := make([]float64, epochs)
		for epoch := 0; epoch < epochs; epoch++ {
			before := cache.Stats()

			for op := 0; op < hotOps; op++ {
				key := hotKeys[rng.Intn(hotKeysN)]
				if _, err := cache.Get(key); err == ErrEntryNotFound {
					// hot key was evicted: write it back, this is the
					// evict/rewrite churn visible in the hit rate
					assertEqual(t, nil, cache.Set(key, value))
				}
			}
			for op := 0; op < coldOps; op++ {
				assertEqual(t, nil, cache.Set(fmt.Sprintf("cold-%d-%d", epoch, op), value))
			}

			after := cache.Stats()
			hits := after.Hits - before.Hits
			misses := after.Misses - before.Misses
			hitRates[epoch] = float64(hits) / float64(hits+misses)
		}

		var tailSum float64
		for _, rate := range hitRates[epochs/2:] {
			tailSum += rate
		}
		results[i] = result{name: tc.name, hitRates: hitRates, tailAvgHit: tailSum / (epochs / 2)}

		t.Logf("%s hit-rate curve (hot set of %d keys on 2/%d shards, cache at full capacity):", tc.name, hotKeysN, shardsCount)
		for epoch, rate := range hitRates {
			t.Logf("  epoch %02d  %5.1f%%  %s", epoch, rate*100, strings.Repeat("#", int(rate*50)))
		}
		t.Logf("%s average hit rate over second half: %.1f%%", tc.name, results[i].tailAvgHit*100)

		assertEqual(t, nil, cache.Close())
	}

	local, global := results[0], results[1]
	t.Logf("second-half average hit rate: shard-local %.1f%%, global %.1f%%",
		local.tailAvgHit*100, global.tailAvgHit*100)
	if global.tailAvgHit <= local.tailAvgHit {
		t.Errorf("expected global eviction to protect the skewed hot set better than shard-local eviction, got global %.1f%% <= local %.1f%%",
			global.tailAvgHit*100, local.tailAvgHit*100)
	}
}

// TestGlobalEvictionRequiresHardLimit validates config wiring.
func TestGlobalEvictionRequiresHardLimit(t *testing.T) {
	t.Parallel()

	// given
	config := DefaultConfig(time.Second)
	config.GlobalEviction = true
	config.HardMaxCacheSize = 0

	// when
	cache, err := New(context.Background(), config)

	// then
	assertEqual(t, "GlobalEviction requires HardMaxCacheSize > 0", err.Error())
	assertEqual(t, true, cache == nil)
}

// TestGlobalEvictionKeepsHotShardEntries checks that with global eviction a
// shard hotter than its 1/Shards share does not lose its entries while cold
// shards hold older data.
func TestGlobalEvictionKeepsHotShardEntries(t *testing.T) {
	t.Parallel()

	// given
	clock := mockedClock{value: 0}
	cache, err := newBigCache(context.Background(), Config{
		Shards:             4,
		LifeWindow:         10 * time.Minute,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       512,
		HardMaxCacheSize:   1,
		GlobalEviction:     true,
	}, &clock)
	noError(t, err)
	defer cache.Close()

	value := blob('a', 1024)

	// when: cold entries are written first to three of the four shards
	coldKeys := keysLandingOnShards(500, map[uint64]bool{1: true, 2: true, 3: true}, 3)
	for _, key := range coldKeys {
		assertEqual(t, nil, cache.Set(key, value))
	}
	// and later a single hot shard stores more than its 1/4 share of the
	// 1MB budget, pushing the cache over the limit
	clock.set(10)
	hotKeys := keysLandingOnShards(500, map[uint64]bool{0: true}, 3)
	for _, key := range hotKeys {
		assertEqual(t, nil, cache.Set(key, value))
	}

	// then: hot entries survive on their over-budget shard, the globally
	// oldest (cold) entries are evicted from the other shards instead
	hotHits := 0
	for _, key := range hotKeys {
		if _, err := cache.Get(key); err == nil {
			hotHits++
		}
	}
	assertEqual(t, len(hotKeys), hotHits)

	coldHits := 0
	for _, key := range coldKeys {
		if _, err := cache.Get(key); err == nil {
			coldHits++
		}
	}
	if coldHits >= len(coldKeys) {
		t.Errorf("expected some cold entries to be evicted, all %d are still present", coldHits)
	}
}

// TestGlobalEvictionConcurrentSetAtFullCapacity hammers a full cache from
// many goroutines so concurrent global evictions race with each other and
// with shard-local writes.
func TestGlobalEvictionConcurrentSetAtFullCapacity(t *testing.T) {
	t.Parallel()

	// given
	cache, err := New(context.Background(), Config{
		Shards:             8,
		LifeWindow:         10 * time.Minute,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       512,
		HardMaxCacheSize:   1,
		GlobalEviction:     true,
	})
	noError(t, err)
	defer cache.Close()

	// when
	var wg sync.WaitGroup
	value := blob('a', 1024)
	for goroutine := 0; goroutine < 8; goroutine++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				assertEqual(t, nil, cache.Set(key, value))
				if i%3 == 0 {
					cache.Get(key)
				}
			}
		}(goroutine)
	}
	wg.Wait()

	// then: the shared byte budget is still enforced after concurrent evictions
	if got := cache.scope.bytes.Load(); got > cache.scope.maxBytes {
		t.Errorf("global byte budget exceeded: %d > %d", got, cache.scope.maxBytes)
	}
}
