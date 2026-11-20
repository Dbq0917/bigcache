package bigcache

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cleanupLoadScenario describes one peak-traffic run. Two runs are compared in
// TestAdaptiveCleaningPeakLoad:
//   - legacy: fixed-period sweeps that evict every expired entry per pass
//     (CleanBatchSize = -1, write-driven wake-ups disabled)
//   - adaptive: write-driven, bounded incremental sweeps (the new default behavior)
type cleanupLoadScenario struct {
	name           string
	legacySweeper  bool
	cleanBatchSize int
	shards         int
	writers        int
	lifeWindow     time.Duration
	cleanWindow    time.Duration
	duration       time.Duration
	sampleInterval time.Duration
	maxWrites      uint64
	entrySize      int
}

type cleanupLoadSample struct {
	elapsedMS   int64
	heapAllocMB float64
	heapInuseMB float64
	queueMB     float64
	backlog     int64
	expired     int64
	liveEntries int
	cleanRuns   int64
	cleaned     int64
}

type cleanupLoadResult struct {
	scenario        cleanupLoadScenario
	samples         []cleanupLoadSample
	writes          uint64
	latency         latencySummary
	stats           Stats
	residualExpired int64
}

type latencySummary struct {
	count int
	p50   time.Duration
	p99   time.Duration
	max   time.Duration
	mean  time.Duration
}

// TestAdaptiveCleaningPeakLoad drives a sustained write peak against two cleaner
// configurations and compares memory growth, expired-entry backlog and write
// latency. It is opt-in via BIGCACHE_LOAD_TEST=1 so the regular test suite stays fast.
//
// Run with:
//
//	BIGCACHE_LOAD_TEST=1 go test -run TestAdaptiveCleaningPeakLoad -v -timeout 10m .
//
// Sampled time series are written to artifacts/cleanup_load_<mode>.csv so the
// memory and backlog curves can be plotted after the run.
func TestAdaptiveCleaningPeakLoad(t *testing.T) {
	if os.Getenv("BIGCACHE_LOAD_TEST") == "" {
		t.Skip("peak load comparison is opt-in; set BIGCACHE_LOAD_TEST=1 to run")
	}

	base := cleanupLoadScenario{
		shards:         8,
		writers:        runtime.GOMAXPROCS(0),
		lifeWindow:     time.Second,
		cleanWindow:    time.Second,
		duration:       25 * time.Second,
		sampleInterval: 100 * time.Millisecond,
		maxWrites:      20_000_000,
		entrySize:      256,
	}

	legacy := base
	legacy.name = "legacy_fixed_period_full_sweep"
	legacy.legacySweeper = true
	legacy.cleanBatchSize = cleanUnlimited

	adaptive := base
	adaptive.name = "adaptive_write_driven_batched"
	adaptive.cleanBatchSize = defaultCleanBatchSize

	legacyResult := runCleanupLoadScenario(t, legacy)
	adaptiveResult := runCleanupLoadScenario(t, adaptive)

	for _, result := range []cleanupLoadResult{legacyResult, adaptiveResult} {
		if err := writeCleanupLoadSamples(result); err != nil {
			t.Fatalf("write samples: %v", err)
		}
	}

	printCleanupLoadComparison(t, legacyResult, adaptiveResult)

	// The primary deliverable is the before/after report and sampled curves (CSV).
	// Hard invariants instead of environment-sensitive numeric comparisons:
	// both modes must drain everything once traffic stops, and the adaptive mode
	// must not regress p99 write latency beyond 2x of the fixed-period sweeper.
	if legacyResult.residualExpired != 0 || adaptiveResult.residualExpired != 0 {
		t.Fatalf("both cleaners must drain the backlog after the burst: legacy=%d adaptive=%d",
			legacyResult.residualExpired, adaptiveResult.residualExpired)
	}
	if adaptiveResult.latency.p99 > legacyResult.latency.p99*2 {
		t.Fatalf("adaptive cleaner p99 write latency regressed: adaptive=%s legacy=%s",
			adaptiveResult.latency.p99, legacyResult.latency.p99)
	}
}

func runCleanupLoadScenario(t *testing.T, scenario cleanupLoadScenario) cleanupLoadResult {
	t.Helper()

	config := Config{
		Shards:             scenario.shards,
		LifeWindow:         scenario.lifeWindow,
		CleanWindow:        scenario.cleanWindow,
		CleanBatchSize:     scenario.cleanBatchSize,
		MaxEntriesInWindow: scenario.writers * 50_000,
		MaxEntrySize:       scenario.entrySize,
		Verbose:            false,
	}
	cache, err := New(context.Background(), config)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	cache.disableWriteDrivenCleaning = scenario.legacySweeper

	runtime.GC()

	var writes atomic.Uint64
	stop := make(chan struct{})
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, 1<<16)

	var workers sync.WaitGroup
	for worker := 0; worker < scenario.writers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			payload := make([]byte, scenario.entrySize)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if writes.Load() >= scenario.maxWrites {
					return
				}
				key := fmt.Sprintf("w%d-k%d", worker, writes.Add(1))
				start := time.Now()
				if err := cache.Set(key, payload); err != nil {
					t.Errorf("unexpected set error: %v", err)
					return
				}
				elapsed := time.Since(start)
				latencyMu.Lock()
				latencies = append(latencies, elapsed)
				latencyMu.Unlock()
			}
		}(worker)
	}

	samples := sampleCleanupLoad(cache, stop, scenario)
	workers.Wait()

	result := cleanupLoadResult{
		scenario: scenario,
		samples:  samples,
		writes:   writes.Load(),
		latency:  summarizeLatencies(latencies),
	}

	// After the write burst stops, a well-behaved cleaner must still drain the
	// expired backlog on its own; wait briefly and record what remains.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cache.Stats().ExpiredBacklog > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	result.stats = cache.Stats()
	result.residualExpired = result.stats.ExpiredBacklog
	if err := cache.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	return result
}

func sampleCleanupLoad(cache *BigCache, stop chan struct{}, scenario cleanupLoadScenario) []cleanupLoadSample {
	var samples []cleanupLoadSample
	deadline := time.NewTimer(scenario.duration)
	defer deadline.Stop()
	ticker := time.NewTicker(scenario.sampleInterval)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-deadline.C:
			close(stop)
			return samples
		case <-ticker.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			stats := cache.Stats()
			samples = append(samples, cleanupLoadSample{
				elapsedMS:   time.Since(start).Milliseconds(),
				heapAllocMB: float64(mem.HeapAlloc) / 1024 / 1024,
				heapInuseMB: float64(mem.HeapInuse) / 1024 / 1024,
				queueMB:     float64(cache.Capacity()) / 1024 / 1024,
				backlog:     stats.CleanBacklog,
				expired:     stats.ExpiredBacklog,
				liveEntries: cache.Len(),
				cleanRuns:   stats.CleanRuns,
				cleaned:     stats.CleanedEntries,
			})
		}
	}
}

func summarizeLatencies(latencies []time.Duration) latencySummary {
	if len(latencies) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, latency := range sorted {
		sum += latency
	}
	return latencySummary{
		count: len(sorted),
		p50:   percentile(sorted, 0.50),
		p99:   percentile(sorted, 0.99),
		max:   sorted[len(sorted)-1],
		mean:  time.Duration(int64(sum) / int64(len(sorted))),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func peakBacklog(result cleanupLoadResult) int64 {
	var peak int64
	for _, sample := range result.samples {
		if sample.expired > peak {
			peak = sample.expired
		}
	}
	return peak
}

func peakQueueMB(result cleanupLoadResult) float64 {
	var peak float64
	for _, sample := range result.samples {
		if sample.queueMB > peak {
			peak = sample.queueMB
		}
	}
	return peak
}

func writeCleanupLoadSamples(result cleanupLoadResult) error {
	path := "artifacts/cleanup_load_" + result.scenario.name + ".csv"
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, "elapsed_ms,heap_alloc_mb,heap_inuse_mb,queue_mb,clean_backlog,expired_backlog,live_entries,clean_runs,cleaned_entries"); err != nil {
		return err
	}
	for _, sample := range result.samples {
		if _, err := fmt.Fprintf(file, "%d,%.2f,%.2f,%.2f,%d,%d,%d,%d,%d\n",
			sample.elapsedMS, sample.heapAllocMB, sample.heapInuseMB, sample.queueMB,
			sample.backlog, sample.expired, sample.liveEntries, sample.cleanRuns, sample.cleaned); err != nil {
			return err
		}
	}
	return nil
}

func printCleanupLoadComparison(t *testing.T, legacy, adaptive cleanupLoadResult) {
	t.Helper()
	t.Logf("\npeak-load comparison (writers=%d, lifeWindow=%s, cleanWindow=%s, duration=%s)",
		legacy.scenario.writers, legacy.scenario.lifeWindow, legacy.scenario.cleanWindow, legacy.scenario.duration)
	t.Logf("%-34s  %12s  %12s", "metric", "legacy", "adaptive")
	t.Logf("%-34s  %12s  %12s", "writes",
		fmt.Sprintf("%d", legacy.writes), fmt.Sprintf("%d", adaptive.writes))
	t.Logf("%-34s  %12s  %12s", "write latency p50",
		formatLatency(legacy.latency.p50), formatLatency(adaptive.latency.p50))
	t.Logf("%-34s  %12s  %12s", "write latency p99",
		formatLatency(legacy.latency.p99), formatLatency(adaptive.latency.p99))
	t.Logf("%-34s  %12s  %12s", "write latency max",
		formatLatency(legacy.latency.max), formatLatency(adaptive.latency.max))
	t.Logf("%-34s  %12s  %12s", "peak expired backlog (entries)",
		fmt.Sprintf("%d", peakBacklog(legacy)), fmt.Sprintf("%d", peakBacklog(adaptive)))
	t.Logf("%-34s  %12s  %12s", "peak queue capacity (MB)",
		fmt.Sprintf("%.2f", peakQueueMB(legacy)), fmt.Sprintf("%.2f", peakQueueMB(adaptive)))
	t.Logf("%-34s  %12s  %12s", "cleaner passes",
		fmt.Sprintf("%d", legacy.stats.CleanRuns), fmt.Sprintf("%d", adaptive.stats.CleanRuns))
	t.Logf("%-34s  %12s  %12s", "entries cleaned",
		fmt.Sprintf("%d", legacy.stats.CleanedEntries), fmt.Sprintf("%d", adaptive.stats.CleanedEntries))
	t.Logf("%-34s  %12s  %12s", "expired backlog after 3s drain",
		fmt.Sprintf("%d", legacy.residualExpired), fmt.Sprintf("%d", adaptive.residualExpired))
}

func formatLatency(latency time.Duration) string {
	return fmt.Sprintf("%.1fµs", float64(latency.Microseconds())+float64(latency.Nanoseconds()%1000)/1000.0)
}
