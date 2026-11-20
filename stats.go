package bigcache

import "sync/atomic"

// Stats stores cache statistics
type Stats struct {
	// Hits is a number of successfully found keys
	Hits int64 `json:"hits"`
	// Misses is a number of not found keys
	Misses int64 `json:"misses"`
	// DelHits is a number of successfully deleted keys
	DelHits int64 `json:"delete_hits"`
	// DelMisses is a number of not deleted keys
	DelMisses int64 `json:"delete_misses"`
	// Collisions is a number of happened key-collisions
	Collisions int64 `json:"collisions"`
	// CleanRuns is a number of background cleaning passes performed across all shards.
	// One pass is counted per shard visited by the cleaner.
	CleanRuns int64 `json:"clean_runs"`
	// CleanedEntries is a number of expired entries reclaimed by the background cleaner.
	CleanedEntries int64 `json:"cleaned_entries"`
	// CleanBacklog is a snapshot of entries still occupying queue memory that are not
	// reachable from the live key index (expired, overwritten or explicitly deleted
	// entries waiting for the cleaner). A value staying flat or shrinking means the
	// cleaner keeps up with writes; sustained growth means expiration outruns cleaning.
	CleanBacklog int64 `json:"clean_backlog"`
	// ExpiredBacklog is a snapshot of entries whose life window has elapsed but that
	// have not yet been reclaimed. It counts memory the cleaner owes regardless of
	// whether the entries are still reachable through the key index, and is the key
	// signal for detecting expiration piling up under peak write traffic.
	ExpiredBacklog int64 `json:"expired_backlog"`
}

// shardStats uses atomic.Int64 so the counters stay 8-byte aligned on 32-bit platforms.
type shardStats struct {
	hits       atomic.Int64
	misses     atomic.Int64
	delHits    atomic.Int64
	delMisses  atomic.Int64
	collisions atomic.Int64
}

// cleanerStats holds cache-wide counters updated by the background cleaner goroutine
// and read without locks from Stats().
type cleanerStats struct {
	runs           atomic.Int64
	cleanedEntries atomic.Int64
}
