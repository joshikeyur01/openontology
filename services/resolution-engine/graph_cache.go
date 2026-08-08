package main

import (
	"sync"
	"time"
)

// maxGraphCacheEntries bounds the resolved-context cache. The graph tier sits on
// the ingestion hot path, so the cache must never become the thing that pages
// the engine out of memory.
const maxGraphCacheEntries = 4096

// graphContextCache is the TTL cache that fronts every graph provider. It
// absorbs the repeat lookups a re-alerting asset generates: a machine in alarm
// produces a mutation every re-alert interval, and none of them need a fresh
// round trip to the topology store.
//
// Values are cloned on the way in and on the way out, so a cached context can
// never be mutated by a worker goroutine holding a returned copy.
type graphContextCache struct {
	ttl time.Duration

	mu      sync.RWMutex
	entries map[string]graphCacheEntry
}

type graphCacheEntry struct {
	context   OntologyContext
	expiresAt time.Time
}

func newGraphContextCache(ttl time.Duration) *graphContextCache {
	return &graphContextCache{ttl: ttl, entries: make(map[string]graphCacheEntry)}
}

// Lookup returns a cached context when one is present and unexpired.
func (c *graphContextCache) Lookup(assetID string) (OntologyContext, bool) {
	c.mu.RLock()
	entry, ok := c.entries[assetID]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return OntologyContext{}, false
	}
	return entry.context.Clone(), true
}

// Store caches a resolved context. A non-positive TTL disables caching
// entirely, which is what an operator debugging a topology change wants.
func (c *graphContextCache) Store(assetID string, value OntologyContext) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Opportunistic eviction keeps the cache bounded without a background loop.
	if len(c.entries) > maxGraphCacheEntries {
		now := time.Now()
		for key, entry := range c.entries {
			if now.After(entry.expiresAt) {
				delete(c.entries, key)
			}
		}
	}
	c.entries[assetID] = graphCacheEntry{context: value.Clone(), expiresAt: time.Now().Add(c.ttl)}
}

// Len reports the number of entries currently held, expired ones included.
func (c *graphContextCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
