package cache

import (
	"sync"
	"time"
)

// entry holds a cached value with expiration.
type entry struct {
	value     interface{}
	expiresAt time.Time
}

// Cache is a thread-safe in-memory TTL cache.
type Cache struct {
	mu      sync.RWMutex
	items   map[string]entry
	ttl     time.Duration
	stopCh  chan struct{}
}

// New creates a cache with the given TTL and starts a cleanup goroutine.
func New(ttl time.Duration) *Cache {
	c := &Cache{
		items:  make(map[string]entry),
		ttl:    ttl,
		stopCh: make(chan struct{}),
	}
	go c.cleanup()
	return c
}

// Get retrieves a value from the cache. Returns nil, false if expired or missing.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.items[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.value, true
}

// Set stores a value in the cache with the default TTL.
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// SetWithTTL stores a value with a custom TTL.
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

// Size returns the number of items in the cache (including expired).
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// cleanup runs every 5 minutes to evict expired entries.
func (c *Cache) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.evictExpired()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.items {
		if now.After(e.expiresAt) {
			delete(c.items, k)
		}
	}
}

// Stop terminates the cleanup goroutine.
func (c *Cache) Stop() {
	close(c.stopCh)
}
