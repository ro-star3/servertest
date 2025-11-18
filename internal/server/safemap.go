package server

import "sync"

// SafeClientMap is a concurrent-safe map for storing clients.
type SafeClientMap struct {
	mu   sync.RWMutex
	data map[uint64]*Client
}

// NewSafeClientMap initializes a new safe map.
func NewSafeClientMap() *SafeClientMap {
	return &SafeClientMap{
		data: make(map[uint64]*Client),
	}
}

// Store adds or updates a client in the map.
func (sm *SafeClientMap) Store(key uint64, value *Client) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data[key] = value
}

// Load retrieves a client from the map.
func (sm *SafeClientMap) Load(key uint64) (*Client, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	val, ok := sm.data[key]
	return val, ok
}

// Delete removes a client from the map.
func (sm *SafeClientMap) Delete(key uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.data, key)
}

// Range iterates over the clients in the map and calls the given function for each.
func (sm *SafeClientMap) Range(f func(key uint64, value *Client) bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for k, v := range sm.data {
		if !f(k, v) {
			break
		}
	}
}