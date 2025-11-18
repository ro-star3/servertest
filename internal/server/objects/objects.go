package objects

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

var nextID uint64

// init ensures that the first ID is randomized to avoid collisions between server restarts.
func init() {
	nextID = uint64(rand.Int64())
}

// NextID provides a thread-safe, unique ID for game objects.
func NextID() uint64 {
	return atomic.AddUint64(&nextID, 1)
}

// Player represents a player on the overworld map.
// It only holds data relevant to the immediate game world, not persistent data.
type Player struct {
	ID        uint64  // The temporary WebSocket connection ID
	DbId      int64   // The permanent User ID from the database
	Name      string
	MapID     string  // The map the player is currently on (e.g., "PalletTown")
	X         float64 // Grid X position
	Y         float64 // Grid Y position
	Direction int     // e.g., 0:Down, 1:Up, 2:Left, 3:Right
}

// NewPlayer creates a new player object for the overworld.
func NewPlayer(dbId int64, name string, mapId string, x, y float64) *Player {
	return &Player{
		ID:    NextID(),
		DbId:  dbId,
		Name:  name,
		MapID: mapId,
		X:     x,
		Y:     y,
	}
}

// Spore object is removed as it's part of the old minigame.

// SharedCollection is a thread-safe generic map for managing game objects.
// This is a great, reusable component.
type SharedCollection[T any] struct {
	mx sync.RWMutex
	m  map[uint64]T
}

// NewSharedCollection creates a new, empty shared collection.
func NewSharedCollection[T any]() *SharedCollection[T] {
	return &SharedCollection[T]{
		m: make(map[uint64]T),
	}
}

// AddWithId adds an item to the collection with a specific ID.
func (c *SharedCollection[T]) AddWithId(id uint64, value T) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.m[id] = value
}

// Get retrieves an item from the collection by its ID.
func (c *SharedCollection[T]) Get(id uint64) (T, bool) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	val, ok := c.m[id]
	return val, ok
}

// Remove deletes an item from the collection by its ID.
func (c *SharedCollection[T]) Remove(id uint64) {
	c.mx.Lock()
	defer c.mx.Unlock()
	delete(c.m, id)
}

// Len returns the number of items in the collection.
func (c *SharedCollection[T]) Len() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return len(c.m)
}

// ForEach executes a function for each item in the collection.
func (c *SharedCollection[T]) ForEach(f func(id uint64, value T)) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	for id, value := range c.m {
		f(id, value)
	}
}