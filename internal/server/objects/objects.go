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

// Player represents a player in the minigame.
type Player struct {
	ID        uint64
	DbId      int64
	Name      string
	X         float64
	Y         float64
	Radius    float64
	Direction float64
	Speed     float64
	Color     int32
	BestScore int64
}

// NewPlayer creates a new player object with default values.
func NewPlayer(dbId int64, name string, color int32) *Player {
	return &Player{
		ID:     NextID(),
		DbId:   dbId,
		Name:   name,
		Radius: 20.0,
		Speed:  150.0,
		Color:  color,
	}
}

// Spore represents a consumable object in the minigame.
type Spore struct {
	ID     uint64
	X      float64
	Y      float64
	Radius float64
}

// NewSpore creates a new spore object.
func NewSpore(x, y, radius float64) *Spore {
	return &Spore{
		ID:     NextID(),
		X:      x,
		Y:      y,
		Radius: radius,
	}
}

// SharedCollection is a thread-safe generic map for managing game objects.
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