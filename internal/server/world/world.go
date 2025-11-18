package world

import (
	"chaogarden-server/internal/server/db"
	"log"
	"os"
	"sync"
)

// World manages all the maps and players within them.
type World struct {
	maps    map[string]*Map
	players *sync.Map // [uint64]*Player -> clientID to Player
	logger  *log.Logger
	db      *db.Queries
}

// Map holds the state of a single game map.
type Map struct {
	ID        string
	collision [][]bool
}

// Player represents a player in the game world.
type Player struct {
	ID        uint64
	Username  string
	MapID     string
	X, Y      float64
	Direction int
}

// NewWorld creates a new world manager.
func NewWorld(queries *db.Queries) *World {
	w := &World{
		maps:    make(map[string]*Map),
		players: &sync.Map{},
		logger:  log.New(os.Stdout, "[World] ", log.Ldate|log.Ltime),
		db:      queries,
	}
	w.loadMaps()
	return w
}

// loadMaps loads map data from a source (e.g., files, database).
func (w *World) loadMaps() {
	w.logger.Println("Loading maps...")
	w.maps["PalletTown"] = &Map{ID: "PalletTown", collision: make([][]bool, 20)}
	w.logger.Printf("Loaded %d maps.", len(w.maps))
}

// AddPlayer adds a player to the world.
func (w *World) AddPlayer(player *Player) {
	w.players.Store(player.ID, player)
	w.logger.Printf("Player %s (ID: %d) added to world at map %s.", player.Username, player.ID, player.MapID)
}

// RemovePlayer removes a player from the world.
func (w *World) RemovePlayer(playerID uint64) {
	w.players.Delete(playerID)
	w.logger.Printf("Player ID %d removed from world.", playerID)
}

// GetPlayer retrieves a player from the world by their ID.
func (w *World) GetPlayer(playerID uint64) (*Player, bool) {
	if p, ok := w.players.Load(playerID); ok {
		if player, ok := p.(*Player); ok {
			return player, true
		}
	}
	return nil, false
}

// UpdatePlayerPosition updates the stored position of a player.
func (w *World) UpdatePlayerPosition(playerID uint64, x, y float64, direction int) bool {
	player, ok := w.GetPlayer(playerID)
	if !ok {
		return false
	}

	player.X = x
	player.Y = y
	player.Direction = direction
	return true
}

// RangePlayers iterates over all players in the world.
func (w *World) RangePlayers(f func(player *Player) bool) {
	w.players.Range(func(key, value interface{}) bool {
		if player, ok := value.(*Player); ok {
			return f(player)
		}
		return true
	})
}