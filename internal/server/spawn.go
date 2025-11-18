package server

import (
	"math/rand/v2"
	"chaogarden-server/internal/server/objects"
)

var getPlayerPosition = func(p *objects.Player) (float64, float64) { return p.X, p.Y }
var getPlayerRadius = func(p *objects.Player) float64 { return p.Radius }

func isTooClose(x, y, radius float64, players *objects.SharedCollection[*objects.Player]) bool {
	if players == nil {
		return false
	}
	
	tooClose := false
	players.ForEach(func(_ uint64, player *objects.Player) {
		if tooClose { return }

		objX, objY := getPlayerPosition(player)
		objRad := getPlayerRadius(player)
		dx := objX - x
		dy := objY - y
		distSq := dx*dx + dy*dy

		if distSq < (radius+objRad)*(radius+objRad) {
			tooClose = true
		}
	})
	return tooClose
}

// SpawnCoords finds a safe place for a player to spawn.
func SpawnCoords(radius float64, playersToAvoid *objects.SharedCollection[*objects.Player]) (float64, float64) {
	bound := 500.0 // Start spawning closer to the center
	const maxTries = 25

	for i := 0; i < maxTries; i++ {
		x := bound * (2*rand.Float64() - 1)
		y := bound * (2*rand.Float64() - 1)
		if !isTooClose(x, y, radius, playersToAvoid) {
			return x, y
		}
	}
	// Failsafe if it can't find a spot
	return 0, 0
}