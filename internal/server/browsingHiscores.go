package server

import (
	"context"
	"fmt"
	"log"
	"chaogarden-server/internal/server/db"
	"chaogarden-server/pkg/packets"
)

type BrowsingHiscores struct {
	client  ClientInterfacer
	logger  *log.Logger
	queries *db.Queries
	dbCtx   context.Context
}

func (b *BrowsingHiscores) Name() string { return "BrowsingHiscores" }

func (b *BrowsingHiscores) SetClient(client ClientInterfacer) {
	b.client = client
	loggingPrefix := fmt.Sprintf("Client %d [%s]: ", client.Id(), b.Name())
	b.logger = log.New(log.Writer(), loggingPrefix, log.LstdFlags)
	b.queries = client.DbTx()
	b.dbCtx = context.Background()
}

func (b *BrowsingHiscores) OnEnter() {}
func (b *BrowsingHiscores) OnExit()  {}

func (b *BrowsingHiscores) HandleMessage(senderId uint64, packet *packets.Packet) {
	switch packet.Msg.(type) {
	// CORRECTED: Use LeaveMinigameRequest
	case *packets.Packet_LeaveMinigameRequest:
		b.client.SetState(&Connected{})
	}
}