//go:build trust_e2e

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"agentfm/internal/boss"
	"agentfm/internal/network"
	"agentfm/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTrustEndToEnd(t *testing.T) {
	hosts := testutil.NewConnectedMesh(t, 3)
	relayHost, bossAHost, bossBHost := hosts[0], hosts[1], hosts[2]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	relayLedger := testutil.OpenTestLedgerArchive(t, relayHost)
	defer relayLedger.Close()

	bossALedger := testutil.OpenTestLedger(t, bossAHost, "bossA")
	defer bossALedger.Close()

	subject := testutil.NewHost(t).ID()
	rating := testutil.NewSignedRating(t, bossAHost, subject, -0.3, "test")
	if _, err := bossALedger.Append(ctx, rating); err != nil {
		t.Fatalf("boss A append: %v", err)
	}

	testutil.Eventually(t, 5*time.Second, func() bool {
		head, _ := relayLedger.Head(ctx)
		return head != nil && head.TreeSize >= 1
	}, "relay should ingest boss A's rating via gossip")

	bossBLedger := testutil.OpenTestLedgerWithCatchup(t, bossBHost, "bossB", relayHost.ID())
	defer bossBLedger.Close()

	testutil.Eventually(t, 10*time.Second, func() bool {
		entries, _ := boss.GatherPeerEntries(ctx, bossBLedger.Store(), subject, 100)
		return len(entries) == 1
	}, "boss B should catch up via relay archive")

	b := boss.NewForTest(&network.MeshNode{Host: bossBHost})
	b.SetLedger(bossBLedger)
	known, _ := b.ListKnownPeers(ctx)
	found := false
	for _, kp := range known {
		if kp.PeerID == subject && !kp.IsOnline {
			found = true
		}
	}
	if !found {
		t.Fatal("expected subject to appear in offline section of ListKnownPeers")
	}

	rendered := b.RenderPeerView(ctx, subject.String())
	if !strings.Contains(rendered, "-0.30") {
		t.Fatalf("TUI peer-view did not render the propagated rating:\n%s", rendered)
	}
}

// Compile-time anchor: peer.ID is used in the loop body; keep the import alive.
var _ peer.ID
