// peer_view.go implements GatherPeerEntries, a shared helper that collects
// ledger entries (both own-log and inbox) for a given subject peer,
// sorted newest-first, and capped at limit. Used by both:
//   - GET /v1/peers/{id}/log (HTTP API, sub-task 1.3)
//   - GET /v1/peers/{id}     (single-peer summary, sub-task 1.4)
//
// Also implements KnownPeer / ListKnownPeers (Phase 6 offline-peer
// visibility, sub-task 6.2): combines in-memory activeWorkers (online
// peers from telemetry) with store.DistinctSubjects (peers known only
// via ledger entries) into one sorted list.
package boss

import (
	"context"
	"sort"
	"time"

	pb "agentfm/internal/ledger/pb"
	"agentfm/internal/ledger/store"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// PeerEntry is one decoded ledger entry about a subject peer.
type PeerEntry struct {
	ReceivedAt        time.Time `json:"received_at"`
	Kind              string    `json:"kind"` // "Rating" | "Comment"
	Rater             peer.ID   `json:"rater_peer_id"`
	Dimension         string    `json:"dimension,omitempty"`
	Score             float64   `json:"score,omitempty"`
	Context           string    `json:"context,omitempty"`
	Language          string    `json:"language,omitempty"`
	TextCID           []byte    `json:"text_cid,omitempty"`
	RaterStatus       string    `json:"rater_status"`
	RaterHonestyScore float64   `json:"rater_honesty_score"`
}

// GatherPeerEntries walks both IterateAllOwnEntries and
// IterateAllInboxEntries, decodes each SignedEntry proto, filters to those
// whose SubjectPeerId matches subject, returns newest-first sorted, capped
// at limit. RaterStatus and RaterHonestyScore are left zero — callers that
// need them should decorate after calling (see handlePeerLog).
//
// This is a lifted/refactored version of the CLI's gatherReputationView in
// cmd/agentfm/reputation.go. Both can co-exist — the CLI version returns a
// richer reputationView struct; this one returns []PeerEntry for HTTP use.
func GatherPeerEntries(ctx context.Context, s *store.Store, subject peer.ID, limit int) ([]PeerEntry, error) {
	subjectBytes := []byte(subject)

	var entries []PeerEntry

	collect := func(payload []byte, receivedAtNs int64) {
		var signed pb.SignedEntry
		if err := proto.Unmarshal(payload, &signed); err != nil {
			return
		}
		receivedAt := time.Unix(0, receivedAtNs)
		switch body := signed.GetBody().(type) {
		case *pb.SignedEntry_Rating:
			r := body.Rating
			if r == nil {
				return
			}
			if !bytesEqualPB(r.SubjectPeerId, subjectBytes) {
				return
			}
			entries = append(entries, PeerEntry{
				ReceivedAt: receivedAt,
				Kind:       "Rating",
				Rater:      peer.ID(r.RaterPeerId),
				Dimension:  r.Dimension,
				Score:      r.Score,
				Context:    r.Context,
			})
		case *pb.SignedEntry_Comment:
			c := body.Comment
			if c == nil {
				return
			}
			if !bytesEqualPB(c.SubjectPeerId, subjectBytes) {
				return
			}
			entries = append(entries, PeerEntry{
				ReceivedAt: receivedAt,
				Kind:       "Comment",
				Rater:      peer.ID(c.RaterPeerId),
				Language:   c.Language,
				TextCID:    c.TextCid,
			})
		}
	}

	if err := s.IterateAllOwnEntries(ctx, func(e *store.Entry) error {
		collect(e.Payload, e.InsertedAt)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := s.IterateAllInboxEntries(ctx, func(e *store.InboxEntry) error {
		collect(e.Payload, e.ReceivedAt)
		return nil
	}); err != nil {
		return nil, err
	}

	// Sort newest-first.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ReceivedAt.After(entries[j].ReceivedAt)
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// bytesEqualPB compares two byte slices for equality.
func bytesEqualPB(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// KnownPeer is the operator-facing view of a peer the boss has heard about,
// whether currently online (in activeWorkers) or only seen via ledger entries
// (offline / never-seen-alive).
type KnownPeer struct {
	PeerID        peer.ID
	AgentName     string    // empty for never-seen-alive peers
	LastSeen      time.Time // zero for never-seen-alive peers
	IsOnline      bool
	HonestyScore  float64
	IsEquivocator bool
}

// ListKnownPeers returns every peer the boss has heard about, sorted with
// online peers first (newest-first by LastSeen), then offline by LastSeen
// desc. Uses activeWorkers for online status and store.DistinctSubjects for
// the rest. Decorates each entry with honesty score and equivocator flag.
func (b *Boss) ListKnownPeers(ctx context.Context) ([]KnownPeer, error) {
	known := map[peer.ID]*KnownPeer{}

	b.mu.RLock()
	for pidStr, p := range b.activeWorkers {
		pid, err := peer.Decode(pidStr)
		if err != nil {
			continue
		}
		ls := b.lastSeen[pidStr]
		known[pid] = &KnownPeer{
			PeerID:    pid,
			AgentName: p.AgentName,
			LastSeen:  ls,
			IsOnline:  true,
		}
	}
	b.mu.RUnlock()

	if b.readStore != nil {
		subjects, err := b.readStore.DistinctSubjects(ctx)
		if err != nil {
			return nil, err
		}
		for _, pidBytes := range subjects {
			pid := peer.ID(pidBytes)
			if _, ok := known[pid]; ok {
				continue // already online — don't overwrite
			}
			known[pid] = &KnownPeer{PeerID: pid, IsOnline: false}
		}
	}

	for _, kp := range known {
		if b.reputationEngine != nil {
			kp.HonestyScore = b.reputationEngine.Score(kp.PeerID.String())
		}
		if b.ledger != nil {
			marked, _ := b.ledger.IsEquivocator(ctx, []byte(kp.PeerID))
			kp.IsEquivocator = marked
		}
	}

	out := make([]KnownPeer, 0, len(known))
	for _, kp := range known {
		out = append(out, *kp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsOnline != out[j].IsOnline {
			return out[i].IsOnline
		}
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out, nil
}
