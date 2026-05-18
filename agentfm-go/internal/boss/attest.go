package boss

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"agentfm/internal/obs"
	pb "agentfm/internal/ledger/pb"
	"agentfm/internal/trustedagents"
	"agentfm/internal/types"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ErrAttestationFailed is returned by checkAttestation when a
// worker's L1 advertisement does not pass the configured mode. The
// HTTP layer surfaces this as 403 agent_attestation_failed.
var ErrAttestationFailed = errors.New("agent_attestation_failed")

// ErrPeerIsEquivocator is defined in trust_gate.go (Phase 8).

// L1Outcome is the attestation result for a single worker — both
// the routing decision and the rating value to write to the ledger.
type L1Outcome struct {
	Allowed      bool
	Reason       string
	RatingScore  float64 // 0.0 means "no rating to write"
	RatingNote   string
	Status       trustedagents.DigestStatus
}

// checkAttestation evaluates a worker's profile against the trusted
// registry per the configured mode. Returns (decision) — caller
// dispatches or refuses based on Allowed; writes the Rating to the
// ledger via writeAttestationRating if RatingScore != 0.
//
// Mode semantics:
//   - AttestOff:    always allow, never rate.
//   - AttestWarn:   always allow, log mismatches, rate -0.3 on
//                   mismatch / -0.5 on malformed digest.
//   - AttestStrict: refuse on mismatch (-0.5) or malformed (-0.7).
//                   Unknown image: refuse only when rejectUnknown
//                   is also true.
//
// Equivocator check is orthogonal to mode and ALWAYS runs.
func (b *Boss) checkAttestation(ctx context.Context, w types.WorkerProfile) L1Outcome {
	// Equivocator gate (Fix-finding: always applied regardless of mode).
	if b.ledger != nil {
		pid, err := peer.Decode(w.PeerID)
		if err == nil {
			marked, ierr := b.ledger.IsEquivocator(ctx, []byte(pid))
			if ierr == nil && marked {
				return L1Outcome{
					Allowed:    false,
					Reason:     ErrPeerIsEquivocator.Error(),
					RatingScore: 0, // already recorded permanently in equivocators table; no extra rating
					Status:     trustedagents.Unknown,
				}
			}
		}
	}

	if b.attestation == AttestOff {
		return L1Outcome{Allowed: true, Status: trustedagents.Unknown}
	}

	// In warn/strict, an empty image_ref means unattested — treat as
	// Unknown for routing purposes.
	if w.AgentImageRef == "" {
		if b.attestation == AttestStrict && b.rejectUnknownImages {
			return L1Outcome{
				Allowed:     false,
				Reason:      "unattested_no_image_ref",
				RatingScore: -0.3,
				RatingNote:  "no_image_ref",
				Status:      trustedagents.Unknown,
			}
		}
		return L1Outcome{Allowed: true, Status: trustedagents.Unknown}
	}

	matched, status, expected := b.trusted.VerifyDigest(w.AgentImageRef, w.AgentImageDigest)
	switch status {
	case trustedagents.KnownTrusted:
		if matched {
			return L1Outcome{Allowed: true, Status: status}
		}
		// shouldn't happen — defensive
		return L1Outcome{
			Allowed:     false,
			Reason:      "trusted_but_unmatched_logic_error",
			RatingScore: -0.5,
			RatingNote:  "logic_error",
			Status:      status,
		}
	case trustedagents.KnownMismatch:
		// Worker advertised an image_ref WE recognise but with the
		// WRONG digest. Active attack.
		score := -0.3
		if b.attestation == AttestStrict {
			return L1Outcome{
				Allowed:     false,
				Reason:      "digest_mismatch:expected=" + expected,
				RatingScore: -0.5,
				RatingNote:  "digest_mismatch",
				Status:      status,
			}
		}
		return L1Outcome{
			Allowed:     true,
			RatingScore: score,
			RatingNote:  "digest_mismatch_warn",
			Status:      status,
		}
	case trustedagents.Unknown:
		// Image isn't in the registry. Allow unless operator explicitly
		// configured rejectUnknownImages.
		if b.attestation == AttestStrict && b.rejectUnknownImages {
			return L1Outcome{
				Allowed:     false,
				Reason:      "unknown_image:" + w.AgentImageRef,
				RatingScore: -0.3,
				RatingNote:  "unknown_image",
				Status:      status,
			}
		}
		return L1Outcome{Allowed: true, Status: status}
	case trustedagents.MalformedDigest:
		// Worker's advertised digest is unparseable. Treat as
		// adversarial.
		if b.attestation == AttestStrict {
			return L1Outcome{
				Allowed:     false,
				Reason:      "malformed_digest",
				RatingScore: -0.7,
				RatingNote:  "malformed_digest",
				Status:      status,
			}
		}
		return L1Outcome{
			Allowed:     true,
			RatingScore: -0.5,
			RatingNote:  "malformed_digest_warn",
			Status:      status,
		}
	default:
		return L1Outcome{Allowed: true, Status: status}
	}
}

// writeAttestationRating appends a machine-issued Rating to the
// boss's own ledger documenting the L1 verification outcome. The
// rating is keyed to the dispatch attempt via a synthetic context
// id, so multiple identical outcomes don't dedupe at the merkle
// layer (each gets a unique timestamp).
//
// Best-effort: failures are logged, not propagated. Refusing
// dispatch shouldn't fail because the audit log is unavailable.
func (b *Boss) writeAttestationRating(ctx context.Context, w types.WorkerProfile, outcome L1Outcome) {
	if b.ledger == nil || outcome.RatingScore == 0 {
		return
	}
	subjectPID, err := peer.Decode(w.PeerID)
	if err != nil {
		slog.Debug("attest: skip rating; bad peer id",
			slog.String("peer_id", w.PeerID),
			slog.Any(obs.FieldErr, err))
		return
	}
	myPID := b.node.Host.ID()
	rating := &pb.SignedEntry{Body: &pb.SignedEntry_Rating{Rating: &pb.Rating{
		RaterPeerId:     []byte(myPID),
		SubjectPeerId:   []byte(subjectPID),
		Dimension:       "honesty",
		Score:           outcome.RatingScore,
		Context:         "attest:" + outcome.RatingNote,
		TimestampUnixNs: time.Now().UnixNano(),
	}}}
	if _, err := b.ledger.Append(ctx, rating); err != nil {
		slog.Warn("attest: failed to append rating",
			slog.String("peer_id", w.PeerID),
			slog.String("status", outcome.Status.String()),
			slog.Any(obs.FieldErr, err))
	}
}
