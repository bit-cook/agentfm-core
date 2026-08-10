package network

import "time"

// This file was RECONSTRUCTED on 2026-08-10 from the project documentation.
//
// It had been excluded from version control since the initial commit
// (35e9925, .gitignore rule "constants.go"), which meant the module could
// not be compiled from a clean clone. Every value below is sourced from a
// documented reference, cited inline. See docs/security/AUDIT-2026-08-10.md
// §2 for the full context.
//
// Two rules apply to this file:
//
//  1. The protocol IDs and GossipSub topic names are wire contracts. Changing
//     one without bumping its version suffix silently partitions the mesh:
//     old and new nodes simply stop seeing each other. Treat any edit as a
//     release-gating event requiring a coordinated rebuild of every node
//     (docs/development.md:59).
//
//  2. The timeouts below are the worker's only effective resource control
//     today (roadmap item R2). They are hard-coded here purely to restore
//     parity with the pre-existing binaries — this contradicts the
//     "policy values are configurable, never hard-coded" rule in CLAUDE.md
//     and is expected to be replaced by a --task-timeout flag plus a
//     per-task field when R2 lands. Do not add new policy values here.

// ---------------------------------------------------------------------------
// Stream protocol IDs
// ---------------------------------------------------------------------------
//
// Declared as untyped string constants so they convert implicitly to
// protocol.ID at libp2p call sites (SetStreamHandler / NewStream) while
// remaining usable as plain strings in logs and metrics.

const (
	// TaskProtocol carries the JSON task envelope Boss → Worker, then the
	// container's live stdout/stderr back. Source: docs/architecture.md:57.
	TaskProtocol = "/agentfm/task/1.0.0"

	// ArtifactProtocol carries the length-prefixed zip of /tmp/output
	// Worker → Boss. Source: docs/architecture.md:58.
	ArtifactProtocol = "/agentfm/artifacts/1.0.0"

	// CommentFetchProtocol pulls a content-addressed comment body by CID
	// from its author or a relay. Source: docs/architecture.md:60.
	CommentFetchProtocol = "/agentfm/comment-fetch/1.0.0"

	// LedgerFetchProtocol pulls entries a peer authored itself.
	// Source: docs/architecture.md:62, CHANGELOG.md:26.
	LedgerFetchProtocol = "/agentfm/ledger-fetch/1.0.0"

	// InboxFetchProtocol pulls third-party entries a peer holds about
	// others. Source: docs/architecture.md:62.
	InboxFetchProtocol = "/agentfm/inbox-fetch/1.0.0"
)

// ---------------------------------------------------------------------------
// GossipSub topics
// ---------------------------------------------------------------------------

const (
	// TelemetryTopic carries the ~2s worker heartbeat (CPU / GPU / RAM /
	// queue depth). Source: docs/architecture.md:56, docs/development.md:59.
	TelemetryTopic = "agentfm-telemetry-v1"

	// FeedbackTopic carries signed rating and comment ledger entries.
	// Source: docs/trust.md:18, CHANGELOG.md:15.
	FeedbackTopic = "agentfm-feedback-v1"

	// EquivocationTopic carries witness alerts about peers that signed
	// two conflicting ledger heads. Source: CHANGELOG.md:25.
	EquivocationTopic = "agentfm-equivocation-v1"
)

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

const (
	// TaskPayloadReadTimeout bounds how long a worker waits for the Boss to
	// deliver the task envelope after the stream opens. Source:
	// docs/architecture.md:57 ("30s to receive payload").
	TaskPayloadReadTimeout = 30 * time.Second

	// TaskExecutionTimeout is the idle ceiling while a task streams output.
	// It is also the worker-side ctx that SIGKILLs the Podman container.
	// Source: docs/architecture.md:57 ("then 10 min idle while streaming").
	TaskExecutionTimeout = 10 * time.Minute

	// ArtifactStreamTimeout bounds the artifact zip transfer.
	// Source: docs/architecture.md:58.
	ArtifactStreamTimeout = 30 * time.Minute

	// StreamDialTimeout bounds DHT lookups and relay dials so routing never
	// blocks indefinitely. Source: docs/architecture.md:175.
	StreamDialTimeout = 20 * time.Second
)

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

// MaxArtifactBytes caps the artifact zip a worker may return: 5 GiB.
// Explicitly typed int64 — it is compared against file sizes and written
// through encoding/binary, both of which need a fixed-size type.
// Source: docs/architecture.md:58.
const MaxArtifactBytes int64 = 5 << 30

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

const (
	// PublicLighthouse is the bootstrap relay dialled when neither
	// -bootstrap nor -swarmkey is supplied. Source: README.md:179-180
	// ("The public lighthouse is baked in").
	PublicLighthouse = "/ip4/78.47.21.107/tcp/4001/p2p/12D3KooWQHw8mVQkx17kLTNiRTbYckU2cAGcAwFFLzVJhhmBs5zL"

	// RendezvousString is the Kademlia DHT key workers advertise under and
	// Boss nodes sweep with FindPeers. Source: docs/architecture.md:166.
	RendezvousString = "agentfm-rendezvous"

	// MDNSServiceTag is the mDNS service tag for same-LAN discovery.
	// Source: docs/architecture.md:165.
	MDNSServiceTag = "agentfm-local"
)
