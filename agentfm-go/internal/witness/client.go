package witness

import (
	"context"
	"errors"
	"fmt"
	"io"

	"agentfm/internal/network"

	pb "agentfm/internal/ledger/pb"
	witnesspb "agentfm/internal/witness/pb"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// Client is the requester side of /agentfm/witness/1.0.0. Held by the
// ledger so it can ask N witnesses to co-sign each new LogHead. One
// Client per process; safe to call CoSign concurrently from many
// goroutines (libp2p host handles concurrent streams).
type Client struct {
	host host.Host
}

// NewClient constructs a Client around an existing libp2p host. The
// host MUST already have the WitnessProtocol multistream selector
// negotiable with the target peer — i.e. the target is running our
// witness role.
func NewClient(h host.Host) *Client {
	return &Client{host: h}
}

// CoSign opens a stream to witnessID, writes a WitnessRequest, reads
// the response, and returns it. The returned response is either a
// WitnessSig (success path) or an EquivocationAlert (the witness
// detected the head does not extend the previous one it had signed
// for the head's peer).
//
// consistencyProof MUST be a valid RFC 6962 proof from the witness's
// last-seen size up to head.TreeSize when the witness has prior
// state for this peer. On first sighting the witness ignores the
// proof; pass nil. See merkle.Tree.ConsistencyProof.
//
// Operational errors (dial failure, stream error, decode failure)
// surface as Go errors so the caller can retry or skip. A successful
// "the witness rejected with an alert" is NOT an error — the caller
// must inspect resp.Outcome.
func (c *Client) CoSign(ctx context.Context, witnessID peer.ID, head *pb.LogHead, consistencyProof [][]byte) (*witnesspb.WitnessResponse, error) {
	if head == nil {
		return nil, errors.New("witness client: nil head")
	}
	s, err := c.host.NewStream(ctx, witnessID, network.WitnessProtocol)
	if err != nil {
		return nil, fmt.Errorf("witness client: open stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &witnesspb.WitnessRequest{Head: head, ConsistencyProof: consistencyProof}
	if err := writeLengthPrefixed(s, req); err != nil {
		return nil, fmt.Errorf("witness client: write request: %w", err)
	}
	// Half-close so the server sees EOF on its read side — some
	// transports don't deliver the response until the request side
	// signals end of write.
	if err := s.CloseWrite(); err != nil {
		return nil, fmt.Errorf("witness client: close-write: %w", err)
	}

	resp, err := readLengthPrefixed[witnesspb.WitnessResponse](s)
	if err != nil {
		return nil, fmt.Errorf("witness client: read response: %w", err)
	}
	return resp, nil
}

// -----------------------------------------------------------------------------
// length-prefixed wire helpers. We use a uint32 BE length-prefix to
// frame proto messages on the stream — matches the existing
// /agentfm/feedback/1.0.0 style.
// -----------------------------------------------------------------------------

func readLengthPrefixed[T any, PT interface {
	*T
	proto.Message
}](r io.Reader) (PT, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, fmt.Errorf("read length prefix: %w", err)
	}
	n := uint32(prefix[0])<<24 | uint32(prefix[1])<<16 | uint32(prefix[2])<<8 | uint32(prefix[3])
	if n > 1<<20 {
		// 1 MiB cap protects the receiver from a malicious peer
		// declaring a multi-GB body and tying up a goroutine.
		return nil, fmt.Errorf("framed message too large: %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	out := PT(new(T))
	if err := proto.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return out, nil
}

func writeLengthPrefixed(w io.Writer, m proto.Message) error {
	body, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	n := uint32(len(body))
	prefix := []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	if _, err := w.Write(prefix); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}
