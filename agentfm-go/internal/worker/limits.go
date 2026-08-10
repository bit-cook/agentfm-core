package worker

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Resource limits applied to the task container's cgroup (roadmap R1).
//
// Until these existed the sandbox had no bound of any kind: a fork bomb, a
// runaway allocation or a busy loop in the agent consumed the worker host
// until the kernel OOM killer picked a victim — which is as likely to be the
// worker daemon, or another tenant's task, as the offender.
//
// Every value is operator-configurable via flags on cmd/agentfm; none is
// baked in. Zero means "do not emit the flag", which reproduces the historical
// unbounded behaviour — deliberate, because a memory ceiling that is wrong for
// a given agent kills legitimate work, and existing workers must keep running
// across this upgrade.
//
// PidsLimit is the exception: it defaults to a non-zero value because a fork
// bomb is contained by a ceiling far above anything a real agent needs, so the
// default carries security value at negligible compatibility risk. See
// DefaultPidsLimit.
type ResourceLimits struct {
	// CPUs caps CPU time as a fraction of host cores, mapping to podman's
	// --cpus (e.g. 1.5 = one and a half cores' worth). 0 disables the cap.
	CPUs float64

	// MemoryBytes caps resident memory, mapping to --memory. It is also
	// applied to --memory-swap at the identical value, which disables swap
	// for the container: without that, a container at its memory ceiling
	// silently swaps instead of being killed, degrading the whole host.
	// 0 disables the cap.
	MemoryBytes int64

	// PidsLimit caps the number of processes AND threads in the container's
	// cgroup, mapping to --pids-limit. Threads count, so this must stay well
	// clear of what a threaded runtime legitimately uses. 0 disables the cap.
	PidsLimit int64
}

// DefaultPidsLimit is the process/thread ceiling applied when the operator
// does not choose one.
//
// Sized to be uncontroversial rather than tight: a numerical Python agent with
// OpenMP or torch threads on a large host runs in the low hundreds of threads,
// while a fork bomb reaches this in milliseconds and stops. Operators who need
// more raise it; -task-pids-limit=0 disables it entirely.
const DefaultPidsLimit int64 = 1024

var (
	// ErrInvalidLimit reports a limit that cannot be honoured — a negative
	// value, or a size string that does not parse. Always a configuration
	// error, surfaced at startup rather than at task time.
	ErrInvalidLimit = errors.New("invalid resource limit")
)

// Validate rejects limits that cannot be applied. It is called at startup so a
// misconfigured worker refuses to boot instead of discovering the problem on
// its first task.
func (l ResourceLimits) Validate() error {
	// NaN and ±Inf must be caught explicitly: every ordered comparison
	// against NaN is false, so `CPUs < 0` passes it and `CPUs > 0` then
	// suppresses the flag — the worker would start, report success, and run
	// with no CPU ceiling at all while the operator believed they had set
	// one. +Inf is the mirror image: it validates, emits "--cpus +Inf", and
	// every task fails at the runtime instead of at startup.
	if math.IsNaN(l.CPUs) || math.IsInf(l.CPUs, 0) {
		return fmt.Errorf("%w: cpus must be a finite number, got %v", ErrInvalidLimit, l.CPUs)
	}
	if l.CPUs < 0 {
		return fmt.Errorf("%w: cpus must be >= 0, got %v", ErrInvalidLimit, l.CPUs)
	}
	// Below roughly a hundredth of a core the cgroup quota is refused by the
	// runtime, so the task fails at run time rather than at configuration
	// time — the same class of mistake as an unbootably small memory ceiling.
	const minUsableCPUs = 0.01
	if l.CPUs > 0 && l.CPUs < minUsableCPUs {
		return fmt.Errorf("%w: cpus %v is below the %v minimum a container can run with",
			ErrInvalidLimit, l.CPUs, minUsableCPUs)
	}
	if l.MemoryBytes < 0 {
		return fmt.Errorf("%w: memory must be >= 0, got %d", ErrInvalidLimit, l.MemoryBytes)
	}
	if l.PidsLimit < 0 {
		return fmt.Errorf("%w: pids limit must be >= 0, got %d", ErrInvalidLimit, l.PidsLimit)
	}
	// A memory ceiling below a few MiB cannot start any real container image;
	// it would turn every task into an immediate OOM kill that looks like an
	// agent bug rather than a configuration mistake.
	const minUsableMemory = 6 << 20 // 6 MiB
	if l.MemoryBytes > 0 && l.MemoryBytes < minUsableMemory {
		return fmt.Errorf("%w: memory %d is below the %d minimum a container can start with",
			ErrInvalidLimit, l.MemoryBytes, minUsableMemory)
	}
	return nil
}

// Describe renders the limits for operator-facing output. Used at worker
// startup and in the per-task log line so the applied policy is observable
// without reading the process's command line.
func (l ResourceLimits) Describe() string {
	parts := make([]string, 0, 3)
	if l.CPUs > 0 {
		parts = append(parts, fmt.Sprintf("cpus=%g", l.CPUs))
	} else {
		parts = append(parts, "cpus=unlimited")
	}
	if l.MemoryBytes > 0 {
		parts = append(parts, fmt.Sprintf("memory=%s", FormatSize(l.MemoryBytes)))
	} else {
		parts = append(parts, "memory=unlimited")
	}
	if l.PidsLimit > 0 {
		parts = append(parts, fmt.Sprintf("pids=%d", l.PidsLimit))
	} else {
		parts = append(parts, "pids=unlimited")
	}
	return strings.Join(parts, " ")
}

// ParseSize converts a podman-style size string to bytes.
//
// Suffixes b, k, m, g are accepted, case-insensitively, and are 1024-based —
// matching podman's own --memory parsing, so an operator can copy a value
// between `podman run` and agentfm and get the same ceiling. A bare number is
// bytes. The empty string is 0, meaning "unset".
//
// Rejecting rather than guessing is deliberate: silently reading "512"
// as megabytes when the operator meant bytes would produce a limit 1000x off.
func ParseSize(s string) (int64, error) {
	orig := strings.TrimSpace(s)
	if orig == "" {
		return 0, nil
	}

	// Every error below quotes `orig`, not the progressively trimmed working
	// copy: an operator who typed "512Mo" must see "512Mo" in the message.
	invalid := func(reason string) error {
		return fmt.Errorf("%w: %q %s (expected e.g. 512m, 2g, 1GiB, or a plain byte count)",
			ErrInvalidLimit, orig, reason)
	}

	// Accept both the short (512m) and the explicit binary (512MiB) spellings
	// for the same value; the units are 1024-based either way.
	work := strings.ToLower(orig)
	if trimmed := strings.TrimSuffix(work, "ib"); trimmed != work {
		work = trimmed
	} else {
		work = strings.TrimSuffix(work, "b")
	}
	if work == "" {
		return 0, invalid("has a unit but no number")
	}

	mult := int64(1)
	switch work[len(work)-1] {
	case 'k':
		mult, work = 1<<10, work[:len(work)-1]
	case 'm':
		mult, work = 1<<20, work[:len(work)-1]
	case 'g':
		mult, work = 1<<30, work[:len(work)-1]
	}

	n, err := strconv.ParseInt(strings.TrimSpace(work), 10, 64)
	if err != nil {
		return 0, invalid("is not a size")
	}
	if n < 0 {
		return 0, invalid("must be >= 0")
	}
	// Reject overflow rather than silently wrapping to a nonsensical ceiling.
	if mult > 1 && n > (1<<62)/mult {
		return 0, invalid("overflows")
	}
	return n * mult, nil
}

// FormatSize renders a byte count using the same binary units ParseSize
// accepts, so operator output round-trips through the flag.
func FormatSize(b int64) string {
	switch {
	case b >= 1<<30 && b%(1<<30) == 0:
		return fmt.Sprintf("%dg", b/(1<<30))
	case b >= 1<<20 && b%(1<<20) == 0:
		return fmt.Sprintf("%dm", b/(1<<20))
	case b >= 1<<10 && b%(1<<10) == 0:
		return fmt.Sprintf("%dk", b/(1<<10))
	default:
		return strconv.FormatInt(b, 10)
	}
}
