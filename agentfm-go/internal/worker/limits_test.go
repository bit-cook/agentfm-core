package worker

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestParseSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"0", 0},
		{"512", 512},
		{"1k", 1 << 10},
		{"512m", 512 << 20},
		{"2g", 2 << 30},
		{"2G", 2 << 30},
		// The explicit binary spellings must mean exactly the same as the
		// short ones — an operator copying "2GiB" from podman docs and
		// "2g" from ours must get one ceiling, not two.
		{"2GiB", 2 << 30},
		{"512MiB", 512 << 20},
		{"1KiB", 1 << 10},
		{"1024b", 1024},
		{"  256m  ", 256 << 20},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSize(tc.in)
			if err != nil {
				t.Fatalf("ParseSize(%q) returned an error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseSize(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// A size that cannot be understood must be rejected, never guessed at. Reading
// an ambiguous value as the wrong unit would silently set a ceiling orders of
// magnitude away from what the operator intended.
func TestParseSize_RejectsGarbage(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"m", "b", "abc", "12x", "1.5g", "-5", "-1m", "1 2 m", "0x10"} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSize(in)
			if !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("ParseSize(%q) = %d, %v; want ErrInvalidLimit", in, got, err)
			}
		})
	}
}

// Error messages must quote what the operator actually typed, not an
// internally trimmed fragment, or the message sends them looking in the wrong
// place.
func TestParseSize_ErrorQuotesOriginalInput(t *testing.T) {
	t.Parallel()

	_, err := ParseSize("512Mo")
	if err == nil {
		t.Fatal("expected an error for \"512Mo\"")
	}
	if got := err.Error(); !strings.Contains(got, "512Mo") {
		t.Errorf("error %q does not quote the original input", got)
	}
}

func TestParseSize_RejectsOverflow(t *testing.T) {
	t.Parallel()

	// 9223372036854775807 bytes is fine on its own; with a G suffix it
	// overflows int64 and must be refused rather than wrapping to a small or
	// negative ceiling.
	if _, err := ParseSize("9223372036854775807g"); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("overflowing size was accepted: %v", err)
	}
}

func TestFormatSize_RoundTripsThroughParseSize(t *testing.T) {
	t.Parallel()

	for _, want := range []int64{0, 512, 1 << 10, 512 << 20, 2 << 30, 1536 << 20} {
		got, err := ParseSize(FormatSize(want))
		if err != nil {
			t.Fatalf("FormatSize(%d) = %q, which ParseSize rejects: %v", want, FormatSize(want), err)
		}
		if got != want {
			t.Errorf("round trip of %d produced %d (via %q)", want, got, FormatSize(want))
		}
	}
}

func TestResourceLimits_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limits  ResourceLimits
		wantErr bool
	}{
		{"all unset", ResourceLimits{}, false},
		{"typical", ResourceLimits{CPUs: 1.5, MemoryBytes: 512 << 20, PidsLimit: 1024}, false},
		{"negative cpus", ResourceLimits{CPUs: -1}, true},
		// NaN passes every ordered comparison, so without an explicit guard
		// it validates AND suppresses the flag: the operator sets a ceiling,
		// the worker starts, and there is no ceiling. That silent no-op is
		// worse than a rejected config.
		{"NaN cpus", ResourceLimits{CPUs: math.NaN()}, true},
		{"positive infinite cpus", ResourceLimits{CPUs: math.Inf(1)}, true},
		{"negative infinite cpus", ResourceLimits{CPUs: math.Inf(-1)}, true},
		{"cpus too small for a cgroup quota", ResourceLimits{CPUs: 0.001}, true},
		{"negative memory", ResourceLimits{MemoryBytes: -1}, true},
		{"negative pids", ResourceLimits{PidsLimit: -1}, true},
		// A ceiling no container can boot under turns every task into an
		// instant kill that looks like an agent bug.
		{"memory too small to start a container", ResourceLimits{MemoryBytes: 1024}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.limits.Validate()
			if tc.wantErr && !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("got %v, want ErrInvalidLimit", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// The default must contain a fork bomb while leaving ample room for a threaded
// runtime. This test exists so lowering it into dangerous territory, or
// raising it to uselessness, is a deliberate decision.
func TestDefaultPidsLimit_IsSaneAndEnabled(t *testing.T) {
	t.Parallel()

	if DefaultPidsLimit <= 0 {
		t.Fatal("the default pid ceiling is disabled: a fork bomb has nothing to stop it")
	}
	if DefaultPidsLimit < 256 {
		t.Errorf("DefaultPidsLimit = %d is low enough to break threaded runtimes", DefaultPidsLimit)
	}
	if DefaultPidsLimit > 8192 {
		t.Errorf("DefaultPidsLimit = %d is too high to contain a fork bomb usefully", DefaultPidsLimit)
	}
}
