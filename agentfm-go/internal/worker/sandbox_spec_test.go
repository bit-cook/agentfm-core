package worker

import (
	"errors"
	"slices"
	"testing"
)

// The two Golden tests below pin the EXACT argv handed to `podman run`.
//
// If you are reading this because one of them just failed: that is the point.
// Any change to the sandbox's argv changes the isolation posture of every task
// this worker executes, so it must be an explicit, reviewed edit of the
// expected slice — never an incidental diff absorbed by a loose assertion.
//
// They currently encode an UNHARDENED sandbox: host networking, no dropped
// capabilities, a writable root filesystem, no cgroup limits, a blanket
// --env-file pass-through, and the task prompt in argv. That is the state this
// refactor preserves on purpose. Roadmap lots 2 and 3 change it, and these
// tests are the tripwire that forces each change to be seen.

func assertArgs(t *testing.T, got, want []string) {
	t.Helper()
	if slices.Equal(got, want) {
		return
	}
	t.Errorf("podman argv mismatch\n got: %q\nwant: %q", got, want)
	// Point at the first divergence — with ~12 arguments a bare dump of both
	// slices is hard to read in a failure report.
	for i := 0; i < max(len(got), len(want)); i++ {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if g != w {
			t.Errorf("first difference at index %d: got %q, want %q", i, g, w)
			return
		}
	}
}

// All four combinations of the two optional inputs are pinned. The two crossed
// cases matter as much as the extremes: --device and --env-file are emitted at
// different points in the vector, so a future edit that reorders them would
// slip past a matrix that only covered "neither" and "both".
func TestBuildRunArgs_Golden(t *testing.T) {
	base := SandboxSpec{
		ContainerName: "agentfm-sandbox-deadbeef",
		Image:         "agentfm-test:v1",
		Prompt:        "draft a sick-leave email",
		OutputDir:     "/tmp/agentfm/run_deadbeef",
		ModelName:     "llama3.2",
		Network:       NetworkHost,
	}

	tests := []struct {
		name   string
		mutate func(*SandboxSpec)
		want   []string
	}{
		{
			name:   "no gpu, no env file",
			mutate: func(*SandboxSpec) {},
			want: []string{
				"run", "--rm", "--name", "agentfm-sandbox-deadbeef",
				"--network", "host",
				"-v", "/tmp/agentfm/run_deadbeef:/tmp/output:z",
				"-e", "AGENTFM_MODEL=llama3.2",
				"agentfm-test:v1", "draft a sick-leave email",
			},
		},
		{
			name:   "gpu, no env file",
			mutate: func(s *SandboxSpec) { s.GPUDevice = defaultGPUDevice },
			want: []string{
				"run", "--rm", "--name", "agentfm-sandbox-deadbeef",
				"--network", "host",
				"--device", "nvidia.com/gpu=all",
				"-v", "/tmp/agentfm/run_deadbeef:/tmp/output:z",
				"-e", "AGENTFM_MODEL=llama3.2",
				"agentfm-test:v1", "draft a sick-leave email",
			},
		},
		{
			name:   "env file, no gpu",
			mutate: func(s *SandboxSpec) { s.EnvFilePath = "/agents/demo/.env" },
			want: []string{
				"run", "--rm", "--name", "agentfm-sandbox-deadbeef",
				"--network", "host",
				"-v", "/tmp/agentfm/run_deadbeef:/tmp/output:z",
				"--env-file", "/agents/demo/.env",
				"-e", "AGENTFM_MODEL=llama3.2",
				"agentfm-test:v1", "draft a sick-leave email",
			},
		},
		{
			name: "gpu and env file",
			mutate: func(s *SandboxSpec) {
				s.GPUDevice = defaultGPUDevice
				s.EnvFilePath = "/agents/demo/.env"
			},
			want: []string{
				"run", "--rm", "--name", "agentfm-sandbox-deadbeef",
				"--network", "host",
				"--device", "nvidia.com/gpu=all",
				"-v", "/tmp/agentfm/run_deadbeef:/tmp/output:z",
				"--env-file", "/agents/demo/.env",
				"-e", "AGENTFM_MODEL=llama3.2",
				"agentfm-test:v1", "draft a sick-leave email",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := base
			tc.mutate(&spec)

			got, err := BuildRunArgs(spec)
			if err != nil {
				t.Fatalf("BuildRunArgs returned an unexpected error: %v", err)
			}
			assertArgs(t, got, tc.want)
		})
	}
}

// Optional inputs must be omitted entirely rather than emitted empty: a bare
// `--device` or `--env-file` with no value would shift every following
// argument and make podman consume the next flag as its operand.
func TestBuildRunArgs_OmitsOptionalFlagsWhenUnset(t *testing.T) {
	got, err := BuildRunArgs(SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		OutputDir:     "/out",
		Network:       NetworkHost,
	})
	if err != nil {
		t.Fatalf("BuildRunArgs returned an unexpected error: %v", err)
	}
	for _, flag := range []string{"--device", "--env-file"} {
		if slices.Contains(got, flag) {
			t.Errorf("%s emitted although the corresponding field is unset: %q", flag, got)
		}
	}
}

// Deny-by-default on the network mode. An unrecognised value — including the
// zero value, which is what a caller that forgets the field produces — must be
// an error and never fall through to a permissive namespace.
func TestBuildRunArgs_RejectsUnknownNetworkMode(t *testing.T) {
	for _, mode := range []NetworkMode{"", "bridge", "none", "HOST"} {
		t.Run(string(mode), func(t *testing.T) {
			_, err := BuildRunArgs(SandboxSpec{
				ContainerName: "c",
				Image:         "img",
				OutputDir:     "/out",
				Network:       mode,
			})
			if !errors.Is(err, ErrUnsupportedNetwork) {
				t.Fatalf("network mode %q: got error %v, want ErrUnsupportedNetwork", mode, err)
			}
		})
	}
}

func TestBuildRunArgs_RejectsIncompleteSpec(t *testing.T) {
	base := SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		OutputDir:     "/out",
		Network:       NetworkHost,
	}

	tests := []struct {
		name    string
		mutate  func(*SandboxSpec)
		wantErr error
	}{
		{"no container name", func(s *SandboxSpec) { s.ContainerName = "" }, ErrNoContainerName},
		{"no image", func(s *SandboxSpec) { s.Image = "" }, ErrNoImage},
		{"no output dir", func(s *SandboxSpec) { s.OutputDir = "" }, ErrNoOutputDir},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := base
			tc.mutate(&spec)

			args, err := BuildRunArgs(spec)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			if args != nil {
				t.Errorf("args must be nil on error, got %q", args)
			}
		})
	}
}

// The reference agents read their task from argv[1]
// (agent-example/*/run.py: sys.argv[1]), so the prompt must stay the final
// element. Lot 3 moves it to stdin — a breaking change for existing agent
// images, which is precisely why it needs its own deliberate commit.
func TestBuildRunArgs_PromptIsFinalArgument(t *testing.T) {
	const prompt = "summarise this document"

	got, err := BuildRunArgs(SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		Prompt:        prompt,
		OutputDir:     "/out",
		Network:       NetworkHost,
	})
	if err != nil {
		t.Fatalf("BuildRunArgs returned an unexpected error: %v", err)
	}
	if len(got) < 2 {
		t.Fatalf("argv too short: %q", got)
	}
	if got[len(got)-1] != prompt {
		t.Errorf("last argument = %q, want the prompt %q", got[len(got)-1], prompt)
	}
	if got[len(got)-2] != "img" {
		t.Errorf("second-to-last argument = %q, want the image reference", got[len(got)-2])
	}
}

// A prompt is attacker-controlled input: it arrives from the Boss. It must be
// carried as a single argv element regardless of the shell metacharacters,
// spaces or newlines it contains. exec.Command passes argv directly without a
// shell, so this holds — the test guards against anyone later introducing
// `sh -c` or string concatenation.
func TestBuildRunArgs_HostilePromptStaysOneArgument(t *testing.T) {
	const hostile = "; rm -rf / # $(id) `whoami`\nsecond line --network host"

	got, err := BuildRunArgs(SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		Prompt:        hostile,
		OutputDir:     "/out",
		Network:       NetworkHost,
	})
	if err != nil {
		t.Fatalf("BuildRunArgs returned an unexpected error: %v", err)
	}

	var seen int
	for _, a := range got {
		if a == hostile {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("prompt should appear exactly once as a whole argument, found %d times in %q", seen, got)
	}

	// Detect splitting or interpolation differentially rather than by
	// substring search: build the same spec with a harmless prompt and require
	// every argument except the last to be identical. A substring heuristic
	// cannot work here — the hostile prompt deliberately quotes "--network
	// host", so any argv element it happens to contain would trip a false
	// positive on correct code.
	benign, err := BuildRunArgs(SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		Prompt:        "BENIGN",
		OutputDir:     "/out",
		Network:       NetworkHost,
	})
	if err != nil {
		t.Fatalf("BuildRunArgs (benign) returned an unexpected error: %v", err)
	}
	if len(got) != len(benign) {
		t.Fatalf("prompt changed the argv length: %d vs %d (%q)", len(got), len(benign), got)
	}
	for i := range got[:len(got)-1] {
		if got[i] != benign[i] {
			t.Errorf("argv[%d] differs with a hostile prompt: %q vs %q", i, got[i], benign[i])
		}
	}
}

// BuildRunArgs is pure: identical input yields identical output, and the input
// struct is left untouched. Later lots derive specs from a shared policy
// object, so aliasing bugs here would silently leak one task's settings into
// another's.
func TestBuildRunArgs_IsPure(t *testing.T) {
	spec := SandboxSpec{
		ContainerName: "c",
		Image:         "img",
		Prompt:        "p",
		OutputDir:     "/out",
		ModelName:     "m",
		EnvFilePath:   "/e",
		GPUDevice:     defaultGPUDevice,
		Network:       NetworkHost,
	}
	before := spec

	first, err := BuildRunArgs(spec)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := BuildRunArgs(spec)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if !slices.Equal(first, second) {
		t.Errorf("not deterministic:\nfirst:  %q\nsecond: %q", first, second)
	}
	if spec != before {
		t.Errorf("spec was mutated: got %+v, want %+v", spec, before)
	}

	// Mutating the returned slice must not affect a later call.
	first[0] = "MUTATED"
	third, err := BuildRunArgs(spec)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if third[0] != "run" {
		t.Errorf("returned slice aliases shared state: third call starts with %q", third[0])
	}
}
