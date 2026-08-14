package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"agentfm/internal/metrics"
	"agentfm/internal/network"
	"agentfm/internal/obs"
	"agentfm/internal/types"
	"agentfm/internal/utils"
	"agentfm/internal/version"

	netcore "github.com/libp2p/go-libp2p/core/network"
	"github.com/pterm/pterm"
)

// isDirEmpty returns true if the directory contains no entries. Conservative
// default: any error from Open or Readdirnames returns false so the caller
// runs the artifact-zip path even on a permission-error / missing-dir case.
// Callers downstream surface a clearer error than this helper could.
func isDirEmpty(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	return err != nil
}

type cancelOnWriteError struct {
	w      io.Writer
	cancel context.CancelFunc
}

func (c *cancelOnWriteError) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err != nil {
		c.cancel()
	}
	return n, err
}

func (w *Worker) handleTaskStream(rootCtx context.Context, s netcore.Stream) {
	// Per-task observability — declared first so the deferred closure runs
	// LAST (Go runs defers in LIFO), capturing the full lifetime including
	// the stream Close/Reset.
	started := time.Now()
	status := metrics.StatusError
	defer func() {
		metrics.TaskDurationSeconds.Observe(time.Since(started).Seconds())
		metrics.TasksTotal.WithLabelValues(status).Inc()
	}()

	// Default to Reset on any early/error exit. A caller that reaches the
	// normal end of the task flips this to a graceful Close.
	reset := true
	defer func() {
		if reset {
			_ = s.Reset()
		} else {
			_ = s.Close()
		}
	}()

	// Short deadline for the incoming task JSON. Extended below once the
	// payload is accepted and we start streaming stdout back. If arming
	// the deadline fails the stream is already unhealthy, so we surface
	// it and let the Reset defer clean up.
	if err := s.SetDeadline(time.Now().Add(network.TaskPayloadReadTimeout)); err != nil {
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonDeadline).Inc()
		slog.Error("arm task stream deadline", slog.Any(obs.FieldErr, err), slog.String(obs.FieldProtocol, "task"))
		return
	}

	fmt.Println()
	pterm.Info.Println("Incoming P2P task tunnel established...")

	bossID := s.Conn().RemotePeer()

	w.mu.Lock()
	if w.currentTasks >= w.config.MaxConcurrentTasks {
		w.mu.Unlock()
		status = metrics.StatusRejected
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonCapacityRejected).Inc()
		pterm.Error.Printfln("Rejected task: Worker is at max capacity (%d/%d).", w.currentTasks, w.config.MaxConcurrentTasks)
		_, _ = s.Write([]byte(fmt.Sprintf("❌ ERROR: Worker is at max capacity (%d/%d). Try another worker.\n", w.currentTasks, w.config.MaxConcurrentTasks)))
		// App-level rejection delivered — close gracefully so the peer sees the message.
		reset = false
		return
	}

	w.currentTasks++
	cpuLoad := w.currentCPU
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.currentTasks--
		w.mu.Unlock()
	}()

	if cpuLoad >= w.config.MaxCPU { // Dynamic Threshold
		status = metrics.StatusRejected
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonCapacityRejected).Inc()
		pterm.Error.Printfln("Rejected task: Worker CPU is overloaded at %.1f%%.", cpuLoad)
		_, _ = s.Write([]byte(fmt.Sprintf("❌ ERROR: Worker is under heavy load (CPU %.1f%%). Try again later.\n", cpuLoad)))
		reset = false
		return
	}

	hasGPU, _, _, gpuPct := getGPUStats()
	if hasGPU && gpuPct > w.config.MaxGPU { // Dynamic Threshold
		status = metrics.StatusRejected
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonCapacityRejected).Inc()
		pterm.Error.Printfln("Rejected task: Worker GPU VRAM is busy (%.1f%% used).", gpuPct)
		_, _ = s.Write([]byte(fmt.Sprintf("❌ ERROR: Worker GPU is busy (%.1f%% VRAM used). Try another worker.\n", gpuPct)))
		reset = false
		return
	}

	var payload types.TaskPayload

	limitedReader := io.LimitReader(s, 1*1024*1024)

	if err := json.NewDecoder(limitedReader).Decode(&payload); err != nil {
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonDecode).Inc()
		slog.Error("decode task payload (or >1MiB)", slog.Any(obs.FieldErr, err), slog.String(obs.FieldProtocol, "task"))
		return
	}

	if payload.Version != version.AppVersion {
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonVersionMismatch).Inc()
		_, _ = s.Write([]byte(fmt.Sprintf("❌ ERROR: Version mismatch! Worker is running v%s.\n", version.AppVersion)))
		reset = false
		return
	}

	// Payload accepted. Extend the deadline to cover long running stdout
	// streaming. If the extension fails, abort now so we don't run a
	// Podman container whose output channel is already doomed.
	if err := s.SetDeadline(time.Now().Add(network.TaskExecutionTimeout)); err != nil {
		metrics.StreamErrorsTotal.WithLabelValues(metrics.ProtocolTask, metrics.ReasonDeadline).Inc()
		slog.Error("extend task stream deadline", slog.Any(obs.FieldErr, err), slog.String(obs.FieldTaskID, payload.TaskID), slog.String(obs.FieldProtocol, "task"))
		return
	}

	// Task-scoped ctx: cancels on worker shutdown (rootCtx), on
	// TaskExecutionTimeout, and on remote conn death (watcher below).
	// Passed to executePodman so exec.CommandContext SIGKILLs the
	// container the instant the tunnel dies.
	taskCtx, cancelTask := context.WithTimeout(rootCtx, network.TaskExecutionTimeout)
	defer cancelTask()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				if s.Conn().IsClosed() {
					pterm.Warning.Println("Remote Boss connection died; cancelling sandbox...")
					cancelTask()
					return
				}
			}
		}
	}()

	pterm.Info.Printfln("Executing task %s in Podman sandbox...", payload.TaskID)

	streamSink := &cancelOnWriteError{w: s, cancel: cancelTask}
	outputDir, execErr := w.executePodman(taskCtx, payload.Data, streamSink, streamSink)
	defer os.RemoveAll(outputDir)

	if execErr != nil {
		// The human-readable reason is already on the Boss's stream (both
		// sinks are this stream). What matters here is the terminal status:
		// a worker that reports OK for work it never performed is the
		// "billed non-execution" failure mode, and it is reachable by
		// accident — podman missing from PATH, container service stopped,
		// disk full.
		status = metrics.StatusError
		switch {
		case errors.Is(execErr, ErrSandboxOOMKilled):
			// Checked before the deadline test: a task killed by the memory
			// cgroup may well also have been near its time budget, and the
			// limit is the actionable cause.
			status = metrics.StatusOOMKilled
		case errors.Is(taskCtx.Err(), context.DeadlineExceeded):
			// Distinguish "ran out of time" from "broke". This is the first
			// worker-side emitter of StatusTimeout; until now the label was
			// pre-warmed but only ever set by the Boss.
			status = metrics.StatusTimeout
		}
		slog.Error("sandbox execution",
			slog.Any(obs.FieldErr, execErr),
			slog.String(obs.FieldTaskID, payload.TaskID),
			slog.String(obs.FieldProtocol, "task"))

		if errors.Is(execErr, ErrSandboxNotRun) {
			// No container ever started, so no artifact can exist: skip the
			// collection path entirely. Close gracefully rather than reset so
			// the Boss reads the failure text instead of a bare stream reset.
			writeTaskFailed(s, "not_run")
			reset = false
			return
		}
		// ErrSandboxFailed: the container ran and ended badly, but may have
		// written partial output. Fall through and return what exists — while
		// keeping the failure status set above. The failure marker is written
		// AFTER the artifact markers so it stays genuinely terminal; emitting
		// it first would let a Boss that treats it as end-of-task drop the
		// very partial output this branch exists to preserve.
	}

	if !isDirEmpty(outputDir) {
		_, _ = s.Write([]byte("\n[AGENTFM: FILES_INCOMING]\n"))
		pterm.Info.Println("Artifacts detected. Preparing zip...")

		zipPath := outputDir + "_payload.zip"
		defer os.Remove(zipPath)

		if err := utils.ZipDirectory(outputDir, zipPath); err == nil {
			pterm.Info.Println("Routing artifacts to Boss over secure channel...")
			artifactCtx, cancelArtifact := context.WithTimeout(rootCtx, network.ArtifactStreamTimeout)
			if sendErr := network.SendArtifacts(artifactCtx, w.node.Host, bossID, zipPath, payload.TaskID); sendErr != nil {
				slog.Error("route artifacts", slog.Any(obs.FieldErr, sendErr), slog.String(obs.FieldTaskID, payload.TaskID), slog.String(obs.FieldProtocol, "artifacts"))
			}
			cancelArtifact()
		} else {
			slog.Error("zip artifacts", slog.Any(obs.FieldErr, err), slog.String(obs.FieldTaskID, payload.TaskID))
		}
	} else {
		_, _ = s.Write([]byte("\n[AGENTFM: NO_FILES]\n"))
		if execErr == nil {
			pterm.Success.Println("No artifacts generated. Task complete.")
		} else {
			pterm.Warning.Println("No artifacts generated; sandbox terminated abnormally.")
		}
	}

	// Terminal failure marker, emitted last so a Boss may treat it as
	// end-of-task without losing anything written before it.
	if execErr != nil {
		writeTaskFailed(s, "abnormal_exit")
	}

	reset = false
	// Only a clean run is a success. execErr is non-nil here exactly when the
	// container ran and ended abnormally (the never-ran case returned above),
	// and the status set in that branch must survive.
	if execErr == nil {
		status = metrics.StatusOK
	}
}

// writeTaskFailed emits the machine-readable terminal marker for a failed
// task. Without it the failure reaches the Boss only as human prose, so the
// Boss records the task as completed and fires a "completed" webhook.
//
// Safe to emit without a protocol version bump: every consumer filters on the
// "[AGENTFM:" prefix rather than an enumerated list (boss/openai.go
// isSentinelLine, agentfm-python streaming.py _classify, the desktop
// useDispatch hook), so an older Boss or SDK drops the line instead of
// surfacing it as task output. Boss-side interpretation is a follow-up — see
// R2 in docs/security/HARDENING-ROADMAP.md.
func writeTaskFailed(w io.Writer, reason string) {
	_, _ = w.Write([]byte("\n[AGENTFM: TASK_FAILED " + reason + "]\n"))
}
