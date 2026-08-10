# Run a Worker

A worker exposes one Podman-containerized agent to the mesh. The agent itself can call any model (Ollama, vLLM, llama.cpp, OpenAI API, custom) — the mesh doesn't care.

## Boot a worker

```bash
agentfm -mode worker \
  -agentdir "./my-agent" -image "my-agent:v1" \
  -agent "My Bot" -model "llama3.2" -author "you" \
  -maxtasks 10 -maxcpu 60 -maxgpu 70
```

The agent directory must contain a `Dockerfile` or `Containerfile`. On startup the worker builds the image, then advertises capabilities via GossipSub every 2 s.

**Circuit breakers** auto-reject tasks and flip status to BUSY when any of `-maxtasks`, `-maxcpu`, `-maxgpu` is exceeded. A node serving the public mesh can't be DoS'd into hurting its operator.

## Authoring agents — the three streaming rules

Because AgentFM pipes your container's stdout directly over a libp2p stream, *how* you write to stdout is the single most important UX decision in your agent.

```python
# 1. Always flush
print("Analyzing the CSV file...", flush=True)

# 2. Set PYTHONUNBUFFERED=1 in your Dockerfile

# 3. Trap noisy framework chatter into a StringIO so the Boss sees clean output:
import io, sys
from contextlib import redirect_stdout, redirect_stderr

boss_stream = sys.stdout
trap = io.StringIO()
with redirect_stdout(trap), redirect_stderr(trap):
    result = run_heavy_pipeline()
print(str(result), file=boss_stream, flush=True)
```

Anything you write to `/tmp/output` gets zipped and streamed back to the Boss automatically. No SDK, no decorators, no callbacks.

```dockerfile
RUN mkdir -p /tmp/output && chmod 777 /tmp/output
ENV PYTHONUNBUFFERED=1
```

## Container security caveat

Workers launch containers with `--network host` so the agent can reach a local Ollama at `127.0.0.1:11434`, vLLM, or other loopback services. The flip side: **the agent container has full access to the worker host's network namespace**, including loopback (Ollama, internal admin endpoints, cloud metadata at `169.254.169.254`).

Treat agent images as **trusted code**; review their Dockerfiles before running. The worker prints a startup warning to this effect.

If you need a stricter sandbox, run the worker inside a VM or a hardware-isolated container runtime.

## Local sandbox testing

Test offline before broadcasting to the mesh:

```bash
agentfm -mode test -agentdir "./my-agent" -image "my-agent:v1" \
  -agent "My Bot" -model "llama3.2" \
  -prompt "Write a haiku about compilers."
```

`-mode test` runs the same Podman command and prints the same `--network host` warning, but bypasses libp2p entirely. Useful for validating an agent image on a developer laptop before publishing it to the mesh.

## Capacity tuning

Workers reject incoming task streams when any of these limits trip:

| Limit | Default | What it bounds |
|---|:---:|---|
| `-maxtasks` | `1` | Concurrent task streams (semaphore) |
| `-maxcpu` | `80.0` | Aggregate CPU % across all cores |
| `-maxgpu` | `80.0` | GPU VRAM % (if a CUDA GPU is detected) |

Telemetry broadcasts the current values every 2s so the Boss radar shows real load. The matcher (in OpenAI-routed mode) prefers least-loaded peers within a tier.

## Per-task resource ceilings

The three limits above are **admission thresholds** — they decide whether to accept a task, and constrain nothing once a container is running. The flags below bound the running container itself, through its cgroup:

| Flag | Default | What it bounds |
|---|:---:|---|
| `-task-cpus` | `0` (unlimited) | CPU time in cores, e.g. `1.5`. Maps to `--cpus`. |
| `-task-memory` | empty (unlimited) | Resident memory, e.g. `512m`, `2g`, `1GiB`. Maps to `--memory`, and to `--memory-swap` at the same value so the container cannot swap instead of dying. |
| `-task-pids-limit` | `1024` | Processes **and threads**. Maps to `--pids-limit`. `0` disables. |

CPU and memory default to unlimited on purpose: a ceiling that is wrong for a given agent kills legitimate work, so the operator opts in after observing what their agent actually uses. The pid ceiling defaults **on** because a fork bomb is contained at a level no real agent approaches — a threaded Python or torch agent on a large host runs in the low hundreds of threads. Raise it if your agent needs more; note that threads count, not just processes.

A worker with an unusable ceiling refuses to start rather than failing on its first task, where the failure would look like a broken agent.

Sizes are **integers** with an optional `k`/`m`/`g` suffix (1024-based, as in podman). Fractional values such as `1.5g` are rejected rather than rounded — write `1536m`.

The worker prints the ceilings it will apply at startup, so you can confirm your flags took effect.

When a task is killed for exceeding its memory ceiling, the worker reports it distinctly — `agentfm_tasks_total{status="oom_killed"}` rather than `error` — so a limit set too low is distinguishable from an agent that genuinely crashes. See [Observability](observability.md).

**Caveat on that attribution.** It is inferred from the container's `137` exit code, which is a strong signal but not proof. A task timeout is never mislabelled (context cancellation is distinguishable), but two other cases are counted as `oom_killed`: a kill by the *host's* OOM killer under global memory pressure, and an agent that exits `137` deliberately. Both affect the metric only, and only when a memory ceiling is configured.

```bash
# 2 cores, 4 GiB, default pid ceiling
agentfm -mode worker -agentdir ./my-agent -image my-agent:v1 \
  -task-cpus 2 -task-memory 4g
```

## Related

- [OpenAI-Compatible API](openai.md) — how clients dispatch tasks to your worker
- [CLI Reference](cli.md) — full flag list
- [Architecture](architecture.md) — task-stream wire protocol
- [Security Model](security.md) — Podman + libp2p threat model
