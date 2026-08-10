# Conception — durcissement du worker et confiance bidirectionnelle

**Date** : 2026-08-10 · **Statut** : proposition, non implémentée
**Prérequis** : `docs/security/AUDIT-2026-08-10.md` · **Roadmap** : S2-S4, R1-R3, T1-T3, O1-O2, L2

Ce document répond à quatre questions : où intercepter la tâche, comment étendre le flux, quelles structures de données, quels fichiers créer.

---

## 0. Trois conclusions qui précèdent la conception

### 0.1 — La réputation bidirectionnelle ne demande aucun changement de protocole

C'est la découverte principale. Le ledger est déjà **symétrique et agnostique du rôle** :

- `pb.Rating` porte `RaterPeerId` / `SubjectPeerId` — rien n'impose que le rater soit un boss (`internal/ledger/pb/ledger.pb.go:49-51`).
- `Dimension` est une **chaîne libre** (`:55`). Les valeurs employées aujourd'hui sont `honesty` et `reliability`. En ajouter d'autres ne casse rien.
- `ledger.New(path, key, pubsub)` (`internal/ledger/ledger.go:159`) n'a aucune dépendance au paquet `boss`.
- Le worker détient déjà tout le nécessaire : `w.node.Host` et `w.node.PubSub` (`internal/network/p2p.go:33-35`).

**Conséquence** : T1 se réduit à instancier un ledger côté worker et à y écrire des `Rating` signées. Les entrées se répliquent d'elles-mêmes sur `agentfm-feedback-v1`, transitent par `/agentfm/ledger-fetch/`, `/agentfm/inbox-fetch/` et sont co-signées par les witnesses **sans une ligne de code protocolaire**. Aucun *release-gating event*.

### 0.2 — Il n'existe aucune couche de paiement : la dimension `payment` n'est pas évidençable

Aucun code de paiement, facturation, crédit, escrow ou wallet n'existe dans le dépôt. Une dimension `payment` produirait des notes que rien ne peut corroborer — donc un vecteur de diffamation gratuit entre pairs, et un affaiblissement du graphe EigenTrust.

**Règle retenue** : *on ne note que ce que le worker observe lui-même.* Les dimensions livrables en v1 sont donc comportementales, pas financières (§3.2).

### 0.3 — Le « gage » se heurte à une contrainte projet déclarée

`CHANGELOG.md:80` liste en *hard project constraint* : « No blockchain, tokens, staking, or on-chain governance ». L2 y contrevient frontalement. Deux options :

1. **Caution en réputation** (recommandée v1) — le boss engage son capital de réputation, déjà la ressource rare du système. Aucun token, aucune chaîne, pas de conflit avec la contrainte.
2. **Caution monétaire** — nécessite un ADR renversant explicitement `CHANGELOG.md:80`, plus une couche de paiement inexistante.

La conception ci-dessous implémente (1) derrière une interface `StakeProvider` pour que (2) soit ajoutable plus tard sans toucher au protocole.

---

## 1. Points d'interception dans `internal/worker/`

### 1.1 — Le flux actuel, ligne à ligne

`handleTaskStream` (`internal/worker/handler.go:50-208`) :

| Ligne | Étape | Extension possible |
|---|---|---|
| `:54-59` | defer métriques (durée, statut) | **O1** — remplacer par un enregistrement forensic complet |
| `:76` | `SetDeadline(TaskPayloadReadTimeout)` | — |
| `:85` | `bossID := s.Conn().RemotePeer()` | **★ Hook A** — T2 denylist, T1 plancher de réputation, T3 PoW |
| `:87-97` | capacité (`MaxConcurrentTasks`) | à absorber dans la chaîne d'admission |
| `:109-126` | seuils CPU / GPU | idem |
| `:130` | `io.LimitReader(s, 1 MiB)` + decode | — |
| `:138` | contrôle de version | — |
| `:148-158` | deadline étendue + `taskCtx` | **R2** — timeout par tâche configurable |
| `:181` | `w.executePodman(taskCtx, payload.Data, …)` | **★ Hook B** — R1 limites, O2 dry-run |
| `:182` | `defer os.RemoveAll(outputDir)` | **C1** — effacement sécurisé |
| `:184-197` | zip + `SendArtifacts` | **O1** — hash des sorties |

`bossID` est capturé en `:85` et **n'est jamais utilisé pour une décision d'admission** — uniquement plus tard pour `SendArtifacts` (`:194`). C'est le point d'accroche naturel, déjà présent.

### 1.2 — Hook A : une chaîne d'admission en deux phases

Deux phases, parce que certains contrôles ne nécessitent que l'identité du boss (bon marché, avant toute lecture) et d'autres le payload.

```go
// internal/worker/admission.go
package worker

// Phase distingue les contrôles applicables avant lecture du payload
// (identité seule) de ceux qui exigent la tâche décodée.
type Phase int

const (
	PhasePrePayload Phase = iota
	PhasePostPayload
)

type AdmissionRequest struct {
	Phase    Phase
	BossID   peer.ID
	Payload  *types.TaskPayload // nil en PhasePrePayload
	InFlight int
	CPUPct   float64
	GPUPct   float64
	Now      time.Time
}

// Decision est volontairement sans erreur : un refus n'est pas une panne.
// Code est un identifiant machine stable, jamais un message libre.
type Decision struct {
	Allow      bool
	Code       string        // "boss_denylisted", "reputation_below_floor", …
	RetryAfter time.Duration // 0 = refus définitif
}

// Check est un maillon. Deny-by-default : un Check qui panique ou
// retourne une Decision zéro refuse la tâche.
type Check interface {
	Name() string
	Evaluate(ctx context.Context, req AdmissionRequest) Decision
}

type Chain struct {
	checks []Check
	mode   EnforceMode // ModeWarn | ModeEnforce, par contrôle
}
```

Insertion en `handler.go:85`, en remplacement des blocs `:87-126` :

```go
bossID := s.Conn().RemotePeer()

w.mu.RLock()
snap := AdmissionRequest{
	Phase: PhasePrePayload, BossID: bossID,
	InFlight: w.currentTasks, CPUPct: w.currentCPU, Now: time.Now(),
}
w.mu.RUnlock()

if d := w.admission.Evaluate(ctx, snap); !d.Allow {
	status = metrics.StatusRejected
	metrics.AdmissionRefusals.WithLabelValues(d.Code).Inc()
	w.writeRefusal(s, d)   // sentinelle structurée, cf. §1.4
	reset = false          // fermeture propre : le pair doit voir le motif
	return
}
```

Note : `w.mu` devient un `sync.RWMutex` (aujourd'hui `sync.Mutex`, `worker.go:46`) — les lectures d'admission sont fréquentes et sans effet de bord.

**Ordre des maillons** (du moins cher au plus cher, pour que le DoS coûte le moins possible) :

```
capacity → resources → denylist(T2) → reputation(T1) → pow(T3) → stake(L2)
```

### 1.3 — Hook B : rendre l'exécution testable — le prérequis de tout le reste

`executePodman` (`sandbox.go:52-112`) construit ses arguments en ligne puis exécute. **Aucun test ne le couvre** : `sandbox_test.go` ne teste que `buildSandboxImage`. Tant que c'est le cas, retirer un durcissement ne casse aucun test — l'invariant « un test qui échoue si le contrôle est retiré » est inatteignable.

La refonte minimale : extraire une fonction **pure**.

```go
// internal/worker/sandbox_spec.go
package worker

type NetworkMode string

const (
	NetworkNone      NetworkMode = "none"  // défaut cible
	NetworkAllowlist NetworkMode = "allowlist"
	NetworkHost      NetworkMode = "host"  // hérité, exige un opt-in explicite
)

type ResourceLimits struct {
	CPUs        float64       // → --cpus
	MemoryBytes int64         // → --memory ; --memory-swap identique (swap coupé)
	PidsLimit   int64         // → --pids-limit
	VRAMBytes   int64         // → quota GPU
	BlkioWeight uint16        // → --blkio-weight
	Timeout     time.Duration // horloge murale de la tâche
}

type SandboxSpec struct {
	Image     string
	OutputDir string
	Network   NetworkMode
	Limits    ResourceLimits
	EnvAllow  []string // allowlist de clés, jamais --env-file entier
	GPUs      string   // "" = aucun GPU exposé
	ReadOnly  bool
	DryRun    bool
}

// BuildRunArgs est PURE : pas d'I/O, pas d'horloge, pas d'environnement.
// C'est la seule fonction que les tests de politique ont besoin d'appeler.
func BuildRunArgs(spec SandboxSpec, containerName string) ([]string, error) {
	if spec.Image == "" {
		return nil, fmt.Errorf("sandbox spec: %w", ErrNoImage)
	}
	args := []string{"run", "--rm", "--name", containerName}

	// Deny-by-default : chaque capability rendue doit être nommée ailleurs.
	args = append(args,
		"--cap-drop=ALL",
		"--security-opt", "no-new-privileges",
		"--userns=auto",
	)
	switch spec.Network {
	case NetworkNone:
		args = append(args, "--network", "none")
	case NetworkHost:
		args = append(args, "--network", "host")
	default:
		return nil, fmt.Errorf("network %q: %w", spec.Network, ErrUnsupportedNetwork)
	}
	if spec.ReadOnly {
		args = append(args, "--read-only",
			"--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m",
			"--tmpfs", "/run:rw,noexec,nosuid,nodev,size=16m")
	}
	if l := spec.Limits; true {
		if l.CPUs > 0 {
			args = append(args, "--cpus", strconv.FormatFloat(l.CPUs, 'f', 2, 64))
		}
		if l.MemoryBytes > 0 {
			m := strconv.FormatInt(l.MemoryBytes, 10)
			args = append(args, "--memory", m, "--memory-swap", m) // swap coupé
		}
		if l.PidsLimit > 0 {
			args = append(args, "--pids-limit", strconv.FormatInt(l.PidsLimit, 10))
		}
	}
	// Le prompt ne passe PLUS en argv (fuite via /proc/<pid>/cmdline) :
	// il est écrit sur stdin par l'appelant.
	args = append(args, "-v", spec.OutputDir+":/tmp/output:z,rw")
	for _, k := range spec.EnvAllow {
		if v, ok := os.LookupEnv(k); ok {
			args = append(args, "-e", k+"="+v)
		}
	}
	if spec.GPUs != "" {
		args = append(args, "--device", spec.GPUs)
	}
	return append(args, spec.Image), nil
}
```

`executePodman` devient une coquille : `BuildRunArgs` → `exec.CommandContext` → `cmd.Stdin = strings.NewReader(prompt)`.

**Le prompt sur stdin est un changement de contrat pour les images d'agent.** `agent-example/*/run.py:20` lit `sys.argv[1]`. Migration : le worker écrit le prompt sur stdin **et** conserve argv pendant une version, avec un avertissement ; l'agent d'exemple lit `sys.stdin.read()` en priorité et retombe sur `argv[1]`.

### 1.4 — Refus structuré sans casser le protocole

Aujourd'hui le refus est du texte libre (`handler.go:92`), qui divulgue en prime l'état interne (`CPU 87.3%`, `2/4`) à tout pair non authentifié.

Le dépôt possède déjà une convention de sentinelle sur le flux stdout : `[AGENTFM: FILES_INCOMING]` / `[AGENTFM: NO_FILES]` (`architecture.md:110`). On la réutilise — **donc aucun bump de version** :

```go
func (w *Worker) writeRefusal(s io.Writer, d Decision) {
	// Une ligne JSON derrière une sentinelle : les bosses récents la
	// parsent, les anciens l'affichent comme du texte. Rétrocompatible.
	b, _ := json.Marshal(struct {
		Code       string `json:"code"`
		RetryAfter int    `json:"retry_after_s,omitempty"`
	}{d.Code, int(d.RetryAfter.Seconds())})
	fmt.Fprintf(s, "[AGENTFM: REFUSED] %s\n", b)
}
```

Le motif est un **code**, jamais un état interne chiffré.

### 1.5 — Dry-run (O2) : un préréglage, pas un second chemin de code

Erreur classique à éviter : un dry-run implémenté à part dérive de l'exécution réelle et devient du théâtre. Ici, dry-run = **le même `SandboxSpec`, contraint** :

```go
func DryRunSpec(base SandboxSpec, p Policy) SandboxSpec {
	base.DryRun = true
	base.Network = NetworkNone            // toujours, non négociable
	base.ReadOnly = true
	base.GPUs = ""                        // aucun GPU en simulation
	base.EnvAllow = nil                   // aucun secret
	base.Limits.Timeout = p.DryRun.Timeout // typiquement 15-30s
	base.Limits.CPUs = min(base.Limits.CPUs, p.DryRun.MaxCPUs)
	return base
}
```

Le rapport comportemental exploite ce que la sandbox durcie fournit déjà : code de sortie, pics CPU/RSS, octets écrits, et — une fois S2 livré — le compteur de syscalls refusés par seccomp. **O2 n'a aucune valeur avant S2/S4** : sans profil seccomp ni isolation réseau, il n'y a rien à observer.

---

## 2. Points d'extension du flux de tâche

### 2.1 — Extension de `TaskPayload` sans bump de version

`types.TaskPayload` (`internal/types/types.go:46-51`) n'a que 4 champs. `encoding/json` **ignore les champs inconnus par défaut** : ajouter des champs `omitempty` est rétrocompatible dans les deux sens. Le dépôt a déjà ce précédent (`CHANGELOG.md:56`).

```go
type TaskPayload struct {
	Version string `json:"version"`
	Task    string `json:"task"`
	Data    string `json:"data"`
	TaskID  string `json:"task_id"`

	// --- extensions rétrocompatibles ---

	// PoW résolvant le challenge d'admission (T3).
	PoW *PoWProof `json:"pow,omitempty"`

	// Référence à un engagement de caution publié au ledger (L2).
	StakeRef string `json:"stake_ref,omitempty"`

	// Limites demandées par le boss. INVARIANT : ne peuvent que
	// RESSERRER la politique du worker, jamais l'assouplir.
	Limits *TaskLimits `json:"limits,omitempty"`

	// Le boss demande une simulation plutôt qu'une exécution réelle.
	DryRun bool `json:"dry_run,omitempty"`
}
```

L'invariant sur `Limits` est central et doit être codé, pas documenté :

```go
func (p Policy) Clamp(req *TaskLimits) ResourceLimits {
	out := p.Limits // plafond du worker
	if req == nil { return out }
	out.CPUs = math.Min(out.CPUs, req.CPUs)
	out.MemoryBytes = min64(out.MemoryBytes, req.MemoryBytes)
	out.Timeout = minDur(out.Timeout, req.Timeout)
	return out // jamais au-dessus de la politique locale
}
```

### 2.2 — Vérification de la réputation du boss avant acceptation

Le worker instancie son propre moteur, en miroir exact du boss (`cmd/agentfm/bossbootstrap.go`) :

```go
// internal/worker/bossgate.go
type ReputationCheck struct {
	engine *reputation.Engine
	ledger ledger.Ledger
	floor  float64
}

func (c *ReputationCheck) Evaluate(ctx context.Context, req AdmissionRequest) Decision {
	// L'équivocation est un refus absolu, indépendant du plancher —
	// même sémantique que le trust gate du boss (boss/trust_gate.go:30-46).
	if eq, err := c.ledger.IsEquivocator(ctx, []byte(req.BossID)); err == nil && eq {
		return Decision{Code: "boss_is_equivocator"}
	}
	score := c.engine.Score(req.BossID.String())
	if score < c.floor {
		return Decision{Code: "reputation_below_floor", RetryAfter: time.Hour}
	}
	return Decision{Allow: true}
}
```

**Piège à ne pas reproduire** : un boss inconnu a un score de 0. Si le plancher est à 0, tout nouveau boss est banni et le mesh se ferme. Le plancher worker doit être **strictement négatif** (défaut `-0.5`, comme `--reputation-floor` côté boss) et distinguer *inconnu* de *mauvais*.

### 2.3 — La caution, avant `podman run`

Interface conçue pour que la v1 non monétaire et une v2 monétaire coexistent :

```go
// internal/worker/stake.go
type StakeProvider interface {
	// Verify établit qu'un engagement existe, couvre cette tâche, et
	// n'a pas déjà été consommé. Purement local et hors-ligne en v1.
	Verify(ctx context.Context, bossID peer.ID, ref string, need Amount) error
	// Release libère après validation de la tâche.
	Release(ctx context.Context, ref string) error
	// Slash consomme la caution et publie la preuve.
	Slash(ctx context.Context, ref string, reason string, evidence []byte) error
}
```

**Implémentation v1 — caution en réputation.** Avant dispatch, le boss publie dans son propre ledger une entrée `Rating{Subject: self, Dimension: "stake_commitment", Context: task_id}`. Elle est signée, horodatée, publiquement visible et chaînée. Le worker la vérifie via `ledger.VerifyEntry`. En cas d'abus prouvé, le worker publie `Rating{Subject: boss, Dimension: "stake_slashed", Score: -1.0, Context: task_id}` en citant l'engagement.

Propriétés : aucun token, aucune chaîne, aucun règlement — le coût est réputationnel et déjà mesuré par EigenTrust. Faiblesse à assumer par écrit : **c'est une caution sans dépôt réel**. Un boss jetable qui n'a pas de réputation à perdre n'est pas dissuadé — c'est précisément le rôle de T3 (PoW), qui impose un coût *avant* toute identité. Les deux sont complémentaires : T3 couvre le boss anonyme, L2 le boss récurrent.

---

## 3. Structures de données

### 3.1 — Ledger local du worker

Aucun schéma nouveau. Le worker ouvre son ledger comme le boss :

```go
// dans Worker.Start, après la résolution du digest
priv := w.node.Host.Peerstore().PrivKey(w.node.Host.ID())
lg, err := ledger.NewWithOptions(
	filepath.Join(home, ".agentfm", "worker_ledger.db"),
	priv, w.node.PubSub,
	ledger.Options{Host: w.node.Host}, // sert ledger-fetch / inbox-fetch
)
if err != nil {
	panic(fmt.Errorf("open worker ledger: %w", err)) // erreur de démarrage
}
```

Réplication, preuves d'inclusion, co-signature par les witnesses et rattrapage : **acquis sans code supplémentaire**.

### 3.2 — Dimensions worker → boss

Une seule règle : *on ne note que ce que l'on observe soi-même.*

| Dimension | Déclencheur observable | Score |
|---|---|---|
| `load_conformance` | écart entre `Limits` annoncées et consommation réelle | −0.1 … +0.1 |
| `protocol_behavior` | payload malformé, version incompatible, abandon en cours de flux | −0.2 … +0.05 |
| `abuse` | limite atteinte, syscall refusé en rafale, tentative de sortie réseau bloquée | −0.5 … 0 |
| `stake_slashed` | caution consommée après abus prouvé | −1.0 |

`payment` est **délibérément absent** : rien ne permet de l'évidencer (§0.2).

Plafond anti-Sybil obligatoire, calqué sur `boss/completion_rating.go:34` (`HourlyCap ±0.5`) : sans plafond **par contrepartie**, un worker peut enterrer un boss avec un flot de notes négatives.

### 3.3 — Trust badge

Aucune structure nouvelle : `reputation.Engine.Score(peerID) float64` existe (`internal/reputation/eigentrust.go:298`) et produit déjà le `+0.67 · Allowed` du dashboard. Côté worker, on l'affiche à l'admission :

```go
type TrustBadge struct {
	Score       float64 `json:"score"`        // -1.0 … +1.0
	Label       string  `json:"label"`        // "allowed" | "unknown" | "refused"
	Equivocator bool    `json:"equivocator"`
	Samples     int     `json:"samples"`      // distinguer 0.0-inconnu de 0.0-mesuré
}
```

`Samples` est la nuance qui manque aujourd'hui : sans lui, un pair neuf et un pair médiocre sont indiscernables.

### 3.4 — Enregistrement forensic (O1)

```go
type TaskRecord struct {
	TaskID     string        `json:"task_id"`
	BossID     string        `json:"boss_id"`
	ImageDigest string       `json:"image_digest"`
	PromptSHA256 string      `json:"prompt_sha256"` // JAMAIS le prompt
	OutputSHA256 string      `json:"output_sha256"`
	Started    time.Time     `json:"started"`
	Duration   time.Duration `json:"duration_ns"`
	ExitCode   int           `json:"exit_code"`
	Terminal   string        `json:"terminal"` // ok|error|timeout|oom|killed|refused
	PeakRSS    int64         `json:"peak_rss_bytes"`
	CPUSeconds float64       `json:"cpu_seconds"`
	DeniedSyscalls int       `json:"denied_syscalls"`
	DryRun     bool          `json:"dry_run"`
}
```

Invariant : **hachage, jamais contenu**. Cette structure satisfait l'invariant « ne jamais logger de données de tâche » tout en rendant tout litige arbitrable.

---

## 4. Fichiers à créer et à modifier

### À créer

| Fichier | Rôle | Pourquoi cohérent |
|---|---|---|
| `internal/worker/policy.go` | Charge et valide `~/.agentfm/policy.yaml`, rechargement à chaud | Surface de configuration unique pour T2/R1/R2/L2/O3 ; satisfait « jamais codé en dur » |
| `internal/worker/admission.go` | `Check`, `Chain`, `Decision` | Rend l'admission ordonnée et testable ; absorbe les contrôles épars de `handler.go:87-126` |
| `internal/worker/sandbox_spec.go` | `SandboxSpec`, `ResourceLimits`, `BuildRunArgs` **pure** | **Prérequis de tout** : sans fonction pure, aucun test négatif possible |
| `internal/worker/bossgate.go` | `ReputationCheck`, `DenylistCheck` | Miroir de `boss/trust_gate.go` — symétrie de conception |
| `internal/worker/bossrating.go` | Écriture des notes worker → boss | Miroir de `boss/completion_rating.go`, plafond horaire compris |
| `internal/worker/stake.go` | `StakeProvider` + implémentation réputation | Isole la partie contestée derrière une interface |
| `internal/worker/taskrecord.go` | `TaskRecord` + écriture | O1 |
| `internal/worker/dryrun.go` | `DryRunSpec` | Préréglage, pas un chemin parallèle |

### À modifier

| Fichier | Modification | Risque de régression |
|---|---|---|
| `internal/worker/handler.go:85` | insérer la chaîne d'admission, retirer `:87-126` | **Moyen** — chemin de rejet ; couvrir par tests d'intégration |
| `internal/worker/sandbox.go:52-112` | consommer `SandboxSpec`, prompt sur stdin | **Élevé** — casse les images lisant `argv[1]` ; double écriture pendant une version |
| `internal/worker/worker.go:17-58` | `Config` gagne `Policy` ; `Worker` gagne `ledger`, `engine`, `admission` ; `mu` → `RWMutex` | Faible |
| `internal/types/types.go:46` | champs `omitempty` | Nul (JSON ignore l'inconnu) |
| `cmd/agentfm/main.go` | `--policy`, `--task-timeout`, `--enforce`, `--dry-run` | Faible |

---

## 5. Séquencement

L'ordre demandé (réputation → limites → dry-run → staking) place les mécanismes de confiance avant l'isolation. Or **un dry-run sans seccomp ni isolation réseau n'observe rien**, et une caution ne protège pas d'un conteneur qui lit le `.env` de l'opérateur.

| # | Lot | Effort | Pourquoi ici |
|---|---|---|---|
| 1 | `BuildRunArgs` pure + tests golden (aucun changement de comportement) | S | Débloque tout le reste |
| 2 | R1/R2 — limites cgroup + `--task-timeout`, `warn` → `enforce` | M | Le plus gros gain par unité d'effort |
| 3 | Allowlist d'env + prompt sur stdin + `--network none` | M | Coupe la chaîne d'exfiltration active |
| 4 | T2 — denylist locale | S | Isolé, sans dépendance |
| 5 | O1 — `TaskRecord` | M | Fournit les données de 6, 7 et 9 |
| 6 | T1 — notes worker → boss | M | Aucun changement de protocole |
| 7 | T3 — PoW d'admission | M | Couvre le boss anonyme |
| 8 | O2 — dry-run | M | N'a de sens qu'après 2 et 3 |
| 9 | L2 — caution | L | Exige un ADR et T1 |

---

## 6. Tests

**Principe directeur** : chaque contrôle livre un test qui **échoue si le contrôle est retiré**.

```go
// internal/worker/sandbox_spec_test.go
func TestBuildRunArgs_DropsAllCapabilities(t *testing.T) {
	args, err := BuildRunArgs(SandboxSpec{Image: "x", Network: NetworkNone}, "c")
	if err != nil { t.Fatal(err) }
	if !slices.Contains(args, "--cap-drop=ALL") {
		t.Fatal("--cap-drop=ALL absent : régression d'isolation")
	}
}

func TestBuildRunArgs_RejectsUnknownNetworkMode(t *testing.T) {
	if _, err := BuildRunArgs(SandboxSpec{Image: "x", Network: "bridge"}, "c");
		!errors.Is(err, ErrUnsupportedNetwork) {
		t.Fatal("deny-by-default violé sur le mode réseau")
	}
}

func TestBuildRunArgs_NeverPutsPromptInArgv(t *testing.T) {
	args, _ := BuildRunArgs(SandboxSpec{Image: "x", Network: NetworkNone}, "c")
	for _, a := range args {
		if strings.Contains(a, "SECRET_PROMPT") {
			t.Fatal("prompt en argv : fuite via /proc/<pid>/cmdline")
		}
	}
}
```

Tests négatifs / d'évasion (`make test-integration`, worker réel) :

| Test | Attendu |
|---|---|
| fork bomb | tuée par `--pids-limit`, statut `killed`, événement émis |
| `malloc` sans fin | OOM-kill, terminal `oom` distinct de `error` |
| `curl 169.254.169.254` | échoue en `NetworkNone` |
| `env` dans le conteneur | ne contient aucune clé hors allowlist |
| boss en denylist | refusé avec `[AGENTFM: REFUSED] {"code":"boss_denylisted"}` |
| boss inconnu (score 0) | **accepté** — non-régression contre la fermeture du mesh |
| écriture hors `/tmp/output` | échoue en rootfs read-only |

Commandes :

```bash
# itération locale, sans libp2p
agentfm -mode test -agentdir ./agent-example/sick-leave-generator/agent \
  -image agentfm-sick-leave:v1 -prompt "test" -policy ./policy.test.yaml

# essaim privé isolé, deux terminaux
agentfm -mode genkey > swarm.key
agentfm -mode worker -swarmkey swarm.key -policy ./policy.test.yaml -enforce ...
agentfm -mode api    -swarmkey swarm.key -bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<id>

# chaîne complète
make test && make test-integration && make test-race
```

---

## 7. Risques

| Risque | Atténuation |
|---|---|
| **Prompt sur stdin casse les agents existants** | Double écriture (stdin + argv) pendant une version, avertissement, migration de l'agent d'exemple |
| **`--network none` casse Ollama en loopback** | Proxy à allowlist livré *avant* le basculement ; `NetworkHost` reste possible en opt-in explicite et journalisé |
| **Plancher de réputation ferme le mesh aux nouveaux** | Plancher strictement négatif + champ `Samples` séparant inconnu de mauvais ; test de non-régression dédié |
| **Notes worker → boss comme arme** | Plafond horaire par contrepartie ; dimensions bornées à l'observable ; pas de dimension `payment` |
| **`--userns=auto` incompatible avec podman rootless** | Détection au démarrage, dégradation documentée, jamais silencieuse |
| **La caution v1 ne dissuade pas un boss jetable** | Assumé par écrit ; c'est T3 (PoW) qui couvre ce cas, pas L2 |
