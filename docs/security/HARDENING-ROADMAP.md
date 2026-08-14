# Roadmap de durcissement — agentfm-core

Statuts : `TODO` · `WIP` · `DONE`. Chaque item = 1 PR + tests + doc.

## 1. Isolation / Sandboxing
| ID | Item | Détail d'implémentation | Statut |
|----|------|-------------------------|--------|
| S0 | Couture testable pour la politique conteneur | Extraction de la construction de l'argv `podman run` dans une fonction pure `BuildRunArgs(SandboxSpec)` (`internal/worker/sandbox_spec.go`), verrouillée par des tests golden sur les 4 combinaisons GPU × env-file. Aucun changement de comportement sur le chemin nominal. **Prérequis de S2, S3, S4 et R1** : avant cette couture, retirer un drapeau de sécurité ne cassait aucun test. | DONE |
| S1 | Sandboxing matériel (TEE) | Abstraction `TEEProvider` (Intel SGX/TDX, AMD SEV-SNP) : attestation à l'admission de la tâche, chiffrement mémoire, refus de la tâche si `require_tee=true` et attestation invalide. Fallback explicite documenté. | TODO |
| S2 | Politique conteneur stricte | `--cap-drop=ALL` + ajout minimal explicite, `--security-opt no-new-privileges`, profil **AppArmor** et politique **SELinux** dédiés, profil **seccomp** allowlist (deny `ptrace`, `mount`, `kexec_load`, `bpf`, `perf_event_open`, `unshare`, `keyctl`, `add_key`, `userfaultfd`, `process_vm_*`). | TODO |
| S3 | Filesystem en lecture seule | `--read-only` + `tmpfs` sur `/tmp` et `/run` (`noexec,nosuid,nodev,size=…`), volumes de travail montés `ro` sauf répertoire de sortie dédié. | TODO |
| S4 | Durcissement complémentaire | `--pids-limit`, `--userns-remap` / user namespaces, réseau `none` par défaut + allowlist egress, pas de socket Docker monté, runtime alternatif optionnel (gVisor/Kata). | TODO |

## 2. Contrôle fin des ressources
| ID | Item | Détail | Statut |
|----|------|--------|--------|
| R1 | Limites dures | `--cpu-quota`/`--cpu-period`/`--cpuset-cpus`, `--memory` + `--memory-swap` (swap désactivé), quotas VRAM par GPU, `--blkio-weight`, `--device-read/write-bps`. Dépassement = kill + événement. **Partiel** : `--cpus`, `--memory` + `--memory-swap` (swap coupé) et `--pids-limit` sont livrés, configurables par flags (`-task-cpus`, `-task-memory`, `-task-pids-limit`) et validés au démarrage ; le dépassement mémoire est attribué et observable via `agentfm_tasks_total{status="oom_killed"}`. Restent à faire : `--cpuset-cpus`, quotas VRAM par GPU (le worker expose encore `nvidia.com/gpu=all` sans plafond), `--blkio-weight` et `--device-read/write-bps`. Défauts : seul le plafond de pids est actif par défaut (1024) ; CPU et mémoire sont opt-in pour ne pas tuer d'agents existants — à basculer en actif par défaut dans une version ultérieure. | WIP |
| R2 | Timeouts stricts par tâche | Flag `--task-timeout 300s` (+ champ par tâche), watchdog côté worker, SIGTERM puis SIGKILL après grâce, statut `TIMEOUT` remonté au boss. **Partiel** : le worker distingue désormais `ok` / `error` / `timeout` et émet un marqueur `[AGENTFM: TASK_FAILED <raison>]` sur le flux (`internal/worker/handler.go`). Restent à faire : le flag et le champ par tâche (valeur toujours codée en dur dans `constants.go`), la grâce SIGTERM, et **l'interprétation du marqueur côté boss** — sans elle le boss compte encore la tâche en `ok` et notifie « completed ». Bug connu à corriger avec cette interprétation : le marqueur `abnormal_exit` est écrit *après* `SendArtifacts`, dont le budget (30 min) dépasse la deadline du flux tâche (10 min) ; un échec tardif avec un gros zip fait expirer la deadline pendant le transfert, l'écriture du marqueur échoue en silence (`_, _ = w.Write`) et le boss lit une fin propre sans signal d'échec. Correctif : deadline courte dédiée avant l'écriture terminale, et `reset` si elle échoue.<br><br>**Deux défauts à corriger AVANT de brancher l'interprétation côté boss** — sans quoi R2 introduirait une régression de sécurité plutôt qu'un contrôle :<br>1. **Marqueur forgeable.** Le stdout du conteneur est écrit tel quel sur le flux (`sandbox.go` `cmd.Stdout = outStream`) et le marqueur n'est qu'une ligne de texte dans ce même canal. Le prompt étant contrôlé par le boss, un boss hostile fait imprimer `[AGENTFM: TASK_FAILED not_run]` par l'agent après une exécution réussie — soit du non-paiement à la demande (menace #2) dès que le marqueur pilote la facturation. Symétriquement, un worker hostile omet le marqueur. Le verdict terminal doit passer **hors-bande** : frame préfixée en longueur sur un sous-protocole, ou champ signé dans un reçu de tâche — jamais une ligne multiplexée avec stdout.<br>2. **Artefacts annoncés puis jamais livrés.** Si `ZipDirectory` ou `SendArtifacts` échoue alors que le conteneur a réussi, `[AGENTFM: FILES_INCOMING]` a déjà été émis, aucun marqueur d'échec ne suit et le statut reste `ok` : le boss attend un zip qui n'arrivera jamais et le worker compte un succès. Même famille que le défaut « non-exécution facturée ». Correctif : traiter l'échec de transfert comme un échec de tâche (`TASK_FAILED artifact_transfer`). | WIP |
| R3 | File d'attente avec priorité | Scheduler à priorités pondérées par la réputation du boss + staking, anti-famine (aging), préemption des tâches basse priorité. | TODO |

## 3. Réputation et confiance
| ID | Item | Détail | Statut |
|----|------|--------|--------|
| T1 | Réputation bidirectionnelle | Le worker note le boss (paiement, conformité de la charge, comportement) ; scores signés, agrégation résistante au Sybil, décroissance temporelle. | TODO |
| T2 | Blacklist locale worker | `~/.agentfm/policy.yaml` : `deny_bosses`, `allow_bosses`, refus au handshake avec motif. | TODO |
| T3 | Proof-of-Work d'admission | PoW léger (Argon2id/Equihash) exigé du boss, difficulté adaptative selon charge et réputation, anti-rejeu (nonce + horodatage). | TODO |

## 4. Surveillance et journalisation
| ID | Item | Détail | Statut |
|----|------|--------|--------|
| O1 | Journalisation détaillée | Logs structurés JSON + métriques par tâche (CPU, RSS, VRAM, I/O, réseau in/out, durée, exit code, syscalls refusés), export Prometheus/OTel, logs append-only chaînés par hash pour le forensic. | TODO |
| O2 | Mode dry-run / bac à sable | Exécution simulée : réseau coupé, FS factice, syscalls tracés, scoring de comportement suspect avant exécution réelle. | TODO |
| O3 | Alertes temps réel | Règles de seuils (pic CPU, egress anormal, syscalls refusés en rafale) → webhook/Slack/e-mail, avec kill automatique optionnel. | TODO |

## 5. Chiffrement et confidentialité
| ID | Item | Détail | Statut |
|----|------|--------|--------|
| C1 | Chiffrement au repos | AES-256-GCM sur le cache et les artefacts de tâche du worker, clés dans un keystore (TPM/OS keyring), effacement sécurisé en fin de tâche. | TODO |
| C2 | Clé éphémère à usage unique | Clé par tâche fournie par le boss (X25519 + HKDF), stockée en mémoire uniquement, destruction garantie à la fin (`mlock`, zeroize), non réutilisable. | TODO |
| C3 | Preuves ZK d'exécution | Attestation vérifiable de l'exécution sans révéler données ni résultat (zk-SNARK sur trace de calcul, ou attestation TEE signée en v1). | TODO |

## 6. Juridique et incitations
| ID | Item | Détail | Statut |
|----|------|--------|--------|
| L1 | ToS boss/worker | `docs/legal/TERMS.md` versionné, acceptation horodatée et signée enregistrée au handshake, hash des ToS dans le protocole. | TODO |
| L2 | Staking / caution | Dépôt du boss avant exécution, libération après validation, slashing en cas d'abus, arbitrage documenté. | TODO |

## Ordre recommandé
S2 → S3 → R1 → R2 → O1 → T2 → T3 → O3 → C1 → C2 → T1 → R3 → L1 → L2 → S1 → C3
