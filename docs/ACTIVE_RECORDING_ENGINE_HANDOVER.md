# Active Recording Engine Handover

## Scope and safety policy

Active Recording handover moves one Recording between Recorder Engine processes
under the same Runtime Host and canonical storage root. The Host first prepares
the target, then drains source admissions and accepted canonical work, durably
transfers the owner epoch, activates the target, and finally retires the source
entry. A failed pre-transfer attempt leaves the source owner in place. A
post-transfer rollback, when possible, issues a strictly newer epoch; an old
epoch is never restored.

Cold Host recovery is intentionally fail-stop. The Host does not reconnect an
orphan Engine or resume a Recording after a Host crash. The startup Engine
fences every prior active owner before mutating archive recovery, and active
recordings become `interrupted`. The generation lease projection is cleared
only after that recovery Engine is ready. This deterministic policy favors
archive integrity over continuing an acquisition whose worker/queue state the
new Host cannot prove. It is not a guarantee that recording continues through a
Runtime Host crash.

## Runtime ownership and canonical writes

`internal/runtimehost/recordingowner.Store` is the durable authority for a
canonical Recording writer. Its bounded owner record stores:

- `recording_id`
- `engine_generation`
- `worker_instance`
- monotonically increasing `epoch`
- active/fenced state

Generation registry leases are retirement references, not write permission.
The owner-file transfer is a compare-and-swap under a per-Recording OS lock.
Every managed canonical write uses the same lock and verifies the complete
owner tuple while holding it through the logical commit. This applies to
segment payload, sidecar and root publication; manifest snapshots; metadata;
source identity/ordinal/gap state; recording state; and terminal archive
mutation. If the owner record or lock cannot be validated, the mutation fails
closed.

For a normal handover, the source scheduler stops new manifest admissions and
waits for admitted segment HTTP bodies, metadata work, queued payloads, storage
retries, and writer callbacks to finish. The owner transfer cannot begin until
this segment boundary succeeds. A segment body already in progress is allowed
to complete; Core does not cancel or splice a partial response across Engines.
The handover does not wait for the broadcast to end. A timeout or drain error
resumes the source under its current epoch. The target never writes the
canonical archive before the Host has transferred ownership.

## Target continuation preflight

The Host only considers a target whose immutable adapter-set identity exactly
matches the source generation. Existing Recordings are never silently moved to
a newer adapter. The authenticated bounded handover context contains the source
owner tuple, adapter ID, resource reference, current media source, and
Recording ID; secrets, signed URLs, and adapter state are not copied into the
archive or public API. The target generation uses its own pinned immutable
adapter-set directory, while shared configuration, secrets, and adapter-owned
state are loaded through the existing adapter runtime.

The target:

1. Loads the canonical Recording read-only and derives the next ordinal and
   source identity from its root and sidecars.
2. Uses the current resource/media context. If the current source needs a
   refresh, it calls the pinned adapter's refresh operation after the source is
   drained. It does not invent a new `resolve`/`continue` request because
   Protocol v1 does not retain the original resolve input or workflow answer.
3. Fetches and parses a fresh bounded HLS manifest, applies the normal epoch,
   sequence, URI, discontinuity, program-time, MAP, and byte-range identity
   rules, then identifies the next uncommitted media object.
4. Fetches and validates that candidate, and any required init object, into
   bounded shared ingest memory. It records the canonical-root fingerprint,
   expected ordinal, source identity, and payload hash in process-local
   prepared state. It does not write payloads, sidecars, root metadata, metadata
   revisions, or committed ordinals before ownership transfer.
5. After the source has stopped admission and drained, repeats the full
   preflight from the final canonical root. A stale speculative candidate is
   discarded. The staged candidate is submitted through the ordinary fenced
   canonical commit path after the Host owner CAS.

Refresh, manifest, candidate fetch, identity, hash, root-fingerprint, or
readiness failure before CAS leaves the source owner unchanged. The periodic
handover retry can try again. Adapter Protocol v1 does not promise refresh
idempotence, so an expired/refresh-required source is drained before target
refresh. A target cannot allocate a new archive ordinal by itself.

## Live handover state machine

The durable owner tuple is the authority; prepare state and payload staging are
volatile and disposable. A Host-owned operation gate serializes handover with
application release activation and adapter-set reconciliation.

1. **Prepare:** confirm source and target Engine identities/inventories; require
the same adapter set; take a source continuation snapshot; prepare a fresh
manifest and candidate while the source keeps recording where safe.
2. **Drain:** stop new source manifest admission, join metadata observation, and
drain accepted media/storage work. Obtain a final source snapshot.
3. **Revalidate:** discard stale speculative state and repeat target preflight
against the final root. No canonical mutation is permitted at this stage.
4. **Owner CAS:** under the same per-Recording lock as commits, replace
`(source generation, source worker, epoch N)` with
`(target generation, target worker, epoch N+1)`. This is the only point at
which the source loses canonical write permission.
5. **Activate:** target validates the current owner inside the canonical fence,
checks the staged root fingerprint/ordinal/source identity, commits any
prepared adapter state, adopts the already-fetched candidate, and starts the
worker. The first target media commit uses the staged payload where still
current.
6. **Retire:** after target activation, detach the parked source. Its stale
epoch cannot commit even if its process remains alive.
7. **Abort:** before CAS, discard target staging and resume the source. After
CAS, never reuse the old source epoch. If a live Host proves the target is
fenced and the parked source can safely resume, ownership may move back to the
source generation only with a newer epoch. If authority is ambiguous, no
process is granted write permission.

The application release may already be active when an individual Recording
handover fails. The failed Recording remains on its current owner and does not
roll back the release. New recordings use the active generation. Adapter-set
changes are not eligible for existing-recording handover unless the immutable
set is exactly the same.

## Cold Host startup reconciliation

The durable owner file preserves the exact pre- or post-CAS tuple and epoch.
The Host uses one deterministic recovery policy for every in-flight stage,
which removes the need to replay process-local prepare state or keep a separate
handover journal:

1. Load the durable generation registry and select its active application
   release/adapter-set tuple. Incomplete staged activation state is reconciled
   by the existing generation startup rules.
2. Start the selected active Engine in `recover` mode. Before readiness, its
   managed Manager installs the canonical fence, locks all owner shards, checks
   owner records, durably marks every active owner fenced without decreasing
   the epoch, and only then runs mutating `storage.LoadAll()` recovery.
3. Mutating recovery marks active recordings `interrupted`; already committed
   payloads, sidecars, metadata timeline, and archive identity remain. A
   second startup after a crash during this step repeats the same idempotent
   recovery.
4. After Engine readiness, `generation.Registry.ReconcileColdStart` atomically
   clears stale leases and marks unattached draining generations dormant. It
   does not delete immutable generations. The active generation and rollback
   references remain subject to ordinary protection/GC rules.
5. Only then does the Host activate the Control/API and admit normal update or
   background work.

Old child processes may survive a hard Host kill. The new Host does not trust
its old in-memory supervisor or authority maps. Their old owner epoch is fenced
at the common canonical commit boundary before archive recovery starts. Their
continued OS existence is a cleanup/resource concern, not a write-authority
source. Cold recovery does not claim those processes are reattached, and it
does not resume their recordings.

There is no separate handover journal because cold recovery does not need to
choose between resuming the source and target: it fences whichever tuple is
atomically present, terminally recovers active archive state, and clears the
lease projection as one idempotent policy. If future product behavior resumes
Recordings after Host crash, it will require stable authenticated Engine
reattachment or a new higher-epoch continuation worker plus explicit durable
handover/recovery state; the current policy must not be silently changed to
assume that process-local state survived.

## Crash and failure outcomes

| Boundary/failure | Durable authority before restart | Cold-start outcome |
| --- | --- | --- |
| Before or during target prepare | Source tuple/epoch | Fence source tuple; preserve committed archive; mark recording interrupted; clear stale leases after Engine readiness |
| Target ready, before source admission stop | Source tuple/epoch | Same fail-stop outcome; speculative candidate is discarded with the orphan process |
| Source admission stopped or drain incomplete | Source tuple/epoch | Fence source; preserve all fully committed data; interrupted recovery handles durable pending state according to storage recovery rules |
| Source drain complete, before CAS | Source tuple/epoch | Fence source; preserve the drained archive; mark interrupted |
| Immediately before CAS | Source tuple/epoch | Same as pre-CAS; old epoch never gains a second process-local authority |
| Immediately after CAS / before registry lease move | Target tuple at epoch N+1, lease may still name source | Fence target tuple; cold recovery clears the stale lease projection; source epoch is never restored |
| After lease move / before target activation | Target tuple at epoch N+1, lease names target | Same fail-stop outcome |
| Before target first canonical commit | Target tuple at epoch N+1 | Reject the orphan's staged commit after recovery fencing; preserve prior committed archive |
| After target first canonical commit | Target tuple at epoch N+1 | Preserve the committed target segment exactly once, then mark interrupted |
| During source retirement | Target tuple at epoch N+1 | Source remains stale; preserve archive and recover interrupted |
| During cold lease reconciliation | All owner tuples already fenced; old or new atomic registry file | Repeat fenced archive recovery and idempotent lease clearing |
| Target preflight/refresh/manifest/candidate failure before CAS | Source tuple/epoch in live Host | Abort that Recording's handover; source continues and may be retried |

The lock serializes a canonical write already in progress against owner fencing:
if the write acquired the lock first, its complete payload/sidecar/root commit
finishes before the fence; otherwise its stale token is rejected. A transfer
never rewrites an already committed source object. Cold Host recovery may update
the root's lifecycle state to `interrupted`; it does not rewrite committed
segment payloads or sidecars.

## Production acceptance evidence

The production E2E tests build and launch real Runtime Host, Control Plane,
Recorder Engine, and fixture adapter executables. Only the source, signed local
release feed, and signing key are fixtures.

- `TestProductionSignedUpdateAcceptanceE2E` runs three A→B iterations. It
  checks source sequences 1..60 exactly once, payload hashes, no gaps, metadata
  continuity, refresh continuity, new recordings on B, Watch handoff, target
  failure preserving A, same adapter-set pinning, and A retirement after its
  lease moves.
- `TestProductionTargetAdapterRefreshPreflightE2E` proves target adapter
  refresh and sequence-4 candidate fetch before the owner CAS. Its refresh
  failure case verifies that A remains owner and resumes capture without a
  gap.
- `TestProductionHandoverHostCrashRecoveryE2E` SIGKILLs and restarts the real
  Host across prepare, source admission/drain, owner CAS, target activation,
  target first commit, source retirement, and lease transfer boundaries. The
  before-CAS, after-CAS, and after-first-commit boundaries repeat three times.
  Every case confirms deterministic active generation/owner epoch, explicit
  interrupted state, unchanged committed payload/sidecar hashes and metadata,
  no duplicate ordinal/source identity, no stale orphan commit, and lease
  reconciliation.

The crash E2E proves archive safety and deterministic fail-stop recovery; it
does not claim live Recording continuity through a Runtime Host crash. Normal
handover is separately proven to preserve the live Recording across application
release activation.

## Compatibility and limits

- No Recording format-version bump is required; owner and generation runtime
  records live under `DATA_DIR/runtime`.
- Existing archive payloads and adapter provenance are unchanged. Runtime
  generation history is not canonical adapter provenance.
- A different adapter-set ID is not eligible for v1 handover.
- Protocol v1 workflow sessions are not migrated. Current resource/media
  context and adapter refresh are used when sufficient.
- Handover is local to one Runtime Host and one shared archive root. There is
  no multi-host consensus, cross-machine Engine migration, Plugin Store,
  adapter sandbox, or active-Recording handover to a different adapter set.
- Logs include only bounded lifecycle identities, epoch, stage, safe error
  code, and duration; they do not include signed URLs, request headers, secrets,
  or adapter-owned state.
