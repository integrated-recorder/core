# RC12 아카이브/신뢰 경계 기준선

- Tag: `v0.1.0-rc.12`
- Tag commit: `d4a14d4fab908a0cb415dacd47e7f0d9bc63be8d`
- Initial checkout branch: `fix/setup-stepper-autofill` at the same commit as `origin/main`
- Initial tracked tree: clean. Existing untracked `.research/` was preserved.

## Reused boundaries

- `internal/authn`: local accounts, session/CSRF and RBAC; it is not Global Identity or a Passkey credential store.
- `internal/runtimehost/installation`: local installation lifecycle; identity creation must not bypass its fail-closed recovery state.
- `internal/runtimehost/recordingowner`: local Engine generation/worker epoch fence; it is not cross-node consensus.
- `internal/archiveindex`: session identity, source coordinates, claims, PRESENT/GAP, repair and conflict semantics.
- `internal/storage` and `internal/storageproto`: trusted local storage API and canonical publication. A remote peer must never become a direct Storage Provider v1 client.
- `internal/acquire`: local live/historical acquisition and commit behavior, which must remain operational without identity/discovery availability.
- `internal/server` and `internal/runtimehost/httpapi`: existing local API/session/permission boundaries.

## Existing regression gates to retain

These are references to RC12 behavior, not new distributed tests:

- `go test -short -count=1 ./internal/archiveindex ./internal/integrity ./internal/acquire ./internal/storage`
- `internal/archiveindex/model_test.go`: source session identity, coordinates, claim provenance, gap coverage and recovery mapping.
- `internal/acquire/archive_v2_test.go`: sharded V2 archive recovery, segment ordering and integrity.
- `internal/acquire/ownership_fence_test.go` and `internal/acquire/handover_test.go`: canonical writer fence and generation handover.
- `internal/integrity/service_test.go`: V2 media/init/manifest payload verification.
- `internal/storage/sharded_archive_test.go`: V2 publication ordering and canonical index behavior.

The first sandboxed run passed `archiveindex`, `integrity`, and `storage` but `acquire` could not start its localhost `httptest` listeners. The exact command was then rerun with permission for local loopback fixtures; all four packages passed uncached:

| Package | Result |
|---|---:|
| `internal/archiveindex` | PASS, 0.374s |
| `internal/integrity` | PASS, 5.377s |
| `internal/acquire` | PASS, 113.296s |
| `internal/storage` | PASS, 56.254s |

The loopback exception was limited to the Go test process; no external service was contacted. This establishes a local RC12 regression baseline, not the full multi-node/network/security acceptance.

## Migration constraint

RC12 archive roots, payloads, sidecars, ownership epochs, plugin configuration and storage sets remain the source data. v0.2 state is staged in versioned, separate records and attached only after an explicit owner claim. No implementation may interpret an RC12 local login as a global identity or rewrite original segment bytes during identity, snapshot, encryption or replication migration.
