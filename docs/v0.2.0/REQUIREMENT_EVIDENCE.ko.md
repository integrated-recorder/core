# v0.2.0 Requirement-to-Evidence Matrix

이 표는 WIP checkpoint `f605f3addc1af7236ad7b413e8f2b899550b9c0b`와 현재 미커밋 작업 트리를 대조한 진행 기록이다. 현재 작업 트리의 변경사항은 아직 별도 commit SHA가 없으며, 아래 로컬 테스트 결과는 그 실행 시점의 코드에만 적용된다. `LOCALLY VERIFIED`는 해당 행에 명시한 시험 범위만 뜻하며 마일스톤 전체 완료를 뜻하지 않는다.

## 요구사항과 증거

| M | 요구사항 | 제품 경로 / 명세 | 검증 증거 | 상태 및 남은 조건 |
|---|---|---|---|---|
| M0 | Wire 계약, 암호 프로파일, 위협 모델, 엄격한 파서, fork 동결 | `protocol/identity-v1`, `protocol/authority-v1`, `protocol/archive-snapshot-v1`, `protocol/cryptoarchive-v1`; `docs/v0.2.0/ADR-0001-cryptographic-profile.ko.md`, `THREAT_MODEL.ko.md`, `MODULE_BOUNDARIES.ko.md` | Go·JavaScript 공유 벡터: `node --test protocol/*/vector.test.mjs` — 9개 통과; 최종 작업 트리 전체 Go short 시험 통과 | **LOCALLY VERIFIED (프로토콜/회귀)**. 최종 후보 CI, 독립 보안 검토, 독립 저장소의 악의적 fork 수렴은 미완료. |
| M1 | Global Identity, Passkey, recovery-key 복구와 credential 교체 | `internal/globalidentity`, `internal/server/routes_global_identity.go`, `web/src/identity`, `web/src/routes/identity.tsx` | 가상 authenticator를 사용하는 Go backend Chromium E2E `global-identity.spec.ts` 통과; `internal/globalidentity` 및 `internal/authority` 단위/통합 테스트 | **LOCALLY VERIFIED (가상 authenticator)**. 실제 보안키/기기 Passkey, 장기 오프라인 복구, 독립 보안 검토는 미검증. |
| M2 | Node key·위임·폐기, 등록 증명, 공개 서명 이력, anti-rollback/fork-freeze | `internal/nodeidentity`, `internal/globalidentity`, `internal/discovery`, `internal/server/routes_node_identity.go`; `NODE_ENROLLMENT_V1.ko.md`, `PROTOCOL_IDENTITY_V1.ko.md` | `internal/nodeidentity`, `internal/globalidentity` 테스트; Chromium 가상 authenticator 등록/revoke E2E | **PARTIAL**. 테스트 저장소와 로컬 mirror 검증은 있으나 별도 Recorder에서 재시작·폐기·fork 해결이 수렴하는지 검증하지 않음. Discovery mirror 합의는 암호학적 최신성 증명이 아님. |
| M3 | Signed advertisement, Discovery federation, 인증된 signaling, WebRTC/TURN | `internal/discovery`, `internal/signalingprotocol`, `internal/peertransport`, `internal/peerapi`; `PROTOCOL_DISCOVERY_V1.ko.md` | 두 테스트 HTTPS Discovery endpoint 및 loopback Pion/TURN 통합 시험; `internal/peertransport`, `internal/peerapi` 테스트 | **PARTIAL**. 독립 운영 Discovery 두 곳, 서로 다른 네트워크/NAT, 외부 TURN 자격 증명 및 실제 relay acceptance는 미검증. |
| M4 | Archive ID, immutable snapshot/history, RC12 archive 호환 | `internal/archivesnapshot`, `internal/archivecatalog`, `internal/recorderengine`, `internal/acquire`; `PROTOCOL_ARCHIVE_SNAPSHOT_V1.ko.md`, `PROTOCOL_ARCHIVE_SNAPSHOT_V2.ko.md` | snapshot hash·init-map 공유 벡터; Engine `SealArchive`와 sharded RC12 fixture 테스트; `internal/archivecatalog` 및 `internal/archivesnapshot` | **LOCALLY VERIFIED (snapshot/fixture)**. 전체 snapshot history의 독립 노드 복제와 원격 권한·재생 acceptance는 미완료. |
| M5 | DEK envelope, 객체 암호화, 재개 가능한 ciphertext 복제, byte-exact restore/playback | `internal/cryptoarchive`, `internal/replication`, `internal/peerapi`, `web/src/identity/peer-playback.ts`; `ADR-0002-archive-encryption.ko.md` | Go/browser 암호 벡터; `internal/replication` 및 Pion lease/ciphertext/readback 테스트; WebKit 모바일 에뮬레이션의 암호화 fixture 재생·seek 통과 | **PARTIAL**. RestoreSnapshot은 검증된 평문을 별도 private tree에 원자적으로 복원하지만 Core VOD/archive API에 등록하지 않는다. 이를 연결하려면 키 접근, `archive.restore` 재검증 및 local archive identity 정책이 정의되어야 한다. iPhone 및 독립 노드 재생은 미검증. |
| M6 | OwnerSubject claim, capability 발급/위임/폐기, 권한 신선도, transfer/rekey | `internal/authority`, `internal/lease`, `internal/server/routes_owner_authority.go`, peer command authorizer; `PROTOCOL_AUTHORITY_V1.ko.md` | `internal/authority`, `internal/lease`, `internal/server`의 서명·범위·폐기/fork 테스트 | **PARTIAL**. 보호된 peer/replication 경로는 매 요청 권한을 재검증한다. RC12 local archive 전체의 OwnerSubject claim/migration, 서로 다른 운영자 간 최신성·transfer/rekey acceptance는 미완료. |
| M7 | 명시적 Provider consent, checkpoint/readback, lease 갱신·만료, tombstone 전달/정리 | `internal/lease`, `internal/replication`, `cmd/control-plane/replica_tombstone_delivery.go`, `internal/server/routes_replica_leases.go`; `PROTOCOL_REPLICA_LEASE_V1.ko.md` | 전체 Go short 시험 통과; 현재 diff의 lease v1→v2 migration·malformed cursor·reopen 및 Control Plane bounded retry 회귀시험; focused normal/race 시험 | **LOCALLY VERIFIED (durable retry path)**. cursor는 비권한 scheduling metadata이며 ACK는 서명된 receipt로만 확정된다. 독립 provider, 실제 원격 삭제, WAN/storage 장애 및 quota/repair acceptance는 미완료. |
| M8 | 여러 노드의 archive 탐색, 권한 기반 재생, 브라우저 복호화 | `web/src/components/remote-archive-browser.tsx`, peer archive API, WebCrypto/MSE | 최종 frontend Vitest 60 files / 288 tests 통과; 권한·peer identity 변경 및 한 Node ID의 다중 Identity 모호성 fail-closed 회귀; Playwright WebKit iPhone emulation encrypted fixture play/seek 통과 | **LOCALLY VERIFIED (desktop browser engine/emulation)**. 외부 네트워크, 물리 iPhone Safari, 실제 장시간 스트림·재연결은 미검증. 최근 코드 변경의 fixture 증거이지 독립-node acceptance가 아님. |
| M9 | RC12 데이터 migration, 재시작/rollback, Docker·filesystem 배포 | `internal/server/rc12_upgrade_compatibility_test.go`, `scripts/install.py`, `docs/DEPLOYMENT.md`, `docs/DEPLOYMENT.ko.md` | `python3 -m unittest test_install` — 49 통과 / 1 skip; RC12 synthetic fixture tests; full Go short suite | **PARTIAL**. Synthetic fixture는 계정·세션·설정·plugin/provider/generation pins 및 archive bytes를 검증한다. 실제 Docker install/upgrade/rollback, 실제 RC12 사용자 설치, NFS/UID·GID, independent backup restore는 미검증. |

## 실행 증거

| 검증 | 대상 | 결과 | 증명하지 않는 것 |
|---|---|---|---|
| `go test -short -count=1 ./...` | 현재 WIP 작업 트리 | 통과; storage diagnostic child-exit 테스트의 스케줄링 동기화를 명시적 시작 marker로 보강한 뒤 재실행 | race, 외부망, Docker 및 실제 노드 acceptance |
| `go test -race -short -count=1 -p 1 ./...` | 현재 작업 트리 전체 | 통과 | package 간 병렬 부하 상태, 외부 운영 노드·네트워크·실기기 acceptance |
| `go test -race -short -count=1 ./...` | 현재 작업 트리 전체, 기본 package 병렬성 | 1회 실패: `internal/peerapi/TestAuthenticatedPionSessionServesCapabilityCheckedSnapshot/same_identity`가 데이터 채널 handshake에서 20초 문맥 만료; 해당 테스트 격리 `-count=20` 통과, 전체 `-p 1` 통과 | 실패의 원인이 머신 부하인지 간헐적 Pion handshake 문제인지 아직 확정하지 못함. CI 재확인 필요. |
| `go vet ./...` / `go build ./...` | 현재 M7/M8 수정 포함 작업 트리 | 통과 | Linux multi-arch package 및 공식 release artifact |
| `npm run typecheck`, `npm run lint`, `npm test`, `npm run build` | `web/` | 통과; 60 files / 287 tests | Safari Passwords UI 또는 물리 모바일 재생 |
| `node --test protocol/*/vector.test.mjs` | 공유 wire-format vectors | 9 통과 | 독립 구현체·운영 노드 간 상호운용성 |
| `python3 -m unittest test_install` | installer tests | 49 통과 / 1 skip | 실제 Docker daemon 설치 |
| Chromium Global Identity E2E | 로컬 Go backend + virtual authenticator | 통과 | 실제 하드웨어 Passkey 및 두 네트워크 간 discovery |
| WebKit peer playback E2E | iPhone 13 browser emulation + encrypted fixture | 통과 | 물리 iPhone Safari 및 실제 peer network |
| `npm run test:e2e` | production-process browser suite + isolated Go backend | 23 tests 통과; HLS/VOD, auth/setup, RBAC, HTTPS switch, timed events 포함 | Docker installation, independent nodes, WAN/TURN, physical iPhone |

**Release eligibility는 NO-GO다.** 독립 운영 노드/Discovery, 외부 NAT/TURN, 실제 Docker upgrade, 물리 iPhone Safari 및 독립 보안 검토 같은 필수 acceptance 게이트가 남아 있다. Docker와 실기기 검증은 이 개발 세션에서 실행하지 않는다.
