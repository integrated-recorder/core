# v0.2 Full Distributed Recorder 위협 모델 (M0)

상태: baseline threat model; M1–M9가 새 trust boundary를 도입할 때 갱신한다. 이 문서는 보안 인증이나 외부 감사 결과가 아니다.

## 보안 목표와 비목표

보호 대상은 Global Identity와 recovery key, Passkey credential/public-key history, Node Key, OwnerSubject/Capability, canonical archive metadata와 원본 bytes, archive DEK/key envelope, node advertisement, replica lease/tombstone, local recording availability다.

목표는 위조 identity successor·무권한 node enrollment·권한 escalation·event replay·fork의 자동 해결·archive byte substitution·평문 DEK 유출을 막고, 네트워크/identity 장애가 기존 로컬 capture를 멈추지 않게 하는 것이다.

비목표는 이미 복호화된 콘텐츠의 복사 방지, 악의적인 원격 디스크의 모든 사본 삭제 증명, 소유자 키/브라우저/OS가 장악된 뒤의 비밀 회복, partition 상태에서 전역 최신성을 보장하는 것, 악성 커널로부터 process memory 보호다.

## 자산과 신뢰 경계

| 경계/구성요소 | 신뢰할 수 있는 책임 | 신뢰하지 않을 데이터 또는 한계 |
|---|---|---|
| User browser + recovery client | 로컬 seed 생성/복구, WebAuthn UI, 단기 session key | XSS/악성 extension/가짜 origin 위험. JS memory zeroization 보장 불가 |
| HTTPS WebAuthn RP | challenge 발급·1회 소비, RP/origin 검증, 공개 credential 보관 | RP domain 자체가 credential scope이므로 다른 origin에서 동일 ceremony를 재생할 수 없음 |
| Runtime Host | 해당 node의 로컬 process supervision, 기존 install/runtime 정책 | Global identity 소유권이나 원격 노드의 archive 권한을 임의로 발급하지 않음 |
| Recorder Engine/Core archive | local canonical coordinate, writer fence, raw segment bytes | 분산 합의자가 아님. Local recording truth와 global identity event history를 섞지 않음 |
| Local Storage Provider | 제한된 local object I/O | Unix socket/OS account 안의 trusted local process. 공개 P2P peer와 같은 경계로 노출 금지 |
| Source/plugin executable | platform/source interaction | 별도 trust review 없으면 native code이며 hostile input/compromise 가능. Identity signer가 아님 |
| Discovery/rendezvous | candidate address 및 signaling 전달 | spoof/replay/stale/missing 가능. owner, identity, node authority를 결정하지 않음 |
| WebRTC/TURN relay | encrypted transport/relay | IP·timing·size 등 metadata 노출 가능. relay는 content key와 authorization authority를 얻지 않음 |
| Remote replica/peer | 명시된 Lease로 ciphertext object 저장/전달 | malicious, stale, corrupt, deleted, replaying 또는 resource-exhaustion peer로 취급 |
| Identity event registry peers | append-only event history 복제/witness | 서로 독립이어도 signed head보다 최신임을 단독 보장하지 않음. 상충 head면 freeze |
| GitHub release/image/installer | software supply-chain provenance | 사용자 identity root나 archive owner authority가 아님 |

## 공격 표면과 STRIDE 대응

| 위협 | 예 | 필수 통제 | 검증 증거 |
|---|---|---|---|
| Spoofing | Discovery가 다른 Node ID의 주소를 광고 | Node Key proof + peer handshake에서 양측 ID/session nonce 결합 | forged advertisement/SDP injection negative tests |
| Tampering | event payload, archive snapshot, ciphertext, media segment 변경 | domain-separated signature/hash, AEAD AAD, plaintext hash 재검증, immutable snapshot | cross-language vectors, mutation/tag/hash failure tests |
| Repudiation | owner가 node 위임을 부인 | canonical operation·signer evidence·event hash·approval evidence 보존 | event audit/export fixtures |
| Information disclosure | Recovery seed, DEK, raw URL credentials, address metadata 유출 | key never-upload rule, ciphertext-only untrusted replica, structured log redaction, bounded private storage | secret scan + wire/log tests |
| Denial of service | malformed frame, huge payload, ICE reconnect loop, storage exhaustion | strict size/rate/time/concurrency quotas, bounded chunk streaming, cancel/backpressure | fuzz/limit/out-of-space/partition tests |
| Elevation of privilege | delegated Capability scope/time/depth escalation, revoked node reused | parent grant intersection, current policy epoch, revocation/freshness gate, fail-closed | adversarial capability matrix |
| Replay/rollback | stale signed head or expired grant accepted | monotonic pinned head, nonce one-use record, expiry/skew window, operation idempotency | stale-head/replay/clock tests |
| Fork ambiguity | two valid successors have same previous hash | freeze; preserve both; authorized explicit fork resolution only | deterministic two-node fork harness |
| Local availability | Discovery/identity service offline during capture | local ingest path independent of identity network; defer high-risk remote actions | network-disconnected capture acceptance |

## 공격자 가정과 잔여 위험

원격 peer, discovery operator, TURN provider, public network, user-supplied HLS/metadata, third-party source plugin은 적극적으로 악의적일 수 있다. Local OS/container administrator와 control하는 browser origin을 장악한 공격자는 이 모델로 차단하지 못한다. 여러 registry의 독립성은 설정으로 확인해야 하며, “두 provider”라는 숫자만으로 Sybil 저항이나 quorum은 성립하지 않는다.

네트워크 partition 중에는 아직 관측되지 않은 revoke/fork가 존재할 수 있다. 서명만으로 freshness를 증명하지 않는다. High-risk remote operations는 최신성 정책/TTL이 허용하지 않으면 거부하지만 local recording은 계속한다. External replica에 복호화 키를 전달한 뒤에는 해당 사본을 원격 회수할 수 없다.

## 중단 조건

Recovery private key가 서버로 전송/로그/백업되는 경로, discovery/TURN에 권한 판정 권한이 생기는 경로, 평문 DEK가 untrusted replica로 전달되는 경로, 유효 fork를 자동 선택하는 경로, archive byte mismatch 수용, 기존 녹화가 discovery 장애로 멈추는 경로가 발견되면 구현을 중지하고 M0 contract/design을 수정한다.
