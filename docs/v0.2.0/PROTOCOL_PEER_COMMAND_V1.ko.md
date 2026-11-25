# 인증된 Peer Command v1

상태: 제한된 archive/replica peer API에 연결된 command protocol. 명령 envelope, Node Key 서명, 상호 인증된 Pion control channel 전달, 세션 바인딩, 현재 identity key 검증, capability 검사 및 durable replay 소비가 Control Plane session manager와 `peerapi.Service`에서 사용된다. 이 문서는 모든 Runtime Host/Recorder 설정을 원격 제어한다거나 M6 완료를 뜻하지 않는다.

기계 판독 schema: [`protocol/peer-command-v1/schema.json`](../../protocol/peer-command-v1/schema.json)

## 보안 경계

- 명령은 이미 상호 Node Key transcript 인증을 마친 WebRTC control channel에서만 전달한다. Discovery와 TURN은 주소 전달·암호화된 transport일 뿐 권한을 부여하지 않는다.
- `session_id`는 handshake가 인증한 실제 세션 ID다. 송신자가 임의로 다른 세션을 고를 수 없으며 수신기는 현재 session, authenticated remote Node, local target Node가 모두 일치해야 한다.
- source 및 target Global Identity history는 실행 직전에 다시 해석한다. history가 stale/frozen이거나 Node Key 위임이 없거나 폐기됐으면 거부한다. 전송된 identity ID 또는 Node ID만 신뢰하지 않는다.
- 서명된 command는 정확한 payload wire bytes의 SHA-256(`IR/peer-command/v1/payload\0` domain prefix)을 포함한다. receiver는 duplicate JSON property, invalid UTF-8, trailing JSON 및 64단계 초과 nesting을 거부한다. envelope signature는 다른 필드와 payload digest를 `IR/peer-command/v1/sign\0` domain으로 서명한다.
- `grant_chain`은 소유권 증명이 아니다. command 수신기는 현재 검증된 OwnerSubject log, policy epoch, revocation, archive/snapshot/action scope 및 freshness를 확인하는 authorizer를 매 요청 호출해야 한다. 미구현 action은 거부한다.
- 최대 frame 32 KiB, payload 12 KiB, grant chain 16개, command lifetime 2분이다. request ID와 32-byte nonce는 재사용하지 않는다. 수신 Node는 Node Key 보호 폴더의 원자적 replay journal에 식별자 hash만 기록한다.
- authorization과 replay 확인은 handler가 side effect를 수행하기 직전에 해야 한다. 작업을 queue하거나 지연 실행하는 dispatcher는 실제 처리 직전에 현재 capability를 다시 확인해야 한다.

## 연결된 범위와 한계

현재 dispatcher는 immutable snapshot 조회, 암호문 manifest/range, 수신자에 바인딩된 key envelope, replica offer/acceptance/checkpoint/renewal/tombstone 전달과 source readback 같은 bounded archive/replica 동작만 제공한다. 미등록 operation, scope 불일치, capability 없는 요청은 거부한다. Transport 수신 시 session-bound signature/freshness/replay와 capability 검사를 수행하고, `peerapi.Service`도 side effect 또는 archive read 직전에 현재 OwnerSubject 권한을 다시 검사한다. 일반 계정 관리, Recorder 설정, 임의 plugin 관리 또는 일반 recording control command를 노출하는 dispatcher는 없다.

로컬 archive/replica API의 연결된 경로와 loopback Pion fixture는 단위·통합 테스트로 검증됐다. 다만 서로 다른 운영자의 최신 history를 독립된 네트워크에서 동기화하고 stale/fork/revocation을 전파하는 acceptance, WAN/TURN, 두 운영 노드 및 장기 분할 복구 검증은 남아 있다. Discovery 제공자 합의는 cryptographic transparency 증명이 아니며 공모 stale replay를 막지 않는다. 따라서 이 경로를 M6 또는 전체 v0.2.0 완료로 간주하지 않는다.
