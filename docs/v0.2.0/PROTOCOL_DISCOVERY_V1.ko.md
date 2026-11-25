# `ir.node.advertisement/1` protocol contract (M3 increment)

상태: 서명된 NodeAdvertisement 계약, Go/JavaScript 시험 벡터, durable Directory 저장/API와 HTTPS 전용 단일-provider 실행기, 2+ distinct HTTPS hostname을 요구하는 Federation Client의 publish/lookup/merge가 구현됐다. 공개 서명 신원 이력 mirror 및 모든 설정 provider 간 exact-history agreement lookup도 구현됐다. 주소 봉투에 recovery encryption key 대상 HPKE 암호화와 Go↔브라우저 시험 벡터가 추가됐다. 이후 bounded signed peer-signaling wire protocol, current Node Key 검증, durable replay consumption, HTTPS relay federation, encrypted SDP 및 Pion session coordinator가 구현됐다. Loopback integration test는 두 별도 TLS Discovery fixture를 거쳐 relay-only TURN candidate로 상호 인증된 data channel을 연다. 독립 운영 provider, Docker-isolated network, external NAT/TURN and product lifecycle acceptance remain unverified. 이 문서를 M3 완료로 해석하지 않는다.

## 서명 대상

`protocol/discovery-v1/schema.json`과 `address-envelope.schema.json`은 광고와 내부 주소 봉투의 wire 구조 및 상한을 고정한다. `protocol/discovery-v1/test-vector.json`은 JSON 구조, RFC 8785-compatible canonical bytes 및 Ed25519 서명을 고정한다. `address-envelope-test-vector.json`은 테스트 전용 recovery key로 Go가 생성한 HPKE 봉투를 Go와 브라우저가 여는 교차 언어 벡터다. 서명 입력은 다음과 같다.

```text
"IR/node-advertisement/v1/sign\0" || canonical_advertisement
```

정수는 decimal string만 허용하고 property 이름은 identity protocol의 엄격한 JSON profile을 따른다. 알 수 없는 필드, 중복 property, 숫자, non-canonical bytes, 잘못된 Base64url, Node ID와 public key 불일치, 지원하지 않는 capability, 정렬되지 않은 중복 항목, 만료/미래 광고는 거부한다. Advertisement 최대 크기는 32 KiB, address envelope 최대 크기는 16 KiB, relay 수는 8, TTL은 10분, 발급 시 clock skew 허용치는 2분이다.

## 필드와 신뢰 경계

- `node_id`는 포함된 Ed25519 Node public key에서 다시 계산한다. `internal/nodeidentity.Store.SignAdvertisement`는 로컬 보호 키만 사용하고 영속 sequence를 먼저 증가시킨다. sequence 파일이 손상되면 새 광고를 서명하지 않는다.
- `global_identity_hint`는 Global Identity ID의 도메인 분리 SHA-256 앞 128 bit를 Base64url로 인코딩한다. 이는 pseudonym이지 비밀이나 unlinkability 보장이 아니다.
- `identity_head_hash`와 Node 서명은 현재 Node 위임을 증명하지 않는다. 수신자는 별도로 서명된 identity history의 head, 노드 위임, 알려진 폐기 및 최신성 정책을 검증해야 한다. Discovery 조회 결과만으로 권한을 부여하지 않는다.
- `address_envelope`는 `ir.node-address-envelope/1` canonical JSON을 Base64url 인코딩한다. 내부 봉투는 RFC 9180 Base-mode `DHKEM(X25519, HKDF-SHA256) / HKDF-SHA256 / AES-256-GCM`을 사용하고 identity hint, identity head hash, Node ID 및 recovery public-key digest를 HPKE `info`와 AAD에 결합한다. Plaintext는 최대 8 KiB의 canonical JSON이고 봉투는 최대 16 KiB다. Recovery private key는 브라우저 recovery key material에만 남는다.
- 공개 광고 validator는 봉투의 canonical encoding, suite, 크기와 필드 형식만 검사한다. 로컬 광고 서명 API는 추가로 봉투의 공개 recipient digest가 현재 Global Identity genesis에 고정된 recovery encryption public key와 일치하는지 확인한다. 어느 검사도 AEAD 진위·복호화 가능성, HPKE의 identity/head/Node context 결합 또는 최신 Node 위임을 증명하지는 않는다. 수신 클라이언트는 NodeAdvertisement 서명과 현재 identity history/delegation을 별도로 검증한 뒤 recovery key로 봉투를 열어야 한다. Recovery encryption public key는 공개 genesis 정보다. Discovery provider는 주소/ICE 자격 증명의 plaintext를 받지 않는다.
- Advertisement 서명은 Node Key의 소유만 증명한다. 원격 명령, archive 접근, Owner 권한, identity freshness를 부여하지 않는다.

## 단일 Directory API slice

`internal/discovery.Directory`는 독립 루트와 private state 파일을 사용하는 durable index다. 각 저장 root에서는 파일 잠금으로 쓰기를 직렬화한다. `POST /v1/advertisements`는 서명·만료·Node ID binding·sequence를 검사하고, `GET /v1/advertisements?identity_hint=...`는 만료되지 않고 동결되지 않은 signed envelope만 반환한다. 상태는 atomic rename 및 디렉터리 sync로 저장하며 root 0700/file 0600을 요구한다. 만료 광고도 sequence high-water 상태에 남기므로 같은/낮은 sequence가 replay될 수 없다. 같은 Node/sequence에 다른 유효 서명이 들어오면 두 서명된 증거를 보존하고 그 Node를 영속 동결한다. API는 CORS와 forwarded-address 신뢰를 사용하지 않고 bounded body, per-remote-IP request limit 및 no-store lookup을 적용.

`internal/discovery.FederationClient`는 최소 두 개의 hostname이 다른 HTTPS bootstrap URL을 요구한다. Publish는 모든 provider에 concurrently 전송하고 최소 두 provider의 성공을 확인한다. Lookup은 모든 provider를 병렬 조회하고 각 광고를 서명·TTL·hint 기준으로 검증한다. 부분 outage는 `Complete=false`와 sanitized provider status로 드러낸다. 여러 provider가 같은 Node/sequence에 서로 다른 서명 광고를 돌려주면 해당 Node를 후보에서 제외하고 fork 목록에 기록한다. 서로 다른 sequence라면 최신 광고만 통신 후보로 유지하지만, 이는 identity authority/현재 위임 검증을 대신하지 않는다.

`cmd/discovery-directory`는 한 HTTPS provider 인스턴스를 실행한다. `IR_DISCOVERY_DATA_DIR`, `IR_DISCOVERY_TLS_CERT`, `IR_DISCOVERY_TLS_KEY`가 필요하고, 선택 `IR_DISCOVERY_LISTEN_ADDR`의 기본값은 `127.0.0.1:8443`이다. 각 provider는 별도 호스트·인증서·저장 루트로 운영해야 한다. 실행 예:

```sh
IR_DISCOVERY_DATA_DIR=/var/lib/ir-discovery \
IR_DISCOVERY_TLS_CERT=/etc/ir-discovery/tls.crt \
IR_DISCOVERY_TLS_KEY=/etc/ir-discovery/tls.key \
go run ./cmd/discovery-directory
```

## Peer signaling and session setup (M3 progress)

`ir.peer-signaling/1` envelopes bind source/target identity IDs, Node IDs, source head, session, operation and short expiry. Source Node Key signatures cover the full message. Init/accept carry one-time X25519 public keys; both nodes derive a session key with HKDF-SHA-256, and offer/answer SDP (including ICE candidates) is AES-256-GCM encrypted with routing/session metadata as associated data. A recipient must resolve the sender's historical and current identity heads and the target's current delegated Node Key before accepting; it then durably records the signal ID in the Node Key replay journal. Relay storage is per-process, bounded, expires entries and persists no SDP plaintext. Repeated reads are expected; process loss requires the sender to retry with a new session.

`FederationClient.PublishPeerSignal` requires acknowledgement from at least two configured HTTPS provider hosts. Lookup merges exact copies and fails on a same-ID byte conflict. `peertransport.EstablishOutgoing`/`AcceptIncoming` repeatedly query those providers, revalidate caller-supplied node-connect authorization at each handshake boundary, encrypt gathered SDP, and only return a bounded data channel after mutual Node-Key transcript authentication. The caller must still enforce archive/action/snapshot Capability on each application command. TURN relay-only mode is explicit; a relay ID or candidate never grants authority.

개발 브랜치의 Control Plane signaling에는 optional ICE 설정이 연결되어 있다. 브랜치 installer 인터페이스는 `--peer-stun-urls stun:stun.example.net:3478`, `--peer-turn-urls turn:turn.example.net:3478?transport=udp`, `--peer-turn-secret-file /absolute/private/turn-secret` 및 선택적인 `--peer-turn-credential-ttl 5m`, `--peer-ice-relay-only`다. 기존 설치에서 설정을 생략하면 저장된 값이 유지되고, `--disable-peer-ice`는 이를 명시적으로 제거한다. secret 파일은 32–1024 바이트의 텍스트 shared secret, regular file, owner-only mode여야 한다. installer는 값을 metadata에 저장하지 않고 파일만 `/run/secrets/ir-peer-turn-secret`에 read-only bind mount하며, Compose 서비스 UID:GID 10001:10001이 읽을 수 있는지도 별도 one-shot probe로 검사한다. Control 설정은 `IR_PEER_STUN_URLS`, `IR_PEER_TURN_URLS`, `IR_PEER_TURN_SHARED_SECRET_FILE` 등 operator 환경으로 전달된다. Runtime Host는 값이 아닌 파일 경로와 공개 ICE URL만 Control 자식에게 전달한다. 각 handshake 직전에 Control Plane이 `expiry:NodeID` 사용자명과 HMAC-SHA1 암호를 만들어 Pion에만 넘긴다. 기본 credential TTL은 5분이며 설정 가능 범위는 1–10분이다. `IR_PEER_ICE_RELAY_ONLY=true`는 TURN URL과 secret이 있을 때만 유효하다. 설정이 없거나 잘못되면 peer transport만 unavailable로 fail-closed되고 로컬 녹화·VOD에는 영향이 없다. secret 파일의 값과 파생 credential은 로그/API/지속 저장소에 쓰지 않는다.

자동 통합 시험은 서로 다른 두 loopback HTTPS hostname과 두 격리 directory를 사용하고, coturn REST HMAC credential을 검증하는 로컬 UDP TURN 서버의 relay-only candidate 연결을 검사한다. credential derivation, private-file validation, production child-environment projection 및 Compose secret mount persistence에도 단위 회귀시험이 있다. 이 개발 브랜치의 코드는 설치기/Runtime Host/Control Plane 연결을 포함하지만 현재 공개 RC12 바이너리에는 해당 변경이 아직 없으며, Docker daemon이 없어 실제 installer image 검증도 수행하지 않았다. 이 검증은 독립 운영자, Docker 네트워크 분리, 외부 NAT, 운영 TURN 발급·quota, 장기 세션의 credential refresh/ICE restart 또는 partition 회복을 입증하지 않는다. 따라서 M3 진행 상태이며 수용 완료가 아니다.

## 서명 신원 이력 mirror와 freshness 한계

`PUT /v1/identity-histories/{identityID}`와 `GET /v1/identity-histories/{identityID}`는 공개 서명 이력만 bounded body로 교환한다. provider는 전체 서명 체인과 fork evidence를 검증하고 anti-rollback/fork-freeze 규칙으로 영속화하지만 신원 이벤트에 서명하거나 branch를 선택하지 않는다. 응답은 `no-store`이고 CORS 및 forwarded-address 신뢰를 사용하지 않는다. WebAuthn credential, user handle, private/recovery key, session 자료는 이 wire format에 없다.

Control Plane은 운영자가 `IR_DISCOVERY_PROVIDERS=https://one.example,https://two.example`로 distinct HTTPS host를 둘 이상 명시한 경우에만 이 경로를 사용한다. Global Identity history가 로컬에서 변경되거나 새 노드가 이력에 합류하면 Control Plane은 bounded best-effort 방식으로 최신 unfrozen 공개 이력을 모든 설정 provider에 게시한다. 이 게시 실패는 이미 commit된 로컬 identity mutation을 롤백하거나 로컬 녹화를 중단하지 않으며 `X-IR-Identity-History-Sync: pending`과 민감정보를 포함하지 않는 경고 로그로 드러난다. Owner의 인증된 `POST /api/global-identity/history/publish`는 재시도 수단이다. 각 remote authority 검증 동작은 모든 설정 provider에서 완전히 같은 유효 signed history를 다시 조회하고 로컬 registry의 현재 head도 그 head와 일치할 때만 허용한다. 이전 조회 결과를 다음 인가 동작에 재사용하지 않는다. 부분 장애·불일치·fork·만료는 fail-closed다. 모든 유효 응답 이력은 합의 여부를 판단하기 전에 로컬 registry로 가져와 충돌 증거를 보존한다. provider의 import timestamp는 사용하지 않는다.

이 정책은 설정한 provider 집합이 stale history를 함께 재생하지 않는다는 운영 가정에 기반한 freshness quorum이지, 암호학적으로 최신성을 증명하는 transparency log가 아니다. 두 hostname은 서로 다른 운영자나 인프라임을 보장하지 않는다. identity event 변경 뒤 모든 provider로 publish가 완료되기 전 다른 노드는 최신 revocation을 보지 못할 수 있다. 독립 provider, 이벤트 자동 전파/복구, adversarial replay 및 cross-network 수렴을 실제로 검증하기 전에는 이 기능을 보안 완료나 M2/M3 완료 근거로 사용하지 않는다.

## OwnerSubject signed history mirror (M6 increment)

`PUT /v1/owner-authority-histories/{subjectID}`와 `GET /v1/owner-authority-histories/{subjectID}`는 bounded public signed event history를 교환한다. Directory는 envelope, identifier, size, signature encoding, duplicate JSON key, rate limit과 durable state integrity만 검사한다. 이 검사는 Ed25519 서명이나 authority chain을 검증하지 않는다. 비밀키·recovery material은 wire format에 없다. Directory는 공통 prefix만 압축하며 서로 다른 candidate successor를 모두 보존한다. 저장소 한계에 닿으면 branch를 지우거나 자동 선택하지 않고 쓰기를 거부한다.

Control Plane은 기존 authority mutation을 로컬 durable store에 반영하고 해당 signed history를 모든 configured provider에 게시한다. 일부 게시만 성공하면 API는 실패를 보고하고 다음 mutation 전에 현재 로컬 head를 먼저 재게시·비교한다. remote peer command는 매번 모든 provider에서 candidate set의 완전한 byte-equivalent agreement를 요구한 뒤, Recorder가 각 event signature, historical Global Identity head와 current Global Identity status를 독립 검증하고 원자적 authority import를 수행한다. stale head, provider outage/disagreement, invalid event 또는 valid fork는 remote authorization을 거부한다. 로컬 녹화는 이 경로와 무관하다.

OwnerSubject mirror는 Global Identity history와 마찬가지로 Discovery transport를 freshness authority로 승격하지 않는다. 설정한 provider 전원이 같은 stale history를 재생하거나 운영자가 공모하면 stale capability가 인정될 수 있다. history publication 시점과 실제 provider agreement 사이에는 mutation이 성공으로 응답하지 않는 pending 구간이 있다. 독립 운영자·실제 network partition·재시작·공모 stale replay를 포함한 분산 acceptance가 완료되기 전에는 이것을 cryptographic revocation finality로 간주하지 않는다.

원격 연결은 운영자가 listen address를 명시 설정하고 방화벽/TLS를 운영해야 한다. Federation Client에는 fake transport 단위시험과, 서로 다른 hostname·인증서·Directory 저장소를 둔 두 loopback HTTPS listener 사이의 실제 TLS/HTTP wire 시험이 있다. 이들은 독립 운영 provider, 공용 TLS PKI/DNS, 서로 다른 네트워크, network partition, rate/cost 운영 정책을 검증하지 않는다. hostname 분리는 조직/인프라 독립성을 증명하지 않는다. Directory는 identity별 publish 권한이나 anti-Sybil 보장을 제공하지 않는다. exact-history provider agreement는 stale mirror 공모를 cryptographically 방지하지 않는다. `global_identity_hint`는 결정적 pseudonym이며 열거/상관관계를 막지 않는다. 주소 봉투의 wire 형식은 엄격히 검사하지만 recipient 개인키 없이 암호화 tag를 검증할 수는 없다. 실제 signaling endpoint/ICE 후보의 운영 정책 및 UI 연결은 M3 후속 통합이 필요하다.

## 검증 벡터

- Go: `internal/discoveryprotocol.TestSignedAdvertisementSharedVector`
- Node.js: `node --test protocol/discovery-v1/vector.test.mjs`
- Go/browser envelope vector: `internal/discoveryprotocol.TestAddressEnvelopeSharedVector`, `web/src/identity/discovery.test.ts`

두 구현은 고정된 canonical bytes와 서명을 교차 검증한다. 이 시험은 실제 다른 노드, bootstrap 제공자, signaling, NAT traversal 또는 TURN 연결을 검증하지 않는다.

## 보안 후속 조건

Public deployment 전에는 identity별 publish 권한, hint 열거 위험 처리, independently operated provider federation, per-identity/service quotas 및 operational retention을 확정해야 한다. Authenticated signaling, SDP/ICE replay·cross-node binding·IP leak·ICE restart·TURN credential quota 및 backpressure를 실제 network tests로 검증해야 한다. Local Pion test는 public signaling, 별도 network direct path 또는 TURN relay 시험이 아니다.
