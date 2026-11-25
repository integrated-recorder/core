# Capability / OwnerSubject v1 구현 초안

상태: M6 구현 중. 서명된 OwnerSubject event의 로컬 durable append/fork freeze, 보호된 API, HTTPS Discovery mirror를 통한 history 교환과 원격 peer command 직전의 exact-provider-set 확인이 구현됐다. Recorder는 opaque mirror를 권위로 믿지 않고 각 이벤트를 Global Identity의 서명된 해당 시점 head로 재검증하고 영속 로그에 병합한다. Provider 합의는 운영상 freshness quorum일 뿐 cryptographic transparency proof가 아니다. Capability가 peer data commands에 일부 강제 적용되며 archive 제품 흐름·transfer 재패키징과 실제 분산 환경 acceptance는 완료되지 않았다.

## OwnerSubject

- `OwnerSubject`는 archive authority의 안정적인 소유자 ID다. Global Identity, passkey credential, node ID, RC12 사용자 ID와 서로 다른 식별자다.
- v1에서 활성 지원 유형은 `person`이다. `group`은 거부되며 사용 가능한 그룹 권한처럼 표시하지 않는다.
- 사람 subject는 하나의 controller principal, 고정된 recovery-root controller policy, decimal-string `policy_epoch`를 가진다. 이 epoch는 Global Identity event sequence와 별도다. 새 subject ID는 128-bit CSPRNG에서 생성한 `os_<base64url>` 값이다.
- RC12 archive를 subject에 연결하는 것은 명시적이고 원자적인 claim/migration 승인 작업이어야 한다. 로그인 이름으로 기존 owner를 자동 추론하지 않는다.
- `subject_create`의 선택적 정렬 `archive_ids` 또는 이후 `archive_claim` 이벤트는 controller recovery root가 서명한다. 한 authority store 안에서는 모든 subject 이력과 유효 fork 증거를 확인해 한 Archive ID의 중복 claim을 거부한다. claim되지 않은 Archive ID에 대한 grant는 거부하며, 이 연결은 archive 바이트나 RC12 recording root를 수정하지 않는다.
- 이 로컬 검사만으로 독립 노드 사이의 전역 중복 방지는 제공하지 않는다. Configured Discovery providers에 같은 subject history의 bounded 후보를 교환하고 각 원격 command마다 전체 provider set의 byte-equivalent 응답을 요구한다. Recorder가 서명과 Global Identity history를 직접 검증하고 유효한 분기 후보를 모두 import해 fork를 동결한다. 이는 provider 운영자 집합의 freshness quorum이며 stale replay를 암호학적으로 증명하거나 두 공모 provider를 견디지 않는다.

## OwnerSubject 복제 보존 정책 event

`replication_policy_set`은 현재 OwnerSubject controller의 recovery root가 서명하는 append-only authority event다. payload는 `policy` 객체 하나이며 다음 canonical decimal string 필드를 포함한다: `minimum_replicas` (0–16), `grace_period_seconds` (0–2,592,000), `capacity_limit_bytes` (0–1,125,899,906,842,624), `relocate_after_grace` (boolean). 누락 필드, 알 수 없는 필드, 비정규 숫자, 상한 초과 값, delegated Node 서명은 거부한다. 정확한 이벤트 재전달은 idempotent하다.

기존 authority event log는 재생 시 `minimum_replicas=0`, `grace_period_seconds=0`, `capacity_limit_bytes=0`, `relocate_after_grace=false`를 기본값으로 얻으므로 기존 subject ID, Archive claim, grant, key와 서명이 변하지 않는다. 복제본 수 0은 자동 복제 비활성화, 용량 상한 0은 무제한을 뜻한다. transfer 후 OwnerSubject의 보존 목표는 이어지지만 새 controller만 다음 정책 event를 서명할 수 있다. 이 event는 설정을 보존하는 것뿐이며 현재 automatic scheduling/provider selection은 적용하지 않는다. 모르는 operation을 거부하는 이전 구현은 이 event가 포함된 최신 이력을 소비할 수 없으므로, 혼합 버전 노드에서는 업데이트 전까지 해당 이력을 사용해 관리 명령을 실행하지 않고 fail closed해야 한다.

## Grant wire profile

`ir.capability.grant/1`은 RFC 8785 호환 canonical JSON과 domain-separated Ed25519 서명을 사용한다. 숫자는 허용하지 않고 depth/epoch 같은 정수는 정규 decimal string이다. JSON parser는 중복 키, 알 수 없는 필드, trailing value, 과대 입력을 거부한다.

Authority event 및 subject의 시각은 정규 UTC 초 단위 RFC3339(`YYYY-MM-DDTHH:MM:SSZ`)로 인코딩한다. 브라우저의 `Date.toISOString()`을 그대로 서명하면 밀리초가 포함될 수 있으므로, 서명 전에 프로토콜 형식으로 초 단위 정규화해야 한다. 서버는 서명 검증을 위해 허용 형식을 느슨하게 바꾸지 않는다.

Grant는 다음 권한을 명시한다.

- OwnerSubject, issuer principal, audience principal
- 정확한 Archive ID와 선택적인 Snapshot 제한
- 정렬되고 중복 없는 action 목록
- `not_before`, `expires_at`, 위임 depth, 일회성 nonce
- 선택적 parent grant hash와 policy epoch
- 현재 신원 이력에서 resolve되는 issuer key ID

지원 action은 `archive.view`, `archive.export`, `archive.replicate`, `archive.annotate`, `archive.manage`, `archive.delegate`, `archive.transfer`, `archive.delete`, `node.manage`, `node.enroll`, `identity.manage`, `key.rotate`다. 공식 `view`와 `export`는 UX 및 API 권한 구분이며, 이미 받은 평문 복사를 기술적으로 금지하는 DRM은 아니다.

## 위임 규칙

1. Root grant는 현재 OwnerSubject controller가 서명하고 parent hash가 없어야 한다.
2. 하위 grant issuer는 바로 위 grant의 audience와 같고, 그 issuer key는 현재 검증된 신원 상태에서 활성 상태여야 한다.
3. parent에 `archive.delegate`가 있어야 한다. child action은 parent action의 부분집합이어야 한다.
4. child는 동일 archive를 지정한다. parent가 특정 snapshot이면 child도 정확히 같은 snapshot을 지정한다. archive 전체 권한에서 특정 snapshot으로 좁히는 것은 허용한다.
5. child 시작 시간은 parent보다 빠를 수 없고 만료 시간은 parent보다 늦을 수 없다. 위임 depth는 매 단계 줄어든다.
6. chain은 최대 16 grant, depth는 최대 8이다. action, scope, 시간, epoch, audience의 불일치는 fail-closed다.
7. 검증기는 frozen identity, 불일치한 OwnerSubject policy epoch, 만료된 freshness, chain 안의 알려진 폐기를 거부한다. Global Identity head와 OwnerSubject policy epoch를 같은 카운터로 취급하지 않는다. 원격 identity history는 각 인가 동작마다 모든 설정 provider에서 다시 조회한다. 이 provider 합의는 조회 시점에 대한 운영상 freshness 신호이며 암호학적 최신성 증명은 아니다.

## 소유권 이전

`ir.owner-subject.transfer/1` offer는 현재 controller의 identity head와 새 controller의 별도 Global Identity head에 결속한다. 기존 controller는 offer domain으로 서명하고, 새 person controller는 같은 canonical offer에 acceptance domain 서명을 추가한다. 양쪽 신원 모두 현재·비동결 상태여야 하며, offer는 최대 24시간이고 policy epoch는 정확히 1 증가해야 한다. 검증 결과는 동일 OwnerSubject ID에 새 controller/epoch를 지정하고 이전 epoch를 무효화하며 `rekey_required`를 반환한다.

Authority projection은 현재 검증된 Global Identity genesis에 서명된 X25519 recovery encryption public key도 보존한다. 이 값은 로컬·등록 이력의 서명 검증 이후에만 생성되며 API 요청이 제시한 임의 키를 신뢰하는 데 사용하지 않는다. 이는 새 Owner에게 향하는 DEK envelope를 검증하기 위한 신뢰 입력이다.

`internal/authority`의 signed event store는 transfer event를 단일 순서로 기록하며, 동일 parent에서 발생한 서로 다른 유효 successor를 자동 선택하지 않고 동결한다. exact retry는 idempotent하다. transfer event 재생은 당시 claim된 Archive ID별 `rekey_required` 상태를 복구한다. 재키잉이 끝나지 않은 기존 Archive에는 새 Capability 발급과 인가를 거부하며, 별도로 새로 claim된 Archive까지 불필요하게 잠그지 않는다. 이 상태는 서명된 durable event에서 재구성되므로 재시작 후에도 유지된다.

`/api/owner-authority/subjects` 응답은 해당 subject의 `rekey_required_archives`를 정렬된 Archive ID 배열과 이를 만든 transfer event hash로 반환한다. Settings UI는 이를 명시적으로 경고하고 해당 Archive용 Capability 발급을 비활성화한다. API는 같은 거부를 HTTP 409 `archive_rekey_required`로 분류한다. 이 projection과 오류 코드는 잠금 상태를 설명할 뿐 잠금 해제 증거가 아니다.

`replication.Store.RekeySnapshot`은 `key.rotate`와 `archive.replicate` 권한을 확인하고, 기존 key epoch의 모든 ciphertext object가 이전 DEK로 검증되는지 확인한 뒤 더 높은 epoch 및 별도 DEK로 완전한 새 암호문 bundle을 만든다. `RekeyArchiveHistory`는 이 동작을 catalog의 전체 snapshot predecessor chain에 적용하고, 전체 bundle 성공 후에만 수신자 envelope를 저장한 뒤 이전·신규 epoch를 다시 검증한다. 중간 실패 시 Authority gate는 그대로 잠기며 이미 게시된 불변 bundle은 동일 key/epoch로 안전하게 재시도할 수 있다. 기존 epoch는 rollback/audit을 위해 보존되고, 기존 object set이 손상됐거나 이전 DEK를 재사용하면 새 epoch를 게시하지 않는다. 이 저장소 primitive는 새 Owner용 envelope 전달 또는 수신측 envelope 열기 확인, 새 controller의 completion 서명 UI 및 durable `rekey-complete` authority event 연결을 수행하지 않는다.

`replication.Store.VerifyRekeyHistory`는 로컬 catalog의 전체 snapshot predecessor chain을 고정해 이전·새 epoch의 모든 object를 각각 복호화하고 snapshot의 immutable plaintext digest와 대조한다. 또 새 epoch envelope의 수신 공개키 식별·hash와 기존 identity 서명을 확인하고 비밀이 아닌 history commitment를 반환한다. 이 검사는 envelope가 실제 새 DEK를 담았는지, 수신자가 envelope를 열었는지, 별도 노드에 암호문이 도달했는지를 증명하지 않는다. 검증 결과만으로 `rekey_required`를 해제해서는 안 된다.

`archive_rekey_complete` authority event는 새 controller recovery root가 transfer event hash와 모든 대기 Archive ID에 대한 정렬된 key epoch, snapshot head, bundle commitment, envelope hash, recipient key ID를 서명해 제출할 때만 잠금을 지운다. Authority는 event의 현재 controller 서명, transfer binding, evidence 형식, pending Archive 전체 포함 여부 및 각 envelope recipient ID가 검증된 새 controller의 recovery encryption public key에서 계산된 ID와 일치하는지 재검증하고 replay 시 같은 결과를 재구성한다. 이 payload는 새 controller의 서명된 완료 선언이며, Authority 서버가 복구 개인키를 받아 재복호화한다는 뜻도, envelope의 HPKE 평문을 서버가 직접 확인한다는 뜻도 아니다. Settings UI는 수신 브라우저에서 envelope를 열고 전체 snapshot predecessor chain, 두 epoch manifest commitment, 새 epoch ciphertext 복호화·plaintext digest를 확인한 경우에만 evidence 파일을 만들고, 새 controller는 pending 전체의 evidence를 제출해 완료 이벤트를 서명한다. 이 검증은 서버가 아니라 신뢰하는 클라이언트 코드에서 수행되며, recovery root를 가진 임의 클라이언트는 프로토콜상 완료 선언을 직접 서명할 수 있다. 서로 다른 운영자·장비 간 UI 수렴과 adversarial acceptance는 별도 검증이 필요하다.

현재 HTTPS 전용 `/api/recordings/{id}/replicas/rekey`와 authenticated Engine IPC는 fresh federation-verified recipient identity를 Control Plane에서 resolve하고, source Node의 `key.rotate` 및 `archive.replicate` grants를 재검증한 뒤 이전 DEK로 전체 immutable snapshot chain을 확인·재암호화하고 새 recovery recipient용 signed envelope를 준비한다. 이전 plaintext claim을 현재 sealed inventory에서 찾을 때 snapshot의 정확한 coordinate/size/digest와 일치하는 verified payload만 허용한다. 이 operation은 partial immutable output 뒤 같은 epoch/key로 retry할 수 있고 authority gate를 변경하지 않는다. UI는 준비 파일을 통한 수신자 검증과 완료 event signing을 연결한다. 수신자는 이전 DEK를 받지 않고 새 envelope와 전체 history를 직접 검증한다. 서버는 브라우저 복호화를 다시 수행하지 않으므로 완료 event는 새 controller의 서명 선언이며 임의 클라이언트가 신뢰 경계를 우회해 서명할 수 있다는 위협 모델 한계가 남는다. 이전 복호화 키 권한을 새 소유자 권한으로 간주하지 않는다.

## 신뢰 경계 및 미완료

`internal/authority`는 현재 서명 grant chain을 검증하는 핵심이다. Grant 발급은 현재 OwnerSubject 이력에 Archive ID가 명시적으로 claim된 뒤에만 허용된다. 검증 호출자는 `IdentityState`를 verified Global Identity history에서 구성해야 하며, 임의의 peer가 제공한 active key/revocation map을 넣어서는 안 된다. Discovery와 transport는 이 상태를 권위 있게 만들지 않는다.

`internal/authority.Store`는 `security/owner-authority-v1.json`에 권한 이력을 0600 원자 교체 방식으로 저장하고, sequence/hash 연결을 재시작 때 재검증한다. 서로 다른 유효 successor는 fork 증거와 함께 동결하며, 잘못된 서명은 fork로 취급하지 않는다. 첫 subject 생성의 정확한 재시도는 idempotent하고 동시 경쟁 생성도 동결한다. Authority event는 해당 identity head에서 활성 상태인 root 또는 delegated Node key로 서명할 수 있지만, subject 생성·이전·archive claim은 controller root만 수행하고 grant 발급은 chain의 leaf issuer, grant 폐기는 controller 또는 해당 grant의 issuer만 수행한다. 동일 저장소 내 Archive claim은 파일 잠금 안에서 중복을 막고, claim된 scope는 다른 OwnerSubject가 이미 수락했거나 유효 fork 증거에 나타나면 거부한다. 자식 grant 제출은 이미 저장된 동일 parent prefix를 재사용할 수 있고, 새 grant는 history 전체에서 이미 사용된 nonce를 재사용할 수 없다. `/api/owner-authority/*`는 로컬 사용자 RBAC와 CSRF 검사를 통과해야 하며, 공개 응답에는 개인키나 로컬 인증 비밀이 포함되지 않는다. UI는 browser-held recovery key로 서명한 OwnerSubject 생성·Archive claim 및 Capability 발급·폐기를 지원한다. sealed archive의 Archive/Snapshot ID는 녹화 ID로 읽기 전용 조회할 수 있지만, 이 API는 snapshot 본문·digest·경로를 반환하지 않는다.

다음은 아직 구현/검증되지 않았다: Discovery 운영자들이 공모하지 않는다는 가정 외의 cryptographic authority freshness, 다른 Global Identity controller가 소유한 subject의 안전한 mutation 흐름, archive product API 전체의 Capability enforcement, 소유권 이전의 DEK envelope 재패키징, lease/tombstone을 포함한 외부 management command 전체의 end-to-end 최신성, 서로 다른 장비·네트워크에서의 adversarial test. 실제 두 노드가 이력을 교환하고 revoke/fork를 일관되게 차단하는 독립 acceptance도 아직 없다. 따라서 이 단계만으로 M6 완료나 v0.2.0 릴리스 GO를 판정하지 않는다.
