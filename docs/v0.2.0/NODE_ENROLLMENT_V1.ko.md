# Node Enrollment/Event v1 — M2 진행 중

이 문서는 구현된 노드 등록·이력 교환 경로와 아직 별도 acceptance가 필요한 경계를 구분한다. UI는 5분짜리 공개 등록 증명을 QR로 표시하고 Owner 화면에서 카메라로 스캔할 수 있으며, JSON 파일과 수동 텍스트 경로도 유지한다. QR은 증명 전달 UX일 뿐 pairing channel, 노드 인증 또는 실제 네트워크 discovery/peer sync가 아니다.

## Stable Node ID

`node_id = "nd_" || base64url(SHA-256("IR/node-id/v1\0" || raw_ed25519_public_key)[0:16])`.

Node ID는 공개 Node Key에서 파생되며 Global Identity ID 및 Installation ID와 구별된다. Node Key 개인키는 Global Identity recovery root와 독립적이어야 한다.

## Recovery-root authorized event

`node_delegate`는 recovery signing key가 서명한 `ir.identity.event/1` successor다. payload는 정확히 다음 필드를 가진다.

```json
{
  "enrollment_nonce": "<32-byte base64url>",
  "node_id": "nd_<16-byte-base64url>",
  "node_proof": "<64-byte Ed25519 signature, base64url>",
  "node_public_key": "<32-byte Ed25519 public key, base64url>"
}
```

Node proof message는 다음 raw bytes다.

```text
IR/node-enrollment/v1/pop\0
|| identity_id || \0
|| previous_event_hash || \0
|| enrollment_nonce
```

이 필드는 대상 Node Key의 소유 증명을 서명된 이력에 결속한다. `internal/nodeidentity`는 32-byte nonce를 생성하고 현재 identity/head에 묶어 5분 동안 유효하게 저장한다. 한 번 소비된 nonce는 같은 event에 대해서만 멱등 재시도를 허용하며, 다른 event로 재사용할 수 없다. 대상 노드는 공개 증명을 QR 또는 JSON으로 보여 주고 Root는 이를 스캔하거나 가져온 뒤 브라우저의 recovery key로 `node_delegate` successor를 서명한다. QR scanner는 HTTPS 또는 localhost의 secure context에서만 카메라를 연다. QR payload는 16 KiB 제한 및 exact-field schema로 파싱되며, identity/head/만료 검증과 서버의 nonce·Node proof 검증은 기존 경계를 그대로 사용한다. Component test는 scanner lifecycle을, desktop WebKit E2E는 실제 QR 이미지 encode/decode 왕복을, UI 통합 test는 스캔 문자열에서 기존 signed delegation까지를 확인한다. 실제 카메라나 독립 장비 간 물리 QR 인수시험은 아니다.

`node_revoke` payload는 `{"node_id":"nd_..."}` 하나만 허용한다. 위임과 폐기는 Global Identity recovery signing key가 승인한다. Owner Control Plane API는 로컬 계정 권한·CSRF와 root 서명을 모두 확인한다. Chromium UI E2E는 격리된 저장소를 가진 두 HTTP listener를 별도 native OS 프로세스로 실행하고, 서명 이력을 파일로 옮겨 위임·폐기한 Node의 권한 집합을 확인한다. 두 프로세스는 같은 개발 장비와 loopback 네트워크를 사용한다. E2E는 두 번째 프로세스를 재시작한 뒤 로컬 Node ID와 폐기된 위임 이력이 유지되는지도 확인한다. 이는 별도 장비·네트워크, 오프라인 분할, 물리 Passkey 또는 fork-resolution acceptance가 아니다. 현재 Node Key가 원격 management authority를 갖거나 Discovery provider가 권한을 발급하지 않는다.

## Fork 처리

저장소는 현재 알려진 head에서 연속 successor만 정상 적용한다. 이미 수락된 이력의 parent에 대해 다른 cryptographically valid successor가 들어오면 수락된 event와 경쟁 event의 canonical bytes 및 signature를 함께 영속 보존하고 `frozen=true`로 설정한다. `HistoryBundle`은 fork를 관찰했을 때 각 직접 successor 뒤에서 함께 관찰한 서명 이벤트 suffix도 선택 없이 보존한다. 동결 후 동일한 직접 fork의 더 긴 suffix를 다시 받으면 기존 transcript의 prefix 확장인 경우에만 증거를 추가한다. 잘못된 서명이나 서로 다른 continuation은 권한 상태를 변경하지 않는다. 저장된 head는 수신 순서상 먼저 있던 branch를 가리킬 수 있지만 동결 중에는 어느 branch도 권한 근거로 사용할 수 없다. 시간, 순번, arrival order로 승자를 고르지 않는다.

동결 상태에서는 credential 및 Node Key 권한 변경을 거부한다. 관리 UI는 두 검증된 branch head를 보여 주며, 선택 전에 운영자가 서명된 공개 이력 bundle을 내보내 transcript를 검토할 수 있다. 복구 root가 서명하는 `fork_resolve`는 `fork_id`, 공통 parent, 양측 관찰 head, 선택한 head를 모두 결합하고 선택된 branch 끝에 append된다. 선택되지 않은 branch 및 중첩 fork transcript는 증거로 계속 보존한다. 중첩 fork의 parent가 명시적으로 폐기된 ancestor branch에 속하면 그 fork는 unresolved authority 충돌에서만 제외되며 증거 자체는 제거되지 않는다. 불완전하거나 선택 이력을 재구성할 수 없는 증거는 자동 정리하지 않고 frozen 상태를 유지한다. Resolution은 RBAC·CSRF 보호 API로 제출되고 재시작 후 및 다른 노드/registry로 history sync 시 서명과 branch 관계를 다시 검증한다.

## 현재 구현된 Node Key 저장

`internal/nodeidentity`는 `security/node-key-v1.json`에 설치별 Ed25519 개인키를 0600 파일로 저장하고, 디렉터리 접근도 private로 제한한다. 키는 생성 시에만 저장되며 재시작 후 동일한 Node ID를 읽는다. 손상되거나 권한이 잘못된 파일은 fail-closed 처리하며 자동 회전하지 않는다. `security/node-enrollment-challenge-v1.json`은 verifier nonce와 허용된 event hash를 보호 저장한다. Control Plane의 상태·생성·challenge API는 로컬 RBAC와 CSRF를 요구하고 공개 Node ID·공개키·증명만 반환한다. Node Key 개인키는 API에 노출하지 않는다.

M3의 첫 protocol slice는 `POST /api/node-identity/advertisement/sign`이다. 로컬 RBAC/CSRF, identity 미동결, 현재 signed history의 Node delegation을 확인한 후 보호 Node Key로 5분 TTL 광고에 서명한다. `security/node-advertisement-sequence-v1.json`은 0600 원자 저장과 디렉터리 sync를 사용해 Node마다 sequence 재사용을 막는다. 서명은 후보 위치 광고만 인증하며 원격 권한을 만들지 않는다. 수신 측은 head, 위임, 폐기 및 최신성을 다시 검증해야 한다. 주소 필드는 bounded HPKE envelope 형식과 Go/browser 복호화 벡터를 갖고, signing endpoint는 recipient digest가 해당 identity의 공개 recovery encryption key와 일치하는지 확인한다. 다만 recipient private key 없이 AEAD tag/HPKE context는 검증할 수 없으며, 실제 주소 payload 작성 및 Directory federation 연결 UI도 아직 없다. 수신기는 NodeAdvertisement 서명과 identity freshness/위임을 검증한 다음 recovery key로 복호화해야 한다. 이 구현을 authenticated signaling이나 독립 노드 Discovery acceptance로 간주하지 않는다.

Global Identity store v2는 기존 v1 credential inventory를 `complete`로 보존하며, 새로 이력을 가져온 설치는 로컬에 실제 등록된 Passkey만 `local` inventory로 관리한다. `HistoryBundle`은 recovery root가 서명한 공개 event와 fork 증거만 전달하고 WebAuthn credential object, user handle, Node Key 개인키 또는 recovery secret은 포함하지 않는다. Import는 identity ID를 고정하고 stale bundle로 head를 rollback하지 않는다. 처음 join할 때는 해당 로컬 Node Key의 활성 `node_delegate`와 유효한 일회성 proof가 모두 필요하다.

같은 설치에서 여러 identity의 공개 history가 필요한 경우 별도 `HistoryRegistry`가 `security/global-identity-registry-v1.json`에 bundle만 보관한다. 로컬 Passkey 상태·Node Key 개인키를 복사하지 않으며 각 bundle의 전체 signature와 선형 successor를 다시 검증한다. 관리자 `system.settings.manage` 및 CSRF 보호를 받는 `POST /api/global-identity/registry/import`만 원격 identity registry를 변경한다. 현재 local identity ID는 이 endpoint에서 거부되고 기존 delegated-node sync 경로를 사용한다. Registry의 `verified_at`은 로컬 import/검증 시각일 뿐 이력이 전역 최신이라는 증거가 아니다. 독립적이고 인증된 다중 경로 최신성 확인이 구현되기 전까지 remote OwnerSubject authorization은 항상 fail-closed된다. Discovery 결과는 registry 신뢰나 최신성을 대체하지 않는다.

## 미완료 M2 경계

- 별도 장비의 카메라·화면을 사용한 물리 QR 교환과 독립 노드 등록 acceptance (UI 및 component/integration 자동 테스트는 있으나 실제 장비 간 시험은 안 함)
- 별도 운영 장비·네트워크에 놓인 두 노드 간 재부팅·재시도·offline/fork acceptance (현재 브라우저 E2E는 별도 native 프로세스와 durable store를 쓰지만 한 장비의 loopback에서 실행)
- 여러 독립 운영 노드에서 fork 증거 교환, 복구 root 승인, 후속 재시작 및 registry 수렴 acceptance (resolution store/API/UI의 로컬 회귀 시험은 추가됐지만 외부 네트워크 인수시험은 아님)

이 항목들이 완료되기 전에는 M2 완료로 표시하지 않는다.
