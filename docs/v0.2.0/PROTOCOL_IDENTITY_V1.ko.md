# `ir.identity.event/1` 프로토콜 계약 초안

상태: M0/M1 구현 기준 초안. 이 계약은 signed identity event의 직렬화·해시·서명을 정의한다. 네트워크 저장소, 최신성 quorum, 분산 이벤트 권한 판정은 아직 구현하지 않으며 서명만 검증해도 권한이 성립하지 않는다. 로컬 M1 genesis는 검증된 WebAuthn 등록 ceremony와 복구 키 서명의 상호 결속을 요구한다.

## 인코딩과 제한

- Wire format은 UTF-8 JSON이며 서명 대상은 RFC 8785 JCS bytes다.
- Property name은 `[a-z][a-z0-9_]*`로 제한한다. 같은 object의 중복 property, invalid UTF-8, lone surrogate, trailing JSON value는 거부한다.
- Signed profile에는 JSON number를 허용하지 않는다. sequence, epoch, byte count 등 정수는 선행 0이 없는 decimal string으로 운반한다. 시간은 UTC RFC 3339 string이다. Unicode string은 NFC/NFD 정규화하지 않는다.
- 알 수 없는 critical field와 event version은 거부한다. 구현이 해당 operation을 이해하지 못하면 event를 authority state에 적용하지 않는다.
- Base64url은 padding 없는 RFC 4648 URL alphabet이다. ID나 hash는 아래 접두사와 길이를 지킨다.

## Event v1

```json
{
  "event_version": "ir.identity.event/1",
  "identity_id": "gi_<16-byte-base64url>",
  "sequence": "0",
  "previous_event_hash": null,
  "operation": "genesis",
  "issued_at": "2026-10-25T00:00:00Z",
  "signer_key_id": "ik_<sha256-base64url>",
  "signer_public_key": "<32-byte-ed25519-public-key-base64url>",
  "payload": {
    "credential_id": "<registered-WebAuthn-credential-id-base64url>",
    "identity_format": "global-identity/v1",
    "recovery_encryption_public_key": "<32-byte-X25519-public-key-base64url>",
    "webauthn_challenge": "<32-byte-WebAuthn-challenge-base64url>"
  }
}
```

- Local M1 genesis는 위 payload 필드를 모두 요구한다. `credential_id`와 `webauthn_challenge`는 동일 등록 요청에서 얻은 검증된 Passkey credential 및 challenge에 결속한다. `recovery_encryption_public_key`는 브라우저에서 복구 entropy로 파생한 유효한 non-low-order X25519 공개키이며 개인 복구키는 서버로 보내지 않는다.

- Genesis만 `sequence="0"`, `previous_event_hash=null`을 사용한다. 이후 event는 연속 sequence와 직전 event hash를 반드시 갖는다.
- `identity_id`는 `"gi_" || base64url(SHA-256("IR/global-identity-id/v1\0" || recovery_signing_public_key)[0:16])`이다. 256-bit CSPRNG recovery root에서 파생되므로 복구 구문만으로 같은 ID를 재구성할 수 있다. Passkey credential ID, RP domain, node ID, installation ID 또는 OwnerSubject ID에는 의존하지 않는다.
- `signer_key_id = base64url(SHA-256("IR/identity-key/v1\0" || raw_public_key))` 앞에 `ik_`를 붙인다.
- `canonical_event = JCS(event)`.
- `signature = Ed25519.Sign(private_key, "IR/identity-event/v1/sign\0" || canonical_event)`.
- `event_hash = "sha256:" || lowercase_hex(SHA-256("IR/identity-event/v1/hash\0" || canonical_event))`.
- Signature property는 event body에 포함하지 않는다. 전송 envelope는 서명과 canonical event를 분리한다. Signature만 검증한 것은 signer의 현재 권한·폐기·최신성 증명이 아니다.

## 체인 적용과 충돌

1. Parser는 duplicate property를 포함한 모호한 JSON을 거부한다.
2. Schema, identity ID, sequence, previous hash, key encoding, signature, event hash를 검증한다.
3. 수신 노드는 이미 신뢰한 head에서만 연속 successor를 적용한다. Discovery 또는 peer가 제공한 임의의 더 긴 체인을 자동으로 정본으로 선택하지 않는다.
4. 같은 `previous_event_hash`에서 두 개의 서로 다른 유효 event가 발견되면 identity를 freeze하고 두 증거를 보존한다. timestamp, sequence 또는 도착 순서로 승자를 고르지 않는다.
5. Fork resolution은 양측 head를 명시적으로 참조하는 별도 승인 event가 필요하다. 오프라인/분할 중 고위험 mutation은 freshness 정책이 허용하지 않으면 거부한다. 이미 진행 중인 로컬 capture는 신원 네트워크 장애로 중단하지 않는다.

## 시험 벡터

`../../protocol/identity-v1/test-vector.json`은 canonical JSON, domain-separated event hash, key ID와 signature를 고정한다. Go와 Node.js 테스트는 동일 fixture를 읽는다. RFC 8032 test seed는 공개 시험 데이터이며 제품 키로 사용할 수 없다. JS harness는 시험 전용 in-memory model로 실제 분산 저장소/합의 구현을 의미하지 않는다.
