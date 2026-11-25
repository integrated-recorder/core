# ADR-0001: v0.2 cryptographic profile

- 상태: M0 baseline 승인. 구현은 각 후속 milestone에서 해당 primitive의 실제 API/security review를 거쳐야 한다.
- 문맥: Go Runtime Host와 browser JavaScript 사이에서 서명, 복구, peer transport 및 archive encryption을 상호운용 가능하게 해야 한다.
- 선택: 기존 알고리즘 협상 없이 protocol version이 suite를 고정한다. Unknown suite는 fail closed다.

| 용도 | v1 선택 | 식별·검증 규칙 |
|---|---|---|
| Identity/Node event signature | Ed25519, RFC 8032 | raw 32-byte public key, 64-byte signature, 명시된 domain separator |
| Browser account approval | WebAuthn ES256 / P-256 | WebAuthn assertion 검증. `clientDataJSON`, challenge, RP ID hash, origin, UP/UV 정책, credential 및 signature를 모두 검증. 임의 메시지에 대한 raw WebAuthn signature로 취급하지 않음 |
| Recovery entropy | CSPRNG 256-bit | signing과 encryption 용도는 서로 다른 HKDF-SHA-256 info label로 파생. Seed/private key는 browser recovery client에만 남음 |
| Global identity ID | SHA-256 truncated to 128 bits | `SHA-256("IR/global-identity-id/v1\0" || recovery_signing_public_key)[0:16]`, `gi_` + unpadded base64url; mnemonic-only restore recreates the same identity ID |
| Recipient key wrapping | HPKE RFC 9180: DHKEM(X25519, HKDF-SHA256) + HKDF-SHA256 + AES-256-GCM | HPKE Base mode 자체는 sender authentication·replay prevention을 제공하지 않으므로 envelope에 별도 authority signature, recipient binding, key epoch 필요 |
| Archive chunk encryption | AES-256-GCM | 독립 bounded chunk. 객체마다 CSPRNG 256-bit salt로 DEK에서 별도 object key를 HKDF-SHA-256 파생하고, 그 키 안에서 chunk index nonce를 사용한다. 정확한 binary/AAD contract는 ADR-0002와 양방향 Go/browser vector에 고정한다. |
| Signed JSON | RFC 8785 JCS profile | Duplicate object name 거부, 숫자 금지, integer는 decimal string, ASCII schema key 제한, Unicode code point 보존 |
| Key ID / event hash | SHA-256 with domain separator | 접두사가 다른 용도 간 해시 재사용을 막음 |

## 결정 이유와 경계

- M1 recovery file wire format: `IRRC` magic 4 bytes, version byte `1`, entropy 32 bytes, domain-separated SHA-256 앞 16 bytes checksum. Checksum은 손상 탐지이지 암호화나 악의적 변경 방지 MAC이 아니다. File과 mnemonic은 동일 entropy를 담는다.

- Ed25519는 Go 표준 라이브러리와 브라우저/Node 구현으로 동일 fixture를 만들 수 있고 event signing에서 WebAuthn assertion과 역할이 분리된다.
- WebAuthn은 HTTPS RP에서 사용자 검증을 하는 credential ceremony이며 global identity key material이나 복구키 저장소가 아니다.
- HPKE는 공개키 수신자별 archive DEK wrap에 사용한다. TURN/Discovery/replica는 private key나 평문 DEK를 받지 않는다.
- JCS는 JSON 필드 순서/공백의 차이를 제거한다. Protocol profile에서 숫자를 금지해 JavaScript의 정밀도 차이를 없앤다.
- 객체 salt 충돌 확률은 256-bit CSPRNG에 의존한다. 같은 입력의 재시도도 새 salt를 생성하며, 절대로 DEK를 직접 AES-GCM 키로 사용하지 않는다. Vector와 retry 시험은 형식 상호운용성을 확인하지만, 이 설계의 독립 암호 검토와 production archive/replication 통합은 별도 출시 gate다.
- 암호 라이브러리 구현, seed phrase wordlist/KDF, signing/AEAD API는 별도 security review가 필요하다. 자체 암호 primitive를 구현하지 않는다.

## 현재 검증 상태

M5에서 Go 표준 `crypto/hpke`와 browser `hpke-js`를 고정한 X25519/HKDF-SHA-256/AES-256-GCM envelope 구현, bounded chunk object format, 변조/잘못된 identity 거부 및 Go↔browser 시험 벡터를 추가했다. 이것은 primitive/API 간 호환성 검증이다. `cryptoarchive`는 아직 RC12 archive catalog, immutable snapshot, encrypted replica, key authority history 또는 Capability에 연결되지 않았다. 복구 키는 browser memory에서 사용되고 서버 API로 보내지지 않는다. 부분 출력은 staging sink에서 폐기되어야 한다. 이 상태로 M5나 v0.2.0을 완료했다고 선언할 수 없다.

## 참조

- [RFC 8032 — EdDSA](https://www.rfc-editor.org/rfc/rfc8032.html)
- [RFC 8785 — JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785.html)
- [RFC 9180 — Hybrid Public Key Encryption](https://www.rfc-editor.org/rfc/rfc9180.html)
- [W3C Web Authentication Level 3](https://www.w3.org/TR/2026/REC-webauthn-3-20260825/)
