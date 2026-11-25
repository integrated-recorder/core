# ADR-0002: archive object encryption and key envelope v1

- 상태: M5 구현 진행 중. Wire format과 교차 언어 시험, immutable snapshot에서 ciphertext-only local replica store까지의 제한된 구현은 있다. 전체 archive lifecycle/peer transport 통합과 독립 보안 검토는 미완료다.
- 범위: 기존 RC12 원본 archive 파일은 변경하지 않는다. 이 형식은 이후 복제/암호화 object layer에서만 사용한다.
- 신뢰 경계: DEK는 256-bit CSPRNG 값이다. Recorder가 이를 보호하며 untrusted replica, Discovery, TURN에는 보내지 않는다. 공개키 수신자를 위한 DEK 전달은 HPKE Base mode와 recovery-authority Ed25519 envelope signature를 사용한다.

## Object format `IRAC v1`

| Offset | Length | Encoding |
|---|---:|---|
| 0 | 4 | ASCII `IRAC` |
| 4 | 1 | format version `1` |
| 5 | 4 | chunk size, uint32 big-endian (`65536`) |
| 9 | 8 | plaintext size, uint64 big-endian |
| 17 | 32 | fresh CSPRNG object salt |
| 49 | variable | consecutive independently sealed chunks |

Each object derives a 256-bit AES key with HKDF-SHA-256:

```text
IKM  = archive DEK
salt = 32-byte object salt
info = UTF8("IR/archive-object-key/v1\0") || JCS(object_binding)
```

`object_binding` fixes archive ID, complete canonical source coordinate, object type, plaintext size/hash, format version and key epoch. Integers in signed/canonical metadata are decimal strings. The original payload SHA-256 is computed before encryption and remains a separate identity from the ciphertext digest.

Chunk `i` uses AES-256-GCM, a 128-bit tag and 96-bit nonce `00000000 || uint64_be(i)`. Its AAD is:

```text
UTF8("IR/archive-chunk-aad/v1\0") || JCS({
  archive_id, object_coordinate, object_type, plaintext_size,
  plaintext_sha256, chunk_index, byte_offset,
  crypto_format_version, key_epoch
})
```

The DEK is never used directly as the object AEAD key. A fresh 256-bit salt is generated on every encryption attempt, so retries derive a distinct key/nonce domain. Implementations must fail closed if a hash, size, coordinate, epoch, tag, or trailing-byte check fails. Encrypt/decrypt APIs write only to an unpublished staging sink; callers must discard all output on any error and publish after final whole-object digest verification.

## DEK envelope `ir.key-envelope/1`

The suite is fixed, with no negotiation: RFC 9180 Base mode `DHKEM(X25519, HKDF-SHA256) / HKDF-SHA256 / AES-256-GCM`. HPKE `info` and `aad` both bind the envelope version, archive ID, epoch, recipient key ID/public-key digest and signer key ID. The envelope separately contains `enc`, ciphertext, a SHA-256 binding digest and an Ed25519 signature over the canonical unsigned fields plus binding digest.

HPKE Base mode does not authenticate a sender or establish current authority. Before opening an envelope, the caller must verify the signer against the current non-forked identity-history head, authority policy, owner subject, capability and key epoch. A cryptographically valid signature alone grants no archive access. Stale/revoked signers or frozen history must not be accepted.

The recovery X25519 private scalar is derived from the existing 256-bit recovery entropy with the existing domain-separated HKDF label. It is used in the browser only and never included in requests. For initial source encryption, the browser sends the DEK over authenticated HTTPS to the trusted local Control Plane, which forwards it over authenticated Engine IPC; neither response nor peer APIs return it. The Engine clears its short-lived input and persists only the Node-Key-wrapped local copy. A retry may omit both DEK and envelope: the Control Plane reloads the immutable signed envelope and the Engine recovers the DEK from its local wrapped slot, after the request's current capability and envelope-signature checks.

## Test vectors and implementation

- `protocol/cryptoarchive-v1/object-test-vector.json`: deterministic Go encryption and browser decryption vector.
- `protocol/cryptoarchive-v1/browser-object-test-vector.json`: browser encryption and Go decryption vector.
- `protocol/cryptoarchive-v1/envelope-test-vector.json`: browser-created signed HPKE envelope opened and signature-verified by Go.
- `protocol/cryptoarchive-v1/go-envelope-test-vector.json`: Go-created signed HPKE envelope opened by the browser.
- All vector keys and payloads are public test data and must never be used in production.
- Go object implementation: `internal/cryptoarchive`.
- Browser streaming encrypt/decrypt and envelope implementation: `web/src/identity/cryptoarchive.ts`.
- Dependencies: Go standard `crypto/hpke` and `@hpke/core` plus `@hpke/dhkem-x25519` for browser X25519. The project has not completed an independent audit of these protocol integrations; package-level tests and RFC interoperability do not replace one.
- `internal/replication.Store` durably stores the signed envelope under an immutable `(archive, recipient, key epoch)` slot with private filesystem permissions. A source Engine additionally stores the DEK as an immutable, locally wrapped `(archive, key epoch)` value. The wrapping key is HKDF-derived with a dedicated domain from the local Node Key seed; it is not exported, and the recovery private key remains browser-only. This protects the DEK file from independent disclosure, while the local Runtime Host/Node security directory remains trusted: compromise of that directory can recover the DEK. Replica nodes receive ciphertext and only the separately authorized envelope addressed to them. This storage operation checks canonical structure and binding, not signer authorization; clients must verify signer freshness and authority against current, unfrozen history before opening the envelope. Replacing an existing slot is rejected; rotation uses a new epoch.
- Source encryption retries can omit the clear DEK and envelope after their durable immutable slots exist. The Control Plane still resolves the current identity root and verifies the envelope signature and replication grant on each request; the Recorder Engine accepts only a no-key IPC request and recovers the key internally through the Node Key protector. No retry API returns the recovered bytes.

## Release gates still open

1. Independent cryptographic review, including side-channel and nonce/salt lifecycle review.
2. Complete automated DEK lifecycle management, recipient selection, explicit key rotation and key revocation policy. Initial DEK creation remains browser-triggered; source-side wrapped-key recovery is implemented for retries, but no background coordinator rotates or packages keys automatically.
3. Validate identity freshness and OwnerSubject/Capability revocation across independently operated nodes and Discovery providers; local Control Plane checks and package tests do not establish cross-node freshness under provider failure or collusion.
4. The authenticated peer transport carries ciphertext ranges and separately authorized recipient-bound envelopes; local Pion tests verify this boundary, but no independently operated, two-network acceptance has run.
5. Extend crash, partial-publication, retry, key-rotation and revocation tests across process restarts and actual independent storage providers.
6. Integrate restored original bytes into a playable archive on two independent nodes. A local vertical test now covers sealed RC12 payload → snapshot → ciphertext → signed recovery envelope → byte-exact restore and HLS projection; the restored tree is still separate from the Core archive/VOD API and no two-node acceptance has run.
7. Real-device iOS Safari P2P playback/seek acceptance remains an M8 gate.

Until these gates pass, encrypted archive functionality is not release-ready and M5 remains in progress.
