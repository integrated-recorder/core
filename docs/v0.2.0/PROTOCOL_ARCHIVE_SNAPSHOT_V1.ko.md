# Archive Snapshot v1 — read compatibility

상태: v1은 이미 저장됐거나 외부에서 받은 snapshot을 읽기 위한 호환 형식이다. 신규 snapshot은 init-map 연결을 포함하는 v2로 생성한다. v1은 v2 playable restoration에 필요한 init-object 관계를 보증하지 않는다.

## ID 및 hash

- 새 recording은 생성 시 독립적인 128-bit 난수 Archive ID를 V2 root의 `archive_id`에 기록한다. 이 값은 로컬 Recording ID와 분리되고 복사본에서 그대로 보존한다.
- RC12 기존 root처럼 `archive_id`가 없는 archive만 `ArchiveIDForRecording`으로 이전 호환 ID를 계산한다. 이 mapping은 `SHA256("IR/archive-id/from-recording/v1\0" || recording_id)`의 앞 128 bit를 Base64url로 표현한다. 기존 archive를 임의로 root에 다시 쓰지 않는다.
- v1 content는 `JCS(content)`에서 계산한다. `content_hash = SHA256("IR/archive-snapshot/v1/content\0" || JCS(content))`; `snapshot_id = "snap_" || hex(content_hash)`. 신규 snapshot은 [v2](PROTOCOL_ARCHIVE_SNAPSHOT_V2.ko.md)의 독립 hash domain을 쓴다.
- `sequence`와 `previous_snapshot_id`는 append-only envelope link이며 content hash에는 들어가지 않는다. v1 Snapshot은 v1 hash domain으로 검증된다.
- Claim source/verification/disposition와 digest/size는 별도 `provenance_hash` 아래 둔다. Claim ID, 로컬 payload path, acquisition timestamp, raw evidence, coverage의 관찰 시각과 자유형 reason은 내보내지 않는다.

## 완료되지 않은 경계

v1 snapshot은 HLS init-map identity cross-reference가 없어 자동 재생 playlist를 생성하지 않는다. Snapshot integrity hash는 owner 권한이나 서명된 출처 증명이 아니다. RC12 media archive 및 로컬 녹화는 읽기 전용이며 이 프로토콜은 기존 archive를 변경하지 않는다.
