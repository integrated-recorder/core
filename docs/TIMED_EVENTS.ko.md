# 플랫폼 독립 timed event

이 문서는 Source Plugin이 방송 시간축 이벤트를 Core에 전달하는 계약을 정의합니다. Core는 chat/event transport를 연결하거나 플랫폼 payload를 해석하지 않습니다.

## 책임 경계

- **Source Plugin**은 플랫폼 연결, 인증, WebSocket/SSE/long-poll, cursor 의미, reconnect/catch-up, payload 정규화와 플랫폼 확장을 소유합니다.
- **Core**는 선택적 `events` capability, bounded batch 수신, 검증, 중복 처리, canonical 보존, owner fence, replay/query를 소유합니다.
- **Storage Provider**는 기존과 같이 logical key와 bytes만 배치합니다.

기존 plugin은 capability를 선언하지 않으면 현재 media/metadata 경로 그대로 동작합니다. 상용 platform chat 구현은 이 Core 저장소에 포함되지 않습니다.

## Protocol v1 확장

`events` capability는 선택 사항입니다. Core는 `events.start`, `events.ack`, `events.stop` request를 보내고 plugin은 `events.batch` notification을 보냅니다. Notification은 무관한 stdout 문자가 아니며 NDJSON 한 frame입니다. 기존 request/response framing은 바뀌지 않습니다.

`events.start`에는 Core 생성 `stream_id`, `recording_id`, opaque resource, 현재 media snapshot, 적용된 plugin configuration/secrets, 마지막 durable `resume_state`가 포함됩니다. Plugin은 `events.batch`에서 `stream_id`, 재시도에도 같은 값을 쓰는 `batch_id`, 1–64개 event와 다음 `resume_state`를 보냅니다. 하나의 batch만 outstanding일 수 있습니다. Core는 batch의 canonical page와 root visibility marker가 durable해진 뒤에만 `events.ack`를 보내며, 그 응답이 성공한 뒤 plugin이 source cursor를 전진시킵니다. Plugin process가 먼저 끝나거나 Core가 ack하지 못한 batch는 durable cursor에서 다시 전달해야 합니다.

기본 상한은 event 32 KiB, batch 256 KiB, extension 8 KiB, resume state 16 KiB, JSON nesting 8, batch당 64개입니다. Core는 stream별 유한한 batch buffer를 두고 queue가 찼을 때 event stream만 중단합니다. Event validation/storage 오류는 media capture를 실패시키지 않습니다. stdout frame 한도와 별도로 event 상한을 적용합니다.

`TimedEvent`는 `event_type`, optional `source_event_id`, `source_sequence`, `source_timestamp`, optional common `actor`/`related_event_id`, normalized `payload`, optional plugin-owned `extension`으로 구성됩니다. `observed_at`은 wire에서 받지 않으며 Core가 수신 시각을 기록합니다. 허용 event type은 `chat.message`, `chat.edit`, `chat.delete`, `chat.system`, `support.donation`, `support.subscription`, `moderation.*`, `source.*` 같은 점으로 분리된 opaque 이름입니다. Core는 unknown valid type을 보존합니다.

Common `chat.message` projection은 선택적 top-level `actor`, `message.{text, fragments}`, `reply_to`, `amount`, `currency`를 사용합니다. `payload`와 `extension`은 그 밖의 normalized/plugin-owned data를 보존하는 bounded JSON object입니다. 공통 model에 손실 없이 표현할 수 없는 값은 `extension`에 넣습니다. key 이름이 password, token, cookie, authorization, secret, credential, signature/private-key 계열인 항목과 credential/signature query가 있는 URL 문자열은 거부합니다. raw transport frame 전체를 넣지 않습니다. Chat 텍스트는 사용자 작성 content로 보존되며 secret 필드를 별도 추출하거나 기록하지 않습니다.

SDK batcher 기본값은 최대 32 events 또는 250 ms 중 먼저 도달한 조건입니다. Plugin은 상한 안에서 이를 조정할 수 있지만 Core ack를 기다리는 동안 source cursor를 앞당기면 안 됩니다.

## Identity와 순서

`event_archive_ordinal`은 durable append 순서이고 `event_timeline_ordinal`은 replay 순서입니다. 두 값은 media `ArchiveOrdinal`, `TimelineOrdinal`, `LivePresentationOrdinal`과 독립입니다. Core는 batch 안의 event에 ordinal을 연속 할당하고, 접수 순서를 canonical 순서로 유지합니다.

Stable `source_event_id`가 있으면 Core는 recording 안에서 event type + ID로 canonical identity를 만듭니다. 없으면 `source_sequence`를 사용하며, plugin은 recording 동안 reconnect 뒤에도 sequence를 안정적으로 유지해야 합니다. 둘 다 없으면 durable `batch_id`와 event index가 retry identity입니다. 같은 batch ID의 재전달은 no-op입니다. source identity가 없는 새 batch의 내용이 이전과 같더라도 별도 event로 보존합니다. 내용과 timestamp만으로는 replay와 실제 반복 메시지를 구별할 수 없기 때문입니다. 같은 timestamp의 서로 다른 event는 각 ordinal로 보존됩니다. Plugin이 cursor를 제공하면 마지막 durable cursor부터 catch-up합니다.

`source_timestamp`는 source provenance로 보존하며 canonical append 순서에는 쓰지 않습니다. `observed_at`은 Core가 받은 시각입니다. `presentation_offset_ms`는 `observed_at - recording.started_at`에서 계산해 0 아래를 막고 직전 offset보다 작아지지 않도록 보정합니다. 따라서 source clock skew나 timestamp 역행이 append order 또는 VOD replay를 뒤로 움직이지 않습니다.

`source_timestamp`는 source-reported provenance일 뿐 canonical order를 바꾸지 않습니다. `observed_at`은 Core UTC 수신 시각입니다. VOD `presentation_offset_seconds`는 `observed_at - recording.started_at`으로 계산하고 0 이상이며 이전 event offset보다 감소하지 않게 clamp합니다. Source clock skew/부재는 이 계산에 영향을 주지 않습니다. 동률은 append ordinal 순서로 표시됩니다.

## Archive와 durability

V2 root에는 bounded event/batch high-water counter만 추가합니다. 각 accepted batch는 `archive/v2/timed-events/batches/<ordinal>`의 bounded immutable page 한 개로 저장합니다. Page에는 event projection, source identity, ordinals, Core timestamps, opaque `resume_state`, retry `batch_id`가 들어갑니다. Page를 쓴 뒤 root의 event high-water와 `ArchiveRevision`을 마지막으로 갱신합니다. 이 root update가 visibility marker이며 `TimelineRevision`은 media playback 순서에만 사용합니다. Existing archive format version과 migration policy는 바뀌지 않습니다.

Page write 후 root update 전에 process가 종료되어도 다음 시도는 같은 batch ID/fingerprint의 orphan page를 검증하여 publish하고 다시 ack합니다. Cursor는 durable page가 root에 보이기 전에는 plugin에 ack되지 않습니다. Sealed archive는 event append를 거부합니다. Stop/completion은 seal이 아니므로 repairable event archive를 seal 전까지 reconciliation할 수 있습니다.

Event query는 bounded cursor pages와 offset/type filter를 지원하며 public response에는 resume state, plugin secrets, acquisition configuration을 반환하지 않습니다. Recording lifecycle/management event(`/events`)와 source timed event(`/timed-events`)는 별도 domain입니다. Timed event JSONL export는 저장된 projection을 순차 출력합니다.

## Generation 및 process 수명

Recording handover는 source timed-event stream을 취소하고 outstanding batch commit/ack를 끝낸 후 기존 owner fence로 transfer합니다. Target은 새 generation에서 마지막 durable cursor로 시작합니다. Stale generation은 canonical event commit을 할 수 없습니다. Adapter child restart 및 Core의 event stream reconnect는 durable cursor로 다시 시작합니다. Core/Runtime Host 자체 재시작의 media capture 동작은 기존 owner/recovery contract에 따르며, cold-start terminalization으로 이어지는 경우 media를 자동 재개했다고 주장하지 않습니다.

## UI

Recording detail은 timed event가 있는 경우에만 compact timeline을 표시합니다. VOD 현재 offset에 맞춰 replay 위치를 표시하고 event 선택은 player를 seek합니다. `chat.edit`와 `chat.delete`도 `related_event_id`를 보존한 별도 timeline 항목으로 표시합니다. Unknown/custom type은 event type과 안전한 common projection만 보여주고 extension field는 표시하지 않습니다. Event가 없는 recording은 기존 화면과 동일하게 조용히 표시됩니다.
