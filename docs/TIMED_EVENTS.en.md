# Platform-independent timed events

This document defines how a Source Plugin delivers events on a recording timeline. Core does not connect chat/event transports or interpret platform payloads.

## Ownership

- **Source Plugin** owns platform connection, authentication, WebSocket/SSE/long-poll, cursor meaning, reconnect/catch-up, payload normalization, and platform extensions.
- **Core** owns the optional `events` capability, bounded batch ingestion, validation, deduplication, canonical preservation, owner fencing, and replay/query.
- **Storage Provider** continues to place logical keys and bytes only.

Existing plugins omit the capability and keep their current media/metadata behavior. Commercial platform chat implementations do not belong in this Core repository.

## Protocol v1 extension

The `events` capability is optional. Core sends `events.start`, `events.ack`, and `events.stop` requests; the plugin sends `events.batch` notifications. A notification is a normal NDJSON frame, not arbitrary stdout text. Existing request/response framing is unchanged.

`events.start` contains a Core-generated `stream_id`, `recording_id`, opaque resource, current media snapshot, effective plugin configuration/secrets, and the last durable `resume_state`. A plugin sends `stream_id`, a retry-stable `batch_id`, 1–64 events, and the next `resume_state` in each `events.batch`. Only one batch may be outstanding. Core sends `events.ack` only after the canonical batch page and root visibility marker are durable. The plugin advances its source cursor only after that response succeeds. If the plugin exits first or Core cannot acknowledge, the batch must be delivered again from the durable cursor.

Default bounds are 32 KiB per event, 256 KiB per batch, 8 KiB per extension, 16 KiB per resume state, JSON nesting depth 8, and 64 events per batch. Core uses a finite per-stream batch buffer and stops only the event stream when that queue fills. Event validation/storage failures do not fail media capture. Event limits apply independently of the protocol frame limit.

`TimedEvent` contains `event_type`, optional `source_event_id`, `source_sequence`, `source_timestamp`, optional common `actor`/`related_event_id`, normalized `payload`, and optional plugin-owned `extension`. The wire does not accept `observed_at`; Core stamps receipt time. Event types are opaque dot-qualified names such as `chat.message`, `chat.edit`, `chat.delete`, `chat.system`, `support.donation`, `support.subscription`, `moderation.*`, and `source.*`. Core preserves unknown valid types.

The optional common `chat.message` projection uses top-level `actor`, `message.{text, fragments}`, `reply_to`, `amount`, and `currency` fields. `payload` and `extension` are bounded JSON objects for other normalized and plugin-owned data. A plugin puts values that cannot be represented without loss in `extension`. Core rejects keys in password/token/cookie/authorization/secret/credential/signature/private-key families and URL values with credential/signature query parameters. Do not include raw transport frames. User-authored chat text is preserved as content; secrets must not be extracted or recorded.

The SDK batcher defaults to at most 32 events or 250 ms, whichever comes first. Plugins may tune this within the limits but must not advance a source cursor while waiting for the Core acknowledgement.

## Identity and order

`event_archive_ordinal` is durable append order; `event_timeline_ordinal` is replay order. Both are independent from media `ArchiveOrdinal`, `TimelineOrdinal`, and `LivePresentationOrdinal`. Core assigns consecutive ordinals to accepted events and keeps canonical acceptance order.

When `source_event_id` exists, Core derives canonical identity from event type + ID within the recording. Otherwise it uses `source_sequence` when present; plugins must keep that sequence stable across reconnects for the recording. If neither exists, the durable `batch_id` plus event index is the retry identity. Re-delivering the same batch ID is a no-op, while a new batch containing the same source-less content is preserved as a separate event: content and timestamps alone cannot distinguish a replay from a legitimate repeated message. Distinct events with equal timestamps retain separate ordinals. Cursor-capable plugins catch up after the last durable cursor.

`source_timestamp` is preserved as source provenance and never controls canonical append order. `observed_at` is Core's receipt time. `presentation_offset_ms` is derived from `observed_at - recording.started_at`, clamped at zero and monotonically clamped against the prior event, so source clock skew or timestamp regression cannot reorder appends or move VOD replay backward.

`source_timestamp` is source-reported provenance and never reorders canonical events. `observed_at` is Core's UTC receipt time. VOD `presentation_offset_seconds` is `observed_at - recording.started_at`, clamped to zero or later and never below the prior event offset. Source clock skew/absence does not affect this calculation. Equal positions display in append ordinal order.

## Archive and durability

The V2 root gains only bounded event/batch high-water counters. Each accepted batch is stored in one bounded immutable page at `archive/v2/timed-events/batches/<ordinal>`. The page contains event projections, source identities, ordinals, Core timestamps, opaque `resume_state`, and retry `batch_id`. Core writes the page first and updates event high-water plus `ArchiveRevision` last. That root update is the visibility marker; `TimelineRevision` remains the media playback-order revision. Archive format version and migration policy do not change.

If the process exits after writing a page but before updating the root, the next attempt validates the orphan page against the same batch ID/fingerprint, publishes it, and acknowledges it again. The plugin never receives a cursor acknowledgement before the durable page is visible through the root. A sealed archive rejects event append. Stop/completion does not seal, so event archive reconciliation remains possible until explicit seal.

Event queries provide bounded cursor pages and offset/type filters. Public responses never return resume state, plugin secrets, or acquisition configuration. Recording lifecycle/management events (`/events`) and source timed events (`/timed-events`) are separate domains. Timed event JSONL export streams the stored projection.

## Generation and process lifetime

Recording handover cancels the source timed-event stream and drains any outstanding batch commit/ack before transferring the existing owner fence. The target starts in the new generation from the last durable cursor. A stale generation cannot commit canonical events. Adapter child restarts and Core event-stream reconnects resume from the durable cursor. Runtime Host restart behavior for media capture remains governed by the existing owner/recovery contract; a cold-start terminalization must not be reported as automatic media resumption.

## UI

Recording detail shows a compact timeline only when timed events exist. VOD replay follows the player's current offset, and selecting an event seeks the player. `chat.edit` and `chat.delete` remain separate timeline entries with their `related_event_id` preserved. Unknown/custom types show only their event type and safe common projection; extension fields are not displayed. A recording without timed events remains as quiet as it is today.
