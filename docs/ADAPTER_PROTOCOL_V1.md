# Adapter Protocol v1

This document is the normative, language-neutral contract for Integrated Recorder
adapter executables. Implementations do not need to import or inspect Integrated
Recorder source code. The current Go wire implementation is in
`internal/adapterproto`; the Core-owned executable validator is
`cmd/adapter-conformance`.

Protocol version: **1**. This document describes the implementation at Core
commit `2df5e407d2a2f0013c8d3034f2b1cb6dc0ec6c36` and is maintained with that
contract. JSON examples in `protocol/adapter-v1/` are executable golden vectors.

## Compatibility policy

Protocol v1 field, method, capability, and value meanings are stable. A change
that changes an existing meaning or removes a supported field/method requires a
new protocol version. Optional additive fields and optional capabilities may be
introduced while retaining v1 when old implementations can safely ignore them.
Adapters must not infer compatibility from release version strings. The
`protocol_version` and descriptor identity are authoritative.

Core ignores syntactically valid, unknown capability identifiers. An adapter
may advertise an extension capability, but Core will not call it unless Core
implements that capability. Capability identifiers must still satisfy the
identifier grammar below.

## Process and installation contract

In production, Runtime Host starts bundled adapters only from its explicit
Host-owned inventory. It imports Registry artifacts only after exact registry
size/SHA-256 and descriptor checks. Operator-supplied source directories are
considered only when `IR_ALLOW_OPERATOR_PLUGINS=1`; the Host never searches
`PATH` and does not infer bundled status from a directory name. All admitted
executables are validated and imported into content-addressed immutable
artifacts and immutable adapter-set snapshots. Control and Recorder Engine
receive only the private `bin` directory assigned to their application
generation. Changing an operator source directory does not mutate a running
generation: Host reconciliation activates a new generation, while existing
Recordings remain pinned to their original Engine and adapter set. The
monolithic development command may use its supplied adapter directory directly.

A discovered filename starts with `integrated-recorder-adapter-`. The
candidate must be a regular file with at least one executable permission bit.
Candidate paths are sorted before startup. Descriptor IDs, rather than filename
suffixes, are the adapter identity. Two executables with the same descriptor ID
are rejected as duplicates.

An adapter is a trusted local executable. It runs as the same operating-system
user as Core. It is **not sandboxed** and can access whatever that user can
access. Configuration secrets required by the adapter's declared schema may be
sent to it over stdin. Do not declare secrets that the adapter does not need.

Core starts one long-lived process for a discovered adapter and serializes
requests to that process. The first operation is `describe`. The descriptor is
validated and its semantic fingerprint is retained. If the process must be
restarted, Core calls `describe` again and requires the same ID, version,
protocol version, and semantic fingerprint. Branding is excluded from that
fingerprint; other descriptor changes are rejected. Workflow state is
process-local and does not survive a process restart.

Core gives initial `describe` up to 5 seconds. Normal adapter calls have a
15-second default deadline and callers may impose a shorter deadline. Shutdown
is bounded: Core asks the adapter to shut down, closes stdin, waits briefly, and
then kills the process if it does not exit. A process that emits malformed
stdout, exits, or times out becomes unavailable and is restarted under Core's
bounded restart policy.

## Transport and frames

Transport is UTF-8 newline-delimited JSON (NDJSON): each frame is one JSON
object followed by LF (`\n`). A frame is at most **8,388,608 bytes before LF**.
CRLF is accepted as JSON whitespace before the terminating LF. Empty lines,
unterminated final frames, invalid UTF-8, malformed JSON, and oversized frames
are protocol errors.

Stdout is exclusively the protocol channel. It must contain only complete
protocol frames. Diagnostics belong on stderr. A startup banner on stdout is a
protocol violation. Core reads one response for each request and serializes
calls; adapters must not send unsolicited response frames.

The ordinary v1 request/response shape has no `type` property:

```json
{"protocol_version":1,"id":"1","method":"describe","params":{}}
{"protocol_version":1,"id":"1","result":{}}
```

The parser also recognizes the equivalent typed request and response forms
(`"type":"request"` and `"type":"response"`). Existing Core writers use
the untyped form. A response must contain exactly one of `result` or `error`.
The `id` and `protocol_version` in a response must match the request.

An error response is:

```json
{"protocol_version":1,"id":"2","error":{"code":"example_error","message":"safe explanation"}}
```

`code` is required and nonempty. `message` is a string. `details` is optional
and may be any JSON value. Core does not define a universal method-not-found
error code. At the adapter boundary, Core only preserves the meaning of the
safe codes `authentication_required`, `interaction_required`, and
`configuration_required`; other adapter messages/details are discarded by Core
and must not be relied upon as a user-visible channel. Never put credentials,
request contents, signed URLs, or secret state into errors.

`protocol_version` is the integer `1`; request IDs are nonempty opaque strings.
Core currently uses decimal sequence strings. Adapters must echo IDs exactly.
`method` is a nonempty string. `params` is optional JSON; methods below define
the object shape expected by Core.

### Notifications

The parser recognizes a reserved typed notification envelope such as
`{"protocol_version":1,"type":"notification","method":"events.emit"}`.
This only means the syntax can be parsed. The current runtime does not deliver
asynchronous notifications. Adapter implementations must not depend on
notification delivery in v1.

## Identifiers, schemas, resources, and configuration

Adapter, capability, resource-type, and schema-field identifiers match
`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`. Adapter IDs and field/resource type names
are opaque to Core. Media type names match
`^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$`.

### Descriptor

`describe` returns one `Descriptor`:

```json
{
  "id":"example",
  "name":"Example Adapter",
  "version":"1.0.0",
  "protocol_version":1,
  "capabilities":["resolve","watch"],
  "input_schema":{"fields":[]},
  "configuration_schema":{"fields":[]},
  "resource_types":[],
  "media_types":["hls"]
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `id` | Stable adapter identifier, matching identifier grammar. |
| `name` | Nonblank display name. |
| `version` | Nonblank adapter version string. |
| `protocol_version` | Must equal 1. |
| `capabilities` | Optional unique capability identifier array. At least `resolve` or `resolve_workflow` is required. |
| `input_schema` | Schema for user-supplied adapter input. A missing schema decodes as an empty schema in current Core; authors should emit it explicitly. |
| `configuration_schema` | Schema for persistent plugin/resource configuration. A missing schema decodes as an empty schema in current Core; authors should emit it explicitly. |
| `resource_types` | Optional declared opaque resource hierarchy; maximum 256. |
| `media_types` | Required nonempty list of supported media type identifiers. |
| `branding` | Optional presentation-only branding described below. |

Identity should remain stable for the same adapter implementation and descriptor
semantics. Core restart comparison uses ID, version, protocol version, and a
canonical JSON SHA-256 fingerprint of the descriptor with `branding` removed.
Changing branding alone does not invalidate a restart. Changing any other
fingerprinted descriptor value does.

`Branding` has optional `icon`. `BrandIcon` has `media_type` and base64 JSON
`data` bytes. Only a decoded PNG is accepted; the encoded image is at most 64
KiB and dimensions are 1–512 pixels in each direction. SVG and remote icon URLs
are not supported.

### Schema vocabulary

`Schema` is `{ "fields": [...] }`, with at most 512 fields. The current Core
decodes an omitted or `null` `fields` array as an empty schema; authors should
emit `fields: []` for an explicit empty schema. A `Field` has:

| Field | Meaning |
| --- | --- |
| `key` | Unique identifier within the schema. |
| `control` | One of `text`, `secret`, `number`, `boolean`, `select`, `multi-select`, `textarea`, `action`, `status`. |
| `label` | Required nonblank UI label. |
| `description` | Optional plain text. |
| `required` | Optional boolean, default false. |
| `inherit` | Optional boolean controlling whether stored values flow to descendant resource scopes; omission is restrictive/false. |
| `default` | Optional JSON value validated against the control. Secret fields cannot have defaults. |
| `constraints` | Optional control-specific constraints. |
| `options` | Optional list of `{value,label}`; required for select controls, forbidden for other controls; at most 2,048 per field. |
| `visible_when` | Optional declarative condition. |
| `persistence` | Optional interaction-answer persistence policy. |

`Constraints` fields are optional `min`/`max` numbers, `min_length`/`max_length`
integers, `pattern` regular expression, and `min_items`/`max_items` integers.
Numeric constraints apply only to `number`; string constraints only to `text`,
`secret`, and `textarea`; item constraints only to `multi-select`. Length counts
Unicode code points. Bounds cannot be negative or inverted. Patterns must
compile. A select or multi-select value must match one of its declared option
values; multi-select values must not repeat.

`visible_when` supports a predicate `{ "field":"key", "equals":VALUE }`,
`not_equals`, or boolean `truthy`, and recursive `{ "all":[...] }` or
`{ "any":[...] }` groups. Groups must be nonempty. References must point to a
different non-secret editable field; condition graphs cannot cycle. Nesting is
bounded to 32 and total condition nodes to 2,048.

`FieldPersistence` is `{ "mode":..., "target":... }`. Modes are `forbidden`,
`optional`, or `required`. Target scopes are `plugin`, `current_resource`, or
`resource`; only `resource` accepts a `resource` reference and requires it.
The workflow's `persistable` flag supplies the default for challenge fields
without an explicit policy: optional for persistable challenges, forbidden
otherwise. Persistence is Core-managed configuration storage; it does not make
adapter-owned state persistent configuration.

### Effective configuration composition

For a resource-scoped call, Core composes scopes in this order: plugin scope,
then resource scopes from the parent-most/root resource through the current
leaf. At a non-leaf scope, only fields declared there with `inherit: true` flow
to descendants; omitted `inherit` is false. Values stored at the current leaf
apply regardless of `inherit`. When an applicable key is present at more than
one scope, the later, more-specific scope wins. Core does not interpret the
resource IDs or otherwise assign platform meaning to this hierarchy.

After scope composition, Core fills applicable schema defaults. It evaluates
`visible_when` against the composed values before forming the adapter request.
A stored override whose field is currently hidden remains stored but is omitted
from that adapter call. The `action` and `status` display controls are never
sent as configuration. Ordinary values are sent in the `configuration` JSON
value map; declared `secret` fields are sent separately in the `secrets` string
map. Secret configuration follows the same scope, inheritance, and visibility
rules and is sent only on adapter calls that need effective configuration.
Public management projections do not return secret values.

Input-schema secrets use a different wire path from configuration secrets.
`input` in `ResolveParams`, `ResolveBeginParams`, and `WatchCheckParams` may
contain values for fields marked `secret` in `input_schema`, alongside ordinary
input values. Core call paths that collect these secret inputs separately
merge them into the input object in memory before sending it; other callers
may already provide them inline. The top-level `secrets` member in these
parameter objects is for effective configuration secrets declared by the
configuration schema, not input-schema secrets. In workflow continuation,
secret answers use the separate `answer_secrets` member. Adapters must treat
`input`, `secrets`, and `answer_secrets` as sensitive and must not log or echo
them.

Workflow answers are distinct from configuration: ordinary answers use
`answers` and secret answers use `answer_secrets` for the current continuation.
Answer secrets are not automatically retained as configuration. The field's
`persistence` policy controls whether an answer is forbidden, optionally saved
when requested through the workflow API, or required to be saved to the
declared configuration scope. State documents and mutations are a third,
separate adapter-owned channel, not configuration.

### Resources

`ResourceType` has `type`, optional `parent_types`, and optional
`configuration_schema`. Every parent type must be declared and the type graph
must be acyclic. These are permitted parent edges, not a requirement that each
resource instance have a parent.

`ResourceRef` is `{ "resource_type":"...", "resource_id":"...",
"parent":... }`. Type and ID are opaque. A supplied parent chain must use
declared types and allowed child-to-parent edges. Chains cannot cycle and are
limited to 64 entries; an ID is at most 4,096 bytes. `Resource` embeds that
reference and may add `display_name` and an opaque JSON `attributes` object.

`resource.list` and `resource.search` return `ResourcePage` with required
`items` array and optional `next_cursor`. Each item must belong to the requested
parent and requested type, when present; duplicate resource references are
rejected. Cursor is opaque, at most 4,096 bytes; search query at most 1,024
bytes. A requested page limit of 0 means 20; positive values above 50 are
capped to 50; negative values are invalid.

Core does not interpret platform meaning in resource IDs, types, attributes,
cursors, or display names.

### Configuration, secrets, and adapter-owned state

`configuration` maps field keys to JSON values. Effective configuration is
composed by Core from plugin and resource scopes, schema defaults, and
inheritance. `secrets` maps declared secret field keys to strings and is sent
only for the adapter call that needs it. Public management projections do not
return secret values. Adapter code and stderr logs must not print them.

`StateDocument` has optional `resource`, ordinary `values`, and `secrets` maps.
The plugin-level state scope omits `resource`; resource-scoped state names that
resource. `StateMutation` has the same optional resource and `values`/`secrets`
maps plus `clear_values` and `clear_secrets` arrays. Mutations merge supplied
keys and explicitly remove keys named by clear arrays; empty values do not mean
deletion. State is Core-persisted, adapter-owned opaque state, separate from
user configuration. Core validates mutation scopes and values against the
declared schema and stores state secrets separately.

## Wire type catalog

This catalog lists every v1 data type currently exchanged on the wire. Optional
JSON properties are omitted when absent unless stated otherwise.

| Type | JSON members |
| --- | --- |
| `Request` | `protocol_version` integer; `id`, `method` strings; optional `params` JSON. |
| `Response` | `protocol_version`; `id`; exactly one of `result` JSON or `error`. |
| `Error` | required nonempty `code`; `message`; optional arbitrary JSON `details`. |
| `Notification` | reserved typed form: `protocol_version`, `type:"notification"`, `method`, optional `params`; no `id`. Runtime delivery is unsupported. |
| `Descriptor` | `id`, `name`, `version`, `protocol_version`, optional `capabilities`, `input_schema`, `configuration_schema`, optional `resource_types`, `media_types`, optional `branding`. |
| `Branding` / `BrandIcon` | optional `icon`; icon `media_type`, base64 `data`. |
| `Schema` / `Field` | `fields`; field members are `key`, `control`, `label`, optional `description`, `required`, `inherit`, `default`, `constraints`, `options`, `visible_when`, `persistence`. |
| `Constraints` | optional `min`, `max`, `min_length`, `max_length`, `pattern`, `min_items`, `max_items`. |
| `Option` | `value` JSON scalar/value and `label`. |
| `FieldPersistence` / `PersistenceTarget` | `mode`; `target` with `scope`, optional `resource`. |
| `ResourceType` | `type`, optional `parent_types`, optional `configuration_schema`. |
| `ResourceRef` | `resource_type`, `resource_id`, optional recursive `parent`. |
| `Resource` | `ResourceRef` members, optional `display_name`, optional opaque `attributes`. |
| `ResourceListParams` | optional `parent`, `resource_type`, `cursor`; `limit`. |
| `ResourceSearchParams` | optional `parent`, `resource_type`, `cursor`; `query`, `limit`. |
| `ResourcePage` | `items`; optional `next_cursor`. |
| `ResolveParams` | required JSON object `input`; optional `resource`, `configuration`, `secrets`, `state`. |
| `ResolveResult` | `media`; optional `state` mutations. Legacy adapters may return `MediaSource` directly instead. |
| `ResolveBeginParams` | `workflow_id`, `input`; optional `resource`, `configuration`, `secrets`, `state`. |
| `ResolveContinueParams` | `workflow_id`; optional `resource`, `configuration`, `secrets`, `answers`, `answer_secrets`, `state`. |
| `ResolveWorkflowResult` | `state`, `workflow_id`; optional `resource`, `challenge`, `media`, `state_mutations`. |
| `WorkflowChallenge` | `schema`; optional `prompt`, optional `persistable`. |
| `StateDocument` | optional `resource`, `values`, `secrets`. |
| `StateMutation` | optional `resource`, `values`, `secrets`, `clear_values`, `clear_secrets`. |
| `MediaSource` | `type`, `manifest_url`; optional `headers`, `request_policy`, `session_ref`, `refresh`, `metadata`, `archive_policy`, `refresh_policy`. |
| `RequestPolicy` / `HeaderForwardingPolicy` | optional `header_forwarding`; policy has optional `mode`, `origins`. |
| `ArchivePolicy` | optional `source_uri` classification (`sensitive` or `public`); omission means `sensitive`. |
| `RefreshPolicy` | optional `expires_at`, `refresh_before_seconds`, `on_http_status`. |
| `RefreshParams` | optional `resource`; required `current` media; optional `state`. |
| `RefreshResult` | replacement `media`; optional state mutations in `state`. |
| `MetadataParams` | optional `resource`; required `current`; optional `configuration`, `secrets`, `state`. |
| `StreamMetadata` | optional nullable `title`, `description` strings. |
| `MetadataResult` | `metadata`; optional `source_updated_at`, `state_mutations`. Core's decoder accepts omitted or `null` `metadata` as a zero `StreamMetadata` (both fields unknown); SDK encoding emits `{}`. Adapters should emit an explicit object. |
| `WatchCheckParams` | required object `input`; optional `resource`, `configuration`, `secrets`, `state`. |
| `WatchCheckResult` | `state`; optional `session_ref`, `title`, `started_at`, `media`, `state_mutations`. |
| `InteractionMessage` | `type`, `interaction_id`; optional `title`, `message`, `fields`, `data`. |
| `InteractionField` | `key`, `control`, `label`; optional `description`, `required`, `options`. |
| `AdapterProvenance` | Core/archive projection: `id`, `version`, `protocol_version`, optional descriptor `fingerprint`; not a method result. |

For ordinary optional strings, omitted and empty generally both mean no extra
value unless that method defines otherwise. `null` is accepted as JSON where
the field's decoder accepts a nullable value. In `StreamMetadata`, a missing or
`null` pointer means unknown/not supplied, while `""` means the adapter knows
the value is empty. For arrays and maps, omitted means absent; an explicit
empty array/object is an empty collection. In particular `ResourcePage.items`
must be present and decode to an array (including `[]`). `ResolveParams.input`
and `WatchCheckParams.input` must be JSON objects; empty `{}` is valid.

Timestamp fields are JSON strings decoded into Go `time.Time` values using
Go's RFC3339 date-time JSON format, including an optional fractional second and
`Z` or a numeric UTC offset; Unix numeric timestamps are not the wire form.
For optional `*time.Time` fields, an omitted property or JSON `null` decodes as
absent. Core additionally rejects a zero time or a year outside 1 through 9999
for `watch.check` result `started_at` and metadata result `source_updated_at`.
`refresh_policy.expires_at` is also decoded as `*time.Time`, but current media
validation imposes no additional zero/year check on that field; it validates
`refresh_before_seconds` and `on_http_status` instead. `source_updated_at` is
source-provided metadata time. Metadata observation time is assigned by Core
and is not supplied by the adapter.

## Capabilities and methods

`describe` is always called and has no capability requirement. A valid
descriptor declares `resolve` or `resolve_workflow`. The Core actively dispatches
the operations in the table below. `status`, `configure`, `interaction`, and
`events` are recognized v1 names/capabilities but currently have no active Core
dispatch path; declaring them does not make Core call them. Capability
declaration is required for the actively gated operation.

| Capability | Declaration means | Core behavior |
| --- | --- | --- |
| `resolve` | Stateless media resolution is supported. | Core may call `resolve`; the descriptor must declare this or `resolve_workflow`. When both are declared, Core prefers workflow resolution. |
| `resolve_workflow` | Stateful resolution workflow is supported. | Core calls `resolve.begin`, then `resolve.continue` for transitions and answers. |
| `watch` | The adapter can observe live/offline state. | Core calls `watch.check` for enabled Watch definitions. Without it, Watch is unsupported. |
| `metadata` | The adapter can report source title/description. | Core calls `metadata` while an active recording is monitored. Failure is isolated from acquisition. |
| `refresh` | The adapter can replace an expired or policy-triggered media source. | Core calls `refresh` when adapter-declared refresh policy triggers. |
| `resource_browse` | The adapter can browse/search its declared resource model. | Core calls `resource.list` or `resource.search`. |
| `status` | Reserved v1 capability identifier. | No active Core dispatch path. |
| `configure` | Reserved v1 capability identifier. | No active Core dispatch path. |
| `interaction` | Reserved v1 capability identifier. | No active Core dispatch path; workflow challenges may carry interaction messages but do not invoke a separate interaction method. |
| `events` | Reserved v1 capability identifier. | No active Core dispatch path. Notifications are not delivered. |

<!-- protocol-v1-capabilities:start -->
resolve
status
configure
interaction
metadata
events
refresh
resolve_workflow
resource_browse
watch
<!-- protocol-v1-capabilities:end -->

<!-- protocol-v1-methods:start -->
describe
resolve
resolve.begin
resolve.continue
shutdown
get_status
configure
interaction.begin
interaction.continue
metadata
events
refresh
resource.list
resource.search
watch.check
<!-- protocol-v1-methods:end -->

| Method | Capability / caller | Params and result | State and error behavior |
| --- | --- | --- | --- |
| `describe` | Required handshake; no capability. | Params `{}`; result `Descriptor`. | Invalid version/descriptor rejects discovery. |
| `resolve` | `resolve`; used when Core selects stateless resolve. | `ResolveParams`; result may be `MediaSource` or `{media,state?}` (`ResolveResult`). | Core validates media and then applies state mutations. An adapter error leaves existing media/acquisition decisions to Core. |
| `resolve.begin` | `resolve_workflow`; Core calls when workflow capability is declared. | `ResolveBeginParams`; result `ResolveWorkflowResult`. | Workflow ID is generated by Core and must be echoed. State mutations are validated and applied at each valid transition. |
| `resolve.continue` | `resolve_workflow`; sent for an active challenge continuation. | `ResolveContinueParams`; result `ResolveWorkflowResult`. | Same workflow ID and adapter process generation are required. Latest effective configuration/state are sent. Answers are ephemeral unless explicit persistence is requested through workflow APIs. |
| `shutdown` | Lifecycle; no capability. | Params `{}`; any JSON result acknowledges. | Adapter should stop accepting work and exit after response/EOF. Core bounds wait and may kill. |
| `get_status` | `status`; reserved, no active Core dispatch. | No active Core-defined params/result behavior. | Do not assume Core calls it. |
| `configure` | `configure`; reserved, no active Core dispatch. | No active Core-defined params/result behavior. | Do not assume Core calls it. |
| `interaction.begin` | `interaction`; reserved, no active Core dispatch. | No active Core-defined params/result behavior. | Do not assume Core calls it. |
| `interaction.continue` | `interaction`; reserved, no active Core dispatch. | No active Core-defined params/result behavior. | Do not assume Core calls it. |
| `metadata` | `metadata`; recording metadata observer. | `MetadataParams`; result `MetadataResult`. | `title` and `description` are optional pointers: omitted/null is unknown, empty string is known empty. Optional source timestamp is metadata source time, not Core observation time. State mutations are committed only after caller persistence validation. Failures are isolated from media acquisition. |
| `events` | `events`; reserved, no active Core dispatch. | No active Core-defined params/result behavior. | Parser-recognized notification syntax does not enable delivery. |
| `refresh` | `refresh`; Core asks for replacement media when adapter-defined refresh policy triggers. | `RefreshParams`; result `RefreshResult`. | Core validates replacement media and state mutation before use. A failed refresh does not make the adapter-provided current media disappear; callers determine recording error handling. |
| `resource.list` | `resource_browse`; called for non-search browse. | `ResourceListParams`; result `ResourcePage`. | Core clamps page limit and validates parent/type membership, item count, cursor size, and duplicates. |
| `resource.search` | `resource_browse`; called when a nonempty query is supplied. | `ResourceSearchParams`; result `ResourcePage`. | Same resource/page validation as list. |
| `watch.check` | `watch`; scheduler asks for a successful offline/live observation. | `WatchCheckParams`; result `WatchCheckResult`. | State must be `offline` or `live`; errors are errors, never offline. Session reference is opaque and bounded. Mutations apply only after validation. |

If both `resolve` and `resolve_workflow` are declared, Core prefers the
workflow path. A workflow result state is `resource_discovered`,
`configuration_required`, `interaction_required`, `resolved`, or `error`.
`resource_discovered` provides a resource and Core continues the workflow;
challenge states provide a `WorkflowChallenge`; `resolved` provides media;
`error` is terminal failure. Workflow IDs are opaque Core-generated strings.
Core bounds active workflows at 128, transitions at 32, and idle lifetime at
30 minutes. Workflow state is in memory and does not survive adapter/Control
process restart.

## Media, refresh, watch, and metadata

`MediaSource` requires a supported `type` and absolute HTTP(S) `manifest_url`
with a host and no userinfo. `headers` is optional; invalid, duplicate, or
transport-controlled headers are rejected. `request_policy.header_forwarding`
is restrictive when omitted. The supported forwarding modes are
`same_origin` and `allowlist`; allowlist entries are HTTP(S) origins without a
path/query/fragment. This policy only controls forwarding adapter-supplied
headers. Core retains independent network/SSRF checks.

`session_ref` and `refresh` are opaque adapter data. `metadata` is opaque JSON
and is not interpreted as the title/description timeline. `refresh_policy`
may provide an RFC3339 `expires_at`, a `refresh_before_seconds` value from 0 to
31,536,000, and unique HTTP statuses from 100 through 599. Core only triggers
refresh according to declared policy; it does not infer platform expiry
semantics. `archive_policy.source_uri` is `sensitive` by default or explicitly
`public`.

`watch.check` returns `{"state":"offline"}` for a successful offline check,
or `{"state":"live", ...}` for live. `session_ref` is an opaque identifier
for a broadcast instance. It must be valid UTF-8, contain no NUL, and be at most
4,096 bytes; it may be empty. `title` is optional, must be valid UTF-8, and is
at most 4,096 bytes; it may be empty. The current validator does not separately
reject NUL in `title`. `started_at` is optional RFC3339 time and, when present,
must decode to a nonzero time in years 1 through 9999. Optional `media` is
valid only for live results and must match the descriptor's media types. A
`watch.check` result may contain at most 64 state mutations. `metadata` results
also have a 64-mutation limit. There is no v1 item-count limit on
`ResolveResult.state`, `RefreshResult.state`, or
`ResolveWorkflowResult.state_mutations`. A timeout,
authentication failure, malformed result, or other adapter error must use the
error path; it must not be translated into `offline`.

`metadata` is polled only for active recordings by Core's metadata monitor; it
is separate from Watch polling. v1 source metadata contains only title and
description. Titles are at most 4 KiB UTF-8 bytes; descriptions at most 64 KiB.
NUL and invalid UTF-8 are rejected. `source_updated_at` is optional; when
present it must decode to a nonzero timestamp in years 1 through 9999. Core
records its own observation time. Metadata failure does not fail segment
acquisition, create a source gap, or trigger a media refetch. Repeated unchanged
values may be deduplicated by Core.

## Interaction message

Although the `interaction` capability has no current Core dispatch path,
`InteractionMessage` is used by workflow challenges. `type` is one of `action`,
`prompt`, `secret_prompt`, `navigate`, `display`, `status`, `complete`, or
`error`. `interaction_id` is nonblank and at most 256 bytes; `title` at most
512 bytes; `message` at most 4,096 bytes; `data` valid JSON at most 64 KiB.
Optional `fields` are `InteractionField` values with unique identifier keys,
nonblank labels, controls from the schema control vocabulary (excluding
display-only restrictions as validated by Core), and options for select
controls.

## Golden vectors and validation

`protocol/adapter-v1/` contains representative request/response fixtures for
the methods above. Core tests parse every frame, validate descriptor/media/
watch/metadata/workflow examples with the production validators, and compare
the capability/method inventories above to the production constants.

Adapter authors should run the Core-owned black-box validator against the
compiled executable:

```sh
go run ./cmd/adapter-conformance --binary /path/to/integrated-recorder-adapter-example
go run ./cmd/adapter-conformance --binary /path/to/integrated-recorder-adapter-example --json
```

The validator starts the process, checks the `describe` handshake and
descriptor, checks a structured error response for an unknown method, asks for
shutdown, and restarts the process to verify semantic descriptor stability.
Each operation has a deadline. The validator does not load or link adapter
implementation code.
