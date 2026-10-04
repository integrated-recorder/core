# Storage Provider Protocol v1

Status: normative for external physical storage provider executables. Protocol v1 is separate from Adapter Protocol v1. It describes physical object I/O only. Integrated-Recorder Core remains the archive authority: it owns recording identity and manifests, logical object keys, segment ordinals, metadata and gap semantics, canonical ordering, retry policy, and recording ownership fencing. A provider never receives or creates a Core `Recording` model.

A provider maps an opaque logical key to physical placement and transport. For example, Core may write `recordings/<id>/tracks/main/00000042.m4s`; a provider stores and retrieves the complete bytes at that key.

## Trust and process model

A provider is a trusted native executable running under the same OS user as its parent. Protocol v1 provides no sandbox. Providers that access cloud services need network access and can execute arbitrary code with their process user's privileges.

The parent starts an absolute executable directly, without a shell, using:

```text
provider --socket <absolute-private-unix-socket-path> --token-file <absolute-private-0600-file>
```

The parent supplies an empty environment. The provider reads the bearer credential from the token file and creates the Unix domain socket with mode `0600`. The parent connects only over that socket and sends `Authorization: Bearer <token>` on every request. The token is compared in constant time. The socket and token are private runtime material and must never be returned by public APIs.

The provider's stdin is reserved as a parent-liveness pipe and carries no protocol or configuration data. The parent keeps it open while the provider is supervised and closes it when forced shutdown is needed. A provider must watch stdin for EOF, cancel in-flight request contexts, close its listener/connections, and exit. The Go `Serve` helper accepts this reader as `ServeOptions.ParentLiveness`; executable providers should pass `os.Stdin`. On Linux the launcher also sets `PR_SET_PDEATHSIG` behavior (`SIGTERM`) as defense in depth; stdin EOF covers other supported Unix platforms and the fork/exec race. Providers must not rely on environment inheritance or use stdin for object data.

Configuration and credentials are sent after connection with `PUT /v1/config`. They must not be put in arguments, environment variables, diagnostic output, or error messages. Provider diagnostics belong on stderr. A provider must not log configuration, secrets, request bodies, authorization, or signed URLs. Stdout is not a protocol channel and the parent discards it. The parent first uses SIGTERM for bounded graceful shutdown; EOF on the liveness pipe is the cancellation/forced-exit fallback, followed by process termination if necessary. There is no public-network listener or shutdown HTTP endpoint.

## Bounds and encoding

Control requests and responses use UTF-8 JSON over HTTP on the private socket. The maximum ordinary control frame, including descriptor, probe result, errors, and list page, is 64 KiB. Configuration request bodies are limited to 1 MiB. Object requests and responses stream raw bytes; base64 is forbidden. The maximum object is 1 GiB. Object requests use an exact `Content-Length`; unknown-length and over-limit writes are rejected. The Core and provider must propagate cancellation and backpressure rather than buffering a whole object.

The provider must bound memory independently of object size. The parent may serialize operations sent to one provider process. A provider may perform only bounded transport-level retries; Core remains authoritative for archive retry and backoff policy.

Every endpoint requires bearer authentication. Unknown routes and methods return a bounded JSON error envelope. Error messages are safe fixed text and must never include arbitrary backend response content:

```json
{"error":{"code":"not_found","message":"object not found"}}
```

Recognized error codes include `invalid_request`, `invalid_key`, `invalid_prefix`, `invalid_cursor`, `invalid_limit`, `invalid_size`, `size_mismatch`, `invalid_range`, `not_found`, `unauthorized`, `method_not_allowed`, `unsupported`, `provider_error`, `canceled`, and `invalid_provider_response`. Callers should act on codes, not provider-controlled text.

## Descriptor and configuration

`GET /v1/descriptor` returns exactly one JSON descriptor:

```json
{
  "protocol_version": 1,
  "id": "example-storage",
  "name": "Example Storage",
  "version": "1.0.0",
  "configuration_schema": {"fields": []},
  "capabilities": [
    "object.read", "object.write", "object.stat", "object.list",
    "object.delete", "object.range_read", "atomic_replace"
  ]
}
```

For canonical primary use, all seven capabilities shown above are required. Protocol v1 accepts only these capability names. ID is a stable lowercase identifier matching `[a-z][a-z0-9-]{0,62}`; name is bounded UTF-8 text; version is a bounded non-empty stable provider version string. The protocol version, identity, schema, and capabilities must stay semantically stable across process restart for one artifact. Descriptor fingerprinting sorts capability names before hashing canonical JSON; schema field order remains significant for presentation.

The independent configuration schema supports `text`, `secret`, `boolean`, `number`, and `select` fields. It is bounded to 64 fields, each with a unique key, label, optional description, required flag, optional default, select options, and basic min/max or min/max-length constraints. Secret fields must not declare defaults. The schema is not Adapter Protocol schema and providers do not import adapter packages.

`PUT /v1/config` takes a bounded JSON body:

```json
{"values":{"region":"ap-northeast-2"},"secrets":{"access_key":"secret"}}
```

Values are JSON values represented on the Go wire API as `json.RawMessage`; secrets are strings. A provider receives only its own values and secrets. It must not persist secrets in logs or include them in errors.

`POST /v1/probe` accepts `{}` and returns `{"ready":true}` only when its configuration is usable. Core additionally performs an object operation probe in a reserved private namespace: random bytes are put, statted, read and hash-compared, deleted, then confirmed missing. Probe residue from a crash must be safely cleanable and must not overlap user archive keys.

## Logical keys

Keys are opaque UTF-8, slash-separated logical names. A key is non-empty and at most 1024 bytes; it has no leading slash, backslash, NUL/control characters, empty path component, `.` component, or `..` component. Examples include `recordings/0123.../recording.json` and `recordings/0123.../tracks/main/00000042.m4s`. Providers must not reinterpret a key as authority to escape their configured namespace.

A list prefix may be empty; a non-empty prefix follows the same safe component rules and may end in exactly one `/` to select a directory prefix. Leading slash, repeated slash, `.`/`..`, backslash, and control characters remain invalid. Pagination is stable lexicographic order. The limit is from 1 through 1000. A cursor is opaque to clients; providers return the last key of a page as the next cursor and subsequent pages contain keys strictly greater than it. Pages must not duplicate keys.

## Endpoints

All URLs below are relative to the authenticated private HTTP connection.

### `PUT /v1/objects?key=<key>`

The request body is the raw object stream and must have an exact `Content-Length` from 0 through 1 GiB. Success is HTTP 200 with a bounded JSON `ObjectInfo`:

```json
{"size":7,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
```

Success means the complete object was atomically published. For a new key, a failure leaves no object. For replacement, failure leaves the old complete object. A crash or timeout may leave either the old complete object or the new complete object, never a partial canonical object. Implementations commonly stream into a private temporary object, flush it, then atomically publish/replace it; object stores may use complete PUT publication or an equivalent atomic mechanism.

A timeout can be ambiguous: the provider may have published the new complete object before the response was lost. Core may stat/read/hash the object and retry idempotently. The provider must not expose partial bytes and must not invent a second logical key for a retry.

### `HEAD /v1/objects?key=<key>`

Returns 200 with exact object `Content-Length` and `X-IR-Object-SHA256`. A missing key returns 404 with `not_found`.

### `GET /v1/objects?key=<key>`

Returns 200 with raw streamed object bytes, exact `Content-Length`, and `X-IR-Object-SHA256`. Missing keys return 404.

A single standard byte range is supported as `Range: bytes=<start>-<end>` with inclusive non-negative offsets. Success is 206, with `Content-Length` equal to the selected byte count, `Content-Range: bytes <start>-<end>/<total-size>`, and `X-IR-Object-SHA256` for the full object. Multiple, suffix, open-ended, invalid, or unsatisfiable ranges are rejected with a bounded safe error; v1 does not require multipart ranges.

### `GET /v1/list?prefix=<prefix>&cursor=<cursor>&limit=<n>`

Returns a bounded JSON page:

```json
{"items":[{"key":"recordings/a","size":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}],"next_cursor":"recordings/a"}
```

`sha256` is optional in a listing. Items are strictly lexicographically ordered, match the requested prefix, and continue strictly after a non-empty cursor. `next_cursor` is omitted on the final page.

### `DELETE /v1/objects?key=<key>`

Returns 204 for a present or absent key. Delete is idempotent. Core owns bounded enumeration and retry for recording deletion; a provider must not turn partial failures into success.

## Provider obligations and security

The provider is a physical object transport only. It does not decide recording boundaries, ordinals, canonical writer ownership, archive recovery, or retries. Core checks recording ownership/fencing before canonical commit and controls the ingest queue. A provider must not communicate with source adapters or accept public web requests that can write archive keys.

The parent passes only minimal process environment. Its own executable arguments and token file are private. Cloud credentials belong only in the provider's secret configuration. Do not expose filesystem paths, IPC token, credential values, upstream URLs, signed URL query strings, raw provider errors, or response bodies in APIs or telemetry. Core should report only bounded safe error codes.

Storage providers are trusted executable code, not sandboxed plugins. Plugin Registry approval and artifact hash verification establish which exact executable was selected; they do not reduce executable privilege.

## Go reference implementation and conformance

`internal/storageproto` is the Core's Go wire/client/server implementation. `Provider` exposes `Put`, `Open`, `OpenRange`, `Stat`, `List`, and `Delete` in terms of streams and logical keys. `Serve` provides the authenticated Unix-socket endpoint surface. Implementations must run the black-box validator against the compiled executable:

```sh
go build -o storage-provider-conformance ./cmd/storage-provider-conformance
go run ./cmd/storage-provider-conformance --binary /absolute/path/to/provider --json
```

An optional private JSON configuration file may be passed with `--config`. The conformance runner reports only fixed check names, provider identity, and descriptor fingerprint; it never includes configuration, token, path, stderr, or arbitrary provider errors.

## Bundled reference provider: `storage.local`

The product bundles `storage.local`, provider ID `local`, as its first
production Storage Provider Protocol v1 implementation. It is a separate
executable and uses the same authenticated control plane and streaming object
data plane as external providers. Its presence does not depend on a Plugin
Registry, and it is a mandatory bundled plugin rather than a Core backend
shortcut.

Runtime Host supplies this provider with a fixed private archive root at the
existing `<DATA_DIR>/recordings` location. Public storage configuration cannot
grant it an arbitrary host path, and the provider does not receive the broader
`/data` tree. This lets existing logical keys in the `recordings/<id>/...`
namespace retain their physical locations during legacy adoption without
copying recording bytes. Other logical namespaces may use a provider-private
physical namespace under that root; such details are not part of the portable
Protocol v1 contract. Core runtime state remains outside the provider root.

`storage.local` implements safe logical-key containment, streaming reads and
writes, bounded listing, byte ranges, idempotent deletion, and complete-object
atomic publication/replacement. For a new object it writes to a private
temporary file, verifies the declared size, syncs the file, atomically publishes
the complete object, and syncs the containing directory where supported. A
replacement is prepared and synced separately so failure leaves either the old
complete object or the new complete object. It rejects path traversal and
symlink escapes within its configured root. The provider still does not
interpret Recording metadata, choose ordinals, validate ownership, or decide
retry and recovery behavior; these remain Core responsibilities. All
production local archive writes, reads, integrity checks, deletion, and
recovery go through this process path, and a provider startup failure does not
authorize Core to perform direct local filesystem I/O.
