# Integrated Recorder 아키텍처

[한국어 README](../README.md) | [English README](../README.en.md)

이 문서는 현재 코드의 동작을 설명합니다. protocol에 예약만 된 형태와 향후 계획은 실제 구현과 구분해 적습니다.

## Core 불변 원칙

canonical archive는 방송사에서 받은 원본 media와 timeline을 재구성할 metadata입니다. acquisition 중 media를 decode, encode, transcode, remux하지 않습니다. Core에는 플랫폼 domain model이 없습니다. resource type, field key, adapter state, interaction data는 opaque string 또는 JSON으로 취급하고 플랫폼별 탐색은 external adapter process에 둡니다. Archive identity, `recording.json`, metadata timeline, track/segment identity와 ordinal, manifest snapshot, gap/integrity semantics, canonical publication 순서, Recording owner fencing, ingest admission, retry policy, logical archive key는 Core만 소유합니다. Storage provider는 완성된 object의 물리적 배치와 전달만 담당합니다.

## 실행 세대와 package 책임

```text
Browser ── 안정된 HTTP listener ── Runtime Host
                                     ├─ Control 세대들
                                     │    ├─ API/UI, 인증, Watch, 설정, 관리 projection
                                     │    ├─ preview, integrity, export, retention 조정
                                     │    └─ 세대가 고정된 Engine IPC client
                                     ├─ Recorder Engine 세대들
                                     │    ├─ acquire.Manager, HLS scheduler, metadata monitor
                                     │    ├─ Core archive semantics, commit ordering, owner fence
                                     │    ├─ adapterhost ── Adapter Protocol ── source adapter
                                     │    └─ PhysicalObjectStore ── Storage Protocol ── pinned provider
                                     ├─ immutable adapter/storage-provider catalog
                                     └─ 공유 ingest/resource coordinator
```

운영 컨테이너는 `runtime-host`를 PID 1로 실행합니다. Runtime Host는 외부 listener, release 검증/설치, 세대 process 감독, routing, lease, drain, rollback, process 간 공유 resource coordinator를 소유합니다. HLS나 recording manifest를 해석하거나 플랫폼 metadata를 이해하지 않습니다. `control-plane`은 교체 가능한 관리 API와 Watch scheduler를 소유하지만 canonical media acquisition은 소유하지 않습니다. 각 `recorder-engine`은 자기 `acquire.Manager`와 자신이 admission한 작업을 소유합니다. Control process를 교체하거나 강제 종료해도 그 Control 세대의 Engine에는 signal을 보내지 않습니다. 보통 녹화는 시작 당시 Engine에서 계속되지만, 대상 Engine이 같은 adapter set과 storage provider set을 사용할 때만 Host handover 경로로 이동할 수 있습니다.

```text
후보 검증 및 준비 완료
  → 기존 Control admission 차단 및 승인된 mutation 처리
  → active Control epoch과 안정된 route 전환
  → 새 세대 background 작업 시작
  → 조건에 맞는 Recording을 새 Engine으로 각각 handover 시도
  → lease가 남은 기존 Engine 세대는 계속 유지
  → inventory가 비었을 때 Engine detach, 종료, 수거
```

애플리케이션 release 활성화 중 대상 Engine을 준비하는 동안 기존 Engine은 계속 실행됩니다. 대상은 Recording이 시작될 때 고정한 정확한 adapter 및 storage provider set과 resource/current-media context를 사용하고, 현재 media가 갱신되어야 할 때 pinned adapter의 refresh를 호출하며, 새 HLS manifest를 가져오고, 다음 미커밋 media payload를 제한된 ingest 메모리에 미리 준비합니다. 이 과정에서는 canonical archive를 수정하지 않습니다. Source는 speculative 준비 중 계속 수집하고, 이후 새 manifest admission을 멈추고 이미 승인된 쓰기를 drain합니다. 대상은 drain 뒤 마지막 canonical root를 기준으로 readiness를 다시 확인합니다. transfer 전 실패하면 source가 계속 owner입니다. Protocol v1은 원래 resolve workflow 입력을 보존하지 않으므로 대상은 새 resolve/continue 요청을 임의로 만들지 않습니다. 현재 resource/media context와 pinned adapter의 refresh 계약으로 continuation을 준비합니다. Handover는 immutable adapter-set과 storage-provider-set identity가 모두 정확히 같은 경우에만 허용하며 기존 plugin 동작을 live-migrate하지 않습니다.

Host가 소유하는 durable `recordingowner.Store` tuple(Recording, Engine generation, worker instance, 단조 증가 epoch)이 canonical write 권한의 기준이며 generation registry lease는 세대 retire를 위한 projection입니다. 모든 canonical mutation은 Recording별 cross-process lock 안에서 tuple을 확인하고 전체 commit이 끝날 때까지 lock을 유지합니다. Host가 비정상 재시작하면 선택된 active Engine이 `LoadAll`의 mutation recovery보다 먼저 owner fence를 설치합니다. 모든 owner shard를 잠그고 이전 active owner를 epoch high-water를 보존한 fenced 상태로 durable하게 바꾼 뒤 active archive를 `interrupted`로 복구합니다. Engine readiness 이후 Host는 stale lease projection을 원자적으로 비우고 연결되지 않은 draining generation을 dormant로 표시한 다음 Control/API를 활성화합니다. 이는 명시적인 fail-stop 복구 정책입니다. Host crash 뒤 진행 중 Recording을 재개하지는 않지만 orphan Engine은 더 이상 commit할 수 없으며 이미 commit된 payload, sidecar, root, metadata timeline은 유지됩니다. 같은 복구를 반복해도 결과가 같으므로 이 정책에는 handover별 journal이 필요하지 않습니다. Durable owner file은 CAS 전/후 epoch를 보존하고 모든 cold start는 active Recording을 일관되게 interrupted로 수렴시킵니다. Runtime Host, 컨테이너 이미지, 운영체제, bundled FFmpeg 교체는 별도의 유지보수 작업이며 application generation의 무중단 업데이트 범위가 아닙니다. owner/crash 처리 상세는 `docs/ACTIVE_RECORDING_ENGINE_HANDOVER.md`를 참고하세요.

Host의 `/data/runtime/state/generations.json`은 세대 상태와 recording-to-Engine lease를 보존합니다. Host는 ready Engine들의 bounded inventory와 lease를 대조한 뒤에만 세대를 retire합니다. 직전 정상 release는 rollback 대상이라 수거하지 않습니다. release directory는 immutable이며 canonical `recordings/`와 분리됩니다. 원격 artifact는 detached Ed25519 signature와 manifest의 size/SHA-256 검증을 통과해야 합니다. GitHub는 배포물 제공처이지 신뢰 기준점이 아닙니다. 호환성은 SemVer 추측이 아니라 runtime, IPC, archive, management schema 계약으로 확인합니다.

Engine 세대가 겹치는 동안 기존 ingest 한도는 Host coordinator가 전역으로 적용합니다. RAM reservation, recording별 한도, pending object/byte admission, 단일 canonical writer permit을 인증된 local IPC로 공유합니다. Storage facade는 bounded read/write throughput, latency, error, queue, buffer, writer metric을 보고하며 remote capacity는 무한대로 표시하지 않고 unknown으로 둡니다. Integrity, preview, export, Watch worker pool은 Control 세대 소유이며 기존 Control이 admission을 막고 이미 승인된 작업을 정리한 다음 후보가 넘겨받습니다. Storage v1은 installation당 canonical primary backend 하나를 선택하며 Recording별·다중 pool 배치, 복제, tiering, archive 자동 이동을 제공하지 않습니다.

Control과 Engine은 protocol 버전, request ID, deadline, generation/instance identity 및 최대 frame 크기가 있는 bounded authenticated local JSON IPC를 사용합니다. application child는 public listener를 직접 열지 않습니다. Host가 새 요청을 active Control로 routing하고 handoff 중 이전 route 요청의 종료를 기다립니다. 후보가 공유 management state를 준비하는 동안 mutation이 잠시 차단되어 재시도 가능한 unavailable 응답이 올 수 있지만 외부 listener는 계속 열려 있습니다.

Runtime Host는 설정된 adapter directory를 application generation이 실행할 경로가 아닌 신뢰된 import source로 취급합니다. Host가 `integrated-recorder-adapter-*` executable regular file의 안정된 snapshot을 만들고 Protocol v1 descriptor를 검증한 뒤 hash 기반 immutable artifact와 adapter-set manifest를 `/data/runtime/adapters`에 보관합니다. Application generation의 identity 조합은 `(application release, adapter set, storage provider set)`입니다. 같은 application release라도 어느 set이 달라지면 다른 generation이며, application release update는 두 현재 set을 새 세대에 이어 붙입니다. Adapter reconciliation, storage provider 변경, application update는 Host의 같은 operation gate로 직렬화되어 서로 다른 후보의 조각이 한 generation에 섞이지 않습니다. Control과 Engine에는 해당 adapter set의 private immutable `bin` 경로만 `ADAPTER_DIR`로 전달합니다. Adapter 추가·교체·삭제는 기존 readiness/fencing 흐름으로 새 generation을 준비하고 Host/container 재시작 없이 활성화합니다. Recording handover는 대상이 정확히 같은 immutable adapter-set과 storage-provider-set identity를 사용할 때만 허용합니다. 실행 중인 Host에서 handover가 진행되는 동안 old adapter artifact는 owner와 generation 참조가 옮겨 drain될 때까지 보존됩니다. Adapter process가 재시작되어도 같은 불변 artifact를 실행합니다. 인증된 `POST /api/runtime/adapters/reconcile`로 즉시 다시 확인할 수 있고 자동 reconciliation도 주기적으로 수행합니다. 잘못된 후보는 content digest 기반의 제한된 Host-process-local quarantine에서 제외하며, 해당 digest가 cache에 있는 동안은 다시 probe하지 않습니다. Host 재시작이나 cache eviction 뒤에는 다시 probe될 수 있습니다. 같은 binary 이름의 잘못된 교체는 이전 artifact를 유지하며 다른 유효한 후보는 함께 활성화할 수 있습니다. source inventory 또는 catalog 저장소의 치명적 오류, 또는 runtime reference를 안전하게 대조할 수 없는 경우 collection은 fail-safe로 중단되고 active set은 바뀌지 않습니다. Active/draining/staged/rollback/lease/Engine reference가 있는 artifact는 모두 보존합니다. Adapter는 신뢰된 로컬 코드이고 sandbox되지 않습니다.

`IR_PLUGIN_REGISTRY_URL`을 설정하면 Runtime Host는 승인된 원격 executable plugin을 가져올 수 있습니다. Registry schema v1은 기존 source adapter 형식과 호환되고, v2는 `type: source | storage`와 `{name, version}` protocol identity를 명시합니다. Registry는 빌드 서비스가 아니라 배포 승인 metadata입니다. Publisher CI가 만든 binary를 GitHub Release/CDN 등에서 전달할 수 있지만, 승인 authority는 설정된 HTTPS Registry이며 artifact hosting은 transport만 담당합니다. Registry는 target platform, filename, exact size/SHA-256, plugin ID/version/protocol을 고정합니다. Host는 private staging에 다운로드하고 byte 크기/hash를 확인한 뒤 type에 맞는 protocol probe와 descriptor identity 검증을 수행하고 해당 immutable catalog에 publish합니다. Registry 장애는 이미 설치된 plugin과 generation-pinned binary를 멈추지 않습니다. Source adapter 변경은 adapter-set generation을 만들고 storage provider는 설치와 설정/활성화를 별도로 합니다. `GET /api/runtime/plugins`는 제한된 typed status만 반환하고, 인증·CSRF 보호가 적용된 refresh/install/update/uninstall mutation은 application-generation 공통 operation gate를 사용합니다. 현재 수동 stable release target은 `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`입니다. Source Registry 문서/schema는 `docs/PLUGIN_REGISTRY_V1.md`, `protocol/plugin-registry-v1/`에, v2 storage 설명은 `docs/STORAGE_PROVIDER_V1.md`에 있습니다. Publisher PKI/signing, community catalog hosting, dependency resolution, automatic plugin updates, sandboxing은 구현하지 않았습니다.

## Storage provider process와 세대 pinning

production storage에는 별도의 Core 내 local 경로가 없습니다. `storage.local`은 독립 실행되는 bundled Storage Provider Protocol v1 executable이자 plugin ID `local`인 첫 reference storage 구현입니다. Plugin Registry가 설정되지 않아도 새 설치와 기존 설치의 기본 primary로 제공됩니다. Core의 `storage.Store` facade와 `ObjectStoreArchiveBackend`가 archive semantics를 소유하며 `PhysicalObjectStore` 경계에는 검증된 logical key와 byte stream만 전달합니다. Provider는 Recording domain object를 만들거나 metadata, segment ordinal, gap, owner, retry policy, canonical commit 순서를 결정하지 않습니다. 모든 production generation은 local primary를 포함해 immutable storage-provider set을 명시합니다. Application process는 generation에 지정된 정확한 executable 및 configuration snapshot을 시작하고 인증된 private Unix socket으로 연결합니다. 제어 frame/configuration은 크기가 제한된 JSON이고 object body는 Content-Length가 정확한 streaming HTTP-style transport로 전달합니다. Base64를 사용하지 않으며 cancellation과 backpressure를 전달합니다. Core는 object를 private runtime staging에 disk 기반으로 hash/size 검증한 뒤 provider에 stream하므로 object 전체를 추가로 RAM에 복제하지 않습니다. Recording write, playback, integrity read, delete, cold recovery는 모두 이 provider process 경로를 통과하며 production archive 경로에 local 전용 syscall bypass는 없습니다.

Runtime Host는 bundled `storage.local`을 다른 provider와 같은 descriptor probe, content-addressed artifact import, immutable provider-set 생성, readiness probe, generation pinning 절차로 처리합니다. Host가 고정 private root로 기존 `<DATA_DIR>/recordings`만 전달하며 public API로 임의 host path를 설정할 수 없습니다. 이 경로를 그대로 사용하므로 기존 archive bytes는 복사되거나 다시 쓰이지 않습니다. `recordings/<id>/...` logical object는 기존 root 아래의 물리 경로를 유지하고, 그 외 logical namespace는 recording 파일과 충돌하지 않는 provider 전용 물리 namespace에 놓입니다. `/data/runtime`은 generation, owner, operation state, adapter/storage catalog, Registry desired state, update, secret, IPC/runtime file, recovery coordination 등 Core runtime state만 보유하며 provider에 `/data` 전체를 주지 않습니다.

Host startup은 bundled executable을 찾아 import/probe하고, 기존에 storage set identity가 비어 있는 legacy generation을 선택된 local provider set으로 한 번 채택한 뒤, 그 generation의 정확한 provider를 실행·probe합니다. Provider readiness 다음에만 archive LoadAll/recovery와 application generation readiness가 진행됩니다. Legacy adoption은 Host-owned identity만 갱신하고 기존 `/data/recordings` 데이터는 이동하거나 재작성하지 않습니다. 선택된 provider가 없거나 손상·실행 불가하면 명시적으로 fail-closed/degraded 상태가 되며 production에서 `LocalFilesystemBackend` 또는 direct filesystem I/O로 fallback하지 않습니다. `storage.local`은 Registry 연결과 독립적인 bundled mandatory plugin이고 plugin API로 제거할 수 없습니다. 그 root는 Host가 관리하므로 provider의 사용자 설정 항목이 아닙니다.

Storage Provider Protocol v1은 `object.read`, `object.write`, `object.stat`, `object.list`, `object.delete`, `object.range_read`, `atomic_replace` capability를 요구합니다. 성공한 `Put`은 하나의 완전한 object를 publish했다는 뜻입니다. 실패/crash 뒤에는 기존의 완전한 object 또는 새 완전한 object만 보이고 canonical key에 partial object를 노출하지 않습니다. 응답이 유실되어 완료 여부가 불확실하면 Core는 stored size와 bytes/hash를 확인한 뒤 commit을 인정하거나 기존 retry 경로로 돌립니다. 재시도/backoff 정책은 Core가 소유하고 provider의 transport retry는 제한되어야 합니다. 상세 계약과 executable validator는 `docs/STORAGE_PROVIDER_PROTOCOL_V1.md`, `cmd/storage-provider-conformance`에 있습니다.

Runtime Host는 bundled 및 Registry provider executable을 `/data/runtime/storage-providers/artifacts/<sha256>`에 content-addressed로 import하고, 설정이 포함된 immutable provider set을 `.../sets/<set-id>`에 저장합니다. Registry artifact는 exact size/SHA-256 검증 후 descriptor의 provider ID, version, protocol, schema, capability를 검사합니다. `local`은 Registry가 아니라 Host가 통제하는 bundled executable source에서 가져오며 동일 Protocol v1 descriptor probe를 통과합니다. 설정과 secret은 private `0600` set snapshot에만 저장됩니다. API는 secret 값을 돌려주지 않고 provider에게는 인증된 local IPC로 보냅니다. `storage.local`의 root는 Host가 고정해서 주며 provider config API에서 바꿀 수 없습니다. Provider child process environment는 비어 있으며 같은 OS user로 실행되는 신뢰된 native code이지 sandbox가 아닙니다. Storage 설정 secret은 파일 권한으로 보호하지만 저장 시 암호화되지는 않습니다.

`GET /api/runtime/storage/provider`는 primary와 설치된 provider의 제한된 status를 반환하며 path, artifact digest, IPC credential, endpoint, secret은 노출하지 않습니다. `local`은 bundled/installed/active 가능 상태로 표시되고 uninstall 대상이 아닙니다. `/api/runtime/plugins`에서 Registry storage plugin을 install/update/uninstall할 수 있지만 install만으로는 active backend가 바뀌지 않습니다. `/api/runtime/storage/providers/{id}/config`가 Registry provider를 설정합니다. `/probe`는 지정된 설정의 정확한 provider process를 열어 설정을 검증한 뒤 Core가 reserved private namespace에서 put/stat/list/read/range/delete/missing-object 동작을 검사합니다. `/activate`는 해당 set을 고정하는 application generation을 준비·활성화합니다. Storage UI도 설치, 설정, probe, primary 선택을 구분합니다. Storage lifecycle은 app/source plugin update와 같은 Host operation gate를 사용합니다. Provider version update는 새 generation을 만들며 실행 중인 Recording은 시작 당시 Engine/provider artifact에 남고 참조가 있는 old set은 보존됩니다.

Storage v1은 installation당 primary backend 하나만 사용합니다. 다른 물리 backend 종류로 전환하려면 활성 Recording generation lease가 없어야 하고 active archive와 target archive가 비어 있음을 확인해야 합니다. 기존 local archive를 자동으로 다른 backend로 옮기지 않습니다. 선택된 provider를 시작하거나 probe하지 못하면 generation readiness가 실패하며 Core는 direct filesystem 경로나 빈 local archive로 조용히 fallback하지 않습니다. Storage-provider set이 다른 Engine으로 Recording handover도 허용하지 않습니다. Provider protocol과 backend-neutral VOD segment endpoint는 단일 byte range를 지원하고 선택된 provider에서 요청한 범위만 stream합니다.

- `internal/adapterproto` — 언어 중립 Protocol v1 envelope, descriptor/schema validation, resource, workflow, media source, refresh policy, adapter-owned state, 선택형 Watch 감지 메시지
- `internal/adapterhost` — Runtime Host가 application generation에 지정한 immutable adapter-set directory만 사용하고, 소유 process에서 장기 실행 process·descriptor/resource·설정·workflow session 관리
- `internal/watch` — 별도 durable management 저장소, bounded polling scheduler, session 중복 방지, 자동 Recording 시작/복귀
- `internal/pluginconfig` — 사용자 설정/secret과 adapter-owned opaque state/state secret을 분리 저장. interface는 backend-neutral하며 현재 구현은 file backend
- `internal/interaction` — 제한과 만료가 적용되는 generic interaction progress message
- `cmd/adapters/owncast`, `internal/adapters/owncast` — 첫 번째 standalone adapter. Core는 Owncast package를 import하지 않음
- `internal/hls` — 지원하는 HLS subset parser. `internal/acquire` — `recorder-engine`이 소유하며 polling, refresh, retry, segment acquisition, sequence epoch, metadata 관측, recording lifecycle 처리
- `internal/network` — public destination 검증 및 매 연결마다 검증한 주소에 고정하는 dial
- `internal/domain` — adapter wire type과 분리된 archive type. `internal/storage` — archive format/canonical commit semantics와 외부 physical placement의 `PhysicalObjectStore` 경계. `internal/storageproto` — 별도 streaming Storage Provider Protocol. `internal/storageprocess` — provider executable 감독. `internal/runtimehost/storagecatalog` — Host-owned imported artifact와 immutable 설정 set. `internal/server` — Control 세대 안에서 API와 embedded React SPA 제공
- `cmd/storage-local`, `internal/storagelocal` — bundled `storage.local` reference executable과 logical key의 물리 file placement. Core Recording type이나 archive 의미를 해석하지 않음

## Adapter process와 protocol

Adapter는 독립 실행 파일입니다. Runtime Host는 명시된 source directory에서 `integrated-recorder-adapter-*` 이름의 executable regular file만 찾고 `PATH` 전체를 검색하지 않습니다. Control과 Engine은 mutable source directory를 탐색하지 않습니다. Host가 private immutable snapshot을 import하고 adapter-set generation을 만듭니다. 부분 복사를 노출하지 않도록 임시 이름으로 복사한 뒤 최종 이름으로 atomic rename하세요. 새 set은 Host/container 재시작 없이 활성화됩니다. Adapter는 신뢰된 로컬 코드이며 application과 같은 OS user로 실행되고 sandbox되지 않습니다.

IPC는 크기가 제한된 newline-delimited JSON입니다. v1 기존 request/response wire 형식은 유지하며 request ID, protocol version, method, result, 구조화 error를 검증합니다. Parser는 typed frame과 예약된 `notification` 형태도 구분하지만 runtime은 비동기 notification을 전달하지 않습니다. response 대기 중 notification이 오면 protocol error로 처리합니다.

각 process의 호출은 직렬화됩니다. timeout, request 전송 후 cancellation, malformed/oversized output, 예상하지 않은 EOF, request ID 불일치, protocol 불일치, process exit는 해당 process를 unusable 상태로 만듭니다. 다음 operation에서 bounded backoff를 거쳐 필요할 때 lazy restart합니다. 매 restart마다 `describe`를 다시 호출하며 최초 discovery의 adapter ID, version, protocol version, canonical descriptor fingerprint가 모두 같아야 수락합니다. 무한 background restart loop는 없습니다. 종료 때 generic shutdown request를 보내고 제한 시간 안에 자식 process를 종료합니다.

v1은 descriptor가 관련 capability를 선언한 경우 `describe`, legacy `resolve`, `resolve.begin`, `resolve.continue`, `refresh`, `shutdown`을 구현합니다. `watch` capability를 선언한 adapter는 `watch.check`로 `offline` 또는 `live` 관측을 반환할 수 있습니다. 네트워크 오류나 잘못된 응답은 offline이 아닙니다. `media`를 함께 반환하면 기존 `StartResolved` 경로로 바로 시작하며, 생략한 경우에만 상호작용을 만들지 않는 stateless `resolve`를 사용합니다. 이 확장은 optional이므로 protocol version은 계속 v1이고 기존 adapter는 변경 없이 작동합니다. 나머지 operation 이름은 예약되어 있거나 구조화된 unsupported error를 반환합니다. 문법에 맞는 알 수 없는 optional capability는 보존하고 무시할 수 있지만 protocol version 불일치는 거부합니다.

Descriptor에는 optional `branding.icon` presentation metadata를 선언할 수 있습니다. 현재 형식은 `image/png`뿐이며 base64 JSON payload는 64 KiB 이하, 각 변은 512 pixel 이하이고 완전한 PNG로 디코딩되어야 합니다. SVG와 외부 URL은 허용하지 않습니다. Branding은 adapter 의미를 바꾸지 않으므로 semantic descriptor fingerprint에서 제외됩니다. Adapter API는 반복되는 목록 응답에 원본 바이트를 포함하지 않고 인증된 `/api/adapters/{id}/icon` URL만 반환합니다.

기본 Owncast adapter는 공식 로고를 포함합니다. 로고 출처, 변형 내역, 별도 CC BY-NC 4.0 조건은 `internal/adapters/owncast/assets/ATTRIBUTION.txt`에 기록되어 있습니다. 상업적 이용에는 별도 허가가 필요하며, Integrated Recorder의 자체 브랜드로 사용하지 않습니다.

## Resource, 설정, workflow

Adapter descriptor는 opaque resource type과 허용된 parent type 관계를 선언합니다. Core는 API hint, config scope, workflow discovery, persistence target에 대해 resource 참조와 모든 parent edge를 이 선언에 따라 검증합니다. type 이름의 의미는 해석하지 않습니다. chain 길이는 제한되고 cycle은 허용되지 않습니다.

설정에는 stored와 effective 두 view가 있습니다. Effective 값은 plugin scope, 가장 상위 parent resource부터 하위 resource, 현재 resource 순서로 합성하고 구체적인 scope가 상위 값을 override합니다. 각 field의 `inherit`가 상위 값을 하위로 전달할지 결정하며, 모든 control의 기본값은 `false`입니다. `clear_values`는 local override를 지워 상속/default 상태로 되돌립니다. `clear_secrets`는 secret을 명시적으로 삭제합니다. 빈 ordinary value는 값이며 빈 secret 제출은 기존 값을 유지합니다.

`resolve.begin`은 안정된 resource ID를 알기 전에 adapter 정의 input을 받습니다. Adapter가 resource를 반환하면 Core가 chain을 검증하고 해당 설정, secret, adapter state를 읽은 후 workflow를 이어갑니다. Challenge는 같은 schema vocabulary와 generic workflow state를 사용합니다. 각 challenge field는 `forbidden`, `optional`, `required` persistence mode와 `plugin`, `current_resource`, 명시적 `resource` target 중 하나를 선언할 수 있습니다. 명시적 resource는 검증된 현재 chain에 포함되어야 합니다. Persistent challenge field는 target scope의 descriptor schema에도 같은 key/control로 선언되어야 하며 그렇지 않은 값은 ephemeral입니다. 이로써 저장된 값이 다음 resolution에서 누락되는 일을 막습니다.

Workflow session은 process-local이며 이를 생성한 adapter process generation에 묶입니다. Process가 restart되면 stale workflow ID를 새 process로 전달하지 않고 정해진 만료 오류를 반환합니다. 유휴 TTL은 30분, 동시 session은 최대 128개, 전체 누적 transition은 최대 32회입니다. `DELETE /api/resolve-workflows/{id}`로 취소할 수 있습니다. 만료/취소 시 title과 연결된 interaction progress도 제거합니다. Core가 재시작되면 workflow session은 유지되지 않습니다.

Browser는 recording input, 설정, challenge에서 같은 schema renderer를 사용합니다. `visible_when` 문법은 `field` 비교 연산 `equals`, `not_equals`, boolean `truthy`와 재귀적인 `all`/`any`입니다. Default, select/multi-select, 상속 source, 명시적 clear 동작을 지원합니다. Secret 원문은 다시 읽지 않습니다. Prompt/display/status는 text로 렌더링하며 navigation은 HTTP(S) URL만 허용하고 `noopener noreferrer`로 엽니다. Adapter HTML/script는 실행하지 않습니다.

## Adapter-owned state와 refresh

Adapter-owned opaque state는 사용자 설정과 별도입니다. State value와 state secret은 adapter 및 선택적인 resource scope에 저장되며 Core는 key 의미를 해석하지 않습니다. Mutation target은 현재 검증된 chain에 속해야 합니다. 다음 resolve/refresh 때 다시 adapter로 전달하고 adapter process 재시작 후에도 보존됩니다. Recording metadata나 public API에는 들어가지 않습니다.

기본 file state-secret backend는 일반 state와 분리되고 파일 권한이 제한되지만 **at-rest encryption은 제공하지 않습니다.** 사용자 secret backend도 같은 한계가 있습니다. 둘 다 backend-neutral interface를 사용해 향후 encrypted 또는 OS-backed backend로 교체할 수 있지만 현재 제공하지 않습니다. 암호화 vault처럼 설명하지 않습니다.

Refresh capability를 지원하는 adapter는 만료 시각, 선행 refresh 시간, refresh를 유발할 HTTP status를 선언할 수 있습니다. Core는 status만으로 token 만료를 추측하지 않습니다. 선제 refresh와 adapter가 선언한 status refresh만 수행하고 새 media source는 adapter가 만듭니다. Core는 새 media와 request policy, public manifest URL을 검증한 뒤 staged adapter state 변경을 저장하고 활성 URL/header/forwarding policy/refresh policy를 교체합니다. 검증이나 refresh 실패는 활성 source를 대체하지 않습니다. 외부에 노출되는 refresh error는 generic하며 signed URL과 secret을 포함하지 않습니다.

## Media acquisition과 HLS 지원 범위

Adapter는 input을 generic media source로 resolve합니다. Playlist parsing, rendition selection, request/header policy, retry, segment 원본 byte 저장, SHA-256, gap detection, VOD 생성은 Core 책임입니다. Adapter header 전달 기본값은 same-origin입니다. Adapter가 정확한 origin allowlist를 선언할 수 있지만 Core는 요청 및 redirect마다 다시 검사하고 SSRF/public-address 정책도 별도로 강제합니다.

현재 HLS subset은 하나의 자체 완결 rendition, MPEG-TS 또는 fMP4 media object, init map, 명시적이고 overflow-safe한 byte range, discontinuity, program date/time, 완료된 `EXTINF` segment를 지원합니다. 동일 playlist에 완료 segment가 있으면 LL-HLS partial tag는 무시할 수 있습니다. 암호화 HLS, external audio/video/subtitle rendition, I-frame-only, URI 변수 대체(`EXT-X-DEFINE`), delta(`EXT-X-SKIP`), partial-only LL-HLS는 명시적으로 거부합니다. DASH, 별도 subtitle rendition, DRM/key acquisition은 구현하지 않았습니다. 거부 동작은 결정적이며 source media byte는 수정하지 않습니다.

Media sequence는 source identifier일 뿐 archive identity가 아닙니다. Core는 source epoch와 증가하는 archive ordinal을 함께 기록하여 sequence reset 이후 재사용된 번호가 새 segment 수집을 막지 않도록 합니다. VOD는 archive ordinal 순서로 만들고 epoch 경계 또는 감지한 gap에 discontinuity를 삽입합니다.

## Storage, privacy, recovery

각 recording은 `recording.json`, raw manifest snapshot, payload, segment sidecar를 포함하는 self-describing directory입니다. Database가 필수는 아닙니다. 새 directory는 초기 metadata를 hidden incomplete 경로에 먼저 기록한 뒤 원자적으로 공개합니다. Rename 전 file sync를 수행하고 지원되는 환경에서는 parent directory도 sync합니다.

Segment와 manifest payload 저장 후 각각 sidecar를 만들고 root recording metadata를 갱신합니다. 시작 시 유효 sidecar가 있으면 crash 직전에 root document에서 누락된 segment 또는 manifest snapshot을 복원할 수 있습니다. Sidecar 없는 payload는 orphan으로 보존하고 보고합니다. 손상되거나 충돌하는 sidecar는 조용히 attach하지 않고 보고합니다. Incomplete/corrupt recording은 보존하고 보고하지만 나머지 정상 recording은 계속 읽습니다. 비정상 종료 후 active recording은 `interrupted`가 되고 pending segment는 명시적인 gap으로 기록됩니다.

Storage facade는 logical archive key와 physical placement를 분리합니다. `recordings/<id>/tracks/main/00000042.m4s` 같은 key는 Core가 검증하며 provider의 bucket/object 배치와 무관합니다. 모든 production generation은 provider set을 pin하며, 기본 `storage.local`도 다른 Storage Provider Protocol v1 구현과 동일하게 `ObjectStoreArchiveBackend`와 `PhysicalObjectStore`를 통과해 Core-owned archive document, sidecar, integrity 확인, recovery 순서, delete marker, VOD projection을 사용합니다.

미디어 수집은 durable write와 분리됩니다. Network worker는 크기와 hash가 확인된 응답을 제한된 volatile RAM ingest buffer에 넣고 bounded storage queue로 넘긴 뒤 acquisition으로 복귀합니다. 단일 canonical writer가 buffer에서 저장하고 storage 실패 시 source를 재다운로드하지 않고 재시도합니다. 기본값은 전역 1 GiB, recording별 768 MiB, payload 512 MiB, queue 128개, writer 1개, 최초 쓰기를 포함한 5회 저장 시도(초기 100 ms, 최대 800 ms), metrics 5초 간격과 24시간 메모리 보존입니다. 이 운영값은 `management/system-settings.json`에 저장되며 Settings → Storage에서 바꿀 수 있습니다. 저장 즉시 반영되지만 storage/ingest 값은 서비스 재시작 뒤 적용하고 활성 녹화 중 buffer/queue를 resizing하지 않습니다. 최대 byte는 제한되며 payload 확장 시 old/new allocation이 일시 공존하는 점을 포함해 한도를 검증합니다. `Content-Length`는 reservation 힌트일 뿐이고 실제 read byte는 계속 제한됩니다. **Buffered는 committed가 아닙니다.** 완전한 payload publication, sidecar publication, durable `recording.json` 갱신이 끝난 뒤에만 preview, playback, integrity 및 canonical timeline에서 object를 봅니다. 짧은 storage 지연은 RAM으로 흡수하고 지속 과부하는 bounded backpressure를 적용합니다. Provider로 나가는 object는 private disk staging에서 stream하며 전체 object를 별도로 RAM에 복제하지 않습니다.

정상 종료는 신규 recording admission을 막고, network acquisition을 취소한 뒤 이미 수락한 ingest 작업을 호출자 deadline 안에서 drain합니다. RAM에만 있는 byte는 휘발성이며 canonical로 표시하지 않습니다. storage가 deadline 이후에도 막혀 있으면 manager는 무한 대기하지 않고 context 오류를 반환합니다.

Storage 관측은 Integrated Recorder storage facade를 통과한 I/O만 측정하며 OS 장치나 cloud account 전체의 traffic은 아닙니다. Remote provider capacity는 unknown으로 보고하며 무한 capacity를 표시하지 않습니다. 기본값은 5초 간격 측정과 24시간 memory retention이며 두 값은 검증된 범위에서 설정됩니다. 보존 기간은 `max(ceil(5분 / 측정 간격) × 측정 간격, 20 × 측정 간격)` 이상이어야 하며, 5분 관측 구간의 정수 sample과 방향별 최소 20개 sample을 보관합니다. 관측 상한은 실제 비유휴 sample 시간을 합산한 5분 이상과 방향별 최소 20개 sample이 있어야 계산합니다. read/write p95는 독립 추정치입니다. 인증된 `/api/storage/pools`, `/api/storage/pools/{pool_id}/metrics` endpoint는 absolute local path를 반환하지 않습니다. Installation마다 primary 하나를 사용합니다. 자동 placement, tiering, migration, replication, eviction, restore는 구현하지 않았습니다.

`internal/domain` archive type은 protocol wire type과 분리되어 있습니다. 기존 JSON field 이름을 유지하고 epoch, ordinal, provenance, URI classification은 optional이므로 구 recording도 읽을 수 있습니다. Adapter ID/version/protocol version/fingerprint는 provenance이며 credential, header, adapter state는 recording metadata에 복사하지 않습니다. Source URL과 raw manifest snapshot은 credential이 포함된 경우에도 원본 canonical data로 보존합니다. 따라서 recording directory 권한을 제한하고 public list/detail 응답에서는 URL을 제거합니다.

## Preview Frame Index

장면 미리보기는 원본 설명이나 archive payload가 아니라 버릴 수 있는 파생 projection입니다. Recording 생성 요청은 `preview_mode`를 받을 수 있으며 값은 `disabled` 또는 `segment`입니다. 생략 시 `disabled`이고, 이 경우 미리보기 전용 FFmpeg 실행도 없습니다. 설정은 canonical `recording.json`에 넣지 않고 `<DATA_DIR>/management/previews/`의 별도 management state에 보존합니다. Workflow로 녹화를 시작하는 경우에도 선택한 정책을 resolution 완료까지 유지한 뒤 만들어진 recording에 연결합니다.

Preview service는 주기적으로 committed canonical recording metadata와 frame index를 대조하여 누락된 작업을 발견합니다. Acquisition worker의 callback이나 결과 대기를 사용하지 않으므로 preview queue가 가득 차거나 FFmpeg가 느리거나 실패해도 segment download, payload 저장, metadata commit은 계속됩니다. Queue는 bounded이며 fixed worker 두 개만 FFmpeg를 실행합니다. Queue에서 밀린 항목은 다음 reconciliation 때 다시 발견됩니다. Preview policy가 활성화된 recording은 primary track의 각 committed media segment마다 최대 한 개의 frame을 생성합니다. Init segment와 manifest는 frame item이 아닙니다.

Preview projection은 `<DATA_DIR>/previews/<recording-id>/`에 보관하며 canonical recording directory와 분리됩니다. Profile v1은 순수 JPEG frame, 최대 480×270 크기입니다. 확정된 primary track 세그먼트마다 재사용 가능한 frame을 최대 하나 생성합니다. 첫 시도는 대상 세그먼트만 사용하고 fMP4라면 필요한 init object를 포함하며, FFmpeg의 범용 keyframe filter로 codec별 분기 없이 가장 이른 decoder 판정 random-access frame을 선택합니다. 이 시도에서 유효한 이미지를 만들지 못한 경우에만 이전 세그먼트 context를 추가해 일반 디코딩으로 다시 시도하고 seek를 대상 세그먼트 시작 경계에 둡니다. 이를 통해 종속된 target picture도 복원할 수 있습니다. Fallback은 최대 3개 이전 세그먼트, 30초·64 MiB 이내이며 source epoch, discontinuity 또는 init codec configuration 경계에서 중단합니다. 입력은 SHA-256/size를 확인한 local canonical payload 복사본으로만 구성합니다. FFmpeg에 remote URI를 전달하지 않고 local-only protocol allowlist와 process timeout을 사용합니다. Unsupported/decode failure도 projection 상태로 남으며 원본 recording은 바뀌지 않습니다.

`index.json`은 profile 정보와 archive ordinal, source identity, segment 및 frame 시각, source hash, frame 크기·용량·생성 시각을 연결합니다. `frame_time_seconds`는 의도적으로 canonical `segment_start_seconds`와 같은 값으로 기록합니다. 이 시작 시각은 primary track의 canonical playback 순서에서 앞선 저장 segment duration을 누적해 계산하며, 디코드된 실제 frame PTS는 수집하지 않습니다. 따라서 이는 근사값이지 선택된 frame의 측정 presentation timestamp가 아닙니다. Archive에 없는 gap duration은 playback timeline에 추정해 추가하지 않습니다. Frame은 검증·sync 후 원자적으로 공개하며 ready frame은 기본적으로 다시 생성하지 않습니다. 재시작 시 기존의 유효한 frame은 재사용하고 누락된 frame만 queue에 넣습니다. Recording 삭제 시 preview policy와 projection도 별도로 정리하며, 오래된 orphan projection은 안전한 ID/path 검사를 거쳐 회수합니다.

Poster, live 최신 미리보기, 녹화 목록 이미지, storyboard 및 향후 seek UI는 모두 같은 Preview Frame Index에서 frame을 선택합니다. Terminal recording의 기본 poster는 전체 playback timeline의 약 25% 지점에 가장 가까운 ready frame입니다. 기본 storyboard는 최대 48개의 고유 frame을 재생 시간 전체에 걸쳐 균등 sampling하여 표시하지만 이 개수와 화면 grid 열 수는 presentation 설정일 뿐 extraction profile이 아닙니다. Sampling 수·poster 위치·grid 배치를 바꿔도 video를 다시 decode하지 않습니다. Browser는 기존 native `<video controls>`를 유지하고 storyboard frame 선택은 해당 VOD playback time으로 seek합니다. `GET /api/recordings/{id}/previews`는 제한된 sample/summary metadata, `/previews/{archive_ordinal}`은 개별 image를 제공합니다. 기존 thumbnail API는 index의 poster를 돌려주는 compatibility view로 유지됩니다.

## Durable Watch와 자동 녹화

Watch는 “이 adapter-defined source의 앞으로 시작할 방송을 계속 기록한다”는 지속적인 management 의도이고 Recording은 단일 방송 회차의 archive입니다. Watch 정의와 실행 상태는 `<DATA_DIR>/management/watches/`에, Watch 입력의 secret 필드는 분리된 private 파일에 저장합니다. 공개 API에는 configured 여부만 반환합니다. Adapter가 반환한 `session_ref`는 Core 안에서만 SHA-256 digest로 바꿔 저장하며 원문은 API, event, log 또는 canonical archive에 기록하지 않습니다. `recording_id → watch_id/session digest/part` 관계도 archive 바깥 projection입니다. Watch 또는 Recording 삭제는 서로의 수명을 암묵적으로 바꾸지 않습니다.

Watch scheduler는 enabled Watch를 bounded queue와 고정 worker pool에서 확인합니다. 정상 offline 확인은 Watch polling 간격을 따르고 오류는 지수 backoff를 사용합니다. 작은 jitter로 startup/polling herd를 줄이고 수동·예약 확인은 Watch별 single-flight로 합칩니다. 확인 실패는 offline으로 표현하지 않습니다. `live` 이후 자동 시작은 기존 `Manager.StartResolved`를 사용하며 Watch 하나당 active Recording을 하나로 제한합니다. Active Recording 중에는 불필요한 live check를 멈추고, Recording이 끝나면 다시 확인합니다. 같은 session의 완료/수동 중지 결과를 dedupe/suppression 상태로 보존하고, interrupted 결과는 같은 session의 다음 part로 복구할 수 있습니다. Watch를 끄거나 삭제해도 연결된 active Recording을 중지하지 않습니다.

활성 Recording은 Watch 확인과 독립적으로 adapter의 optional metadata capability를 관찰할 수 있습니다. Canonical `recording.json` timeline에는 현재 source `title`과 `description`만 저장합니다. Core가 값을 검증한 시각을 `observed_at`으로 기록하고 adapter가 신뢰 가능한 `source_updated_at`을 제공하면 함께 보존합니다. 알려진 제목/설명의 반복 관측은 중복 저장하지 않습니다. `null`은 adapter가 해당 필드를 제공하지 않았다는 뜻이고 빈 문자열은 알고 있는 빈 값입니다. `Recording.Title`은 녹화 시작 시 정해지는 기존 호환/표시 제목으로 유지합니다. Metadata polling은 즉시 시작한 뒤 30초 간격으로 수행하고 실패 시 제한된 backoff를 적용하며, 실패가 media 수집 중단이나 gap을 만들지 않습니다. Metadata capability를 선언한 adapter만 polling합니다. Owncast는 `/api/status`의 `streamTitle`을 제공하지만 이 endpoint에서는 description이나 source update timestamp를 지원하지 않습니다. Chat timeline 수집은 아직 구현되지 않았습니다.

## Management API와 projection

`internal/recordquery`는 canonical recording snapshot에서 필터, 검색, 정렬, stable cursor pagination, 크기·segment·gap statistics를 계산합니다. `/api/v2/recordings`와 dashboard는 매 요청마다 archive를 다시 해석하지만 기록을 수정하지 않습니다. 계산 근거가 없는 gap duration은 `null`입니다. List/dashboard에는 작은 preview summary만 포함하고 frame index 전체를 포함하거나 recording마다 별도 status query를 요청하지 않습니다. 태그, workflow history, recording event projection, audit, notification, adapter enable preference는 `internal/management`의 별도 제한된 JSON store에 기록합니다. 이 데이터가 없어도 canonical recording을 해석할 수 있습니다.

Storage API는 선택된 primary provider의 bounded status, storage/ingest metrics, Core가 생성한 archive index를 제공합니다. Inactive recording만 삭제할 수 있습니다. Server의 per-recording lock은 playback/export input read와 deletion을 직렬화하며 active export/integrity job이 있으면 삭제를 거부합니다. 완료된 export artifact는 별도 projection이며 export job을 삭제할 때까지 유지됩니다. Production provider-backed delete는 Core 소유 bounded object enumeration, idempotent deletion marker, retry-safe object delete를 사용합니다. Monolithic 개발 명령에서만 쓰는 legacy direct-filesystem backend에는 tombstone helper가 남아 있지만 production Runtime Host 경로는 아닙니다. Archive index는 canonical metadata가 참조하는 object만 나열하며 absolute path와 source URI를 API로 노출하지 않습니다.

무결성 확인은 canonical SHA-256/size metadata를 streaming 방식으로 검증하는 bounded job service입니다. 각 job은 recording metadata snapshot을 대상으로 하고 acquisition 중인 녹화는 거절합니다. 동일 recording의 중복 요청은 coalesce됩니다. Job/result는 별도 management projection에 저장되며 재시작 당시 queued/running 작업은 interrupted/failed 상태로 회수됩니다. Verification은 segment capture를 수정하거나 막지 않습니다.

Adapter resource browsing은 protocol capability `resource_browse`를 선언한 adapter에서만 동작하며 list/search 결과, cursor, attributes를 Core가 opaque data로 전달합니다. Query와 결과 크기는 제한되고 호출 timeout이 적용됩니다. Adapter enable/disable preference는 별도 저장하며 세대별 immutable adapter-set discovery 뒤 적용합니다. Manual restart는 descriptor fingerprint를 다시 확인하고 해당 adapter의 workflow만 취소합니다. 이미 resolve가 끝난 recording의 acquisition process는 이 제어와 무관합니다.

첫 dashboard, paginated recording query, tags, delete, archive index, events, integrity, workflow list/history, resource browse/search, global search, notification, system-info/storage, bounded request-log API는 실제 저장소와 process status projection을 사용합니다. `/api/logs`는 HTTP method/status/duration과 안전한 component label만 process memory에 보관합니다. OS/application log 파일을 읽지 않고 URL, query, header, body를 저장하지 않으며 process restart 시 비워집니다. Global resource search는 recording에 알려진 resource chain만 검색하며 외부 network search를 호출하지 않습니다. Notification store와 workflow/audit/event history는 bounded retention을 사용합니다. 현재 recording event projection은 recording 상태, 관측된 manifest, gaps, management job 요청/결과 범위이며 segment마다 무제한 event를 기록하지 않습니다. 무결성 확인은 `POST /api/integrity/jobs/{job_id}/cancel`로 취소할 수 있습니다.

## 인증, settings와 파생 export

기본 실행은 single administrator authentication을 활성화합니다. 최초 설치는 Runtime Host가 소유하는 별도의 installation lifecycle입니다. 로컬 `runtime-host setup-code` 명령은 이미 생성된 mode-`0600` one-time claim code만 출력합니다. Browser는 이 값을 URL이 아닌 bootstrap form body로 제출합니다. Password는 12–72 byte이고 bcrypt hash만 저장합니다. Session token은 CSPRNG에서 생성합니다. token hash, CSRF hash, 만료 시각만 `DATA_DIR/security/sessions` 아래 별도 0600 session record로 저장하므로 Control 세대 교체 중 session과 server-side revocation을 공유합니다. Browser cookie는 HttpOnly, SameSite=Strict이며 mutation에는 CSRF token header가 필요합니다. TLS reverse proxy 뒤에서는 `COOKIE_SECURE=1`로 Secure cookie를 강제합니다. `AUTH_DISABLED=1`은 loopback bind에서만 허용됩니다. 현재 user/role system, password reset, remote identity provider는 없습니다.

설치 identity와 readiness는 application generation과 무관하게 유지되며 canonical recording archive나 product-management record와 분리된 `DATA_DIR/runtime/installation.json`에 저장됩니다. Runtime Host만 bounded mode-`0600` state와 transition을 소유합니다.

```text
uninitialized → setup_in_progress → ready
       └──────────────→ ready (loopback-only AUTH_DISABLED)
critical state 손상/불일치 → recovery_required
```

Installation record가 없고 유효한 기존 administrator가 있으면 legacy 설치로 판정해 `ready`로 원자적 migration합니다. 반대로 `ready` 설치에서 administrator가 사라지면 새 claim을 만들지 않고 `recovery_required`로 fail-closed합니다. `ready` 전 Control은 setup/authentication과 명시적 read-only diagnostics만 제공합니다. Watch scheduler, retention, preview reconciliation, storage metrics producer는 정지하며 일반 product mutation과 update activation은 거부됩니다. Setup 완료 때 Host는 active Control에 lifecycle IPC로 storage readiness를 검증시키고, `ready`를 durable하게 기록한 뒤 idempotent activation signal을 보냅니다. Host/Control 재시작도 durable state로 producer activation을 reconcile하므로 wizard 완료 뒤 재시작은 필요하지 않으며 설치 state는 generation별로 복제되지 않습니다.

Runtime Host는 `GET /api/runtime/update`와 인증 및 CSRF 보호가 적용된 `/check`, `/stage`, `/activate`, `/rollback` POST 작업을 처리합니다. update status는 path, PID, token, 원문 release notes를 제외하며 UI에서 텍스트로 표시하는 길이 제한된 릴리스 요약을 포함할 수 있습니다. 개발 build에서는 원격 update가 fail-closed됩니다. release 활성화는 기본 application 세대를 바꾸지만 진행 중인 Recording은 현재 Engine 세대에 남습니다. 이 API는 Runtime Host나 container image 자체를 업데이트하지 않습니다.

`internal/systemsettings`는 UI theme(즉시 적용), integrity concurrency(저장 후 server restart 적용), storage/ingest 운영 한도(저장 후 restart 적용), 선택적 recording retention을 관리합니다. Storage 설정 API는 저장값과 시작 시 적용된 값을 별도로 반환하고 재시작이 필요한 항목을 표시합니다. canonical commit 순서를 보존하기 위해 writer concurrency는 현재 1만 허용합니다. Retention 기본값은 비활성화와 30일 기준입니다. 활성화하면 설정 일수보다 오래된 completed recording만 후보가 되며, tag가 하나라도 있는 recording과 integrity/derivative job 진행 중인 recording은 보호합니다. Server 시작 시 한 번, 이후 24시간마다 bounded pass를 수행하며 pass당 최대 100개를 삭제합니다. `GET /api/retention/candidates`는 미리보기이고 `POST /api/retention/run`은 명시적으로 pass를 실행합니다. Bind address, storage root, adapter directory는 read-only입니다. Settings JSON은 strict validation 및 atomic private-file replacement로 저장됩니다.

`internal/preview`와 optional `internal/derivative`는 canonical commit 이후에만 동작하는 파생 서비스입니다. FFmpeg가 없는 host 설치에서도 source acquisition, canonical archive, VOD는 정상 동작합니다. Preview 생성과 MKV `-c copy` export만 unavailable이 됩니다. Export는 완료/중지 recording의 원본 HLS payload를 private staging directory에 검증 복사하고 local playlist를 만들어 MKV `-c copy` remux만 수행합니다. FFmpeg 인자는 shell을 통하지 않는 argument vector이고 network protocol은 허용되지 않습니다. Preview와 export job/staging 결과는 canonical archive 외부에 저장하고 cancellation, timeout, bounded concurrency를 사용합니다. Preview extraction은 위 Preview Frame Index 계약을 따르며 기존 ready frame을 교체하지 않습니다. 별도 single-thumbnail decode pipeline은 없고 `GET /api/recordings/{id}/thumbnail`과 regenerate 호환 API는 Preview Frame Index를 통해 동작합니다. FFmpeg 및 JPEG 생성 실패는 canonical archive에 영향을 주지 않습니다. Transcoding과 다른 container는 구현하지 않았습니다.

Recording directory는 `0700`, metadata/payload/sidecar 파일은 `0600`입니다. File secret/state backend는 at-rest encryption을 제공하지 않습니다. Management API는 single-admin 인증을 제공하지만 multi-user/role authorization은 없습니다. Local 실행은 loopback에 bind하고 Compose도 host의 `127.0.0.1`에만 port를 공개합니다. Reverse proxy 사용 시 TLS 및 cookie Secure 설정을 적용하고 untrusted network에 직접 노출하지 마세요. 관리 페이지는 bundled hls.js 1.6.7(Apache-2.0)과 restrictive CSP/security header를 사용합니다. Script는 same-origin만 허용하며, Radix 팝업 위치 계산에 필요한 inline style attribute만 `style-src-attr`로 허용합니다.

## Server lifecycle과 Docker

SIGINT/SIGTERM에서 Runtime Host는 안정된 listener를 닫고 child process의 제한된 종료를 감독합니다. 정상 Host/container 종료 시 Control은 새 management 작업을 막고 Watch scheduler를 종료합니다. Recorder Engine은 이미 승인된 ingest queue를 정리한 뒤 자기 recording worker와 adapter process를 닫습니다. 애플리케이션 release 활성화는 Engine을 종료하지 않습니다. Host/container 교체는 별도 유지보수입니다. Engine이 bounded shutdown을 마치지 못하거나 crash하면 active 녹화가 interrupted로 복구될 수 있습니다. Enabled Watch는 cold startup 뒤 stagger/jitter를 두고 다시 조정됩니다.

Container는 UID 10001로 실행합니다. Compose는 named `/data` volume을 쓰며 Runtime Host listener를 host loopback에 공개합니다. Image에는 Runtime Host, immutable 초기 Control/Engine release, `/adapters`의 Owncast, standalone `storage.local` executable이 포함됩니다. 기존 `/data/recordings`를 `storage.local`의 Host-controlled archive root로 넘겨 data movement 없이 기존 archive를 채택합니다. Provider는 `/data` 전체를 받지 않습니다. Generation registry, owner, operation state, runtime credential, 설치 release, imported adapter/storage artifact와 set, Plugin Registry desired state, recovery coordination은 Core 소유 `/data/runtime` 아래에 둡니다. Runtime image에는 Alpine v3.21 community repository의 `ffmpeg` package도 포함됩니다. Alpine package metadata에서 해당 package의 license expression은 `GPL-2.0-or-later AND LGPL-2.1-or-later`입니다. FFmpeg upstream은 GPL 적용 optional component가 포함된 build의 licensing effect를 별도로 설명합니다. 이는 법률 자문이나 Integrated Recorder 자체 라이선스 변경을 뜻하지 않습니다. 배포자는 실제 image의 package/build 정보를 확인해야 합니다. 참고: [Alpine v3.21 ffmpeg package metadata](https://pkgs.alpinelinux.org/package/v3.21/community/x86/ffmpeg), [FFmpeg legal considerations](https://ffmpeg.org/legal.html). 추가 executable adapter는 `./adapter-binaries`에 두고 `/external-adapters`에 read-only mount합니다. Runtime Host가 주기적으로 이를 import·검증한 뒤 불변 adapter-set을 묶은 새 application generation을 활성화하므로 Host/container 재시작이 필요하지 않습니다. 운영자는 curated HTTPS catalog를 `IR_PLUGIN_REGISTRY_URL`로 지정해 `/adapters`에서 Registry source/storage executable을 관리하고 `/storage`에서 provider 설정과 primary 선택을 합니다. `storage.local`은 Registry와 무관하게 제공되고 제거할 수 없습니다. Authentication을 명시적으로 disable한 배포도 loopback bind 외에는 허용하지 않습니다.

## Playback과 의도적으로 미구현인 항목

중지/완료/interrupted recording의 저장 segment를 참조하는 finite HLS VOD manifest를 생성합니다. Segment endpoint는 저장된 원본 byte를 직접 반환하며 파일을 이어 붙이거나 remux하지 않습니다. 재생 가능한 codec인지 여부는 browser 지원에 달려 있습니다.

미구현: chat timeline, 비동기 adapter notification runtime, 실제 platform authentication flow, Owncast 외 bundled platform adapter, Core 재시작을 넘는 workflow persistence, encrypted HLS, external rendition 동기화, DASH, TAR/archive finalization, LTO, export transcoding/추가 format, multi-user/role authorization, adapter sandbox, publisher PKI/signing, community registry hosting, automatic plugin update, plugin dependency resolution. Watch 감지는 polling 기반이며 webhook/push 알림은 구현하지 않았습니다.

## Web application

관리 UI는 `web/`의 React 19 + TypeScript SPA입니다. Vite, Tailwind, shadcn 스타일의 Radix UI components, TanStack Query/Router/Table, Lucide, hls.js를 사용합니다. API 호출과 CSRF 처리는 공통 client에 모이며, 서버 상태는 TanStack Query가 관리합니다. 브라우저는 같은 출처의 `/api`를 사용하고 VOD 재생에 필요한 hls.js는 bundle에 포함됩니다.

기존 monolithic local development command는 Go API와 Vite를 별도 터미널에서 실행합니다. API/UI 개발에 쓸 수 있지만 Runtime Host 세대 업데이트나 process 간 전역 resource coordinator를 제공하지는 않습니다.

```sh
go run ./cmd/archiver
npm --prefix web ci
npm --prefix web run dev
```

운영 binary에는 Vite build output이 `internal/server/static/ui/` 아래 embed됩니다. `make build`가 web asset build 후 Go build를 순서대로 실행하고, Dockerfile은 별도의 Node build stage에서 asset을 생성합니다. React client route는 allowlist된 경로에 한해서 직접 열 수 있으며 `/api/**`와 `/static/**`는 SPA fallback 대상이 아닙니다.

첫 실행에서 UI는 인증보다 먼저 Runtime Host의 public `/api/setup/status`를 조회합니다. 설치가 미완료면 `/setup`을 열고, 설치가 준비된 뒤에는 `/api/auth/session`을 확인해 인증이 필요하면 `/login`을 표시합니다. 관리자 claim은 기존 `/api/auth/bootstrap`을 재사용하고, 인증된 browser가 Host-owned setup transaction을 시작하고 완료합니다. Setup code는 URL이나 API 응답에 노출되지 않습니다. 로그인 후 mutation은 backend session의 CSRF token을 공통 API client가 자동 전송합니다.
