# Integrated Recorder

**한국어** | [English](README.en.md)

Go로 작성된 헤드리스, segment-native 라이브 스트림 아카이빙 서버입니다.

Integrated Recorder는 source manifest를 직접 추적하고 원본 media segment를 transcoding/remuxing 없이 저장한 뒤, 이를 브라우저에서 재생 가능한 VOD로 재구성합니다. 녹화 경로는 **FFmpeg나 Streamlink에 의존하지 않습니다.**

> [!NOTE]
> Integrated Recorder는 [Twitch Auto Recorder](https://github.com/dltkddnr04/Twitch-Auto-Recorder)와 [AfreecaTV Auto Recorder](https://github.com/dltkddnr04/AfreecaTV-Auto-Recorder)의 **정신적 후계자**입니다.
>
> 앞선 프로젝트들은 플랫폼별 방송 감지와 Streamlink/FFmpeg 기반 로컬 자동 녹화를 목표로 했습니다. Integrated Recorder는 “사람이 계속 지켜보지 않아도 라이브 방송을 자동으로 보존한다”는 목표를 이어가되, headless Go 서비스, 원본 segment 직접 보존, 브라우저 제어, 장기 아카이빙을 중심으로 처음부터 다시 설계한 프로젝트입니다.

## 핵심 원칙

- **원본 우선:** media payload를 변형하지 않고 그대로 보존합니다.
- **Segment-native:** acquisition 중 모든 녹화를 하나의 거대한 파일로 만들지 않습니다.
- **Headless first:** Docker에서 장시간 상시 실행하는 것을 전제로 합니다.
- **Browser controlled:** API와 Web UI가 공식 제어 경로가 됩니다.
- **재생성 가능한 projection:** VOD playlist, preview index, export, 향후 UI 데이터는 archive에서 다시 만들 수 있어야 합니다.
- **FFmpeg는 파생 작업 전용:** 녹화 수집과 원본 보관에는 필요하지 않습니다. 장면 미리보기와 리먹스 내보내기 같은 선택적 파생 기능에만 사용합니다.
- **한 번 등록하는 자동 녹화:** `watch` 감지를 지원하는 어댑터에서는 Watch를 한 번 등록해 두고, 이후 방송 회차를 서버가 감지해 각각 별도의 Recording으로 보존합니다.

상세 설계와 저장 방향은 [아키텍처 문서](docs/ARCHITECTURE.ko.md)를 참고하세요.

Plugin admission provenance와 publisher affiliation은 별도 metadata입니다. Host가 결정하는 provenance는 `bundled`, `registry`, `operator`이며 publisher는 `first_party`, `third_party`, `unknown`입니다. Bundled plugins는 `source.hls`와 `storage.local`입니다. Owncast는 first-party 소프트웨어이며 Core image에는 포함되지 않고, 공식 Registry에서 v0.2.0 plugin으로 배포됩니다. Registry v3는 publisher affiliation을 기록하고 artifact URL, size, SHA-256, descriptor identity, protocol을 고정합니다. Custom Registry와 `/external-adapters`에서 온 로컬 executable은 official Registry-approved plugin으로 표시되지 않습니다. API의 호환 필드 `reviewed`는 authoritative distribution 경로를 통한 admission을 나타내며 독립적인 human review를 뜻하지 않습니다. Registry는 현재 1인 maintainer 모드로 운영되어 PR과 필수 CI/artifact 검증을 유지하고 approving review 수는 0입니다. 두 번째 active trusted maintainer가 참여하면 최소 1건의 required approval을 복구할 예정입니다. 모든 plugin은 native executable이고 sandbox가 없습니다. 자세한 내용은 [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)을 참고하세요.

Runtime Host는 `IR_PLUGIN_REGISTRY_URL`로 설정된 Registry를 수동 refresh/install/update에 사용합니다. 공식 catalog는 `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`이고, Registry v1/v2 custom catalog도 호환됩니다. Registry는 빌드 서버나 publisher 서명 체계가 아닙니다. 기존 녹화는 시작 당시 Engine, adapter artifact/set, storage provider artifact/set을 계속 사용합니다. 기본 file secret store와 storage provider 설정은 private 권한으로 저장하지만 저장 시 암호화되지는 않습니다.

Core는 archive 형식과 쓰기 권한을 계속 소유합니다. `storage.local`은 Storage Provider Protocol v1을 사용하는 첫 bundled reference storage plugin이며, Registry 없이도 기본 primary로 제공됩니다. Core의 recording write, playback, integrity, delete, recovery 경로는 local 저장소에서도 provider executable을 통과합니다. Provider는 logical object key의 bytes를 물리적으로 배치·전달할 뿐 Recording, segment ordinal, metadata, gap, ownership semantics를 결정하지 않습니다. Runtime Host는 bundled executable을 일반 immutable artifact/set lifecycle로 import하고 application generation에 pin합니다. Local provider에는 Host가 통제하는 기존 `/data/recordings` root만 전달하며, API를 통해 arbitrary filesystem path를 지정할 수 없습니다. 기존 archive는 이 root에서 이동 없이 채택됩니다. `/data/runtime`은 generation, owner, catalog, settings, secret, IPC 및 recovery 상태 등 Core runtime state 전용입니다. v1은 한 번에 하나의 primary backend만 사용하며 기존 archive가 있을 때 물리 backend 종류를 바꾸는 자동 migration은 없습니다. 실제 S3/B2/WebDAV provider는 아직 포함되지 않습니다. 자세한 내용은 [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md), [storage provider lifecycle](docs/STORAGE_PROVIDER_V1.md), [아키텍처 문서](docs/ARCHITECTURE.ko.md)를 참고하세요.

## 현재 상태

**Milestone 1, external adapter protocol, restartless immutable adapter lifecycle 완료.**

React 관리 UI는 녹화 검색·페이지네이션, 태그·삭제, 무결성 확인·취소, 어댑터 관리, 리소스 탐색 capability, workflow, 알림, 설정·선택적 녹화 보존, 전역 검색, 로그 조회, Preview Frame Index, 자동 녹화 Watch를 backend API에 연결합니다. Watch는 지속적인 녹화 의도이고 방송 한 회차마다 별도의 Recording을 만듭니다. `watch` capability가 없는 어댑터에는 자동 녹화 등록을 표시하지 않습니다. 새 설치는 `/setup` wizard에서 관리자를 설정하고 설치 진단을 통과한 뒤 운영을 시작합니다. FFmpeg가 설치된 경우 장면 미리보기 프레임 생성과 별도의 remux export를 제공하며 canonical 녹화 데이터는 변경하지 않습니다.

운영 컨테이너는 안정된 listener를 소유하는 Runtime Host, 교체 가능한 Control Plane, segment 수집과 canonical archive를 소유하는 Recorder Engine으로 나뉩니다. 애플리케이션·adapter·storage provider 세대를 활성화해도 이미 녹화 중인 Engine은 자신이 pin한 artifact로 작업을 계속하고, 이후 시작한 녹화는 새 기본 세대를 사용합니다. Host/container 자체 교체는 별도의 유지보수 작업입니다. 세대 runtime, signed application release, rollback 동작은 [architecture 문서](docs/ARCHITECTURE.ko.md)와 [release 절차](docs/RELEASING.md)를 참고하세요.

장면 미리보기는 녹화별 opt-in 파생 기능이며 기본값은 꺼져 있습니다. 각 확정된 primary track 세그먼트에는 최대 한 개의 재사용 가능한 프레임을 비동기로 생성합니다. 먼저 대상 세그먼트만 시도하고(필요한 fMP4 init object 포함), 실패할 때만 제한된 이전 세그먼트 context로 재시도합니다. FFmpeg가 느리거나 실패해도 녹화는 계속되며, 포스터·스토리보드·향후 탐색 UI는 저장된 프레임을 재사용합니다.

현재 지원:

- Core와 분리된 실행 파일 adapter 구조. Core는 platform semantics를 알지 않으며 direct HLS 입력은 bundled `source.hls`, 플랫폼 integration은 Registry plugin으로 제공합니다.
- Adapter schema로 입력 및 설정 form을 생성하고, resource discovery와 설정 challenge를 generic workflow로 이어가는 기반.
- plugin 및 parent resource 설정을 합성하고, 현재 scope의 저장값과 effective 값을 구분해 노출.
- timeout/crash 후 다음 요청에서 backoff와 describe 재검증을 거쳐 adapter process를 lazy restart.
- adapter가 선언한 만료 media source refresh
- 완료 segment가 포함된 LL-HLS playlist를 포함하는 제한된 single-rendition HLS subset
- MPEG-TS/fMP4 형태 source object 직접 수집
- 저장 payload의 SHA-256 및 size 기록
- manifest snapshot
- 중복 억제, 제한된 retry, gap detection
- 프로세스 재시작 후 recording reload
- finite HLS VOD 재구성
- 브라우저 playback 및 seek
- Core-owned archive semantics 위에서 동작하는 Storage Provider Protocol v1. `storage.local`은 Registry와 무관하게 배포되는 bundled production plugin이자 reference implementation입니다. Production cloud provider는 포함되지 않습니다.

이전 Owncast TV 예제 방송 acceptance는 당시 Core에 bundled된 구현으로 수행했습니다. media segment 90개, 약 270초 VOD, detected gap 0개를 기록했고 재시작/reload 및 0:00, 2:15, 4:27 seek에 성공했습니다. 현재 Owncast는 Core에서 분리되어 official Registry의 v0.2.0 first-party plugin으로 배포되며, Core image에는 포함되지 않습니다.

아직 미지원:

- chat timeline
- finalized TAR/index archive format
- HDD/NAS/LTO tiering, multi-pool placement, archive migration, replication/mirroring
- platform-specific adapter 추가 등록
- external audio rendition synchronization
- 암호화 HLS 및 partial-only/delta LL-HLS
- DRM workflow
- 다중 사용자·역할 기반 authorization
- transcoding 및 추가 export format

## 실행

Go 1.23 이상이 필요합니다.

```sh
mkdir -p adapters
go build -o adapters/integrated-recorder-adapter-hls ./cmd/adapters/hls
DATA_DIR=./data ADAPTER_DIR=./adapters ADDR=127.0.0.1:8080 go run ./cmd/archiver
```

위 `cmd/archiver`는 API/UI 개발용 legacy monolithic 실행 경로이며 production Runtime Host의 세대 교체/전역 resource coordination 또는 bundled storage-provider process lifecycle을 제공하지 않습니다. 운영과 generation update는 Docker image의 `runtime-host` 실행 경로를 사용합니다.

실행 후 `http://localhost:8080/`을 엽니다.

Docker 설정도 포함되어 있습니다. 기본 runtime image에는 Alpine Linux의 `ffmpeg` package가 포함되어 장면 미리보기와 MKV 리먹스 파생 기능을 사용할 수 있습니다. Host 설치에서는 FFmpeg가 선택 사항이며, FFmpeg가 없어도 원본 녹화와 VOD 재생은 동작합니다. Alpine v3.21 package metadata는 `ffmpeg`의 license expression을 `GPL-2.0-or-later AND LGPL-2.1-or-later`로 표시합니다. FFmpeg upstream은 선택적 GPL 적용 구성 요소가 포함될 때 배포 조건이 달라질 수 있다고 설명합니다. 배포자는 실제 image의 package metadata와 [Alpine package record](https://pkgs.alpinelinux.org/package/v3.21/community/x86/ffmpeg), [FFmpeg legal considerations](https://ffmpeg.org/legal.html)를 확인하세요.

```sh
docker compose up -d
```

브라우저에서 `http://localhost:8080/`을 열어 설치를 진행하세요. Setup code는 다음 명령으로 확인합니다.

```sh
docker compose exec archiver runtime-host setup-code
```

컨테이너는 named `/data` volume을 사용하며 Runtime Host listener를 host loopback에 공개합니다. Image에는 Runtime Host, initial Control/Engine release, Host manifest에 등록된 bundled `/adapters/integrated-recorder-adapter-hls`, 그리고 `storage.local` executable이 포함됩니다. Owncast executable은 Core image에 포함되지 않습니다. Host는 시작할 때 storage.local을 Protocol v1로 probe하고 immutable storage artifact/set으로 import한 뒤 기존 `/data/recordings`를 archive root로 사용합니다. archive bytes 이동은 없습니다. `/data/runtime`은 이 archive와 분리된 Core runtime state로 유지됩니다.

로컬 개발용 executable은 `./adapter-binaries`에 `integrated-recorder-adapter-*` 이름으로 원자적으로 설치하고, production Runtime Host에서 사용하려면 `IR_ALLOW_OPERATOR_PLUGINS=1`을 명시적으로 설정하세요. Runtime Host가 `/external-adapters` read-only mount에서 이를 probe/hash/import하고 새 immutable adapter set을 활성화합니다. 이들은 `Local / Operator trusted`, `Not reviewed by Integrated Recorder`로 표시됩니다. Host나 container 재시작 없이 generation이 바뀌며 진행 중인 녹화는 원래 set을 계속 사용합니다. 공식 Registry에서 설치하려면 HTTPS Registry 주소를 `IR_PLUGIN_REGISTRY_URL`로 설정하세요. Core가 설정 없이 사용하는 공식 catalog URL은 `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`입니다. `/adapters` 화면에서 Registry source/storage plugin을 관리할 수 있지만 `storage.local`은 bundled mandatory plugin이므로 Registry 설치·제거 대상이 아닙니다. Registry exact size/SHA-256와 descriptor/protocol을 검증한 뒤 기존 immutable import lifecycle을 사용합니다. Custom catalog는 official Registry authority를 부여하지 않습니다. publisher PKI, automatic plugin updates, sandboxing은 제공하지 않습니다. 선택된 storage provider가 시작되지 않으면 generation readiness가 실패하며 Core는 direct-local I/O로 fallback하지 않습니다. 원격 애플리케이션 update에는 별도로 provision한 Ed25519 public trust key가 필요하며, trust key가 없는 배포는 fail-closed됩니다. 인증 없는 control API는 신뢰하는 host/private network 또는 인증 reverse proxy 안에서만 사용하고 untrusted network에 직접 공개하지 마세요.

## API

| Method | Endpoint | 용도 |
| --- | --- | --- |
| `GET` | `/healthz` | Health check |
| `GET` | `/api/adapters` | 사용 가능한 adapter 목록 및 상태 |
| `GET` | `/api/adapters/{id}/schema` | adapter 입력/설정 schema |
| `GET` / `PUT` | `/api/adapters/{id}/config` | plugin 정의 설정. secret 값은 다시 반환하지 않음 |
| `POST` | `/api/recordings` | 녹화 시작 |
| `GET` | `/api/resolve-workflows/{id}` | 일시 중단된 resource/configuration workflow 조회 |
| `POST` | `/api/resolve-workflows/{id}/continue` | 답변 제출 및 workflow 재개 |
| `DELETE` | `/api/resolve-workflows/{id}` | 대기 workflow 취소 |
| `GET` | `/api/recordings` | 녹화 목록 |
| `GET` | `/api/v2/recordings` | 검색·필터·정렬·커서 페이지네이션 녹화 목록 |
| `GET` | `/api/dashboard` | 실제 녹화, 저장소, integrity, adapter 현황 |
| `GET` / `PUT` | `/api/recordings/{id}/tags` | 태그 관리 |
| `DELETE` | `/api/recordings/{id}` | 비활성 녹화 삭제 |
| `POST` | `/api/recordings/{id}/integrity/verify` | 비동기 원본 무결성 확인 |
| `POST` | `/api/integrity/jobs/{job_id}/cancel` | 실행 중인 무결성 확인 취소 |
| `GET` | `/api/logs` | bounded application request-log 조회 |
| `GET` | `/api/recordings/{id}/archive/index` | canonical archive object 목록 |
| `GET` | `/api/recordings/{id}/previews` | Preview Frame Index에서 bounded sample 조회 |
| `GET` | `/api/recordings/{id}/previews/{archive_ordinal}` | 개별 장면 미리보기 프레임 조회 |
| `POST` | `/api/recordings/{id}/previews` | 녹화의 세그먼트별 미리보기 생성 활성화/보충 요청 |
| `GET` | `/api/adapters/{id}/resources` | adapter가 resource browse capability를 선언한 경우 목록 조회 |
| `POST` | `/api/adapters/{id}/restart`, `/enable`, `/disable` | 발견된 adapter process 관리 |
| `POST` | `/api/recordings/{id}/exports` | FFmpeg 설치 시 MKV remux job 요청 |
| `GET` | `/api/recordings/{id}/thumbnail` | Preview Frame Index의 호환 포스터 projection 읽기 |
| `POST` | `/api/recordings/{id}/thumbnail/regenerate` | 미리보기 생성/보충을 요청하는 호환 endpoint |
| `GET` / `PUT` | `/api/settings` | 실제 지원되는 UI theme 및 integrity concurrency 설정 |
| `POST` | `/api/auth/login`, `/logout`, `/bootstrap` | single-admin session 인증 |
| `GET` | `/api/recordings/{id}` | 녹화 상세 |
| `POST` | `/api/recordings/{id}/stop` | 녹화 중지 |
| `GET` | `/api/recordings/{id}/play/master.m3u8` | 생성된 VOD master playlist |
| `GET` | `/api/recordings/{id}/play/tracks/{track}/playlist.m3u8` | 생성된 VOD media playlist |
| `GET` | `/api/recordings/{id}/play/segments/{segmentID}` | 저장된 원본 payload |

bundled direct-HLS adapter로 녹화 시작 예시:

```sh
curl -X POST http://localhost:8080/api/recordings \
  -H 'Content-Type: application/json' \
  -d '{"adapter_id":"hls","input":{"manifest_url":"https://media.example/live/index.m3u8"},"title":"optional title","preview_mode":"segment"}'
```

`preview_mode`는 선택 사항이며 생략하면 `disabled`입니다. `segment`를 지정하면 canonical segment가 저장된 뒤 별도 bounded background worker가 장면 미리보기를 생성합니다.

## 개발

Web UI 개발에는 Node.js 20 이상이 필요합니다. Go API와 Vite 개발 서버를 별도 터미널에서 실행합니다.

```sh
go run ./cmd/archiver
npm --prefix web ci
npm --prefix web run dev
```

운영용 Go binary를 만들기 전에는 React UI asset을 embed 경로에 빌드합니다. `make build`는 UI build와 Go build를 순서대로 실행합니다.

```sh
npm --prefix web run build
go build ./...
# 또는
make build
```

처음 설치할 때 브라우저에서 `/setup`을 열고 `docker compose exec archiver runtime-host setup-code`로 일회용 setup code를 확인합니다. Host가 설치 완료 상태를 저장하며, 미완료 설치에서는 녹화와 자동화가 시작되지 않습니다.

검증 명령:

```sh
go test -race -count=1 ./...
go vet ./...
```

## Roadmap

- [x] 원본 segment acquisition + restart-safe VOD playback
- [x] platform-agnostic Core + Adapter Protocol v1 + bundled HLS bridge
- [x] Owncast source plugin release and official Registry approval
- [x] three-tier plugin admission provenance, separate publisher affiliation, and trust-aware runtime projection
- [x] restartless immutable adapter lifecycle (Host import, adapter-set generations, recording pinning)
- [x] resource discovery, configuration inheritance, and challenge/resume foundation
- [ ] chat timeline
- [ ] Finalized archive packaging + random-access index
- [x] management browser UI v2 및 실제 product API 기초
- [ ] CHZZK, SOOP, Twitch 등 추가 platform adapter
- [ ] HDD/NAS/LTO를 포함한 hot/cold storage lifecycle
- [x] Optional segment-based Preview Frame Index and remux-only export (FFmpeg가 있을 때)

## 문서

- [아키텍처](docs/ARCHITECTURE.ko.md)

## 라이선스

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). 자세한 내용은 [LICENSE](LICENSE)를 참고하세요.
