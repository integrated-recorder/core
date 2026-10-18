# Integrated Recorder

**한국어** | [English](README.en.md)

Integrated Recorder Core는 라이브 스트림의 원본 미디어 세그먼트와 방송 메타데이터를 보존하고 브라우저 재생을 제공하는 실행 본체입니다.

> [!NOTE]
> 현재 공개 버전은 `v0.1.0-rc.8` prerelease입니다. 설치 스크립트는 공개 stable release가 있으면 stable을 선택하고, 없을 때만 가장 최신 prerelease를 선택합니다.

## 빠른 설치

Linux `amd64`/`arm64`, macOS의 Docker Desktop Linux engine, 또는 Docker 통합이 설정된 WSL2에서 설치할 수 있습니다. Docker Engine과 Compose v2.20 이상, Git, Python 3.9 이상이 필요합니다. Docker engine은 현재 host의 로컬 engine이어야 하며, recording 경로도 그 engine에서 볼 수 있어야 합니다.

```sh
git clone https://github.com/integrated-recorder/core.git
cd core
./scripts/install.sh --recordings-dir /absolute/path/to/recordings
```

스크립트는 공식 GitHub release를 선택하고 공개 release tag를 정확한 commit SHA로 확인한 뒤, 그 tag의 source로 이미지를 빌드합니다. Stable release가 없을 경우 prerelease를 선택했다는 알림을 출력합니다. 특정 버전을 고정하려면 `--version v0.1.0-rc.8`을 추가하세요. 기존 설치는 명시적으로 `--upgrade`를 지정하지 않는 한 기록된 버전을 유지합니다.

Application state, 인증 정보, Runtime Host 및 plugin/generation state는 전용 Docker volume의 `/data`에 저장됩니다. Recording archive만 지정한 host directory를 `/data/recordings`에 bind mount합니다. 앱은 container UID:GID `10001:10001`로 실행되며 installer는 그 identity로 파일 생성·읽기·rename·삭제를 검사합니다. 기본 host port `8080`이 사용 중이면 다른 loopback port를 선택합니다. 기본 network는 `127.0.0.1`이며 LAN 접근은 `--bind 0.0.0.0` 또는 특정 interface 주소를 명시해야 합니다.

설치 결과의 browser URL을 열고 `/setup`에서 첫 Owner의 login과 password를 입력하세요. 별도 setup code나 CLI 단계는 없습니다. 최초 계정 생성은 원자적으로 한 번만 허용되고 성공하면 바로 로그인됩니다. LAN binding 상태에서 초기 설정을 열어 두면 먼저 접속한 사람이 Owner를 선점할 수 있으므로 방화벽을 제한하고 본인이 먼저 설정하세요.

기본 설치는 loopback HTTP입니다. LAN 접근은 설치할 때 명시적으로 `--bind 0.0.0.0`을 선택하고 방화벽을 제한하세요. 첫 Owner를 만들기 전에 LAN에 열면 먼저 접속한 사람이 계정을 선점할 수 있습니다. HTTPS는 최초 Owner 생성 후 **설정 → 실행 환경 → HTTPS**에서 켭니다. 자체 서명 인증서는 지정한 실제 호스트명/IP 및 감지된 인터페이스 IP를 SAN에 넣어 생성되며 브라우저 경고가 표시됩니다. 도메인 공개 인증서는 도메인을 한 개 입력하고 ACME 자동 발급을 선택하세요. DNS가 이 host를 가리키고 외부 TCP 443이 도달해야 하며 설치기를 `--https-port 443`로 다시 실행해 포트를 연결해야 합니다. Runtime Host가 첫 HTTPS 접속 시 Go TLS-ALPN challenge로 발급하고 자동 갱신합니다. DNS/포트 사전 검사와 실제 인증서가 준비되기 전에는 HTTP→HTTPS redirect가 시작되지 않으므로, 발급 전 외부 접근은 방화벽으로 막아 두세요. 내부 CA 인증서는 먼저 자체 서명 HTTPS를 켜고 HTTPS URL로 재접속한 뒤 해당 호스트 SAN에 맞는 PEM 인증서와 개인키를 업로드하세요. private key는 cleartext HTTP로 전송되지 않습니다. 자체 서명 인증서는 만료 30일 전 Runtime Host 시작 시 자동 갱신되며 UI에서 즉시 교체할 수도 있습니다. 업로드 인증서는 새 PEM 쌍으로 갱신합니다. 자체 CA를 쓰면 각 클라이언트가 CA를 신뢰하도록 설정해야 합니다. 설치 시 HTTP/HTTPS 호스트 포트는 `--port`와 `--https-port`로 선택할 수 있고, 이후 Web UI에서 HTTP→HTTPS redirect를 설정합니다. 기존 설치의 포트 변경은 같은 설치기를 다시 실행하면 앱 release를 바꾸지 않고 Compose mapping만 갱신합니다.

Installer는 사용할 정확한 Compose project/file prefix와 실제 port를 출력합니다. 그 prefix에 `logs -f archiver`, `stop`, `start`, `restart`를 붙여 운영하고, 출력된 loopback URL의 `/api/setup/status`에서 version/channel을 확인할 수 있습니다. `/data`의 named volume과 지정한 recording directory를 함께 백업하세요. 일반 재실행은 버전을 고정하며, 업그레이드에는 `--upgrade`가 필요합니다. 권한 문제는 지정 archive directory에 대해서만 해결하고, 감지된 기존 데이터 volume을 삭제해 설치를 우회하지 마세요.

운영 명령, 설치 경로, 백업/upgrade, NFS 권한과 장애 복구는 [Deployment guide](docs/DEPLOYMENT.ko.md)를 참고하세요. 영문 문서는 [Deployment guide (English)](docs/DEPLOYMENT.md)에 있습니다.

## 역할과 책임 경계

Core는 Runtime Host, Recorder Engine, Control Plane, Web UI/API와 canonical archive를 소유합니다. Recording 수명주기와 ID, media acquisition 조정, segment ordinal·gap·integrity, metadata timeline, VOD 재구성, plugin 및 application generation 수명주기와 writer fencing도 Core의 책임입니다.

플랫폼별 동작은 Source Plugin이, 물리 object 저장은 Storage Provider가 담당합니다. Core는 third-party plugin source를 호스팅하거나 build하지 않습니다. `source.hls`와 `storage.local` 같은 bundled reference implementation은 Core 저장소에서 유지하며, canonical archive의 의미와 쓰기 권한도 Core가 직접 관리합니다.

## 핵심 동작

- segment-native 방식으로 원본 media bytes를 보존합니다. canonical recording 경로에는 FFmpeg나 Streamlink가 필요하지 않습니다.
- manifest와 metadata를 기록하고, archive에서 브라우저용 VOD playlist를 구성합니다.
- Docker와 browser UI를 중심으로 headless 상시 실행을 지원합니다.
- FFmpeg는 선택적 preview·remux 같은 derivative 기능에 사용할 수 있습니다. Core image에는 이러한 선택 기능을 위한 FFmpeg가 포함됩니다.

## Bundled components

- **source.hls** — 특정 플랫폼에 종속되지 않는 generic direct HLS Source Plugin.
- **storage.local** — Storage Provider Protocol v1을 사용하는 reference Storage Plugin.

Owncast는 first-party Source Plugin이지만 Core나 Docker image에 bundled되지 않습니다. 공식 Plugin Registry를 통해 v0.2.0으로 배포됩니다. 현재 plugin executable은 native code이며 sandbox가 없습니다. 상세한 admission provenance와 한계는 [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)을 참고하세요.

운영자가 직접 제공하는 plugin은 기본적으로 허용되지 않으며, production Runtime Host에서 사용하려면 `IR_ALLOW_OPERATOR_PLUGINS=1`을 명시해야 합니다. 이 plugin은 프로젝트 검토를 거치지 않은 native executable입니다. 기본 file secret store는 접근 권한을 제한하지만 저장값을 암호화하지 않습니다.

## 인증과 네트워크 노출

기본 Runtime Host는 내장 사용자 인증을 사용합니다. 브라우저에서 첫 Owner login/password를 정해 계정을 만들며, 원자적 사용자 저장소가 최초 계정 생성 여부를 표시하므로 bootstrap은 재사용할 수 없습니다. 완료 뒤 browser는 HttpOnly session cookie로 인증하고 mutation 요청에는 CSRF token이 필요합니다. 설치기는 인증을 비활성화하지 않습니다. LAN 또는 domain 공개 전에 방화벽과 HTTPS를 준비하고, plain HTTP LAN에서는 자격 증명이 암호화되지 않는 점을 고려하세요.

HTTPS는 Runtime Host 프로세스가 Go 표준 TLS listener로 직접 제공합니다. 외부 TLS 컨테이너나 프록시는 필요하지 않습니다. TLS 요청으로 발급·갱신되는 세션 cookie에는 Secure와 HttpOnly가 적용되며 SameSite 및 CSRF 검증은 유지됩니다. 설치기 기본은 `127.0.0.1`이며 인터넷에 포트를 공개하지 않습니다. Image-bundled release 설치와 향후 signed application update 신뢰는 별개입니다. 정식 public Ed25519 trust root를 설정하기 전까지 원격 signed update는 비활성화됩니다.

비밀번호 변경과 사용자·세션 관리는 Settings의 `계정·보안`에서 할 수 있습니다. 비밀번호를 잊은 경우 원격 reset endpoint는 제공하지 않습니다. 서버에 직접 접근할 수 있는 운영자는 설치 시 출력된 Compose project/file prefix를 사용해 `exec -it archiver runtime-host reset-password <login>`을 실행하고, 터미널의 prompt에서 새 비밀번호를 입력하세요. 비밀번호는 명령 인자로 전달되지 않습니다.

공식 Plugin Registry 주소는 `https://integrated-recorder.github.io/plugin-registry/catalog-v3.json`입니다. Registry는 plugin 배포 metadata이며 build 서비스가 아닙니다. 이미 설치된 plugin과 archive는 Registry에 연결할 수 없어도 사용할 수 있습니다.

## Archive와 저장소

Core가 recording identity, metadata, ordinal, gap, integrity 및 canonical commit 순서를 결정합니다. Storage Plugin은 logical object key의 bytes를 물리적으로 배치합니다. local 저장도 Storage Provider Protocol을 통과합니다. 저장 구조와 provider 제약은 [아키텍처](docs/ARCHITECTURE.ko.md), [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md), [provider lifecycle](docs/STORAGE_PROVIDER_V1.md)에 설명되어 있습니다.

## 개발

개발용 legacy 실행 경로는 production Runtime Host의 generation lifecycle을 제공하지 않습니다.

```sh
mkdir -p adapters
go build -o adapters/integrated-recorder-adapter-hls ./cmd/adapters/hls
DATA_DIR=./data ADAPTER_DIR=./adapters ADDR=127.0.0.1:8080 go run ./cmd/archiver
```

Web UI 개발과 전체 검증 명령은 저장소의 Makefile 및 CI workflow를 참고하세요. Release와 signed update 관련 절차는 [RELEASING](docs/RELEASING.md)에 있습니다.

## 자세한 문서

- [아키텍처](docs/ARCHITECTURE.ko.md)
- [배포와 운영](docs/DEPLOYMENT.ko.md)
- [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)
- [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md)
- [Storage Provider 수명주기](docs/STORAGE_PROVIDER_V1.md)
- [Release 절차](docs/RELEASING.md)
- [Plugin Registry](https://github.com/integrated-recorder/plugin-registry)

## 현재 제한

Core는 platform-independent timed event archive와 replay 기반을 제공합니다. Streamer별 연동은 Source Plugin을 통해 제공됩니다. Multi-pool storage placement와 replication, DRM은 아직 제공하지 않습니다. 상세 기능과 제한은 문서를 참고하세요.

## 라이선스

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). [LICENSE](LICENSE)를 참고하세요.
