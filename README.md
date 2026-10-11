# Integrated Recorder

**한국어** | [English](README.en.md)

Integrated Recorder Core는 라이브 스트림의 원본 미디어 세그먼트와 방송 메타데이터를 보존하고 브라우저 재생을 제공하는 실행 본체입니다.

> [!NOTE]
> 프로젝트는 활발히 개발 중이며 Core의 안정적인 정식 공개 릴리스는 아직 없습니다.

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

기본 Runtime Host는 내장 사용자 인증을 사용합니다. 최초 설정에는 local console 또는 container log에 표시되는 one-time claim code가 필요하며, 완료 뒤 browser는 session cookie로 인증합니다. 모든 mutation 요청에는 CSRF token이 필요합니다. `AUTH_DISABLED=1`은 개발용 escape hatch이며 listener가 loopback에 bind된 경우에만 허용됩니다. public bind에서는 Runtime Host가 fail closed합니다.

Production에서 network를 통해 접속하려면 HTTPS, trusted reverse proxy, 적절한 network boundary를 구성하세요. TLS reverse proxy 뒤에서는 `COOKIE_SECURE=1`로 Secure cookie를 강제하세요.

## 빠른 시작

Docker가 기본 실행 경로입니다.

```sh
docker compose up -d
docker compose logs archiver
```

로컬 Runtime Host 콘솔 또는 container log에 표시된 one-time setup code를 사용해 브라우저에서 [http://localhost:8080/](http://localhost:8080/)의 `/setup`을 완료하세요. 로그에 접근할 수 없는 경우 `docker compose exec archiver runtime-host setup-code`를 실행하세요. 컨테이너는 named `/data` volume을 사용합니다.

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
- [Plugin Trust Model](docs/PLUGIN_TRUST_MODEL.md)
- [Storage Provider Protocol v1](docs/STORAGE_PROVIDER_PROTOCOL_V1.md)
- [Storage Provider 수명주기](docs/STORAGE_PROVIDER_V1.md)
- [Release 절차](docs/RELEASING.md)
- [Plugin Registry](https://github.com/integrated-recorder/plugin-registry)

## 현재 제한

프로젝트는 active development 상태입니다. chat timeline, 다중 storage placement와 replication, DRM, 다중 사용자 역할 관리 등은 아직 제공하지 않습니다. 현재 기능과 제한은 위 상세 문서 및 release notes에서 확인하세요.

## 라이선스

GNU Affero General Public License v3.0 only (**AGPL-3.0-only**). [LICENSE](LICENSE)를 참고하세요.
