# 배포와 운영

이 문서는 공식 Docker 설치 경로를 설명합니다. 새 설치에서 사용자가 지정할 애플리케이션 정보는 recording archive를 저장할 host의 절대 경로뿐입니다.

## 지원 환경과 준비 사항

Application image는 Linux `amd64` 또는 `arm64`로 실행됩니다. 지원 대상은 Linux, macOS의 Docker Desktop 로컬 Linux engine, Docker 통합이 설정된 WSL2입니다. Installer에는 Docker Engine, Docker Compose v2.20 이상, Git, Python 3.9 이상이 필요합니다. Recording 경로가 같은 host의 경로여야 하므로 원격 Docker context는 거부합니다.

Docker daemon은 checkout source를 읽고 recording 경로에 접근할 수 있어야 합니다. Docker Desktop에서는 두 경로를 Docker Desktop에 공유하세요. 현재 공식 Docker image는 배포되지 않으므로 Installer가 공식 source를 로컬에서 빌드합니다.

## 새 설치

공식 repository를 checkout한 뒤 실행합니다.

```sh
git clone https://github.com/integrated-recorder/core.git
cd core
./scripts/install.sh --recordings-dir /absolute/path/to/recordings
```

Installer는 `integrated-recorder/core`의 공개 GitHub release metadata를 읽습니다. 공개 stable release가 있으면 최신 stable을 고르고, 없을 때만 가장 최근에 공개된 prerelease를 선택합니다. Draft는 제외합니다. 선택한 tag를 commit SHA로 확인하고 release manifest identity와 필수 role artifact를 검사한 뒤 exact tag를 fetch합니다. Git이 해당 tag를 같은 SHA로 해석하는지 확인하고 detached source에서만 image를 빌드합니다. Prerelease를 선택하면 그 사실을 출력합니다. 버전을 고정하려면 `--version v0.1.0-rc.8`을 지정하세요.

Docker build에는 `VERSION`, `COMMIT`, `BUILD_TIME`, `RELEASE_CHANNEL`이 전달됩니다. `BUILD_TIME`은 official release workflow와 동일한 Git `%cI` committer timestamp입니다. Installer는 image label과 실제 Runtime Host 및 Control Plane의 build-info 문자열을 확인하고, Recorder Engine·HLS adapter·storage provider가 image에 있는지도 검사합니다. 시작 후에는 Host가 보고하는 version/channel, 인증 상태, health endpoint와 setup page도 검사합니다. 로컬 build executable이 signed Linux release binary와 바이트 단위로 같다고 주장하지 않습니다. Docker source build에서 signed executable manifest signature를 독립 검증하지 않습니다.

## 배포 경로와 영속 데이터

Installer는 private deployment metadata와 Compose 파일을 운영체제별 사용자 application-data 디렉터리에 기록합니다.

| Host | 기본 설치 디렉터리 |
| --- | --- |
| Linux / WSL2 | `${XDG_DATA_HOME:-$HOME/.local/share}/integrated-recorder` |
| macOS | `$HOME/Library/Application Support/Integrated Recorder` |

`deployment.json`에는 선택한 tag, commit, build time, channel, architecture, host port, source checkout과 archive 경로가 저장됩니다. 중단된 설치를 안전하게 재개하기 위한 pending record도 사용합니다. Exact release source는 `source/<commit>/`에 cache되며 build 전에 변경 여부를 검사합니다.

Compose는 `integrated-recorder`라는 독립 project와 `integrated-recorder_application-state` named volume을 사용하며 이 volume을 `/data`에 mount합니다. `/data`에는 application config, 인증과 session, Runtime Host owner/generation state, plugin catalog와 artifact, storage-provider metadata, 복구 조정 상태 등 durable application state가 저장됩니다. 단순 cache가 아니며 삭제하면 안 됩니다.

Archive root만 host bind mount입니다. 사용자가 지정한 디렉터리가 `/data/recordings`에 mount됩니다. 이 nested bind mount는 parent `/data` named volume 내부에서 overlay되어 media가 Docker application-state volume에 조용히 저장되지 않습니다. Compose는 host path 자동 생성을 끈 long bind 형식을 사용하고, Installer는 service 시작 전에 실제 경로를 검사합니다. 기존 archive 파일을 복사하거나 이동하지 않습니다.

## 소유권과 NFS/POSIX 저장소

Production container는 UID:GID `10001:10001`로 실행됩니다. Application은 전용 archive root를 `0700`으로 만들고 내부에 private archive object를 작성합니다. Installer는 UID:GID 10001로 일회성 파일을 생성·읽기·rename·삭제하고 `chmod`도 검사합니다. Probe 파일은 종료 전에 제거합니다.

새로 만들었거나 비어 있는 전용 archive directory가 쓰기 불가하면 Installer가 짧게 실행되는 container로 그 directory 하나만 `10001:10001` 소유로 바꿀 수 있습니다. 재귀적 소유권 변경을 하지 않으며 parent media directory나 인접 archive도 건드리지 않습니다. Non-empty directory에는 자동 chown을 시도하지 않습니다. 권한 확인에 실패하면 설치 성공을 보고하지 않고 제한된 수정 방법을 알려줍니다. NFS root-squash에서는 client container의 chown이 차단될 수 있으므로 NFS server에서 숫자 UID/GID 또는 적절한 ACL을 지정해야 합니다. 공유 parent 디렉터리 권한을 완화하지 마세요.

Installer는 설치 시 경로만 검증하고, 기존 NFS mount가 계속 연결된다고 가정합니다. NFS watchdog이나 자동 fallback을 추가하지 않습니다. Docker daemon이 해당 경로에 접근하지 못하면 probe가 실패하며 application을 시작하지 않습니다.

## 네트워크와 브라우저 초기 설정

Runtime Host는 container 내부 port `8080`을 사용합니다. Installer는 host의 `8080`이 비어 있으면 사용하고, 이미 사용 중이면 `127.0.0.1`에 사용 가능한 다른 port를 선택해 실제 URL을 출력합니다. `--port N`은 loopback의 특정 port를 요청하며 이미 사용 중이면 실패합니다. 기본 설치는 UI를 LAN이나 public internet에 노출하지 않습니다.

출력된 `/setup` URL을 열고 첫 Owner의 login과 password를 정하세요. setup code나 CLI claim 단계가 없습니다. 최초 계정은 브라우저에서 한 번만 생성되고 바로 로그인됩니다. 초기 설정 전 LAN 접근을 허용하면 먼저 접속한 사람이 Owner가 될 수 있으므로 본인이 먼저 계정을 만들고 방화벽을 제한하세요.

### 네트워크 바인딩과 HTTPS

기본 설치는 HTTP를 `127.0.0.1`에 bind합니다. LAN 접근을 명시하려면 `--bind 0.0.0.0` 또는 host interface IP를 지정하세요. LAN의 HTTP는 암호화되지 않으므로 방화벽을 제한하고 자격 증명을 다룰 때 HTTPS를 우선하세요. 최초 설정을 LAN에 노출하면 첫 방문자가 Owner를 선점할 수 있습니다.

Runtime Host가 Go TLS listener로 HTTPS를 직접 제공합니다. proxy/TLS container는 없습니다. Compose는 HTTP port(기본 `8080`)와 HTTPS port(기본 `8443`)를 선택한 host address에 publish하고, 기본 address는 loopback입니다. `--port`, `--https-port`로 port를, `--bind 0.0.0.0` 또는 특정 interface IP로 LAN 공개를 명시할 수 있습니다. 기존 설치에서 installer를 다시 실행하면 application release를 변경하지 않고 port/bind mapping만 갱신합니다. 방화벽을 설정하고, 최초 설정 전에 LAN을 열면 첫 방문자가 Owner를 만들 수 있음을 고려하세요.

첫 Owner 생성 후 **설정 → 실행 환경 → HTTPS**에서 실제 client가 사용할 DNS 이름/IP를 입력하고 먼저 자체 서명 listener를 켭니다. HTTPS URL로 다시 접속해 해당 이름의 인증서를 신뢰한 뒤 교체할 PEM chain과 일치하는 private key를 업로드합니다. 서버는 key 일치, 유효 기간, 지정한 모든 hostname/IP SAN을 확인합니다. private key 업로드는 HTTPS 연결에서만 허용하고 cleartext HTTP로는 보내지 않습니다. 개인키는 `/data/security/https.json`에 0600 권한으로 저장되고 API 응답에 포함되지 않습니다. HTTPS 요청의 session cookie는 Secure·HttpOnly이며 기존 SameSite/CSRF 정책이 유지됩니다.

HTTPS 화면에서 DNS 도메인 하나에 대해 공개 ACME 인증서를 요청할 수 있습니다. DNS가 이 호스트의 공개 주소를 가리키고 외부 TCP 443이 HTTPS 포트 매핑에 도달해야 하며 설치기에서 `--https-port 443`으로 호스트 443을 공개해야 합니다. Runtime Host가 ACME TLS-ALPN으로 직접 발급하고 개인키는 보호된 ACME 캐시에 저장합니다. 첫 HTTPS 접속에서 발급이 시작되며 실제 인증서를 얻을 때까지 상태는 대기 중이고 인증서가 준비될 때까지 HTTP 리다이렉트는 시작하지 않습니다. 발급 전에는 HTTP 복구 경로가 남으므로 외부 공개를 방화벽으로 제한하세요. 발급 실패는 DNS와 외부 443 접근을 확인하라는 안전한 상태로 표시됩니다. 조건을 확인한 뒤 도메인으로 다시 접속해 재시도하세요. 인증서는 자동 갱신됩니다. 내부 CA는 HTTPS에서 일치하는 PEM 쌍을 입력하며 각 클라이언트가 CA를 신뢰해야 합니다. 자체 서명 인증서는 2년 유효하고 만료 30일 전 Runtime Host 시작 시 자동 교체됩니다. UI에서 미리 교체할 수도 있습니다. 포트 충돌은 안전한 오류로 표시하며 관련 없는 서비스를 중지하지 않습니다.

기존 설치는 일반 재실행 시 release pin, `/data` named volume, archive path, bind 설정을 보존합니다. `--bind`, `--port`, `--https-port`는 network mapping만 수정합니다. TLS 설정과 개인키는 영속 `/data`에 남습니다. UI에서 HTTPS를 끄면 listener만 닫고 설정·인증서·녹화 데이터를 지우지 않습니다. network 변경은 application release upgrade를 의미하지 않습니다.

## 일반 운영

Installer는 생성된 Compose 파일을 참조하는 정확한 project/file command prefix를 출력합니다. 모든 동작에 그 prefix를 사용하세요.

```sh
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml logs -f archiver
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml stop
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml start
docker compose --project-name integrated-recorder --file /path/to/install/compose.yaml restart
```

`docker compose ... ps`로 service 상태를 확인하세요. Container health check는 `/healthz`를 확인합니다. Installer는 설치 성공으로 처리하기 전에 `/api/setup/status`와 browser setup route도 검사합니다. `deployment.json`과 Docker image label에 선택된 release identity가 기록됩니다. Runtime Host setup status/API도 Host build identity를 보고합니다. Owner 자격 증명은 Installer가 저장하지 않습니다.

실행 중인 버전은 Installer가 출력한 host port를 사용해 확인할 수 있습니다.

```sh
curl -fsS http://127.0.0.1:<선택한-port>/api/setup/status
```

응답에는 Runtime Host version과 release channel이 포함됩니다. 선택된 source commit과 build timestamp는 설치 디렉터리의 private `deployment.json`에도 기록됩니다.

일반 운영이나 장애 해결을 위해 `docker compose down -v`, `docker volume prune`, 전체 Docker cleanup을 실행하지 마세요. 영속 application data나 관계없는 service를 삭제할 수 있습니다.

## Version pinning과 upgrade

`--upgrade` 없이 Installer를 다시 실행하면 `deployment.json`에 기록된 version을 유지하며 최신 version으로 교체하지 않습니다. 중단된 신규 설치는 pending release를 재개합니다. 명시적으로 요청한 upgrade가 중단되면 `--upgrade`를 다시 지정해 재개하세요. 버전을 지정했던 경우 같은 `--version`도 지정해야 합니다. 일반 재실행은 현재 pin과 다른 pending version의 적용을 거부합니다. Recording directory도 고정되며 일반 재실행으로 변경할 수 없습니다.

새 공식 release로 upgrade하려면 최신 공식 repository checkout에서 유지되는 Installer를 명시적으로 실행합니다.

```sh
./scripts/install.sh --upgrade --recordings-dir /same/absolute/recordings/path
```

Upgrade version 고정은 `--version vX.Y.Z`를 추가합니다. Installer는 선택한 exact tag를 build하고 새 container가 health/setup 검사를 통과한 뒤에만 pinned metadata를 갱신합니다. 기존 `/data` volume과 archive bind mount를 재사용합니다. 새 container가 준비되지 않으면 기존 Compose 정의를 다시 올리려고 시도합니다. Host/container 교체는 유지보수 작업이며 Runtime Host의 restartless application-generation handover와 다릅니다. Live recording이 없을 때 upgrade하세요. Application data schema가 이전 버전과 호환된다는 보장은 없습니다. Upgrade 전에 검증된 backup을 준비하고 release note를 확인하세요.

기존 수동 Compose 배포나 감지된 `recording-data`/application-state volume은 자동 채택하지 않습니다. Installer는 기존 layout, mount 경로, 소유권을 추측하지 않고 설치를 거부합니다. 이를 보존한 채 별도 검증 migration을 계획하세요. 기존 archive layout을 확인하지 않고 신규 설치에 연결하지 마세요.

## Backup과 복구

Named `/data` volume과 host recording directory를 모두 백업하세요. Archive가 bind mount일 때 두 경로는 배포 데이터의 서로 다른 부분입니다. Archive만 있으면 인증/generation/provider/plugin metadata가 보존되지 않고, `/data`만으로는 media archive가 보존되지 않습니다. 일관된 파일 backup을 위해 service를 정상 정지하거나 동등한 consistency를 제공하는 filesystem/storage snapshot을 사용하세요. `/data`에는 credential과 secret config가 있으므로 backup 접근을 제한하세요. 기본 file store는 권한으로 보호하지만 저장값을 암호화하지 않습니다.

문제 해결을 위해 application-state volume을 지우거나 재생성하지 마세요. Startup 문제가 있으면 해당 배포의 logs와 `docker compose ... ps`를 먼저 확인하세요. Archive 권한 오류는 전용 archive directory의 소유권을 수정한 뒤 Installer를 다시 실행하세요. Metadata가 없거나 깨졌으면 새 project를 시작하기 전에 기존 volume을 조사하세요. Password recovery는 로컬 전용이며 Runtime Host의 `reset-password` 명령을 사용합니다.

## Release trust와 Plugin Registry

Installer는 선택한 공식 Git tag에서 Docker image를 build합니다. Application image를 pull하지 않고 private signing material을 복사하지 않습니다. 생성된 Compose 파일의 `IR_RELEASE_TRUSTED_KEYS_JSON`은 비어 있으므로 bundled application은 사용할 수 있지만, 진짜 Ed25519 public trust root를 추가하기 전에는 signed remote application-generation update가 비활성화됩니다. 공개 키는 프로젝트의 신뢰할 수 있는 release/deployment 절차를 통해 받으세요. GitHub Actions private key를 host에 복사하거나 trust root를 임의로 만들지 마세요.

공식 Plugin Registry catalog는 application update trust와 별도로 설정되어 기본 plugin discovery/update behavior를 유지합니다. Plugin artifact와 desired state는 `/data` 아래에 저장됩니다. Plugin lifecycle update와 Docker image rebuild/replacement는 별개의 동작입니다.
