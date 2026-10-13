# 제품 IA와 설정 소유권

이 문서는 Integrated Recorder의 사용자-facing 영역과 persistent configuration의 canonical editor를 정의한다.

## 기본 원칙

- 하나의 persistent 값은 하나의 canonical editor에서 변경한다.
- 다른 화면은 읽기 전용 요약, 상태 표시, canonical editor 링크 또는 이번 작업의 명시적인 contextual input만 제공한다.
- contextual persistence는 저장 범위와 권한을 UI에 분명하게 표시하고 backend에서도 같은 권한을 검사한다.
- 같은 객체에 대한 빠른 action과 상태 요약은 여러 화면에 둘 수 있다. 중복 configuration authority와 혼동하지 않는다.
- route와 navigation은 제품 domain을 표현한다. 실제 보안 경계는 각 backend permission이 담당한다.

## 도메인과 canonical editor

| 도메인 | persistent scope / source of truth | canonical editor | 허용되는 contextual surface |
| --- | --- | --- | --- |
| 사용자 preference | 사용자별 locale, theme, timezone; user preference store | `/account/preferences` | Header theme chooser는 같은 `UserPreferences.theme`를 갱신하는 빠른 편집기 |
| 작업 입력 | 이번 녹화의 URL, resource, title, preview policy; recording create request | `/new` | Resolve workflow challenge의 일회성 입력 |
| 자동 녹화 정책 | Watch의 resource, 간격, input, title, preview 정책; Watch store | `/watches` 및 Watch 상세 | Dashboard와 Watch 상세의 실행 action은 같은 Watch 객체를 조작 |
| 녹화 객체 | tags, lifecycle, preview state; recording archive/management store | Recording 상세 | 목록과 상세의 stop/delete 등 동일 객체 action, 읽기 전용 요약 |
| Source 설정 | plugin/resource 범위의 values와 secrets; adapter config store | `/sources/{id}`의 Configuration | Workflow에서 `다음에도 저장`을 명시적으로 선택한 경우만 contextual persistence |
| Storage instance | 표시 이름, provider 설정, secret, probe, 활성화 대상; Storage catalog의 instance와 immutable set | `/storage/locations` | Storage overview는 현재 instance/상태/용량을 읽기 전용 표시 |
| Storage tuning | buffer, queue, retry, telemetry 및 metrics retention; system settings store | `/storage/advanced` | 별도 writer 없음 |
| Retention | 자동 정리 여부와 완료 후 보존 기간; system settings store | `/storage/retention` | 후보 목록은 읽기 전용. 실행 action은 `recording.delete` 권한을 별도로 사용 |
| Runtime 운영 | integrity concurrency와 Runtime Host 정보 | `/system` | 별도 writer 없음 |
| Runtime update | 검증·stage·activate·rollback 상태; Runtime update controller | `/system/updates` | Dashboard에는 상태 요약만 둔다 |
| 진단과 감사 | Runtime log와 audit store | `/system/diagnostics`, `/system/audit` | 다른 화면에는 짧은 read-only 상태 요약만 허용 |
| Plugin 배포 | registry, provenance, install/update/uninstall lifecycle | `/plugins` | Storage는 필요한 provider가 없으면 Plugins로 연결 |
| Source 운영 | 설치된 source adapter 상태, resource browse, enable/disable/restart | `/sources`, `/sources/{id}` | New Recording은 Source를 선택하고 사용할 뿐 persistent config를 편집하지 않는다 |

## 권한 경계

- Member는 recording read/control, own account/preferences/session, Source metadata/resource browse를 사용할 수 있다.
- Source 설정, adapter action, plugin distribution, Storage, System, Watch 관리 등 운영 mutation은 backend의 granular permission으로 보호한다. owner-only navigation은 보조 UX이며 보안 경계가 아니다.
- Workflow continuation은 `recording.control`로 허용한다. 일회성 answer만 제출하면 Source 설정을 변경하지 않는다.
- Workflow request가 `persist` 또는 `persist_fields`를 포함하면 `plugin.manage`를 별도로 요구한다. Member 화면에서는 optional persistence 선택을 숨기고, required persistence가 있으면 관리자 설정이 필요하다고 안내한다.
- Retention policy 변경은 `settings.manage`, 실제 정리 실행은 `recording.delete`로 분리한다.

## Source와 New Recording

Source configuration은 `/sources/{id}`가 소유한다. Resource scope 상속 모델은 유지한다. `/new`에는 현재 작업에 필요한 URL/input, resource, title, preview policy만 둔다. 설정이 필요한 Source는 canonical Configuration 화면으로 연결한다. Member에게는 Source 설정 권한이 없다는 점을 분명히 한다.

## Storage Provider와 Storage Instance

Provider는 설치된 driver/plugin type이다. Provider의 사용자-facing 정보는 이름, 버전, 설치 상태, trust, capability/schema metadata로 제한한다. 사용자가 구성하고 연결을 확인하고 활성화하는 대상은 Storage Instance다. 따라서 UI의 구성·probe·activate는 `/storage/locations`의 instance surface만 사용한다.

기존 provider-ID 기반 API와 `DesiredSets`는 upgrade 및 기존 client 호환성을 위해 유지한다. Catalog open 시 기존 DesiredSet에서 결정적인 legacy StorageInstance를 멱등하게 생성하며, legacy provider selection과 instance가 같은 immutable set을 가리키도록 보존한다. 기존 generation은 계속 pin한 exact Storage Set을 사용한다. 새 UI는 provider-level mutation API를 호출하지 않는다.

Local Storage의 archive root는 Host bootstrap 값이 아니라 각 Local Storage Instance의 구성이다. 여러 local location은 독립된 instance와 immutable set으로 유지한다.

## Theme

사용자별 `UserPreferences.theme`가 유일한 제품 theme source of truth다. `/api/settings`의 기존 system UI theme 값은 호환 read surface로 남는다. 새 변경 요청은 `/api/user/preferences`를 사용하며 system UI theme를 별도로 편집할 수 없다. Header chooser는 `system`, `light`, `dark` 의미를 그대로 보존한다.

## Route와 호환성

- 내 계정: `/account/preferences`, `/account/security`, `/account/sessions`
- 운영 시스템: `/system`, `/system/users`, `/system/updates`, `/system/diagnostics`, `/system/audit`
- 저장소: `/storage`, `/storage/locations`, `/storage/retention`, `/storage/advanced`, `/storage/{poolId}`
- 배포 plugin: `/plugins`; source 운영: `/sources`, `/sources/{id}`
- `/settings`는 `/account/preferences`로 redirect한다.
- `/adapters`와 `/adapters/{id}`는 각각 `/sources`와 `/sources/{id}`로 redirect한다.
- `/workflows`와 workflow detail은 history/deep link를 위해 유지하되 static navigation에는 항상 노출하지 않는다. 진행 중인 task가 있을 때만 sidebar에 진입점을 표시한다.

## Mutation surface audit 요약

| Mutation surface | Owner / persistent scope | 최종 UI 위치 |
| --- | --- | --- |
| `userPreferencesAPI.update` | 현재 사용자의 preference | `/account/preferences`; Header theme chooser는 같은 값에 대한 명시적 quick action |
| `productAPI.saveSettings` | integrity concurrency | `/system` |
| `productAPI.saveSettings` | retention policy | `/storage/retention` |
| `productAPI.saveSettings` | buffer/queue/retry/telemetry | `/storage/advanced` |
| `adaptersAPI.saveConfig` | global/resource Source config | `/sources/{id}` Configuration만 |
| `adaptersAPI.action` | 설치 Source 운영상태 | `/sources/{id}` owner controls |
| `workflowsAPI.continue` | 일회성 resolution input; 선택적 Source config persistence | Workflow interaction; persistence는 명시적이고 `plugin.manage` 필요 |
| `watchesAPI.*` | Watch object/policy/lifecycle | `/watches`와 해당 Watch 상세 |
| storage provider instance APIs | Storage Instance config/probe/activate | `/storage/locations`만 |
| legacy provider config/probe/activate APIs | 기존 provider ID client compatibility | 새 UI callsite 없음; backend compatibility 유지 |
| plugin install/update/uninstall/refresh | Plugin distribution state | `/plugins`만 |
| account password/session/user APIs | own account/session 또는 owner user administration | `/account/security`, `/account/sessions`, `/system/users` |
| Runtime update APIs | installed Runtime generation lifecycle | `/system/updates` |
| recording lifecycle/delete/tags/preview | recording object state | Recording detail; 목록 action은 동일 객체에 대한 shortcut |
| integrity/export jobs | recording-scoped operations | Recording detail |
| setup API mutations | first-run installation | setup flow만 |

`productAPI.saveSettings`는 하나의 endpoint지만 각 persistent field group의 editor는 분리되어 있다. `Storage Provider`와 `Storage Instance`의 기존 backend compatibility surface는 data migration을 대체하지 않으며, 같은 instance config를 두 UI에서 편집하지 않는다.
