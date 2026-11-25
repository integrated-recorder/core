# Replica Lease / Tombstone v1 (M7 진행 중)

이 문서는 `internal/lease`의 현재 durable state machine 계약이다. 계약을 실제 peer API, encrypted archive catalog, transfer transport에 연결하고 서로 다른 노드에서 acceptance하기 전에는 M7 완료로 보지 않는다.

## 보안 경계

- Lease는 지정한 provider가 특정 ciphertext object 집합의 제한된 기간 보관을 수락했다는 증거다. 보관의 미래 지속성이나 peer의 정직성을 증명하지 않는다.
- lease offer와 tombstone에는 최신 OwnerSubject capability 확인이 필요하다. `DurableAuthority`는 서명만 유효한 임의 grant가 아니라, 현재 unfrozen authority log에 저장된 정확한 chain만 허용한다.
- key resolver는 Discovery 주소를 신뢰하지 않고 검증된 신원 이력에서 서명 키를 찾아야 한다. Discovery와 TURN은 transport 후보만 제공한다.
- Provider는 암호문만 받고 plaintext DEK는 받지 않는다. Replica readback은 ciphertext의 선언된 길이와 SHA-256을 다시 검증한다.
- 원격 provider의 `deleted` 보고는 서명된 관측값이다. 원격 파일 삭제의 증명으로 취급하지 않는다. 응답이 없거나 기한을 넘기면 `unconfirmed`로 분류한다.
- Tombstone fork가 같은 archive/deletion epoch에서 검증되면 어느 쪽도 선택하지 않고 해당 archive의 복제 작업을 동결한다.

OwnerSubject별 보존 목표는 [Authority v1](PROTOCOL_AUTHORITY_V1.ko.md)의 owner-signed `replication_policy_set` event로 저장한다. Control Plane은 maintenance pass마다 sealed snapshot의 현재 상태를 평가하고, source와 후보 Provider 양쪽의 현재 `archive.replicate` capability 및 연결 상태를 검증한 뒤 replica 수, OwnerSubject 용량, 유예 시간과 재배치 설정에 따라 최대 2개의 signed Offer를 만든다. scheduler는 이미 준비된 local DEK와 Owner recovery envelope만 재사용하며 첫 키/envelope를 만들지 않는다. Provider의 명시적 저장 동의와 encrypted lease 수락 경계는 별도로 유지한다.

## Canonicalization과 서명

모든 signed payload는 `identityprotocol.Canonicalize`로 정규화하며 wire 정수는 10진 문자열로 표현한다. 시간은 소수점이 없는 UTC RFC3339 (`YYYY-MM-DDTHH:MM:SSZ`)다. 서명은 Ed25519와 operation별 domain separator를 사용한다.

| Evidence | Domain |
|---|---|
| Offer | `IR/replica-lease/v1/offer\0` |
| Provider acceptance | `IR/replica-lease/v1/accept\0` |
| Durable object checkpoint | `IR/replica-lease/v1/checkpoint\0` |
| Renewal request/provider receipt | `IR/replica-lease/v1/renew\0` |
| Owner tombstone | `IR/archive-tombstone/v1/sign\0` |
| Provider deletion report | `IR/replica-lease/v1/deletion-report\0` |

주요 ID와 hash는 archive `arc_…`, snapshot `snap_<64 lowercase hex>`, lease `rl_…`, object `ob_…`, OwnerSubject `os_…`, node `nd_…`, SHA-256 `sha256:<64 lowercase hex>` 형식이다. Object ID는 중복 없이 정렬하고, 예약 크기는 모든 ciphertext size의 합과 정확히 일치해야 한다. Lease 최대 기간은 90일, 단일 ciphertext object 검증 상한은 2 GiB다.

Offer에는 `key_epoch`, `plaintext_manifest_hash`, `replica_manifest_hash`가 모두 포함된다. `key_epoch`는 공개 ReplicaManifest 및 복구 키 envelope의 동일 epoch와 반드시 일치한다. 첫 hash는 immutable snapshot의 `content_hash`와 같아야 한다. 둘째 값은 `IR/replica-manifest/v1\0 || JCS(ReplicaManifestHashView)`의 SHA-256이며, 공개 coordinate-to-ciphertext mapping을 Node 서명에 결합한다. HashView는 manifest의 공개 필드와 객체 순서를 보존하되 coordinate 정수는 10진 문자열로 표현한다. Provider는 mapping을 독립 snapshot 및 Offer와 대조하고, offer의 ciphertext object set 외 데이터를 수락하지 않는다.

## 상태 전이

```text
offered → accepted → transferring → verifying → active → renewing → active
   │          │             │            │         ├→ expired
   ├→ expired └─────────────┴────────────┴→ failed
   └→ failed

Owner tombstone: 진행 중 lease → terminating → deleted (provider report)
                                             └→ unconfirmed (report 또는 24h timeout)
```

- 각 전이는 lock 안에서 revision을 증가시키며, 새 전체 상태는 mode `0600` 임시 파일을 fsync한 뒤 원자 rename하고 디렉터리를 fsync해 커밋한다. 상태 디렉터리는 mode `0700`이어야 한다. 손상되거나 알 수 없는 schema는 fail closed한다.
- 각 객체는 Provider가 서명한 단조 증가 durable offset checkpoint를 남긴다. restart 후 마지막 확인 offset부터 이어갈 수 있다. Offset rollback은 거부하며, 여러 fsync가 같은 초에 끝나도 offset 증가가 유효하면 같은 초의 checkpoint를 허용한다.
- 모든 ciphertext를 readback해 size/hash가 일치해야 `verifying → active`로 전이한다. Tombstone이 readback 중 도착하면 revision 재검사로 활성화를 막는다.
- Lease 만료는 그 복제 lease의 보관 책임 종료이며 local archive 삭제나 remote object 삭제를 수행하지 않는다.
- Tombstone은 `archive_id`, 단조 증가 `deletion_epoch`, `previous_head`, 고정 reason code, 발급 시각, capability evidence hash를 서명한다. 새로운 offer를 막고 진행 중 책임을 종료 요청 상태로 바꾼다. 로컬 원본 archive는 이 state machine이 삭제하지 않는다.

Authenticated Peer API의 `replica.tombstone` 명령은 비어 있는 `snapshot_id`와 archive-wide `archive.replicate` capability로 서명된 tombstone을 수신 Node의 durable Lease Store로 전달한다. 수신 Store가 archive-delete capability, signer history, signature, epoch와 fork를 다시 검증하고 같은 이벤트 재전달은 idempotent하다. 저장된 tombstone이 확인되면 provider는 replication lock 아래 해당 archive의 key material·bundle/snapshot metadata·transfer partial을 제거하고, 다른 bundle이 참조하지 않는 ciphertext만 정리한다. 동일 tombstone 재시도로 중단된 정리를 재개한다. Provider는 tombstone hash를 포함한 Node 서명 report를 반환하며 source Lease Store가 signature와 현재 tombstone hash를 원자 검증해 상태를 갱신한다. 이 보고는 provider 관찰이며 secure overwrite 증명이 아니다. source의 canonical archive는 변경하지 않는다. Control maintenance loop는 durable lease가 가리키는 provider에 저장된 tombstone을 재전달하며, cross-identity provider는 verified registry의 후보 projection과 매 pass의 최신 연합 identity resolution을 통해 유일하게 확인된 경우에만 연결한다. stale/revoked/ambiguous identity나 malformed reference index는 fail-closed pending으로 남긴다. Offer scheduling은 bounded maintenance path로 구현됐지만, 독립 네트워크 acceptance는 아직 검증되지 않았다.

## 구현 및 검증 경계

현재 package에는 durable lease store, provider acceptance/checkpoint 서명 확인, restart 가능한 progress, ciphertext readback, expiry, tombstone chain/fork freeze, deletion report와 concurrent duplicate offer 검증이 있다. `DurableAuthority`는 현재 signed grant chain의 OwnerSubject log membership과 scope를 확인한다. `internal/replication`은 Node Key가 서명하는 offset receipt를 받은 뒤 fsync한 ciphertext chunk의 provider-side 재개 저장을 지원하고, 정확한 mapping hash 및 현재 transferring lease를 확인하며 tombstone/expiry를 체크포인트 전후에 재검사한다. 재시작 후에는 uncheckpointed tail을 checkpoint offset으로 잘라내고, 완료 후 전체 객체를 readback한 다음 private coordinate binding과 검증된 immutable snapshot descriptor를 원자 저장한다. Peer API는 source durable store의 Offer를 지정 provider에게 bounded page로 전달하고, provider durable store에서 재검증·수락한 Node-Key 서명 acceptance를 source store에 전달한다. Owner-authorized renewal request와 provider-signed renewal receipt도 authenticated peer command로 전달하며, durable pending 상태 및 정확한 completed-receipt replay를 보존한다. 로컬 Pion 통합 테스트는 offer/acceptance/renewal 전파를 durable provider lease, peer manifest/range transfer, provider signed checkpoint 및 full readback/active lease와 연결한다. readback은 provider가 `replica.verify` 명령을 보내면 source가 기존 offer의 source-audience capability로 provider 저장 ciphertext를 bounded range 단위로 읽어 hash를 확인하는 peer protocol 경로다. 이 명령은 고정된 `archive.replicate` archive/snapshot/lease scope, 인증된 peer session 및 현재 authority 검사에 묶이며, 수신측 lease가 이미 active인 경우 동일 offer hash의 완료 receipt만 idempotently 확인한다. Pion 시험은 이 경로를 호출하지만 authority/federation 최신성 resolver는 fixture다. HTTPS 전용 `POST /api/replica-leases/{leaseID}/verify` 경로도 남아 있으며 source-side 수동 확인에 사용한다. 명시적 HTTPS `POST /api/replica-imports`는 provider-side acceptance, restartable import, source `replica.verify` readback을 순서대로 수행한다. Control maintenance loop는 로컬에서 이미 수락된 `accepted`, `transferring`, `verifying` lease와 source readback 확인이 아직 기록되지 않은 `active` lease를 시작 시와 이후 주기적으로 최대 2건씩 재개한다. 이 경로는 OwnerSubject controller의 현재 연합 신원 이력에서 source Node 위임을 확인하고, source identity를 peer session scope에 보존한다. Lease 서명 검증은 로컬 history와 검증된 공개 identity registry history를 사용하며, 현재 권한 검사는 별도로 매 pass에서 다시 확인한다. `active` lease의 source 확인 성공은 프로세스 내에서만 기억하므로 재시작 뒤 한 번 더 안전하게 확인하며, 확인된 lease를 매 pass마다 다시 읽지는 않는다. Tombstone 전달도 현재 durable lease의 provider를 찾고, 해당 Node가 local 또는 fresh cross-identity history에서 유일하게 현재 위임된 경우에만 peer scope를 구성한다. 만료와 tombstone을 다시 확인하고 수동 HTTP import와 lease별 process lock을 공유한다. offer를 자동 수락하지 않으며 durable checkpoint로 전송을 이어간다. UI는 source가 복사해 전달한 Offer 식별자로 최초 수락·가져오기를 명시적으로 시작한다. OwnerSubject controller의 signed 복제 보존 목표는 현재 bounded scheduler가 이미 준비된 sealed snapshot에 대한 Offer 생성과 provider 선택에 적용한다. source와 대상 provider 각각의 현재 capability 및 fresh identity history를 확인하고, 회차당 최대 2개만 생성하며 Provider acceptance는 자동화하지 않는다. 최초 key/recovery envelope 준비, 자동 acceptance·transfer orchestration과 전체 product coordinator, 서로 독립된 노드/네트워크 운영 검증은 남아 있다.

최근 실패 경계 회귀시험은 실제 durable Lease Store·Node Key·replica Store를 다시 열어, 부분 ciphertext chunk 뒤 source stream 오류가 발생해도 미확정 tail을 자르고 checkpoint 0부터 안전하게 재시도할 수 있음을 확인한다. 신뢰되지 않은 Node Key checkpoint는 lease progress를 바꾸지 않고, 잘못된 tombstone receipt hash도 peer client가 거부한다. 이미 검증된 digest-bound object의 후속 lease 재사용은 source를 다시 읽지 않는 것을 확인하며, 새 lease가 현재 권한 검사를 통과하고 전체 암호문이 선언된 digest와 일치하면 mode 0600 일반 파일로 확인된 손상 객체만 검증된 staging 이후 원자 교체한다. symlink·special file·권한 불일치·inode 변경은 계속 fail-closed된다. 백그라운드 repair scheduling은 아직 없다. 로컬 Provider capacity 한도는 별도 system setting으로 durable하게 저장되며, 현재 Node ID가 Provider인 수신 lease만 명시적 acceptance와 같은 mutation에서 예약한다. 같은 store의 outbound lease는 다른 Node에 저장되므로 로컬 용량을 소모하지 않는다. 검증된 deletion report 뒤에만 예약을 해제한다. 이는 실제 filesystem quota/full-disk 측정이나 물리 저장소 보장을 뜻하지 않는다. 이 기능은 deterministic unit/integration 시험이며 staging write의 ENOSPC fault-injection은 포함하지만 실제 filesystem quota/full-disk, NFS/WAN partition 또는 독립 two-node chaos 시험은 아니다.

아직 완료되지 않은 작업:

- 실제 RC12 archive catalog/snapshot, cryptoarchive output 및 peer API를 background coordinator로 자동화하고 Offer 생성·provider 선택·수락·transfer를 scheduling하는 기능.
- 서로 독립적인 실제 Node와 Discovery 환경에서 offer/accept/checkpoint/renewal/tombstone 전달 시 최신성·폐기·fork 반영을 end-to-end 검증하는 acceptance.
- NFS/WAN partition, malicious provider receipt, node restart, quota/storage-full을 포함한 독립 Linux two-node chaos 시험.
- 로컬 content-addressed digest dedup과 설정 가능한 Provider lease-capacity 예약/반납 회귀시험은 추가됐다. background repair scheduling을 remote transport/storage provider에 연결하고 실제 filesystem quota, storage-full, NFS 및 독립 two-node 환경을 검증하는 작업은 남아 있다.
- 권한 폐기와 fork 최신성 정책이 기존 lease 및 원격 key envelope에 미치는 효과를 end-to-end로 검증.

따라서 이 계약은 M7 구현의 기반이며, full distributed replication을 제공하거나 v0.2.0 릴리스 게이트를 통과했다는 뜻은 아니다.
