# Archive Snapshot v2

상태: 구현·단위 검증. v1 snapshot은 읽을 수 있고, 신규 Build는 v2를 생성한다. 이 문서만으로 M4/M5 완료를 뜻하지 않는다.

v2는 v1의 canonical content 및 provenance 구조를 유지하면서 재생 복원에 필요한 init-map 연결을 명시한다. 각 객체의 `init_identity`는 `SHA256("IR/archive-snapshot/v2/init-identity\0" || source_init_identity)`의 `initref_` 접두 hex 값이다. 따라서 source URI, local path, 원본 init ID는 전송되지 않지만, media object에서 같은 track의 canonical init content를 결정적으로 찾을 수 있다. media가 init identity를 선언하면 같은 track의 init object가 있어야 한다. RC12는 같은 HLS map ID를 여러 source coordinate에 반복 기록할 수 있으므로 동일 identity의 init object가 복수 개일 수 있다. 이 경우 모든 객체의 plaintext size와 SHA-256이 같아야 하며, 복원기는 canonical snapshot 순서에서 처음 나오는 객체를 playlist target으로 사용한다. identity가 같지만 content가 다르거나 연결 객체가 없으면 snapshot 검증을 거부한다.

v2 content hash는 `SHA256("IR/archive-snapshot/v2/content\0" || JCS(content))`이며, provenance hash domain은 v1을 유지한다. v1 snapshot은 v1 domain으로 계속 검증한다. 새 v2 chain entry는 직전 Snapshot ID를 그대로 연결한다.

복호화된 객체는 원본 archive를 변경하지 않고 별도 private restore directory에 원본 바이트로 쓴다. v2 restore는 완전하고 충돌 없는 canonical coverage만 `index.m3u8`와 track VOD playlist로 노출한다. 알려진 missing/acquisition-failed/conflict 범위가 있거나 객체가 부족하면 playable VOD를 만들지 않고 실패한다. Byte-range 원본 객체는 복원 경로에서 독립 canonical payload로 저장되므로 local playlist는 해당 객체 파일 전체를 참조하며 payload bytes는 재작성하지 않는다.

v1은 복호화 가능한 데이터 형식으로 계속 지원하지만 init-object 연결이 명시되지 않았으므로 자동 재생 playlist는 만들지 않는다. v1은 plaintext restoration compatibility이지 v2 playback acceptance가 아니다.

공유 Go/JavaScript vector: `protocol/archive-snapshot-v2/test-vector.json`.

로컬 `archivecatalog.Store.WalkHistory`는 현재 durable head에서 sequence 1까지
snapshot hash와 predecessor 연결을 확인하며 최신 항목부터 callback으로 전달한다.
순회 중 catalog append를 잠시 막아 확인한 head를 고정한다. 이전 항목 누락이나
손상, callback 오류가 있으면 head 정보를 성공으로 반환하지 않는다. callback은
오류 전에 일부 snapshot을 이미 처리했을 수 있으므로, 이를 사용하는 재처리는
멱등이어야 하며 순회 성공 전에는 권한·복제 완료 상태를 확정해서는 안 된다.
이 API는 로컬 이력 검증 기반이며 원격 이력 교환이나 재키잉 완료 절차를 구현하지 않는다.
