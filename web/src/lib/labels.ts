import { humanize } from '@/lib/utils'
import { getFormatPreferences } from '@/lib/formatting'
import { translate, type TranslationKey } from '@/i18n/catalog'

export const commonLabels = {
  adapter: '어댑터',
  workflow: '워크플로',
  resource: '리소스',
  archive: '보관 데이터',
  integrity: '무결성',
  storage: '저장소',
  status: '상태',
  settings: '설정',
  search: '검색',
  export: '내보내기',
  thumbnail: '미리보기',
  runtime: '실행 상태',
  capabilities: '기능',
  mediaTypes: '미디어 형식',
  external: '외부',
  secret: '비밀 값',
  source: '원본',
  recordingId: '녹화 ID',
} as const

const recordingLabels: Record<string, string> = {
  recording: '녹화 중', stopped: '중지됨', completed: '완료', interrupted: '중단됨',
}
const integrityLabels: Record<string, string> = {
  unknown: '미확인', verifying: '검사 중', verified: '확인됨', degraded: '일부 손상', failed: '검사 실패',
}
const adapterLabels: Record<string, string> = {
  ready: '준비됨', disabled: '사용 중지', unavailable: '사용할 수 없음', failed: '실행 실패',
  rejected: '거부됨', restarting: '재시작 중', starting: '시작 중', stopped: '중지됨', unknown: '확인 필요',
}
const workflowLabels: Record<string, string> = {
  started: '시작됨', resource_discovered: '리소스 확인됨', configuration_required: '설정 필요',
  interaction_required: '추가 입력 필요', challenge_required: '입력 대기', resolving: '확인 중',
  continued: '계속 진행',
  resolved: '완료', completed: '완료', canceled: '취소됨', cancelled: '취소됨', expired: '만료됨', failed: '실패',
  queued: '대기 중', running: '진행 중', verifying: '검사 중',
}
const watchLabels: Record<string, string> = {
  disabled: '사용 중지', offline: '오프라인', checking: '확인 중', starting: '녹화 시작 중',
  recording: '녹화 중', backoff: '재시도 대기', attention_required: '주의 필요', suppressed: '현재 방송 제외',
}
const genericLabels: Record<string, string> = {
  queued: '대기 중', running: '진행 중', completed: '완료', failed: '실패', canceled: '취소됨', cancelled: '취소됨',
  recording: recordingLabels.recording, stopped: recordingLabels.stopped, interrupted: recordingLabels.interrupted,
  verified: integrityLabels.verified, degraded: integrityLabels.degraded, unknown: integrityLabels.unknown,
  verifying: integrityLabels.verifying, ready: adapterLabels.ready, disabled: adapterLabels.disabled,
  unavailable: adapterLabels.unavailable, rejected: adapterLabels.rejected, restarting: adapterLabels.restarting,
  started: workflowLabels.started, resource_discovered: workflowLabels.resource_discovered,
  configuration_required: workflowLabels.configuration_required, interaction_required: workflowLabels.interaction_required,
  challenge_required: workflowLabels.challenge_required, resolving: workflowLabels.resolving,
  resolved: workflowLabels.resolved, expired: workflowLabels.expired,
  offline: watchLabels.offline, checking: watchLabels.checking, starting: watchLabels.starting,
  backoff: watchLabels.backoff, attention_required: watchLabels.attention_required, suppressed: watchLabels.suppressed,
}

const recordingEventLabels: Record<string, string> = {
  recording_started: '녹화 시작', manifest_observed: '매니페스트 확인', source_refreshed: '미디어 원본 갱신',
  segment_retry: '세그먼트 재시도', gap_detected: '누락 감지', gap_committed: '누락 기록',
  recording_completed: '녹화 완료', recording_stopped: '녹화 중지', recording_interrupted: '녹화 중단',
  integrity_started: '무결성 검사 시작', integrity_completed: '무결성 검사 완료',
  export_started: '내보내기 시작', export_completed: '내보내기 완료',
}
const auditEventLabels: Record<string, string> = {
  adapter_restarted: '어댑터 재시작', adapter_enabled: '어댑터 활성화', adapter_disabled: '어댑터 비활성화',
  config_changed: '설정 변경', recording_tags_updated: '녹화 태그 변경', recording_deleted: '녹화 삭제',
  integrity_verification_requested: '무결성 검사 요청', export_requested: '내보내기 요청',
}

function labelFrom(map: Record<string, string>, value?: string, locale = getFormatPreferences().locale): string {
  if (!value) return map.unknown ?? '미확인'
  const key = stateTranslationKeys[value.toLowerCase()]
  return key ? translate(locale, key) : map[value.toLowerCase()] ?? humanize(value)
}

const stateTranslationKeys: Record<string, TranslationKey> = {
  recording: 'state.recording', stopped: 'state.stopped', completed: 'state.completed', interrupted: 'state.interrupted', unknown: 'state.unknown',
  queued: 'state.queued', running: 'state.running', failed: 'state.failed', canceled: 'state.cancelled', cancelled: 'state.cancelled',
  verified: 'state.verified', verifying: 'state.verifying', degraded: 'state.degraded', ready: 'state.ready', disabled: 'state.disabled', unavailable: 'state.unavailable',
  rejected: 'state.rejected', restarting: 'state.restarting', starting: 'state.starting', offline: 'state.offline', checking: 'state.checking', backoff: 'state.backoff',
  attention_required: 'state.attention_required', suppressed: 'state.suppressed', started: 'state.started', resource_discovered: 'state.resource_discovered',
  configuration_required: 'state.configuration_required', interaction_required: 'state.interaction_required', challenge_required: 'state.challenge_required',
  resolving: 'state.resolving', resolved: 'state.resolved', expired: 'state.expired',
}

export const recordingStateLabel = (value?: string) => labelFrom(recordingLabels, value)
export const integrityStatusLabel = (value?: string) => labelFrom(integrityLabels, value)
export const adapterStateLabel = (value?: string) => labelFrom(adapterLabels, value)
export const workflowStateLabel = (value?: string) => labelFrom(workflowLabels, value)
export const watchStateLabel = (value?: string) => value?.toLowerCase() === 'starting'
  ? translate(getFormatPreferences().locale, 'watch.starting')
  : labelFrom(watchLabels, value)
export const statusLabel = (value?: string, locale?: Parameters<typeof translate>[0]) => labelFrom(genericLabels, value, locale)
const eventTranslationKeys: Record<string, TranslationKey> = {
  recording_started: 'event.recording_started', manifest_observed: 'event.manifest_observed', source_refreshed: 'event.source_refreshed', segment_retry: 'event.segment_retry',
  gap_detected: 'event.gap_detected', gap_committed: 'event.gap_committed', recording_completed: 'event.recording_completed', recording_stopped: 'event.recording_stopped',
  recording_interrupted: 'event.recording_interrupted', integrity_started: 'event.integrity_started', integrity_completed: 'event.integrity_completed', export_started: 'event.export_started', export_completed: 'event.export_completed',
}
export const recordingEventLabel = (value?: string, locale = getFormatPreferences().locale) => {
  const key = value ? eventTranslationKeys[value.toLowerCase()] : undefined
  return key ? translate(locale, key) : labelFrom(recordingEventLabels, value, locale)
}
export const auditEventLabel = (value?: string) => labelFrom(auditEventLabels, value)
