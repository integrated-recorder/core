import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { PluginTrustBadges } from './plugin-trust'

describe('plugin trust badges', () => {
  it('shows bundled provenance and publisher without calling it registry approved', () => {
    render(<PluginTrustBadges trust={{ provenance: 'bundled', authority: 'core_release', publisher: 'first_party', reviewed: true }} />)

    expect(screen.getByText('내장')).toBeInTheDocument()
    expect(screen.getByText('자체 제공')).toBeInTheDocument()
    expect(screen.queryByText('Registry 승인됨')).not.toBeInTheDocument()
  })

  it('distinguishes official registry approval from publisher affiliation', () => {
    render(<PluginTrustBadges trust={{ provenance: 'registry', authority: 'official', publisher: 'third_party', reviewed: true }} />)

    expect(screen.getByText('공식 Registry')).toBeInTheDocument()
    expect(screen.getByText('제3자')).toBeInTheDocument()
    expect(screen.getByText('Registry 승인됨')).toBeInTheDocument()
    expect(screen.queryByText('Registry 검토됨')).not.toBeInTheDocument()
    expect(screen.queryByText(/안전|safe|secure/i)).not.toBeInTheDocument()
  })

  it('warns for custom registry and local operator executables', () => {
    const { rerender } = render(<PluginTrustBadges trust={{ provenance: 'registry', authority: 'custom', publisher: 'unknown', reviewed: false }} warning />)
    expect(screen.getByText('사용자 지정 Registry')).toBeInTheDocument()
    expect(screen.getByText('게시자 알 수 없음')).toBeInTheDocument()
    expect(screen.getByText(/검토되지 않았으며 샌드박스 없이 실행됩니다/)).toBeInTheDocument()
    expect(screen.queryByText('Registry 승인됨')).not.toBeInTheDocument()

    rerender(<PluginTrustBadges trust={{ provenance: 'operator', authority: 'local', publisher: 'unknown', reviewed: false }} warning />)
    expect(screen.getByText('로컬 운영자')).toBeInTheDocument()
    expect(screen.getByText(/검토되지 않았으며 샌드박스 없이 실행됩니다/)).toBeInTheDocument()
  })

  it('does not infer trust for legacy or inconsistent API responses', () => {
    const { rerender } = render(<PluginTrustBadges />)
    expect(screen.getByText('레거시 플러그인 · 출처 정보 없음')).toBeInTheDocument()

    rerender(<PluginTrustBadges trust={{ provenance: 'bundled', authority: 'official', publisher: 'first_party', reviewed: true }} />)
    expect(screen.getByText('레거시 플러그인 · 출처 정보 없음')).toBeInTheDocument()
    expect(screen.queryByText('First-party')).not.toBeInTheDocument()
  })
})
