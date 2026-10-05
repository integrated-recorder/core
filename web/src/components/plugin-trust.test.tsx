import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { PluginTrustBadges } from './plugin-trust'

describe('plugin trust badges', () => {
  it('shows bundled provenance and publisher without calling it registry approved', () => {
    render(<PluginTrustBadges trust={{ provenance: 'bundled', authority: 'core_release', publisher: 'first_party', reviewed: true }} />)

    expect(screen.getByText('Bundled')).toBeInTheDocument()
    expect(screen.getByText('First-party')).toBeInTheDocument()
    expect(screen.queryByText('Registry approved')).not.toBeInTheDocument()
  })

  it('distinguishes official registry approval from publisher affiliation', () => {
    render(<PluginTrustBadges trust={{ provenance: 'registry', authority: 'official', publisher: 'third_party', reviewed: true }} />)

    expect(screen.getByText('Official Registry')).toBeInTheDocument()
    expect(screen.getByText('Third-party')).toBeInTheDocument()
    expect(screen.getByText('Registry approved')).toBeInTheDocument()
    expect(screen.queryByText('Registry reviewed')).not.toBeInTheDocument()
    expect(screen.queryByText(/안전|safe|secure/i)).not.toBeInTheDocument()
  })

  it('warns for custom registry and local operator executables', () => {
    const { rerender } = render(<PluginTrustBadges trust={{ provenance: 'registry', authority: 'custom', publisher: 'unknown', reviewed: false }} warning />)
    expect(screen.getByText('Custom Registry')).toBeInTheDocument()
    expect(screen.getByText('Unknown publisher')).toBeInTheDocument()
    expect(screen.getByText(/검토되지 않았으며 샌드박스 없이 실행됩니다/)).toBeInTheDocument()
    expect(screen.queryByText('Registry approved')).not.toBeInTheDocument()

    rerender(<PluginTrustBadges trust={{ provenance: 'operator', authority: 'local', publisher: 'unknown', reviewed: false }} warning />)
    expect(screen.getByText('Local operator')).toBeInTheDocument()
    expect(screen.getByText(/검토되지 않았으며 샌드박스 없이 실행됩니다/)).toBeInTheDocument()
  })

  it('does not infer trust for legacy or inconsistent API responses', () => {
    const { rerender } = render(<PluginTrustBadges />)
    expect(screen.getByText('Legacy plugin · provenance unavailable')).toBeInTheDocument()

    rerender(<PluginTrustBadges trust={{ provenance: 'bundled', authority: 'official', publisher: 'first_party', reviewed: true }} />)
    expect(screen.getByText('Legacy plugin · provenance unavailable')).toBeInTheDocument()
    expect(screen.queryByText('First-party')).not.toBeInTheDocument()
  })
})
