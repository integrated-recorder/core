import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { EnrollmentProofQRCode, EnrollmentProofQRScanner } from './node-enrollment-qr'

const qrMocks = vi.hoisted(() => ({
  toDataURL: vi.fn(),
  decodeFromVideoDevice: vi.fn(),
}))

vi.mock('qrcode', () => ({ default: { toDataURL: qrMocks.toDataURL } }))
vi.mock('@zxing/browser', () => ({
  BrowserQRCodeReader: class {
    decodeFromVideoDevice = qrMocks.decodeFromVideoDevice
  },
}))

describe('Node enrollment QR', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    Object.defineProperty(navigator, 'mediaDevices', { configurable: true, value: { getUserMedia: vi.fn() } })
  })

  it('renders an accessible QR image from the public enrollment payload', async () => {
    qrMocks.toDataURL.mockResolvedValue('data:image/png;base64,qr')
    render(<EnrollmentProofQRCode payload="public-proof-json" imageLabel="Enrollment proof QR" preparingLabel="Preparing" unavailableLabel="Unavailable" />)

    const image = await screen.findByRole('img', { name: 'Enrollment proof QR' })
    expect(image).toHaveAttribute('src', 'data:image/png;base64,qr')
    expect(qrMocks.toDataURL).toHaveBeenCalledWith('public-proof-json', expect.objectContaining({ errorCorrectionLevel: 'M' }))
  })

  it('scans one QR, delivers its text and stops the camera stream', async () => {
    const stopCamera = vi.fn()
    let onDecode: ((result: { getText: () => string } | undefined, error: unknown, controls: { stop: () => void }) => void) | undefined
    qrMocks.decodeFromVideoDevice.mockImplementation(async (_device: unknown, _video: unknown, callback: typeof onDecode) => {
      onDecode = callback
      return { stop: stopCamera }
    })
    const onScan = vi.fn()
    render(<EnrollmentProofQRScanner onScan={onScan} startLabel="Scan QR" stopLabel="Stop" scanningLabel="Scanning" unavailableLabel="Unavailable" secureContextLabel="Use HTTPS" />)

    fireEvent.click(screen.getByRole('button', { name: 'Scan QR' }))
    await waitFor(() => expect(qrMocks.decodeFromVideoDevice).toHaveBeenCalledOnce())
    expect(screen.getByRole('status')).toHaveTextContent('Scanning')
    expect(onDecode).toBeTypeOf('function')
    await act(async () => onDecode?.({ getText: () => '{"node_id":"nd_public"}' }, undefined, { stop: stopCamera }))

    expect(onScan).toHaveBeenCalledWith('{"node_id":"nd_public"}')
    expect(stopCamera).toHaveBeenCalledOnce()
    expect(screen.getByLabelText('Scanning')).toHaveAttribute('hidden')
  })

  it('does not request camera access outside a secure context', () => {
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: false })
    render(<EnrollmentProofQRScanner onScan={vi.fn()} startLabel="Scan QR" stopLabel="Stop" scanningLabel="Scanning" unavailableLabel="Unavailable" secureContextLabel="Use HTTPS" />)
    fireEvent.click(screen.getByRole('button', { name: 'Scan QR' }))

    expect(screen.getByRole('alert')).toHaveTextContent('Use HTTPS')
    expect(qrMocks.decodeFromVideoDevice).not.toHaveBeenCalled()
  })

  it('stops camera controls when the scanner unmounts', async () => {
    const stopCamera = vi.fn()
    qrMocks.decodeFromVideoDevice.mockResolvedValue({ stop: stopCamera })
    const view = render(<EnrollmentProofQRScanner onScan={vi.fn()} startLabel="Scan QR" stopLabel="Stop" scanningLabel="Scanning" unavailableLabel="Unavailable" secureContextLabel="Use HTTPS" />)
    fireEvent.click(screen.getByRole('button', { name: 'Scan QR' }))
    await waitFor(() => expect(qrMocks.decodeFromVideoDevice).toHaveBeenCalledOnce())
    await waitFor(() => expect(screen.getByRole('button', { name: 'Stop' })).toBeInTheDocument())

    view.unmount()
    expect(stopCamera).toHaveBeenCalledOnce()
  })
})
