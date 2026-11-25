import { useEffect, useRef, useState } from 'react'
import type { IScannerControls } from '@zxing/browser'
import { Button } from '@/components/ui/button'

type EnrollmentProofQRCodeProps = {
  payload: string
  imageLabel: string
  preparingLabel: string
  unavailableLabel: string
}

export function EnrollmentProofQRCode({ payload, imageLabel, preparingLabel, unavailableLabel }: EnrollmentProofQRCodeProps) {
  const [dataURL, setDataURL] = useState('')
  const [failed, setFailed] = useState(false)

  useEffect(() => {
    let cancelled = false
    setDataURL('')
    setFailed(false)
    void import('qrcode').then(({ default: QRCode }) => QRCode.toDataURL(payload, {
      errorCorrectionLevel: 'M',
      margin: 2,
      width: 320,
      color: { dark: '#000000', light: '#ffffff' },
    })).then(value => {
      if (!cancelled) setDataURL(value)
    }).catch(() => {
      if (!cancelled) setFailed(true)
    })
    return () => { cancelled = true }
  }, [payload])

  if (failed) return <p role="status" className="text-xs text-destructive">{unavailableLabel}</p>
  if (!dataURL) return <p role="status" className="text-xs text-muted-foreground">{preparingLabel}</p>
  return <img src={dataURL} alt={imageLabel} width={320} height={320} className="mx-auto h-64 w-64 rounded bg-white p-2 sm:h-80 sm:w-80" />
}

type EnrollmentProofQRScannerProps = {
  onScan: (payload: string) => void
  startLabel: string
  stopLabel: string
  scanningLabel: string
  unavailableLabel: string
  secureContextLabel: string
}

export function EnrollmentProofQRScanner({
  onScan,
  startLabel,
  stopLabel,
  scanningLabel,
  unavailableLabel,
  secureContextLabel,
}: EnrollmentProofQRScannerProps) {
  const videoRef = useRef<HTMLVideoElement>(null)
  const controlsRef = useRef<IScannerControls | null>(null)
  const activeRef = useRef(false)
  const [scanning, setScanning] = useState(false)
  const [error, setError] = useState('')

  const stop = () => {
    activeRef.current = false
    controlsRef.current?.stop()
    controlsRef.current = null
    setScanning(false)
  }

  useEffect(() => () => {
    activeRef.current = false
    controlsRef.current?.stop()
    controlsRef.current = null
  }, [])

  const start = async () => {
    if (activeRef.current) return
    if (!window.isSecureContext || !navigator.mediaDevices?.getUserMedia) {
      setError(secureContextLabel)
      return
    }
    const video = videoRef.current
    if (!video) return
    setError('')
    setScanning(true)
    activeRef.current = true
    try {
      const { BrowserQRCodeReader } = await import('@zxing/browser')
      if (!activeRef.current) return
      const reader = new BrowserQRCodeReader()
      const started = await reader.decodeFromVideoDevice(undefined, video, (result, _decodeError, controls) => {
        if (!activeRef.current || !result) return
        activeRef.current = false
        controls.stop()
        controlsRef.current = null
        setScanning(false)
        onScan(result.getText())
      })
      if (activeRef.current) controlsRef.current = started
      else started.stop()
    } catch {
      if (activeRef.current) setError(unavailableLabel)
      activeRef.current = false
      controlsRef.current = null
      setScanning(false)
    }
  }

  return <div className="space-y-2">
    <video ref={videoRef} aria-label={scanningLabel} hidden={!scanning} className="mx-auto max-h-72 w-full max-w-sm rounded-md bg-black object-cover" muted playsInline />
    {scanning && <p role="status" className="text-xs text-muted-foreground">{scanningLabel}</p>}
    {error && <p role="alert" className="text-xs text-destructive">{error}</p>}
    {scanning
      ? <Button type="button" variant="outline" onClick={stop}>{stopLabel}</Button>
      : <Button type="button" variant="outline" onClick={() => void start()}>{startLabel}</Button>}
  </div>
}
