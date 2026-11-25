import { BrowserQRCodeReader } from '@zxing/browser'
import QRCode from 'qrcode'

export async function roundTripEnrollmentQR(payload: string): Promise<{ dataURL: string; decoded: string }> {
  const dataURL = await QRCode.toDataURL(payload, {
    errorCorrectionLevel: 'M',
    margin: 2,
    width: 320,
    color: { dark: '#000000', light: '#ffffff' },
  })
  const image = new Image()
  image.src = dataURL
  await image.decode()
  const result = await new BrowserQRCodeReader().decodeFromImageElement(image)
  return { dataURL, decoded: result.getText() }
}
