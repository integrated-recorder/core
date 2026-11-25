import { describe, expect, it } from 'vitest'
import { serializeAssertionCredential, serializeRegistrationCredential, webAuthnChallenge, webAuthnCreationOptions, webAuthnRequestOptions } from './webauthn'

describe('WebAuthn browser boundary', () => {
  const challenge = Uint8Array.from([1, 2, 3, 4]).buffer
  const credentialID = Uint8Array.from([5, 6, 7]).buffer

  it('converts server base64url creation options without changing the challenge', () => {
    const json = { publicKey: { challenge: 'AQIDBA', rp: { id: 'localhost', name: 'Integrated Recorder' }, user: { id: 'BQYH', name: 'identity', displayName: 'identity' }, pubKeyCredParams: [{ type: 'public-key', alg: -7 }] } }
    const options = webAuthnCreationOptions(json)
    expect(options.challenge).toEqual(challenge)
    expect(options.user.id).toEqual(credentialID)
    expect(webAuthnChallenge(json)).toBe('AQIDBA')
    expect(() => webAuthnChallenge({ publicKey: { challenge: 'not valid!' } })).toThrow()
  })

  it('converts discoverable assertion options and validates optional allow-list ids', () => {
    const json = { publicKey: { challenge: 'AQIDBA', rpId: 'localhost', userVerification: 'required', allowCredentials: [{ type: 'public-key', id: 'BQYH', transports: ['internal'] }] } }
    const options = webAuthnRequestOptions(json)
    expect(options.challenge).toEqual(challenge)
    expect(options.allowCredentials?.[0].id).toEqual(credentialID)
    expect(() => webAuthnRequestOptions({ publicKey: { challenge: '!' } })).toThrow()
  })

  it('serializes registration and assertion responses as base64url JSON', () => {
    const common = {
      id: 'BQYH', rawId: credentialID, type: 'public-key', authenticatorAttachment: 'platform',
      getClientExtensionResults: () => ({}),
    }
    const registration = {
      ...common,
      response: { attestationObject: Uint8Array.from([8, 9]).buffer, clientDataJSON: Uint8Array.from([10, 11]).buffer, getTransports: () => ['internal'] },
    } as unknown as Credential
    expect(serializeRegistrationCredential(registration)).toMatchObject({ rawId: 'BQYH', response: { attestationObject: 'CAk', clientDataJSON: 'Cgs', transports: ['internal'] } })

    const assertion = {
      ...common,
      response: { authenticatorData: Uint8Array.from([12]).buffer, clientDataJSON: Uint8Array.from([13]).buffer, signature: Uint8Array.from([14]).buffer, userHandle: null },
    } as unknown as Credential
    expect(serializeAssertionCredential(assertion)).toMatchObject({ rawId: 'BQYH', response: { authenticatorData: 'DA', clientDataJSON: 'DQ', signature: 'Dg', userHandle: null } })
    expect(() => serializeRegistrationCredential(null)).toThrow()
    expect(() => serializeAssertionCredential(null)).toThrow()
  })
})
