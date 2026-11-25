type CredentialDescriptorJSON = { type: 'public-key'; id: string; transports?: AuthenticatorTransport[] }
type CreationOptionsJSON = {
  challenge: string
  user: { id: string; name: string; displayName: string }
  excludeCredentials?: CredentialDescriptorJSON[]
  [key: string]: unknown
}
type RequestOptionsJSON = {
  challenge: string
  allowCredentials?: CredentialDescriptorJSON[]
  [key: string]: unknown
}

export function webAuthnChallenge(value: unknown): string {
  if (!value || typeof value !== 'object' || !('publicKey' in value)) throw new TypeError('WebAuthn options are invalid')
  const publicKey = (value as { publicKey?: { challenge?: unknown } }).publicKey
  if (!publicKey || typeof publicKey.challenge !== 'string') throw new TypeError('WebAuthn challenge is invalid')
  // Validate the encoding and ensure it is usable before signing the event.
  base64URLToBuffer(publicKey.challenge)
  return publicKey.challenge
}

function bytesToBase64URL(bytes: Uint8Array): string {
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, Math.min(offset + 0x8000, bytes.length)))
  }
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}

function base64URLToBuffer(value: string): ArrayBuffer {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new TypeError('WebAuthn binary option is invalid')
  const normalized = value.replaceAll('-', '+').replaceAll('_', '/')
  const binary = atob(normalized + '='.repeat((4 - normalized.length % 4) % 4))
  const bytes = new Uint8Array(binary.length)
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index)
  return bytes.buffer
}

function serializeCredentialDescriptor(value: CredentialDescriptorJSON): PublicKeyCredentialDescriptor {
  return { ...value, id: base64URLToBuffer(value.id) }
}

export function webAuthnCreationOptions(value: unknown): PublicKeyCredentialCreationOptions {
  if (!value || typeof value !== 'object' || !('publicKey' in value)) throw new TypeError('WebAuthn creation options are invalid')
  const source = (value as { publicKey?: CreationOptionsJSON }).publicKey
  if (!source || typeof source.challenge !== 'string' || !source.user || typeof source.user.id !== 'string') throw new TypeError('WebAuthn creation options are invalid')
  return {
    ...source,
    challenge: base64URLToBuffer(source.challenge),
    user: { ...source.user, id: base64URLToBuffer(source.user.id) },
    excludeCredentials: source.excludeCredentials?.map(serializeCredentialDescriptor),
  } as PublicKeyCredentialCreationOptions
}

export function webAuthnRequestOptions(value: unknown): PublicKeyCredentialRequestOptions {
  if (!value || typeof value !== 'object' || !('publicKey' in value)) throw new TypeError('WebAuthn request options are invalid')
  const source = (value as { publicKey?: RequestOptionsJSON }).publicKey
  if (!source || typeof source.challenge !== 'string') throw new TypeError('WebAuthn request options are invalid')
  return {
    ...source,
    challenge: base64URLToBuffer(source.challenge),
    allowCredentials: source.allowCredentials?.map(serializeCredentialDescriptor),
  } as PublicKeyCredentialRequestOptions
}

export function serializeRegistrationCredential(credential: Credential | null): Record<string, unknown> {
  if (!credential || credential.type !== 'public-key' || !('rawId' in credential) || !('response' in credential)) {
    throw new DOMException('No passkey was created.', 'NotAllowedError')
  }
  const publicKeyCredential = credential as PublicKeyCredential
  const response = publicKeyCredential.response as AuthenticatorAttestationResponse
  return {
    id: publicKeyCredential.id,
    rawId: bytesToBase64URL(new Uint8Array(publicKeyCredential.rawId)),
    type: publicKeyCredential.type,
    authenticatorAttachment: publicKeyCredential.authenticatorAttachment,
    clientExtensionResults: publicKeyCredential.getClientExtensionResults(),
    response: {
      attestationObject: bytesToBase64URL(new Uint8Array(response.attestationObject)),
      clientDataJSON: bytesToBase64URL(new Uint8Array(response.clientDataJSON)),
      transports: response.getTransports?.() ?? [],
    },
  }
}

export function serializeAssertionCredential(credential: Credential | null): Record<string, unknown> {
  if (!credential || credential.type !== 'public-key' || !('rawId' in credential) || !('response' in credential)) {
    throw new DOMException('No passkey was selected.', 'NotAllowedError')
  }
  const publicKeyCredential = credential as PublicKeyCredential
  const response = publicKeyCredential.response as AuthenticatorAssertionResponse
  return {
    id: publicKeyCredential.id,
    rawId: bytesToBase64URL(new Uint8Array(publicKeyCredential.rawId)),
    type: publicKeyCredential.type,
    authenticatorAttachment: publicKeyCredential.authenticatorAttachment,
    clientExtensionResults: publicKeyCredential.getClientExtensionResults(),
    response: {
      authenticatorData: bytesToBase64URL(new Uint8Array(response.authenticatorData)),
      clientDataJSON: bytesToBase64URL(new Uint8Array(response.clientDataJSON)),
      signature: bytesToBase64URL(new Uint8Array(response.signature)),
      userHandle: response.userHandle ? bytesToBase64URL(new Uint8Array(response.userHandle)) : null,
    },
  }
}

export function base64UrlBytes(value: Uint8Array): string {
  return bytesToBase64URL(value)
}
