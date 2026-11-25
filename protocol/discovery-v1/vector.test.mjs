import assert from 'node:assert/strict';
import { createHash, createPublicKey, verify } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

const signDomain = Buffer.from('IR/node-advertisement/v1/sign\0');
const vector = JSON.parse(await readFile(new URL('./test-vector.json', import.meta.url), 'utf8'));
const addressEnvelopeSchema = JSON.parse(await readFile(new URL('./address-envelope.schema.json', import.meta.url), 'utf8'));

function canonical(value) {
  if (value === null || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number') throw new TypeError('numbers are forbidden');
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (typeof value !== 'object' || Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError('invalid value');
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
}

test('Go and JavaScript share the signed NodeAdvertisement vector', () => {
  const bytes = Buffer.from(canonical(vector.advertisement));
  assert.equal(bytes.toString(), vector.canonical_json);
  const publicRaw = Buffer.from(vector.advertisement.node_public_key, 'base64url');
  const publicKey = createPublicKey({
    key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), publicRaw]),
    format: 'der',
    type: 'spki',
  });
  assert.equal(verify(null, Buffer.concat([signDomain, bytes]), publicKey, Buffer.from(vector.signature, 'base64url')), true);
  assert.equal(
    `nd_${createHash('sha256').update('IR/node-id/v1\0').update(publicRaw).digest().subarray(0, 16).toString('base64url')}`,
    vector.advertisement.node_id,
  );
  assert.equal(
    `gih_${createHash('sha256').update('IR/discovery/global-identity-hint/v1\0').update('gi_AAAAAAAAAAAAAAAAAAAAAA').digest().subarray(0, 16).toString('base64url')}`,
    vector.advertisement.global_identity_hint,
  );
});

test('advertisement signature binds every address envelope byte', () => {
  const changed = { ...vector.advertisement, address_envelope: `${vector.advertisement.address_envelope.slice(0, -1)}A` };
  const publicRaw = Buffer.from(vector.advertisement.node_public_key, 'base64url');
  const publicKey = createPublicKey({ key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), publicRaw]), format: 'der', type: 'spki' });
  assert.equal(verify(null, Buffer.concat([signDomain, Buffer.from(canonical(changed))]), publicKey, Buffer.from(vector.signature, 'base64url')), false);
});

test('address envelope schema bounds the complete supported plaintext size', () => {
  const maximumCiphertextBytes = (8 * 1024) + 16;
  const maximumBase64URLLength = Math.ceil(maximumCiphertextBytes * 4 / 3);
  assert.equal(addressEnvelopeSchema.properties.envelope_version.const, 'ir.node-address-envelope/1');
  assert.equal(addressEnvelopeSchema.properties.kem.const, 'DHKEM(X25519,HKDF-SHA256)');
  assert.equal(addressEnvelopeSchema.properties.kdf.const, 'HKDF-SHA256');
  assert.equal(addressEnvelopeSchema.properties.aead.const, 'AES-256-GCM');
  assert.equal(addressEnvelopeSchema.properties.ciphertext.maxLength, maximumBase64URLLength);
  assert.equal(addressEnvelopeSchema.properties.encapsulation.maxLength, 43);
  const envelope = JSON.parse(Buffer.from(vector.advertisement.address_envelope, 'base64url').toString('utf8'));
  assert.equal(envelope.envelope_version, addressEnvelopeSchema.properties.envelope_version.const);
  assert.ok(envelope.ciphertext.length <= addressEnvelopeSchema.properties.ciphertext.maxLength);
});
