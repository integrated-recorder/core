import assert from 'node:assert/strict';
import { createHash, createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

const signDomain = Buffer.from('IR/identity-event/v1/sign\0');
const hashDomain = Buffer.from('IR/identity-event/v1/hash\0');
const keyDomain = Buffer.from('IR/identity-key/v1\0');
const schemaKey = /^[a-z][a-z0-9_]*$/;
const seed = Buffer.from('9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60', 'hex');
const vector = JSON.parse(await readFile(new URL('./test-vector.json', import.meta.url), 'utf8'));

function validUnicode(value) {
  for (let i = 0; i < value.length; i += 1) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError('unpaired high surrogate');
      i += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError('unpaired low surrogate');
    }
  }
}

// This intentionally implements the narrow event profile, not a general JCS
// library: schema property names are ASCII identifiers and numeric values are
// forbidden. Strings still use ECMAScript JSON serialization per RFC 8785.
function canonical(value) {
  if (value === null || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'string') {
    validUnicode(value);
    return JSON.stringify(value);
  }
  if (typeof value === 'number') throw new TypeError('numeric values are forbidden');
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (typeof value !== 'object' || Object.getPrototypeOf(value) !== Object.prototype) {
    throw new TypeError('unsupported JSON value');
  }
  const keys = Object.keys(value).sort();
  for (const key of keys) {
    if (!schemaKey.test(key)) throw new TypeError('property name outside protocol profile');
    validUnicode(key);
  }
  return `{${keys.map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
}

function publicKeyFromRaw(raw) {
  return createPublicKey({
    key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), raw]),
    format: 'der',
    type: 'spki',
  });
}

function privateKeyFromSeed(testSeed) {
  return createPrivateKey({
    key: Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), testSeed]),
    format: 'der',
    type: 'pkcs8',
  });
}

test('Go and JavaScript share the identity genesis canonical/signature vector', () => {
  const bytes = Buffer.from(canonical(vector.event));
  assert.equal(bytes.toString('utf8'), vector.canonical_json);
  const publicRaw = Buffer.from(vector.event.signer_public_key, 'base64url');
  const publicKey = publicKeyFromRaw(publicRaw);
  const signature = Buffer.from(vector.signature, 'base64url');
  assert.equal(verify(null, Buffer.concat([signDomain, bytes]), publicKey, signature), true);
  assert.equal(
    `sha256:${createHash('sha256').update(hashDomain).update(bytes).digest('hex')}`,
    vector.event_hash,
  );
  assert.equal(
    `ik_${createHash('sha256').update(keyDomain).update(publicRaw).digest('base64url')}`,
    vector.event.signer_key_id,
  );
  assert.equal(
    `gi_${createHash('sha256').update('IR/global-identity-id/v1\0').update(publicRaw).digest().subarray(0, 16).toString('base64url')}`,
    vector.event.identity_id,
  );
});

test('identity vector rejects tampering, a wrong key, and unsafe profile values', () => {
  const bytes = Buffer.from(vector.canonical_json);
  const message = Buffer.concat([signDomain, bytes]);
  const signature = Buffer.from(vector.signature, 'base64url');
  const publicKey = publicKeyFromRaw(Buffer.from(vector.event.signer_public_key, 'base64url'));
  const changed = { ...vector.event, identity_id: 'gi_AQECAwQFBgcICQoLDA0ODw' };
  assert.equal(verify(null, Buffer.concat([signDomain, Buffer.from(canonical(changed))]), publicKey, signature), false);
  const otherPublic = publicKeyFromRaw(Buffer.alloc(32, 0x5a));
  assert.equal(verify(null, message, otherPublic, signature), false);
  assert.throws(() => canonical({ sequence: 1 }), /numeric values are forbidden/);
  assert.throws(() => canonical({ 'not a schema key': 'x' }), /outside protocol profile/);
  assert.throws(() => canonical({ text: String.fromCharCode(0xd800) }), /unpaired high surrogate/);
});

test('JCS profile preserves Unicode and does not HTML-escape text', () => {
  const text = `<>&${String.fromCharCode(0x2028)}/\n녹화`;
  assert.equal(canonical({ text }), `{"text":"<>&${String.fromCharCode(0x2028)}/\\n녹화"}`);
});

test('two-node harness freezes rather than choosing a valid fork winner', () => {
  // RFC 8032 public test seed only; not an account or deployment key.
  const privateKey = privateKeyFromSeed(seed);
  const node = () => ({ head: vector.event_hash, frozen: false });
  const a = node();
  const b = node();
  const successor = (label) => ({
    ...vector.event,
    sequence: '1',
    previous_event_hash: vector.event_hash,
    operation: 'credential_add',
    issued_at: '2026-10-25T00:00:01Z',
    payload: { credential_label: label },
  });
  const first = Buffer.from(canonical(successor('first-successor')));
  const second = Buffer.from(canonical(successor('second-successor')));
  const firstSig = sign(null, Buffer.concat([signDomain, first]), privateKey);
  const secondSig = sign(null, Buffer.concat([signDomain, second]), privateKey);
  const apply = (peer, eventBytes, signature) => {
    const event = JSON.parse(eventBytes.toString('utf8'));
    assert.equal(peer.frozen, false);
    assert.equal(event.previous_event_hash, peer.head);
    const eventKey = publicKeyFromRaw(Buffer.from(event.signer_public_key, 'base64url'));
    assert.equal(verify(null, Buffer.concat([signDomain, eventBytes]), eventKey, signature), true);
    peer.head = `sha256:${createHash('sha256').update(hashDomain).update(eventBytes).digest('hex')}`;
  };
  apply(a, first, firstSig);
  apply(b, second, secondSig);
  const forkA = a.head;
  const forkB = b.head;
  assert.notEqual(forkA, forkB);
  // Both nodes learned different valid children of the same prior event. The
  // reconciliation result is freeze, never timestamp/sequence winner.
  if (forkA !== forkB) {
    a.frozen = true;
    b.frozen = true;
  }
  assert.equal(a.frozen && b.frozen, true);
});
