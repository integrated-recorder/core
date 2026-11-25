import assert from 'node:assert/strict';
import { createHash, createPrivateKey, createPublicKey, hkdfSync } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { entropyToMnemonic, mnemonicToEntropy } from '@scure/bip39';
import { wordlist } from '@scure/bip39/wordlists/english.js';

const vector = JSON.parse(await readFile(new URL('../../protocol/recovery-v1/test-vector.json', import.meta.url), 'utf8'));
const entropy = Buffer.from(vector.entropy_hex, 'hex');
const recoveryFileDomain = Buffer.from('IR/recovery-file/v1/check\0');
const identityDomain = Buffer.from('IR/global-identity-id/v1\0');
const identityKeyDomain = Buffer.from('IR/identity-key/v1\0');

function rawPublicKey(privateKey) {
  return createPublicKey(privateKey).export({ format: 'der', type: 'spki' }).subarray(-32);
}

test('BIP-39 mnemonic is the same 256-bit entropy, not a PBKDF2 identity seed', () => {
  assert.equal(entropyToMnemonic(entropy, wordlist), vector.mnemonic_english);
  assert.deepEqual(Buffer.from(mnemonicToEntropy(vector.mnemonic_english, wordlist)), entropy);
});

test('HKDF labels, Ed25519/X25519 public keys, identity ID, and recovery file match shared vector', () => {
  const signingSeed = Buffer.from(hkdfSync('sha256', entropy, Buffer.from(vector.hkdf_salt_utf8), Buffer.from(vector.signing_info_utf8), 32));
  const encryptionPrivate = Buffer.from(hkdfSync('sha256', entropy, Buffer.from(vector.hkdf_salt_utf8), Buffer.from(vector.encryption_info_utf8), 32));
  assert.equal(signingSeed.toString('hex'), vector.signing_seed_hex);
  assert.equal(encryptionPrivate.toString('hex'), vector.encryption_private_key_hex);

  const edPrivate = createPrivateKey({
    key: Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), signingSeed]),
    format: 'der',
    type: 'pkcs8',
  });
  const edPublic = rawPublicKey(edPrivate);
  assert.equal(edPublic.toString('base64url'), vector.signing_public_key_base64url);
  assert.equal(`gi_${createHash('sha256').update(identityDomain).update(edPublic).digest().subarray(0, 16).toString('base64url')}`, vector.identity_id);
  assert.equal(`ik_${createHash('sha256').update(identityKeyDomain).update(edPublic).digest('base64url')}`, vector.signer_key_id);

  const xPrivate = createPrivateKey({
    key: Buffer.concat([Buffer.from('302e020100300506032b656e04220420', 'hex'), encryptionPrivate]),
    format: 'der',
    type: 'pkcs8',
  });
  assert.equal(rawPublicKey(xPrivate).toString('base64url'), vector.encryption_public_key_base64url);

  const body = Buffer.concat([Buffer.from('IRRC'), Buffer.from([1]), entropy]);
  const checksum = createHash('sha256').update(recoveryFileDomain).update(body).digest().subarray(0, 16);
  assert.equal(Buffer.concat([body, checksum]).toString('hex'), vector.recovery_file_hex);
});
