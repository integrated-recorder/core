import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const vector = JSON.parse(await readFile(new URL('./test-vector.json', import.meta.url), 'utf8'));

function canonicalize(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(',')}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(',')}}`;
}

test('Go and JavaScript share the archive snapshot JCS content hash', () => {
  const canonical = canonicalize(JSON.parse(vector.input));
  assert.equal(canonical, vector.canonical);
  const hash = createHash('sha256')
    .update(`IR/archive-snapshot/v1/content\0${canonical}`)
    .digest('hex');
  assert.equal(`sha256:${hash}`, vector.content_hash);
});
