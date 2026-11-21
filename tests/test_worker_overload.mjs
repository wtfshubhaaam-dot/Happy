import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const js = readFileSync(new URL('../cloudflare-worker/hub-router.js', import.meta.url), 'utf8');
const worker = (await import('data:text/javascript,' + encodeURIComponent(js))).default;
const env = { ORIGINS: Array.from({length: 50}, (_,i) => `https://node${i}.example`).join(',') };
const req = () => new Request('https://hub.example/abc');
let attempts = 0;
globalThis.fetch = async () => { attempts++; return new Response('busy', {status:503}); };
let result = await worker.fetch(req(), env);
assert.equal(result.status,503);
assert.equal(attempts,3);
console.log('PASS: 50 origins bounded to 3 attempts on explicit 503');

attempts=0;
globalThis.fetch = async () => { attempts++; return new Response('error', {status:500}); };
result = await worker.fetch(req(),env);
assert.equal(result.status,503);
assert.equal(attempts,1);
console.log('PASS: ambiguous HTTP 500 does not fan out');

attempts=0;
globalThis.fetch = async () => { attempts++; throw new Error('timeout'); };
result = await worker.fetch(req(),env);
assert.equal(result.status,503);
assert.equal(attempts,1);
console.log('PASS: network error does not fan out');
