import {test} from 'node:test';
import assert from 'node:assert/strict';
import worker, {preview} from './worker.mjs';

const now = Date.parse('2026-11-18T09:00:00Z');
const base = {kind:'live', channel:'telegram', status:'ready', expires_at:'2026-11-18T10:00:00Z',
  marker:'AS71-EXAMPLE', text:'⚽ Primo tempo 1–0\nAS71-EXAMPLE'};
function bucket(rows) {
  const posts = {};
  rows.forEach((changes, i) => {
    const post = {...base, key:String(i), ...changes};
    post.id = [post.channel, post.kind, post.key].join('/');
    posts[post.id] = post;
  });
  return {async get(key) {
    assert.equal(key, 'publisher/v7.1/state.json');
    return {size:1000, json:async () => ({schema:1, posts})};
  }};
}

test('preserves Python captions, identifies photo and live animation edit', async () => {
  const {proposals} = await preview(bucket([{}, {edit_id:'42', view:{animation:'goal'}}]), now);
  assert.deepEqual(proposals.map(p => [p.method,p.media,p.message_id,p.caption]), [
    ['sendPhoto','photo',null,base.text], ['editMessageMedia','animation','42',base.text]]);
});
test('excludes settled, uncertain, expired, deferred, unmarked and Facebook entries', async () => {
  const rows = [{status:'sent'}, {status:'sending'}, {status:'uncertain'}, {receipt:{id:'1'}},
    {expires_at:'invalid'}, {expires_at:new Date(now).toISOString()}, {retry_at:'invalid'},
    {status:'retry',retry_at:'2026-11-18T09:30:00Z'}, {channel:'facebook'}, {marker:'missing'}];
  assert.deepEqual((await preview(bucket(rows), now)).proposals, []);
});
test('rejects missing or oversized input and exposes no POST route', async () => {
  for (const obj of [null,{size:5_000_001}]) await assert.rejects(preview({get:async()=>obj}, now));
  assert.equal((await worker.fetch(new Request('https://local/preview',{method:'POST'}),{})).status,404);
});
