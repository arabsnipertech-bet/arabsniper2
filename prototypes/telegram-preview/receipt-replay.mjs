import assert from 'node:assert/strict';
import {Miniflare} from 'miniflare';
import {claimShadow} from './delivery-shadow.mjs';
const mf = new Miniflare({modules:true,script:'export default {fetch(){return new Response(null)}}',
  compatibilityDate:'2026-06-01',r2Buckets:['LOCAL']});
try {
  const bucket=await mf.getR2Bucket('LOCAL');
  const key='offline-replay/ledger.json', now=Date.now(), id='telegram/live/123';
  async function seed(changes={},owner='shadow'){
    const post={id,channel:'telegram',status:'ready',attempts:0,expires_at:new Date(now+60000),...changes};
    await bucket.put(key,JSON.stringify({owner,posts:{[id]:post}}));
  }
  await seed();
  const results=await Promise.all(Array.from({length:8},()=>claimShadow(bucket,id,now)));
  assert.equal(results.filter(r=>r==='claimed').length,1);
  assert.equal(await claimShadow(bucket,id,now),'hold');
  for (const state of [{receipt:{id:'42'}},{status:'sending'},{status:'uncertain'},
      {status:'sent'},{status:'retry',retry_at:new Date(now+5000)},{attempts:3},
      {expires_at:new Date(now)},{status:'retry',retry_at:'bad'}]){
    await seed(state);assert.equal(await claimShadow(bucket,id,now),'hold');
  }
  await seed({status:'retry',retry_at:new Date(now-1),attempts:1});
  assert.equal(await claimShadow(bucket,id,now),'claimed');
  await seed({},'growth');assert.equal(await claimShadow(bucket,id,now),'hold');
  console.log('R2 conditional claim: one of eight; interrupted, uncertain, receipt, expiry, retry, owner checks passed');
} finally {await mf.dispose();}
