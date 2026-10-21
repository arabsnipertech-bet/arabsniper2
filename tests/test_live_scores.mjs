import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import worker,{ScoreStore,normalize} from '../workers/live-scores/worker.mjs';
const now=Date.parse('2026-10-21T18:00:00Z');
const ctx=()=>{const data=new Map();return {storage:{get:async k=>structuredClone(data.get(k)),put:async(k,v)=>{data.set(k,structuredClone(v));}}};};
const env={LIVE_ENABLED:'true',API_SPORTS_KEY:'test-only',MAX_CALLS_PER_DAY:'1500',RESERVE_REQUESTS:'1000'};
const row={fixture:{id:123,status:{short:'2H',elapsed:63}},goals:{home:1,away:0},score:{halftime:{home:0,away:0}}};
function fakeFetch(counter,quota=7500){return async url=>{
 counter.push(url);
 if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:'2026-10-21T17:00:00Z'}]});
 if(url.endsWith('/status'))return Response.json({errors:[],response:{subscription:{active:true},requests:{current:100,limit_day:quota}}});
 return Response.json({errors:[],response:[row]});
};}
test('one shared poll, score zero preserved, no fabricated fulltime, dedup interval',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=fakeFetch(calls);
 try{const c=ctx(),s=new ScoreStore(c,env);await s.refresh(now);await s.refresh(now+1000);
 const data=await (await s.fetch(new Request('https://internal/read'))).json();
 assert.equal(data.fixtures['123'].goals.away,0);assert.equal(data.fixtures['123'].fulltime.home,null);
 assert.equal(calls.filter(x=>x.includes('api-sports')).length,2);
 for(let i=0;i<20;i++)await s.fetch(new Request('https://internal/read'));
 assert.equal(calls.length,3);
 }finally{globalThis.fetch=original;}
});
test('quota reserve stops fixture calls and never publishes the secret',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=fakeFetch(calls,1000);
 try{const s=new ScoreStore(ctx(),env);await s.refresh(now);
 const text=await (await s.fetch(new Request('https://internal/read'))).text();
 assert(text.includes('quota_reserve'));assert(!text.includes(env.API_SPORTS_KEY));
 assert(!calls.some(x=>x.includes('/fixtures?')));
 }finally{globalThis.fetch=original;}
});
test('provider error retains last observation with original timestamp',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=fakeFetch(calls);
 try{const c=ctx(),s=new ScoreStore(c,env);await s.refresh(now);
 globalThis.fetch=async()=>Response.json({errors:{requests:'private provider error'},response:[]});
 await s.refresh(now+120000);const data=await(await s.fetch(new Request('https://internal/read'))).json();
 assert.equal(data.fixtures['123'].observed_at,new Date(now).toISOString());assert.equal(data.status,'provider_error');
 }finally{globalThis.fetch=original;}
});
test('midnight polls previous date for unfinished match; final is retained',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=async url=>{
 calls.push(url);if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:'2026-10-21T21:30:00Z'}]});
 if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:0,limit_day:7500}}});
 return Response.json({response:[{...row,fixture:{id:123,status:{short:'FT',elapsed:90}}}]});};
 try{const s=new ScoreStore(ctx(),env);await s.refresh(Date.parse('2026-10-21T22:30:00Z'));
 assert(calls.some(x=>x.includes('date=2026-10-21')));
 await s.refresh(Date.parse('2026-10-21T22:32:00Z'));
 assert.equal(calls.filter(x=>x.includes('/fixtures?')).length,1);
 }finally{globalThis.fetch=original;}
});
test('public route cannot trigger refresh or expose other paths',async()=>{
 let route;const e={SCORES:{idFromName:x=>x,get:()=>({fetch:async x=>{route=x;return new Response('ok');}})}};
 assert.equal((await worker.fetch(new Request('https://site/api/live-scores/refresh'),e)).status,404);
 await worker.fetch(new Request('https://site/api/live-scores'),e);assert(route.endsWith('/read'));
});
test('overlay keeps prematch immutable, uses newer score and marks stale states',()=>{
 const scope={console,Date,Number,Set,Object,AbortSignal};vm.createContext(scope);
 vm.runInContext(fs.readFileSync('web/live-scores.js','utf8'),scope);
 const match={fixture:{fixture_id:'123',kickoff:new Date(now-3600000).toISOString(),live:{observed_at:new Date(now-600000).toISOString()}},multimarket:{probability:.7,odd:2}};
 const before=JSON.stringify(match);scope.ArabLive.accept({schema_version:1,enabled:true,fixtures:{'123':normalize(row,now)}});
 assert(scope.ArabLive.view(match,now).active);assert.equal(scope.ArabLive.view(match,now).score,'1–0');
 assert(scope.ArabLive.view(match,now+301000).stale);assert(!scope.ArabLive.view(match,now+301000).active);
 assert.equal(JSON.stringify(match),before);
 scope.ArabLive.accept({schema_version:1,enabled:false,fixtures:{}});assert.equal(scope.ArabLive.fixture(match),match.fixture);
});
