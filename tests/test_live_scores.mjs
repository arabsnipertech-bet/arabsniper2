import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import worker,{ScoreStore,normalize,relayRows} from '../workers/live-scores/worker.mjs';
const now=Date.now();
const ctx=()=>{const data=new Map();return {storage:{get:async k=>structuredClone(data.get(k)),put:async(k,v)=>{data.set(k,structuredClone(v));}}};};
const env={LIVE_ENABLED:'true',API_SPORTS_KEY:'test-only',MAX_CALLS_PER_DAY:'1500',RESERVE_REQUESTS:'1000'};
const row={fixture:{id:123,status:{short:'2H',elapsed:63}},goals:{home:1,away:0},score:{halftime:{home:0,away:0}}};
function fakeFetch(counter,quota=7500){return async url=>{
 counter.push(url);
 if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:new Date(now-3600000).toISOString()}]});
 if(url.endsWith('/status'))return Response.json({errors:[],response:{subscription:{active:true},requests:{current:100,limit_day:quota}}});
 return Response.json({errors:[],response:[row]});
};}
test('one shared poll, score zero preserved, no fabricated fulltime, dedup interval',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=fakeFetch(calls);
 try{const c=ctx(),s=new ScoreStore(c,env);await s.refresh(now);await s.refresh(now+1000);
 const data=await (await s.fetch(new Request('https://internal/read'))).json();
 assert.equal(data.fixtures['123'].goals.away,0);assert.equal(data.fixtures['123'].fulltime.home,null);
 assert.equal(calls.filter(x=>x.includes('api-sports')).length,2);
 assert(data.health.usage.provider.request_gap_ms >= 250);
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
test('a small batch polls an unfinished match across Rome midnight and retains the final',async()=>{
 const original=globalThis.fetch,calls=[];globalThis.fetch=async url=>{
 calls.push(url);if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:'2026-10-21T21:30:00Z'}]});
 if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:0,limit_day:7500}}});
 return Response.json({response:[{...row,fixture:{id:123,status:{short:'FT',elapsed:90}}}]});};
 try{const s=new ScoreStore(ctx(),env);await s.refresh(Date.parse('2026-10-21T22:30:00Z'));
 assert(calls.some(x=>x.includes('fixtures?ids=123')));
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

test('half-time distinguishes provisional scores, confirmed zeroes and absent data',()=>{
 const scope={console,Date,Number,Set,Object,AbortSignal};vm.createContext(scope);
 vm.runInContext(fs.readFileSync('web/live-scores.js','utf8'),scope);
 const match={fixture:{fixture_id:'123',kickoff:new Date(now-3600000).toISOString(),status_short:'NS'}};
 const before=JSON.stringify(match);
 const send=(status,goals,halftime,offset=0)=>scope.ArabLive.accept({schema_version:1,enabled:true,fixtures:{'123':{fixture_id:'123',status,goals,halftime,observed_at:new Date(now+offset).toISOString()}}});
 assert.equal(scope.ArabLive.firstHalf(match,now).status,'pending');
 send('1H',{home:1,away:0},{home:0,away:0});
 assert.equal(scope.ArabLive.firstHalf(match,now).status,'in_progress');
 assert.equal(scope.ArabLive.firstHalf(match,now).score,'1–0','Ignore the provider halftime placeholder during 1H');
 assert(scope.ArabLive.firstHalf(match,now+301000).stale);
 send('2H',{home:2,away:0},null,1000);
 assert.equal(scope.ArabLive.firstHalf(match,now+1000).score,null,'Never infer the halftime score from 2H goals');
 send('HT',{home:0,away:0},{home:0,away:0},2000);
 assert.equal(scope.ArabLive.firstHalf(match,now+2000).score,'0–0');
 assert.equal(scope.ArabLive.firstHalf(match,now+2000).status,'complete');
 send('2H',{home:2,away:0},{home:null,away:null},3000);
 assert.equal(scope.ArabLive.firstHalf(match,now+3000).score,'0–0','Preserve confirmed halftime when a poll omits it');
 send('FT',{home:3,away:1},{home:1,away:0},4000);
 assert.equal(scope.ArabLive.firstHalf(match,now+4000).score,'1–0','A provider correction supersedes the earlier result');
 assert.equal(JSON.stringify(match),before);
 scope.ArabLive.accept({schema_version:1,enabled:false,fixtures:{}});
 for(const half of [{home:null,away:0},{home:'0',away:0},{home:-1,away:0},{home:.5,away:0}]){
  assert.equal(scope.ArabLive.firstHalf({fixture:{status_short:'FT',live:{halftime:half}}},now).score,null);
 }
 assert.equal(scope.ArabLive.firstHalf({fixture:{status_short:'FT',result:{home_ht:0,away_ht:1}}},now).score,'0–1');
});


test('poll works without AbortSignal.timeout and releases its timer',async()=>{
 let called=0,cleared=0;
 const scope={console,Date,Number,Set,Object,AbortController,Event,setTimeout:()=>42,clearTimeout:id=>{assert.equal(id,42);cleared++;},dispatchEvent(){},fetch:async(_url,options)=>{called++;assert(options.signal instanceof AbortSignal);return {ok:true,json:async()=>({schema_version:1,enabled:false,fixtures:{}})};}};
 vm.createContext(scope);vm.runInContext(fs.readFileSync('web/live-scores.js','utf8'),scope);
 await scope.ArabLive.poll();assert.equal(called,1);assert.equal(cleared,1);
});


test('missing date-feed fixture is recovered by exact ID without inventing results',async()=>{
 const original=globalThis.fetch,calls=[];
 const watch=Array.from({length:21},(_,i)=>({fixture_id:String(123+i),kickoff:new Date(now-3600000).toISOString()}));
 globalThis.fetch=async url=>{
  calls.push(url);
  if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:watch});
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:7500}}});
  if(url.includes('fixtures?date='))return Response.json({response:[]});
  if(url.includes('fixtures?ids=123'))return Response.json({response:[{...row,fixture:{id:123,status:{short:'FT',elapsed:90}},score:{halftime:{home:0,away:0},fulltime:{home:2,away:1}}}]});
  if(url.includes('fixtures?ids='))return Response.json({response:[]});
  throw Error('unexpected fetch');
 };
 try{
  const s=new ScoreStore(ctx(),env);await s.refresh(now);
  const data=await(await s.fetch(new Request('https://internal/read'))).json();
  assert.equal(data.fixtures['123'].status,'FT');
  assert.equal(data.fixtures['123'].fulltime.home,2);
  assert.equal(data.fixtures['123'].fulltime.away,1);
  assert(calls.some(x=>x.includes('fixtures?date=')));
  assert(calls.some(x=>x.includes('fixtures?ids=123')));
  await s.refresh(now+120000);
  assert.equal(calls.filter(x=>x.includes('fixtures?ids=123')).length,1,'Final fixtures are not queried again');
 }finally{globalThis.fetch=original;}
});

test('one fixture request updates a live match and a pending final',async()=>{
 const original=globalThis.fetch,context=ctx(),requests=[];
 const watch=[{fixture_id:'123',kickoff:new Date(now-3600000).toISOString()},
  {fixture_id:'124',kickoff:new Date(now-4*3600000).toISOString()}];
 const final={fixture:{id:124,status:{short:'FT'}},goals:{home:2,away:0},
  score:{halftime:{home:1,away:0},fulltime:{home:2,away:0}}};
 await context.storage.put('state',{scores:{},watch,calls:0,watch_at:now,watch_generated_at:now,
  day:new Date(now).toISOString().slice(0,10),quota:{current:100,limit:75000,checked_at:now}});
 globalThis.fetch=async url=>{requests.push(new URL(url));return Response.json({response:[row,final]});};
 try{
  const s=new ScoreStore(context,env);
  await s.refresh(now);
  assert.equal(requests.length,1);
  assert.equal(requests[0].searchParams.get('ids'),'123-124');
  const state=await context.storage.get('state');
  assert.equal(state.scores['123'].status,'2H');
  assert.equal(state.scores['124'].status,'FT');
  assert.equal(state.provider_usage.query,'by_id');
  assert.equal(state.health.stale_live_fixtures,0);
 }finally{globalThis.fetch=original;}
});

test('a full live batch leaves room to recover a missing final',async()=>{
 const original=globalThis.fetch,context=ctx(),queries=[];
 const watch=Array.from({length:20},(_,i)=>({fixture_id:String(123+i),kickoff:new Date(now-3600000).toISOString()}));
 watch.push({fixture_id:'900',kickoff:new Date(now-4*3600000).toISOString()});
 await context.storage.put('state',{scores:{},watch,calls:0,watch_at:now,watch_generated_at:now,
  day:new Date(now).toISOString().slice(0,10),quota:{current:100,limit:75000,checked_at:now}});
 globalThis.fetch=async url=>{queries.push(new URL(url).searchParams);return Response.json({response:[]});};
 try{
  await new ScoreStore(context,env).refresh(now);
  assert.equal(queries.length,2);
  assert(queries[0].has('date'));
  assert(queries[1].get('ids').split('-').includes('900'));
 }finally{globalThis.fetch=original;}
});

test('a provider pause reports stale live IDs while keeping known scores',async()=>{
 const context=ctx(),observed=now-8*60000;
 await context.storage.put('state',{scores:{'123':normalize(row,observed)},calls:20,
  watch:[{fixture_id:'123',kickoff:new Date(now-3600000).toISOString()}],
  watch_at:now,watch_generated_at:now,day:new Date(now).toISOString().slice(0,10),
  retry_at:now+120000,status:'provider_rate_limit',minute_backoff_v2:true});
 await new ScoreStore(context,env).refresh(now);
 const state=await context.storage.get('state');
 assert.equal(state.health.overdue_fixtures,0);
 assert.equal(state.health.stale_live_fixtures,1);
 assert.deepEqual(state.health.stale_live_fixture_ids,['123']);
 assert.equal(state.scores['123'].observed_at,new Date(observed).toISOString());
});

test('missing date feed and exact ID cannot fabricate a final score',async()=>{
 const original=globalThis.fetch,calls=[];
 globalThis.fetch=async url=>{
  calls.push(url);
  if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:new Date(now-3600000).toISOString()}]});
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:7500}}});
  return Response.json({response:[]});
 };
 try{
  const s=new ScoreStore(ctx(),env);await s.refresh(now);
  const data=await(await s.fetch(new Request('https://internal/read'))).json();
  assert.equal(data.fixtures['123'],undefined);
  assert.equal(data.status,'ok');
 }finally{globalThis.fetch=original;}
});

test('rate limit backs off persistently, then recovers a delayed fixture in a single batch',async()=>{
 const original=globalThis.fetch,calls=[],context=ctx(),kickoff=new Date(now-4*3600000).toISOString();
 const finished={fixture:{id:123,status:{short:'FT',elapsed:90}},goals:{home:1,away:0},
  score:{halftime:{home:0,away:0},fulltime:{home:1,away:0}}};
 let limited=true;
 globalThis.fetch=async url=>{
  calls.push(url);
  if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff}]});
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:75000}}});
  if(limited)return Response.json({errors:{rateLimit:'never expose me'}},{headers:{'Retry-After':'240'}});
  return Response.json({response:[finished]});
 };
 try{
  const first=new ScoreStore(context,env);
  await first.refresh(now);
  const output=await(await first.fetch(new Request('https://scores.internal/read'))).json();
  assert.equal(output.status,'provider_rate_limit');
  assert.equal(output.health.overdue_fixtures,1);
  assert.equal(output.health.retry_after,new Date(now+240000).toISOString());
  assert.equal(output.diagnostic.categories[0],'rateLimit');
  const restarted=new ScoreStore(context,env);
  await restarted.refresh(now+120000);
  assert.equal(calls.filter(u=>u.includes('/fixtures?')).length,1);
  limited=false;
  await restarted.refresh(now+240000);
  const restored=await(await restarted.fetch(new Request('https://scores.internal/read'))).json();
  assert.equal(restored.fixtures['123'].status,'FT');
  assert.equal(restored.health.overdue_fixtures,0);
  assert.equal(restored.health.last_successful_poll_at,new Date(now+240000).toISOString());
  assert.equal((await context.storage.get('final:123')).fulltime.home,1);
 }finally{globalThis.fetch=original;}
});

test('late finals and corrections survive incomplete responses, restarts and watchlist rollover',async()=>{
 const original=globalThis.fetch,context=ctx(),kickoff=new Date(now-26*3600000).toISOString();
 let stage=0;
 globalThis.fetch=async url=>{
  if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff}]});
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:75000}}});
  const version=[{status:'FT',goals:{home:1,away:0},half:{home:0,away:0}},
   {status:'FT',goals:null,half:null},{status:'FT',goals:{home:2,away:0},half:null},
   {status:'NS',goals:null,half:null}][stage];
  return Response.json({response:[{fixture:{id:123,status:{short:version.status}},goals:version.goals,
   score:{halftime:version.half}}]});
 };
 const hour=3600000,read=()=>context.storage.get('state');
 try{
  await new ScoreStore(context,env).refresh(now);
  assert.equal((await read()).scores['123'].fulltime.home,1);
  stage=1;
  await new ScoreStore(context,env).refresh(now+hour);
  assert.equal((await read()).scores['123'].halftime.home,0);
  assert.equal((await read()).scores['123'].fulltime.home,1);
  stage=2;
  await new ScoreStore(context,env).refresh(now+9*hour);
  assert.equal((await read()).scores['123'].fulltime.home,2);
  stage=3;
  await new ScoreStore(context,env).refresh(now+33*hour);
  assert.equal((await read()).scores['123'].status,'FT');
  await new ScoreStore(context,env).refresh(now+72*hour);
  assert.equal((await read()).scores['123'],undefined);
  const archive=await(await new ScoreStore(context,env).fetch(new Request('https://scores.internal/read?id=123'))).json();
  assert.equal(archive.fixtures['123'].fulltime.home,2);
 }finally{globalThis.fetch=original;}
});

test('Gold keeps a saved FT against NS and reopens using the confirmed score',()=>{
 const scope={console,Date,Number,Set,Object,AbortSignal};
 vm.createContext(scope);vm.runInContext(fs.readFileSync('web/live-scores.js','utf8'),scope);
 const match={fixture:{fixture_id:'123',status_short:'NS',kickoff:new Date(now-3600000).toISOString(),
  result:{status:'FT',home_ft:0,away_ft:0,home_ht:0,away_ht:0}}};
 scope.ArabLive.accept({schema_version:1,enabled:true,fixtures:{'123':{fixture_id:'123',status:'NS',observed_at:new Date(now).toISOString()}}});
 assert.equal(scope.ArabLive.fixture(match).status_short,'FT');
 assert.equal(scope.ArabLive.view(match,now+6*3600000).score,'0–0');
 assert.equal(scope.ArabLive.firstHalf(match).score,'0–0');
 assert.equal(match.fixture.status_short,'NS');
});

test('watchlist outage retains recent fixtures and a postponed fixture is recovered at its new kickoff',async()=>{
 const original=globalThis.fetch,calls=[],context=ctx(),kickoff=new Date(now-7*3600000).toISOString();
 let step=0;
 globalThis.fetch=async url=>{
  calls.push(url);
  if(url.includes('raw.githubusercontent')){
   if(step)return new Response('',{status:503});
   return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff}]});
  }
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:75000}}});
  const status=step===0?'PST':step===1?'NS':'FT';
  return Response.json({response:[{fixture:{id:123,date:new Date(now+8*3600000).toISOString(),status:{short:status}},
   goals:status==='FT'?{home:1,away:0}:null,score:status==='FT'?{halftime:{home:0,away:0}}:{}}]});
 };
 try{
  await new ScoreStore(context,env).refresh(now);
  assert.equal((await context.storage.get('state')).scores['123'].status,'PST');
  step=1;
  await new ScoreStore(context,env).refresh(now+8*3600000);
  assert.equal((await context.storage.get('state')).scores['123'].status,'NS');
  step=2;
  await new ScoreStore(context,env).refresh(now+10*3600000);
  const state=await context.storage.get('state');
  assert.equal(state.scores['123'].fulltime.home,1);
  assert.equal(state.status,'watchlist_unavailable');
  assert(calls.some(url=>url.includes('fixtures?ids=123')));
 }finally{globalThis.fetch=original;}
});

test('provider quota telemetry is bounded and authenticated fetches bypass cache',async()=>{
 const original=globalThis.fetch, context=ctx();
 globalThis.fetch=async (url,options)=>{
  if(url.includes('raw.githubusercontent'))return Response.json({generated_at:new Date(now).toISOString(),fixtures:[{fixture_id:'123',kickoff:new Date(now-4*3600000).toISOString()}]});
  assert.equal(options.cache,'no-store');
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:75000}}});
  return Response.json({errors:{rateLimit:'SECRET MUST NOT LEAK'}},{headers:{
   'x-ratelimit-remaining':'0','x-ratelimit-limit':'450','x-ratelimit-requests-remaining':'62940','age':'42'}});
 };
 try{
  const s=new ScoreStore(context,env);await s.refresh(now);
  const body=await(await s.fetch(new Request('https://internal/read'))).json();
  assert.equal(body.diagnostic.limit_scope,'minute');
  assert.equal(body.diagnostic.quota.daily_remaining,62940);
  assert.equal(body.diagnostic.quota.response_age_seconds,42);
  assert.equal(body.health.usage.worker_requests,2);
  assert(!JSON.stringify(body).includes('SECRET MUST NOT LEAK'));
 }finally{globalThis.fetch=original;}
});

test('missing results take priority over routine final-score rechecks',async()=>{
 const original=globalThis.fetch,context=ctx(),watch=[],scores={};
 for(let i=1;i<=21;i++){
  watch.push({fixture_id:String(i),kickoff:new Date(now-4*3600000).toISOString()});
  if(i<21)scores[String(i)]={fixture_id:String(i),status:'FT',fulltime:{home:0,away:0},halftime:{home:0,away:0},observed_at:new Date(now-2*3600000).toISOString()};
 }
 await context.storage.put('state',{scores,watch,calls:0,watch_at:now,watch_generated_at:now});
 let requested;
 globalThis.fetch=async url=>{
  if(url.endsWith('/status'))return Response.json({response:{subscription:{active:true},requests:{current:100,limit_day:75000}}});
  requested=new URL(url).searchParams.get('ids').split('-');return Response.json({response:[]});
 };
 try{await new ScoreStore(context,env).refresh(now);assert.equal(requested[0],'21');assert.equal(requested.length,20);}
 finally{globalThis.fetch=original;}
});

test('repeated provider throttling cannot impose a thirty-minute internal pause',async()=>{
 const original=globalThis.fetch,context=ctx();
 await context.storage.put('state',{scores:{},watch:[{fixture_id:'123',kickoff:new Date(now-4*3600000).toISOString()}],
  calls:1,watch_at:now,watch_generated_at:now,day:new Date(now).toISOString().slice(0,10),
  quota:{current:100,limit:75000,checked_at:now},consecutive_errors:8,
  status:'provider_rate_limit',retry_at:now+30*60000});
 let calls=0;
 globalThis.fetch=async()=>{calls++;return Response.json({errors:{rateLimit:'temporary'}});};
 try{
  await new ScoreStore(context,env).refresh(now);
  assert.equal((await context.storage.get('state')).retry_at,now+4*60000);
  await new ScoreStore(context,env).refresh(now+4*60000);
  assert.equal(calls,1);
  assert.equal((await context.storage.get('state')).retry_at,now+8*60000);
 }finally{globalThis.fetch=original;}
});
test('backoff migration preserves a longer explicit provider Retry-After',async()=>{
 const context=ctx(),retry=now+40*60000;
 await context.storage.put('state',{scores:{},watch:[],calls:1,watch_at:now,watch_generated_at:now,
  status:'provider_rate_limit',retry_at:retry,provider_retry_after:retry});
 await new ScoreStore(context,env).refresh(now);
 assert.equal((await context.storage.get('state')).retry_at,retry);
});

test('trusted relay preserves official time, final persistence and provider backoff',async()=>{
 const context=ctx(),captured=new Date(now-60000).toISOString();
 await context.storage.put('state',{scores:{},watch:[{fixture_id:'123',kickoff:new Date(now-4*3600000).toISOString()}],
  calls:20,watch_at:now,watch_generated_at:now,day:new Date(now).toISOString().slice(0,10),
  retry_at:now+600000,status:'provider_rate_limit',minute_backoff_v2:true});
 const fixture={fixture:{id:123,status:{short:'FT'}},goals:{home:1,away:0},score:{halftime:{home:0,away:0},fulltime:{home:1,away:0}}};
 const relay={schema_version:1,source:'API-Football',captured_at:captured,fixtures:[fixture]};
 const s=new ScoreStore(context,{...env,OFFICIAL_RESULTS:{get:async()=>({size:1000,json:async()=>relay})}});
 await s.refresh(now);
 const data=await(await s.fetch(new Request('https://internal/read'))).json();
 assert.equal(data.fixtures['123'].fulltime.home,1);
 assert.equal(data.fixtures['123'].observed_at,captured);
 assert.equal(data.health.usage.worker_requests,20);
 assert.equal((await context.storage.get('final:123')).fulltime.home,1);
 const reboot=new ScoreStore(context,env);
 assert.equal((await(await reboot.fetch(new Request('https://internal/read?id=123'))).json()).fixtures['123'].fulltime.home,1);
});
test('relay refuses stale live, future timestamps, wrong source and oversized batches',()=>{
 const payload={schema_version:1,source:'API-Football',captured_at:new Date(now-6*60000).toISOString(),fixtures:[row]};
 assert.equal(relayRows(payload,now).length,0);
 assert.equal(relayRows({...payload,captured_at:new Date(now+60000).toISOString()},now).length,0);
 assert.equal(relayRows({...payload,source:'unknown'},now).length,0);
 assert.equal(relayRows({...payload,captured_at:new Date(now).toISOString(),fixtures:Array(41).fill(row)},now).length,0);
});
test('accepts both groups of relayed results while the direct provider is limited',async()=>{
 const context=ctx(),captured=new Date(now-60000).toISOString();
 const fixtures=Array.from({length:28},(_,i)=>({...row,fixture:{id:i+100,status:{short:'HT'}},
  goals:{home:1,away:1},score:{halftime:{home:1,away:1}}}));
 const watch=fixtures.map(f=>({fixture_id:String(f.fixture.id),kickoff:new Date(now-3600000).toISOString()}));
 await context.storage.put('state',{scores:{},watch,calls:20,watch_at:now,watch_generated_at:now,
  day:new Date(now).toISOString().slice(0,10),retry_at:now+240000,status:'provider_rate_limit',minute_backoff_v2:true});
 const relay={schema_version:1,source:'API-Football',captured_at:captured,fixtures};
 await new ScoreStore(context,{...env,OFFICIAL_RESULTS:{get:async()=>({size:10000,json:async()=>relay})}}).refresh(now);
 const state=await context.storage.get('state');
 assert.equal(state.relay.accepted,28);
 assert.equal(state.calls,20);
 assert.deepEqual(state.scores['127'].halftime,{home:1,away:1});
});
test('older relay cannot overwrite a newer official score',async()=>{
 const context=ctx(),old=normalize({...row,fixture:{id:123,status:{short:'FT'}},score:{fulltime:{home:2,away:0},halftime:{home:1,away:0}}},now);
 await context.storage.put('state',{scores:{123:old},watch:[{fixture_id:'123',kickoff:new Date(now-4*3600000).toISOString()}],
  calls:20,watch_at:now,watch_generated_at:now,day:new Date(now).toISOString().slice(0,10),retry_at:now+600000,status:'provider_rate_limit',minute_backoff_v2:true});
 const relay={schema_version:1,source:'API-Football',captured_at:new Date(now-60000).toISOString(),fixtures:[row]};
 await new ScoreStore(context,{...env,OFFICIAL_RESULTS:{get:async()=>({size:1000,json:async()=>relay})}}).refresh(now);
 assert.deepEqual((await context.storage.get('state')).scores['123'],old);
});
