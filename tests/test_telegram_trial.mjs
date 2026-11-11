import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {telegramTrialRoute} from '../workers/stripe-access/telegram-trial.mjs';

async function claimCampaign(s,access=async()=>({active:false}),chat=42,email='person@example.com') {
  const check=msg=>telegramTrialRoute(s.request(chat,msg),s.env,access,'price_monthly',START);
  await check(email);
  const code=s.outs.filter(x=>x.type==='email').at(-1)?.code;
  if(code)await check(code);
  return check;
}

const START=1793685000;
function setup(campaign=false){
  const db=new DatabaseSync(':memory:');
  db.exec(readFileSync(new URL('../workers/stripe-access/telegram-trial-schema.sql',import.meta.url),'utf8'));
  db.exec(readFileSync(new URL('../workers/stripe-access/telegram-campaign-schema.sql',import.meta.url),'utf8'));
  const env={TELEGRAM_TRIAL_ENABLED:'true',TELEGRAM_BOT_TOKEN:'bot-test',
    TELEGRAM_WEBHOOK_SECRET:'webhook-secret',TELEGRAM_TRIAL_HMAC_SECRET:'hmac-secret',
    STRIPE_SECRET_KEY:'sk_test',RESEND_API_KEY:'resend-test',EMAIL_FROM:'noreply@example.org',
    ACCESS_DB:{prepare(sql){return{bind(...args){return{all:async()=>({results:db.prepare(sql).all(...args)}),
      run:async()=>db.prepare(sql).run(...args)}}}}}};
  env.TELEGRAM_TRIAL_CAMPAIGN=campaign?'gold-engine-2026-11':'disabled';
  const calls=[],outs=[],subs=[],customers=[],replays=new Map();
  globalThis.fetch=async(url,options={})=>{
    const u=new URL(url);const body=options.body,fields=body instanceof URLSearchParams?body:null;
    calls.push({url:u.pathname,fields,method:options.method});
    if(u.hostname==='api.resend.com') {
      const message=JSON.parse(body);outs.push({type:'email',code:message.text.match(/\d{6}/)?.[0]});
      return Response.json({id:'email-example'});
    }
    if(u.hostname==='api.telegram.org') {
      outs.push({type:'telegram',message:JSON.parse(body).text});
      return Response.json({ok:true});
    }
    if(u.pathname==='/v1/customers'&&options.method==='POST') {
      if(!customers.length)customers.push({id:'cus_trial',email:fields.get('email')});
      return Response.json(customers[0]);
    }
    if(u.pathname==='/v1/customers')return Response.json({data:customers,has_more:false});
    if(u.pathname==='/v1/subscriptions'&&options.method==='GET')return Response.json({data:subs,has_more:false});
    if(u.pathname.startsWith('/v1/subscriptions/')&&options.method==='GET')return Response.json(subs.find(s=>s.id===u.pathname.split('/').at(-1)));
    if(u.pathname.startsWith('/v1/subscriptions')&&options.method==='POST') {
      const key=options.headers['Idempotency-Key'];
      if(replays.has(key))return Response.json(replays.get(key));
      let sub=u.pathname==='/v1/subscriptions'?null:subs.find(s=>s.id===u.pathname.split('/').at(-1));
      if(!sub){sub={id:'sub_trial',status:'trialing'};subs.push(sub);}
      Object.assign(sub,{trial_end:Number(fields.get('trial_end')),cancel_at:Number(fields.get('cancel_at')),
        metadata:{gold_trial_campaign:fields.get('metadata[gold_trial_campaign]'),gold_trial_claim:fields.get('metadata[gold_trial_claim]')}});
      replays.set(key,structuredClone(sub));
      return Response.json(sub);
    }
    throw Error('Unexpected mock call '+url);
  };
  const request=(chatId,message,opts={})=>new Request('https://arabsniperbet.com/api/telegram-webhook',{
    method:'POST',headers:{'X-Telegram-Bot-Api-Secret-Token':opts.secret??'webhook-secret'},
    body:JSON.stringify({update_id:999,message:{
      chat:{type:opts.type??'private',id:chatId},from:{id:chatId},text:message}})});
  return {db,env,calls,outs,request,subs,customers};
}
test('Only authentic private updates and the enabled feature can start trials',async()=>{
  const s=setup(),check=async(req)=>telegramTrialRoute(req,s.env,async()=>({active:false}),'price_monthly',START);
  assert.equal((await check(s.request(42,'/prova',{secret:'wrong'}))).status,403);
  assert.equal((await check(s.request(42,'/prova',{type:'channel'}))).status,200);
  assert.equal(s.outs.length,0);
  s.env.TELEGRAM_TRIAL_ENABLED='false';
  assert.equal((await check(s.request(42,'/prova'))).status,503);
});
test('Verified email issues one 7-day trial, never configures recurring renewal',async()=>{
  const s=setup(),check=async(req)=>telegramTrialRoute(req,s.env,async()=>({active:false}),'price_monthly',START);
  assert.equal((await check(s.request(42,'/prova'))).status,200);
  assert.equal((await check(s.request(42,'person@example.com'))).status,200);
  const code=s.outs.find(x=>x.type==='email').code;
  assert.match(code,/^\d{6}$/);
  assert.equal((await check(s.request(42,code))).status,200);
  const sub=s.calls.find(x=>x.url==='/v1/subscriptions');
  assert(sub);
  assert.equal(sub.fields.get('items[0][price]'),'price_monthly');
  assert.equal(Number(sub.fields.get('trial_end')),START+7*86400);
  assert.equal(sub.fields.get('cancel_at'),sub.fields.get('trial_end'));
  assert.equal(sub.fields.get('trial_settings[end_behavior][missing_payment_method]'),'cancel');
  assert.equal(sub.fields.has('default_payment_method'),false);
  assert.equal(s.db.prepare('SELECT status FROM telegram_trial_claims').get().status,'granted');
  assert.match(s.outs.at(-1).message,/7 giorni/);
  assert.equal((await check(s.request(42,'person@example.com'))).status,200);
  assert.equal(s.calls.filter(x=>x.url==='/v1/subscriptions').length,1);
});
test('Wrong OTP does not grant; a different Telegram ID cannot reuse claimed email',async()=>{
  const s=setup(),check=async(req)=>telegramTrialRoute(req,s.env,async()=>({active:false}),'price_monthly',START);
  await check(s.request(1,'person@example.com'));
  const code=s.outs.find(x=>x.type==='email').code;
  await check(s.request(1,code==='000000'?'999999':'000000'));
  assert.equal(s.calls.filter(x=>x.url==='/v1/subscriptions').length,0);
  await check(s.request(1,code));
  await check(s.request(2,'person@example.com'));
  assert.equal(s.calls.filter(x=>x.url==='/v1/subscriptions').length,1);
  assert.equal(s.db.prepare('SELECT COUNT(*) AS n FROM telegram_trial_claims').get().n,1);
});

test('Unexpected Stripe renewal state triggers immediate cancellation and never grants',async()=>{
  const s=setup();
  const original=globalThis.fetch;
  const canceled=[];
  globalThis.fetch=async(url,opts={})=>{
    const path=new URL(url).pathname;
    if(path==='/v1/subscriptions'&&opts.method==='POST')
      return Response.json({id:'sub_unsafe',status:'trialing',
        trial_end:Number(opts.body.get('trial_end')),cancel_at:null});
    if(path==='/v1/subscriptions/sub_unsafe'&&opts.method==='DELETE') {
      canceled.push(path);return Response.json({id:'sub_unsafe',status:'canceled'});
    }
    return original(url,opts);
  };
  const check=async(request)=>telegramTrialRoute(request,s.env,async()=>({active:false}),'price_monthly',START);
  await check(s.request(44,'person@example.com'));
  const code=s.outs.find(x=>x.type==='email').code;
  await check(s.request(44,code));
  assert.equal(canceled.length,1);
  assert.equal(s.db.prepare('SELECT status FROM telegram_trial_claims').get().status,'reserved');
  assert.match(s.outs.at(-1).message,/Riprova/);
});

test('Campaign grants returning users once and preserves original trial history',async()=>{
  const s=setup(true);
  s.db.prepare('INSERT INTO telegram_trial_claims VALUES (?,?,?,?,?,?)').run('person@example.com','42',START-700000,START-95200,'granted','sub_old');
  const check=await claimCampaign(s);
  const c=s.db.prepare('SELECT * FROM telegram_campaign_claims').get();
  assert.equal(c.status,'granted');assert.equal(c.expires_at,START+604800);
  assert.equal(s.db.prepare('SELECT subscription_id FROM telegram_trial_claims').get().subscription_id,'sub_old');
  await check('person@example.com');
  await telegramTrialRoute(s.request(43,'person@example.com'),s.env,async()=>({active:false}),'price_monthly',START);
  await check('other@example.com');
  assert.equal(s.subs.length,1);
  assert.equal(s.db.prepare('SELECT COUNT(*) n FROM telegram_campaign_claims').get().n,1);
});

test('Active trial is extended exactly seven days without another subscription or proration',async()=>{
  const s=setup(true),end=START+30*86400;
  s.customers.push({id:'cus_existing',email:'person@example.com'});
  s.subs.push({id:'sub_existing',status:'trialing',trial_end:end,cancel_at:end,items:{data:[{price:{id:'price_monthly'}}]}});
  await claimCampaign(s,async(_env,_email,t)=>t<end?{active:true,expires_at:end}:{active:false});
  assert.equal(s.subs.length,1);assert.equal(s.subs[0].trial_end,end+604800);
  assert.equal(s.subs[0].cancel_at,end+604800);
  const write=s.calls.find(c=>c.url==='/v1/subscriptions/sub_existing'&&c.method==='POST');
  assert.equal(write.fields.get('proration_behavior'),'none');
  assert.equal(s.calls.filter(c=>c.url==='/v1/subscriptions'&&c.method==='POST').length,0);
});

test('Contiguous paid coverage is preserved and does not change an active paid subscription',async()=>{
  const s=setup(true),first=START+86400,last=START+14*86400;
  s.customers.push({id:'cus_paid',email:'person@example.com'});
  s.subs.push({id:'sub_paid',status:'active',items:{data:[{price:{id:'price_monthly'}}]}});
  await claimCampaign(s,async(_env,_email,t)=>t<first?{active:true,expires_at:first}:t<last?{active:true,expires_at:last}:{active:false});
  assert.equal(s.db.prepare('SELECT expires_at FROM telegram_campaign_claims').get().expires_at,last+604800);
  assert.equal(s.calls.some(c=>c.url==='/v1/subscriptions/sub_paid'&&c.method==='POST'),false);
  assert.equal(s.subs.find(s=>s.id==='sub_trial').cancel_at,last+604800);
});

test('Concurrent verified webhook deliveries grant only one week',async()=>{
  const s=setup(true),check=msg=>telegramTrialRoute(s.request(42,msg),s.env,async()=>({active:false}),'price_monthly',START);
  await check('person@example.com');const code=s.outs.find(x=>x.type==='email').code;
  await Promise.all([check(code),check(code)]);
  assert.equal(s.subs.length,1);
  assert.equal(s.db.prepare('SELECT expires_at FROM telegram_campaign_claims').get().expires_at,START+604800);
});

test('A remote success followed by D1 failure is recovered without another grant',async()=>{
  const s=setup(true),original=s.env.ACCESS_DB.prepare;
  let fail=true;
  s.env.ACCESS_DB.prepare=sql=>{
    const stmt=original(sql);
    if(sql.includes("SET status='granted'")&&sql.includes('telegram_campaign_claims')) {
      return {bind(...args){const bound=stmt.bind(...args);return {...bound,run:async()=>{if(fail){fail=false;throw Error('temporary D1 failure');}return bound.run();}}}};
    }
    return stmt;
  };
  const check=await claimCampaign(s);
  assert.equal(s.db.prepare('SELECT status FROM telegram_campaign_claims').get().status,'reserved');
  await check(s.outs.find(x=>x.type==='email').code);
  assert.equal(s.db.prepare('SELECT status FROM telegram_campaign_claims').get().status,'granted');
  assert.equal(s.subs.length,1);
  assert.equal(s.calls.filter(c=>c.url==='/v1/subscriptions'&&c.method==='POST').length,1);
});

test('Campaign requires its own OTP and never modifies unlimited access',async()=>{
  const s=setup(false),check=msg=>telegramTrialRoute(s.request(42,msg),s.env,async()=>({active:false}),'price_monthly',START);
  await check('person@example.com');const old=s.outs.find(x=>x.type==='email').code;
  s.env.TELEGRAM_TRIAL_CAMPAIGN='gold-engine-2026-11';await check(old);
  assert.equal(s.subs.length,0);
  const vip=setup(true);await claimCampaign(vip,async()=>({active:true,source:'vip'}));
  assert.equal(vip.subs.length,0);assert.match(vip.outs.at(-1).message,/senza scadenza/);
});
