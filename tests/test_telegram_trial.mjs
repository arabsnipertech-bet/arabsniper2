import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {telegramTrialRoute} from '../workers/stripe-access/telegram-trial.mjs';

const START=1793685000;
function setup(){
  const db=new DatabaseSync(':memory:');
  db.exec(readFileSync(new URL('../workers/stripe-access/telegram-trial-schema.sql',import.meta.url),'utf8'));
  const env={TELEGRAM_TRIAL_ENABLED:'true',TELEGRAM_BOT_TOKEN:'bot-test',
    TELEGRAM_WEBHOOK_SECRET:'webhook-secret',TELEGRAM_TRIAL_HMAC_SECRET:'hmac-secret',
    STRIPE_SECRET_KEY:'sk_test',RESEND_API_KEY:'resend-test',EMAIL_FROM:'noreply@example.org',
    ACCESS_DB:{prepare(sql){return{bind(...args){return{all:async()=>({results:db.prepare(sql).all(...args)}),
      run:async()=>db.prepare(sql).run(...args)}}}}}};
  const calls=[],outs=[];
  globalThis.fetch=async(url,options={})=>{
    const u=new URL(url);const body=options.body,fields=body instanceof URLSearchParams?body:null;
    calls.push({url:u.pathname,fields});
    if(u.hostname==='api.resend.com') {
      const message=JSON.parse(body);outs.push({type:'email',code:message.text.match(/\d{6}/)?.[0]});
      return Response.json({id:'email-example'});
    }
    if(u.hostname==='api.telegram.org') {
      outs.push({type:'telegram',message:JSON.parse(body).text});
      return Response.json({ok:true});
    }
    if(u.pathname==='/v1/customers'&&options.method==='POST')return Response.json({id:'cus_trial'});
    if(u.pathname==='/v1/customers')return Response.json({data:[],has_more:false});
    if(u.pathname==='/v1/subscriptions')return Response.json({
      id:'sub_trial',status:'trialing',trial_end:Number(fields.get('trial_end')),cancel_at:Number(fields.get('cancel_at'))
    });
    throw Error('Unexpected mock call '+url);
  };
  const request=(chatId,message,opts={})=>new Request('https://arabsniperbet.com/api/telegram-webhook',{
    method:'POST',headers:{'X-Telegram-Bot-Api-Secret-Token':opts.secret??'webhook-secret'},
    body:JSON.stringify({update_id:999,message:{
      chat:{type:opts.type??'private',id:chatId},from:{id:chatId},text:message}})});
  return {db,env,calls,outs,request};
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
