import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {authRoute,cleanupAuth} from '../workers/stripe-access/auth.mjs';
import worker,{access as realAccess} from '../workers/stripe-access/worker.mjs';
const now=1800000000,origin='https://arabsniperbet.com';
function setup(){
 const db=new DatabaseSync(':memory:');db.exec(readFileSync(new URL('../workers/stripe-access/auth-schema.sql',import.meta.url),'utf8'));
 const env={AUTH_ENABLED:'true',AUTH_ORIGIN:origin,ACCESS_DB:{prepare(sql){return {bind(...args){return {all:async()=>({results:db.prepare(sql).all(...args)}),run:async()=>db.prepare(sql).run(...args)}}}}},OTP_SECRET:'a'.repeat(64),RESEND_API_KEY:'test',EMAIL_FROM:'Gold <login@example.com>',GOLD_DATA:{get:async()=>({body:'{"matches":[]}'})}};
 let sent,active=true;globalThis.fetch=async(url,options)=>{assert.equal(url,'https://api.resend.com/emails');sent=JSON.parse(options.body);return Response.json({id:'test'});};
 const access=async(e,email)=>({active,source:'stripe_subscription'});
 function req(path,body,cookie,headers={}){return new Request(origin+path,{method:body?'POST':'GET',headers:{Origin:origin,'Content-Type':'application/json','CF-Connecting-IP':'192.0.2.1',...(cookie?{Cookie:cookie}:{}),...headers},...(body?{body:JSON.stringify(body)}:{})});}
 const call=(path,body,cookie,t=now,headers={})=>authRoute(req(path,body,cookie,headers),env,access,t);
 async function issue(email='user@example.com',t=now){const r=await call('/api/auth/request-code',{email},null,t);assert.equal(r.status,200);return {email,challenge:(await r.json()).challenge,code:sent.text.match(/\d{6}/)[0]};}
 return {db,env,call,issue,disable:()=>active=false};
}
test('OTP verifies mailbox, sets protected cookie and cannot be replayed concurrently',async()=>{
 const s=setup(),body=await s.issue();const rows=s.db.prepare('SELECT * FROM gold_codes').all();assert(!JSON.stringify(rows).includes(body.code));
 const results=await Promise.all([s.call('/api/auth/verify-code',body),s.call('/api/auth/verify-code',body)]);
 assert.deepEqual(results.map(r=>r.status).sort(),[200,400]);
 const cookie=results.find(r=>r.status===200).headers.get('Set-Cookie');assert(cookie.includes('HttpOnly; SameSite=Strict'));assert(cookie.includes('Secure'));
 assert.equal((await s.call('/api/auth/session',null,cookie)).status,200);
 assert.equal((await s.call('/api/gold-data/arab_quote_day1.json',null,cookie)).status,200);
 s.disable();assert.equal((await s.call('/api/gold-data/arab_quote_day1.json',null,cookie)).status,403);
 await s.call('/api/auth/logout',{},cookie);assert.equal((await s.call('/api/auth/session',null,cookie)).status,401);
});
test('Five incorrect attempts lock the code and expiry is exact',async()=>{
 const s=setup(),b=await s.issue();const bad=b.code==='000000'?'000001':'000000';
 for(let i=0;i<5;i++)assert.equal((await s.call('/api/auth/verify-code',{...b,code:bad})).status,400);
 assert.equal((await s.call('/api/auth/verify-code',b)).status,400);
 const next=await s.issue('other@example.com');assert.equal((await s.call('/api/auth/verify-code',next,null,now+600)).status,400);
});
test('Resend cooldown, replacement, origins, and no subscription bypass',async()=>{
 const s=setup(),old=await s.issue();assert.equal((await s.call('/api/auth/request-code',{email:old.email})).status,429);
 const fresh=await s.issue(old.email,now+61);assert.equal((await s.call('/api/auth/verify-code',old,null,now+62)).status,400);
 assert.equal((await s.call('/api/auth/verify-code',fresh,null,now+62,{Origin:'https://evil.example'})).status,403);
 s.disable();assert.equal((await s.call('/api/auth/verify-code',fresh,null,now+62)).status,403);
 assert.equal((await s.call('/api/gold-data/arab_quote_day1.json')).status,401);
});
test('Session expiry and cleanup; missing config fails closed',async()=>{
 const s=setup(),b=await s.issue(),r=await s.call('/api/auth/verify-code',b),c=r.headers.get('Set-Cookie');
 assert.equal((await s.call('/api/auth/session',null,c,now+86400)).status,401);
 await cleanupAuth(s.env,now+86400);assert.equal(s.db.prepare('SELECT count(*) n FROM gold_sessions').get().n,0);
 delete s.env.RESEND_API_KEY;assert.equal((await s.call('/api/auth/request-code',{email:b.email})).status,503);
});
test('Legacy email endpoint cannot grant even VIP when OTP enabled',async()=>{
 const s=setup();s.env.STRIPE_MODE='live';
 const r=await worker.fetch(new Request(origin+'/?email=bruno.pirolo@gmail.com'),s.env);assert.equal(r.status,401);assert.equal((await r.json()).active,false);
});
test('24-hour expiry is absolute on session and private feeds, including VIP',async()=>{
 const s=setup(),b=await s.issue('bruno.pirolo@gmail.com');
 const r=await s.call('/api/auth/verify-code',b),cookie=r.headers.get('Set-Cookie');
 assert.match(cookie,/Max-Age=86400(?:;|$)/);
 assert.equal((await r.json()).session_expires_at,now+86400);
 for(const t of [now+1,now+43200,now+86399]) {
  const response=await s.call('/api/auth/session',null,cookie,t);
  assert.equal(response.status,200);
  assert.equal((await response.json()).session_expires_at,now+86400);
  assert.equal(response.headers.get('Set-Cookie'),null);
 }
 const req=path=>new Request(origin+path,{headers:{Cookie:cookie}});
 for(const path of ['/api/auth/session','/api/gold-data/arab_quote_day1.json']) {
  assert.equal((await authRoute(req(path),s.env,realAccess,now+86399)).status,200);
  assert.equal((await authRoute(req(path),s.env,realAccess,now+86400)).status,401);
 }
 assert.equal(s.db.prepare('SELECT expires_at FROM gold_sessions').get().expires_at,now+86400);
});
test('Legacy seven-day token cannot be reused or renamed to the new cookie',async()=>{
 const s=setup(),token='a'.repeat(64);
 const digest=Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(token))).toString('hex');
 s.db.prepare('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)').run(digest,'bruno.pirolo@gmail.com',now+604800);
 for(const name of ['__Host-gold_session','__Host-gold_session_24h']) {
  const cookie=name+'='+token;
  assert.equal((await s.call('/api/auth/session',null,cookie)).status,401);
  assert.equal((await s.call('/api/gold-data/arab_quote_day1.json',null,cookie)).status,401);
 }
});
test('A fresh code after expiry starts a new independent 24-hour session',async()=>{
 const s=setup(),first=await s.call('/api/auth/verify-code',await s.issue());
 const oldCookie=first.headers.get('Set-Cookie'),t=now+86400;
 const body=await s.issue('user@example.com',t);
 const next=await s.call('/api/auth/verify-code',body,null,t);
 assert.equal((await next.json()).session_expires_at,t+86400);
 assert.equal((await s.call('/api/auth/session',null,oldCookie,t)).status,401);
 assert.equal((await s.call('/api/auth/session',null,next.headers.get('Set-Cookie'),t)).status,200);
});

