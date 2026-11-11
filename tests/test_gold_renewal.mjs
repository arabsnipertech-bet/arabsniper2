import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {createHash,createHmac} from 'node:crypto';
import worker,{PLANS} from '../workers/stripe-access/worker.mjs';

const origin='https://gold-test.example', email='member@example.test';
const clock=Math.floor(Date.now()/1000), day=86400;
const cookieName='__Host-gold_session_24h', cookie=cookieName+'='+'a'.repeat(64);
function fixture() {
 const db=new DatabaseSync(':memory:');
 for(const f of ['schema.sql','auth-schema.sql','renewal-schema.sql'])db.exec(readFileSync(new URL('../workers/stripe-access/'+f,import.meta.url),'utf8'));
 db.prepare('INSERT INTO gold_sessions VALUES (?,?,?)').run(createHash('sha256').update(cookieName+':'+'a'.repeat(64)).digest('hex'),email,clock+day);
 const env={STRIPE_MODE:'test',STRIPE_SECRET_KEY:'isolation-only',STRIPE_WEBHOOK_SECRET:'webhook-isolation-only',TEST_PRICE_MONTHLY:'price_monthly_recurring',
  ...Object.fromEntries(Object.keys(PLANS).map(p=>['TEST_PRICE_'+p.toUpperCase(),'price_'+p])),
  AUTH_ENABLED:'true',AUTH_ORIGIN:origin,OTP_SECRET:'isolation-only',RESEND_API_KEY:'isolation-only',EMAIL_FROM:'login@example.test',
  GOLD_RENEWAL_ENABLED:'true',GOLD_PROMO_ENABLED:'true',GOLD_RENEWAL_COUPON_ID:'coupon_smart',CHECKOUT_ENABLED:'true',
  GOLD_DATA:{get:async()=>({body:'{"matches":[]}'})},
  ACCESS_DB:{prepare(sql){return {bind(...args){return {all:async()=>({results:db.prepare(sql).all(...args)}),run:async()=>db.prepare(sql).run(...args)}}}}}};
 const clients=[],subs=[],invoices={},payments={},sessions=new Map(),keys=new Map(),calls=[];
 const coupon={id:'coupon_smart',valid:true,percent_off:10,duration:'once',livemode:false};
 let mail, ordinal=0, stripeCreatedAt;
 globalThis.fetch=async(input,opts={})=>{
  const u=new URL(input),p=u.pathname;
  if(u.origin==='https://api.resend.com'){mail=JSON.parse(opts.body);return Response.json({id:'simulated-email'});}
  assert.equal(u.origin,'https://api.stripe.com','All external requests are intercepted');
  calls.push([p,opts.method,opts.body]);
  if(p==='/v1/customers')return Response.json({data:u.searchParams.get('email')===email?clients:[],has_more:false});
  if(p==='/v1/subscriptions')return Response.json({data:subs,has_more:false});
  if(p.startsWith('/v1/invoices/'))return Response.json(invoices[p.split('/').at(-1)]);
  if(p.startsWith('/v1/payment_intents/'))return Response.json(payments[p.split('/').at(-1)]);
  if(p==='/v1/coupons/coupon_smart')return Response.json(coupon);
  if(p==='/v1/checkout/sessions'&&opts.method==='POST') {
   const form=opts.body,key=opts.headers['Idempotency-Key'];
   assert(key?.startsWith('gold-renewal-'));
   if(keys.has(key))return Response.json(keys.get(key));
   const plan=form.get('metadata[plan]'),p=PLANS[plan];
   assert.equal(form.get('mode'),'payment');assert.equal(form.get('line_items[0][quantity]'),'1');
   assert.equal(form.get('line_items[0][price]'),'price_'+plan);
   assert.equal(form.get('customer_email'),email);
   assert.equal(form.get('adaptive_pricing[enabled]'),'false');
   assert(!form.has('allow_promotion_codes'));assert(!form.has('payment_method_types'));
   const discount=form.has('discounts[0][coupon]')?p.amount/10:0;
   const id='cs_test_'+ ++ordinal;
   const s={id,livemode:false,mode:'payment',status:'open',payment_status:'unpaid',currency:'eur',amount_subtotal:p.amount,
    amount_total:p.amount-discount,total_details:{amount_discount:discount},created:stripeCreatedAt??Math.floor(Date.now()/1000),expires_at:Number(form.get('expires_at')),
    client_reference_id:form.get('client_reference_id'),metadata:{gold_renewal_id:form.get('metadata[gold_renewal_id]'),plan},
    url:'https://checkout.stripe.com/c/pay/'+id,line_items:{has_more:false,data:[{quantity:1,price:{id:'price_'+plan}}]}};
   sessions.set(id,s);keys.set(key,{...s});return Response.json(s);
  }
  const m=p.match(/^\/v1\/checkout\/sessions\/([^/]+)(\/expire)?$/);
  if(m&&sessions.has(m[1])) {
   const s=sessions.get(m[1]);
   if(m[2]){assert.equal(s.status,'open');s.status='expired';s.url=null;}
   return Response.json(s);
  }
  assert.fail('Unexpected simulated Stripe route: '+p);
 };
 const get=(path,session=cookie)=>worker.fetch(new Request(origin+path,{headers:session?{Cookie:session}:{}}),env);
 const post=(path,body,session=cookie,source=origin)=>worker.fetch(new Request(origin+path,{method:'POST',headers:{Origin:source,'Content-Type':'application/json','CF-Connecting-IP':'192.0.2.10',...(session?{Cookie:session}:{})},body:JSON.stringify(body)}),env);
 const renew=(plan='pass3d',use_promo=false,extra={},session=cookie,source=origin)=>post('/api/auth/renew',{plan,use_promo,...extra},session,source);
 const status=async(fresh=false)=>{const r=await get('/api/auth/session'+(fresh?'?refresh=1':''));assert.equal(r.status,200,await r.clone().text());return r.json();};
 const pass=(end,id='old',start=clock-day)=>{
  payments['pi_'+id]={status:'succeeded',latest_charge:{refunded:false,amount_refunded:0,disputed:false}};
  db.prepare('INSERT INTO passes VALUES (?,?,?,?,?,?,0)').run('cs_'+id,email,'pass3d','pi_'+id,start,end);
 };
 const trial=(end,cancel=end)=>{
  if(!clients.length)clients.push({id:'cus_member'});
  const sub={status:'trialing',trial_end:end,cancel_at:cancel,items:{data:[{price:{id:env.TEST_PRICE_MONTHLY},current_period_end:end}]},latest_invoice:'in_free'};
  subs.push(sub);invoices.in_free={status:'paid',amount_paid:0};return sub;
 };
 const webhook=async(type,obj,created=clock)=>{
  const body=JSON.stringify({type,created,livemode:false,data:{object:obj}}),time=Math.floor(Date.now()/1000);
  const sig=createHmac('sha256',env.STRIPE_WEBHOOK_SECRET).update(time+'.'+body).digest('hex');
  return worker.fetch(new Request(origin+'/api/stripe-webhook',{method:'POST',body,headers:{'Stripe-Signature':`t=${time},v1=${sig}`}}),env);
 };
 const confirm=async(s,created=s.created,kind='checkout.session.completed',expected=200)=>{
  s.status='complete';s.payment_status='paid';s.payment_intent='pi_'+s.id;
  s.customer_details={email:'different@example.test'};
  payments[s.payment_intent]={status:'succeeded',latest_charge:{refunded:false,amount_refunded:0,disputed:false}};
  const result=await webhook(kind,s,created);assert.equal(result.status,expected,await result.clone().text());
 };
 return {db,env,calls,coupon,clients,subs,invoices,payments,sessions,pass,trial,renew,status,webhook,confirm,get,post,latest:()=>[...sessions.values()].at(-1),mail:()=>mail,setStripeCreatedAt:t=>{stripeCreatedAt=t;}};
}

test('Session returns the authenticated email and a cached server expiry, never another account',async()=>{
 const f=fixture();f.pass(clock+3*day);
 const res=await f.get('/api/auth/session?email=other@example.test');
 assert.equal(res.headers.get('Cache-Control'),'no-store');
 const value=await res.json();assert.equal(value.email,email);assert.equal(value.expires_at,clock+3*day);assert(value.server_time>=clock);
 assert.equal(value.renewal.promo_eligible,false);assert.equal(value.authenticated,true);
 const count=f.calls.length;await f.status();assert.equal(f.calls.length,count,'Repeated session loads do not repeat Stripe reads');
 assert.equal((await f.get('/api/auth/session',null)).status,401);
});
test('Paid access enters the promotion at 36h and 6h, but never at expiry',async()=>{
 for(const [hours,eligible] of [[49,false],[36,true],[6,true],[0,false],[-1,false]]) {
  const f=fixture();f.pass(clock+hours*3600);
  const value=await f.status();assert.equal(value.active,hours>0);assert.equal(value.expires_at,clock+hours*3600);
  assert.equal(value.renewal.promo_eligible,eligible);
  assert.deepEqual(value.renewal.plans.map(p=>p.payable_amount),eligible?[450,1350,2610]:[500,1500,2900]);
 }
});
test('Free week and manual extension use the actual Stripe deadline, without an implicit discount',async()=>{
 const f=fixture(),free=f.trial(clock+7*day);
 assert.equal((await f.status()).expires_at,clock+7*day);
 free.trial_end=clock+36*3600;free.cancel_at=clock+40*3600;
 const updated=await f.status(true);assert.equal(updated.expires_at,clock+36*3600);assert.equal(updated.renewal.promo_eligible,false);
 assert.equal((await f.renew('pass3d',true)).status,403);
 f.env.GOLD_PROMO_ALLOW_EMAILS=JSON.stringify([email]);
 assert.equal((await f.status(true)).renewal.promo_eligible,true);
});
test('Legacy invoice grants promotion only with a positive verified Stripe payment',async()=>{
 const f=fixture(),s=f.trial(clock+36*3600);
 s.status='active';s.latest_invoice='in_paid';
 f.invoices.in_paid={status:'paid',amount_paid:2900};
 assert.equal((await f.status()).renewal.promo_eligible,false,'Manually marked paid is not proof');
 f.invoices.in_paid.payments={data:[{status:'paid',amount_paid:2900,payment:{type:'payment_intent',payment_intent:'pi_monthly'}}]};
 f.payments.pi_monthly={status:'succeeded',latest_charge:{amount_refunded:0}};
 assert.equal((await f.status(true)).renewal.promo_eligible,true);
 f.payments.pi_monthly.latest_charge.amount_refunded=100;
 assert.equal((await f.status(true)).renewal.promo_eligible,false);
});
test('Expired mailbox can authenticate, but private Gold data stays forbidden',async()=>{
 const f=fixture();f.pass(clock-1);
 const r=await f.post('/api/auth/request-code',{email},null);assert.equal(r.status,200);
 const v=await f.post('/api/auth/verify-code',{email,challenge:(await r.json()).challenge,code:f.mail().text.match(/\d{6}/)[0]},null);
 assert.equal(v.status,200);const data=await v.json();assert.equal(data.active,false);assert.equal(data.authenticated,true);
 const session=v.headers.get('Set-Cookie');assert(session.includes('HttpOnly; SameSite=Strict'));
 assert.equal((await f.get('/api/gold-data/arab_quote_day1.json',session)).status,403);
 assert.equal((await f.renew('pass14d',false,{},session)).status,200);
});
test('Checkout uses the OTP account, never a client email, price or expiry',async()=>{
 const f=fixture();f.pass(clock+3*day);
 assert.equal((await f.renew('pass3d',false,{},null)).status,401);
 assert.equal((await f.renew('pass3d',false,{},cookie,'https://another.example')).status,403);
 assert.equal((await f.renew('pass3d',true,{expires_at:clock+3600})).status,403);
 assert.equal((await f.renew('pass3d',false,{email:'victim@example.test',amount:1,price:'other',expires_at:0})).status,200);
 const row=f.db.prepare('SELECT * FROM gold_renewals').get();assert.equal(row.email,email);assert.equal(row.amount,500);
 assert(row.expires_at>=clock+3600);assert.equal(row.expires_at,row.created_at+3600);
});
test('Early paid renewal stacks from the real expiry, including an admin trial extension',async()=>{
 const f=fixture();const end=clock+4*day;f.trial(end);
 assert.equal((await f.renew('pass14d')).status,200);
 const s=f.latest();await f.confirm(s);
 const row=f.db.prepare('SELECT * FROM passes').get();
 assert.equal(row.starts_at,end);assert.equal(row.expires_at,end+14*day);assert.equal(row.email,email);
 assert.equal((await f.status()).expires_at,end+14*day);
 assert.equal((await f.get('/api/gold-data/arab_quote_day1.json')).status,200);
});
test('Late paid renewal starts at confirmed payment time and preserves calendar semantics',async()=>{
 const f=fixture();f.pass(clock-1);
 assert.equal((await f.renew('pass1m')).status,200);
 const s=f.latest(),paid=s.created+20;await f.confirm(s,paid);
 const row=f.db.prepare('SELECT * FROM passes WHERE checkout_id=?').get(s.id);
 assert.equal(row.starts_at,paid);
 const local=new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Rome',dateStyle:'short',timeStyle:'short'});
 assert.equal(local.format(row.expires_at*1000).split(',').at(-1),local.format(paid*1000).split(',').at(-1));
 assert(row.expires_at>paid+27*day&&row.expires_at<paid+32*day);
});
test('A successful browser return and unpaid events do not grant anything',async()=>{
 const f=fixture();assert.equal((await f.renew()).status,200);
 assert.equal((await f.get('/api/auth/session?renewal=success')).status,200);
 const s=f.latest();s.status='complete';s.payment_intent='pi_pending';
 await f.webhook('checkout.session.completed',s);
 assert.equal(f.db.prepare('SELECT count(*) n FROM passes').get().n,0);
 assert.equal((await f.status()).active,false);
 f.payments.pi_pending={status:'requires_payment_method'};
 await f.webhook('checkout.session.async_payment_failed',s);
 assert.equal(f.db.prepare('SELECT status FROM gold_renewals').get().status,'expired');
 assert.equal((await f.renew()).status,200);
});
test('An isolated future pass does not postpone a paid renewal after expiry',async()=>{
 const f=fixture();f.pass(clock+5*day,'future',clock+day);
 assert.equal((await f.status()).active,false);
 assert.equal((await f.renew()).status,200);
 const s=f.latest();await f.confirm(s);
 assert.equal(f.db.prepare('SELECT starts_at FROM passes WHERE checkout_id=?').get(s.id).starts_at,s.created);
});
test('All three discount totals, a one-hour Checkout and a single redemption even with concurrent requests',async()=>{
 for(const [plan,amount] of [['pass3d',450],['pass14d',1350],['pass1m',2610]]) {
  const f=fixture();f.pass(clock+600);
  const [a,b]=await Promise.all([f.renew(plan,true),f.renew(plan,true)]);
  assert.equal(a.status,200,await a.clone().text());assert.equal(b.status,200,await b.clone().text());
  assert.equal((await a.json()).url,(await b.json()).url);assert.equal(f.sessions.size,1);
  const s=f.latest();assert.equal(s.amount_total,amount);assert.equal(s.expires_at,f.db.prepare('SELECT created_at FROM gold_renewals').get().created_at+3600);
  const reserved=await f.status();assert.equal(reserved.renewal.promo_eligible,false);assert.equal(reserved.renewal.pending_checkout.url,s.url);
  await Promise.all([f.confirm(s),f.confirm(s,s.created+1,'checkout.session.async_payment_succeeded')]);
  const rows=f.db.prepare('SELECT * FROM gold_renewals').all();assert.equal(rows.length,1);assert.equal(rows[0].status,'fulfilled');assert.equal(rows[0].promo,1);
  assert.equal(f.db.prepare('SELECT count(*) n FROM passes WHERE checkout_id=?').get(s.id).n,1);
  await f.webhook('charge.refunded',{payment_intent:s.payment_intent});
  assert.equal((await f.status()).renewal.promo_eligible,false);
  assert.equal((await f.renew(plan,true)).status,409,'Refund does not reset a consumed promotion');
 }
});
test('Checkout cannot present a discount when Stripe creates it after the access deadline',async()=>{
 const f=fixture();const expiry=clock+600;f.pass(expiry);f.setStripeCreatedAt(expiry);
 assert.equal((await f.renew('pass3d',true)).status,409);
 assert.equal(f.latest().status,'expired');
 assert.equal(f.db.prepare('SELECT status FROM gold_renewals').get().status,'expired');
});
test('Coupon must be valid and exactly 10%; standard Checkout remains available',async()=>{
 const f=fixture();f.pass(clock+36*3600);f.coupon.percent_off=20;
 assert.equal((await f.renew('pass3d',true)).status,503);assert.equal(f.sessions.size,0);
 assert.equal((await f.renew()).status,200);assert.equal(f.latest().amount_total,500);
});
test('Verified expiration releases a reservation without granting access',async()=>{
 const f=fixture();f.pass(clock+36*3600);
 assert.equal((await f.renew('pass3d',true)).status,200);
 const s=f.latest();s.status='expired';s.url=null;
 assert.equal((await f.webhook('checkout.session.expired',s)).status,200);
 assert.equal(f.db.prepare('SELECT status FROM gold_renewals').get().status,'expired');
 assert.equal((await f.status()).renewal.promo_eligible,true);
 assert.equal((await f.renew('pass14d',true)).status,200);
 assert.equal(f.db.prepare('SELECT count(*) n FROM passes').get().n,1);
});
test('No access for a wrong discount or an unsigned Stripe event',async()=>{
 const f=fixture();f.pass(clock+36*3600);await f.renew('pass3d',true);
 const s=f.latest();s.amount_total=499;
 await f.confirm(s,s.created,'checkout.session.completed',503);
 assert.equal(f.db.prepare('SELECT count(*) n FROM passes').get().n,1);
 const r=await worker.fetch(new Request(origin+'/api/stripe-webhook',{method:'POST',body:'{}'}),f.env);
 assert.equal(r.status,400);
});
