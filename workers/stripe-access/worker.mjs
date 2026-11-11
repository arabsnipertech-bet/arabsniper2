import {authRoute, cleanupAuth} from './auth.mjs';
import {renewalEnabled, cachedAccess, invalidateAccessCache, renewalStatus, renewalCheckout, renewalOrder, markRenewalPaid, closeFailedRenewal} from './renewal.mjs';
import {telegramTrialRoute} from './telegram-trial.mjs';
import {goldTestLoginPage} from './test-login.mjs';
// OTP remains disabled until its staged deployment requirements are satisfied.
const API_VERSION = '2026-08-26.dahlia';
const ORIGINS = new Set(['https://arabsniperbet.com','https://www.arabsniperbet.com']);
const OLD_PLANS = {
  daily: {price:'price_1UTM5fH763HaPm5Ufge4HJC5', amount:500, seconds:86400},
  weekly:{price:'price_1UTM5mH763HaPm5UwzU6S0LG', amount:1500, seconds:604800},
  monthly:{price:'price_1UTM5yH763HaPm5Uo2hm0Ped', amount:2900}
};
export const PLANS = {
  pass3d: {price:'price_1UY2SPH763HaPm5UMlHO3hoT', amount:500, seconds:3*86400},
  pass14d:{price:'price_1UY2SQH763HaPm5UISmHmepS', amount:1500, seconds:14*86400},
  pass1m: {price:'price_1UY2SRH763HaPm5UM2R6CHwa', amount:2900}
};
function subscriptionPrice(env) {
  return env.STRIPE_MODE==='test' ? env.TEST_PRICE_MONTHLY : OLD_PLANS.monthly.price;
}
const LEGACY_MONTHLY = new Set(['price_1T8fx8H763HaPm5UVjZgaawF','price_1T6ri6H763HaPm5UiG9xQY5F']);
const VIPS = new Set(['bruno.pirolo@gmail.com']);
const normalize = s => String(s||'').trim().toLowerCase();
const id = o => typeof o === 'string' ? o : o?.id;
function plans(env, catalog=PLANS) {
  if (env.STRIPE_MODE === 'test') {
    return Object.fromEntries(Object.entries(catalog).map(([k,v])=>[k,{...v,price:env['TEST_PRICE_'+k.toUpperCase()]}]));
  }
  return catalog;
}
async function stripe(env, path, params, method='GET', key) {
  const headers = {'Authorization':`Bearer ${env.STRIPE_SECRET_KEY}`, 'Stripe-Version':API_VERSION};
  let url='https://api.stripe.com/v1'+path, body;
  if(params && method==='GET')url+='?'+new URLSearchParams(params);
  else if(params){headers['Content-Type']='application/x-www-form-urlencoded';body=new URLSearchParams(params);}
  if(key)headers['Idempotency-Key']=key;
  const res=await fetch(url,{method,headers,body,signal:AbortSignal.timeout(12000)});
  if(!res.ok)throw new Error('stripe_unavailable');
  return res.json();
}
async function all(env,path,params={}) {
  const rows=[];let after;
  for(let n=0;n<20;n++) {
    const page=await stripe(env,path,{...params,limit:'100',...(after?{starting_after:after}:{})});
    if(!Array.isArray(page.data))throw new Error('invalid_stripe_response');
    rows.push(...page.data);
    if(!page.has_more)return rows;
    if(!page.data.length)throw new Error('invalid_pagination');
    after=page.data.at(-1).id;
  }
  throw new Error('pagination_limit');
}
export async function verifySignature(raw, signature, secret, now=Date.now()/1000) {
  if(!secret||!signature)return false;
  const fields=signature.split(',').map(v=>v.trim().split('='));
  const timestamps=fields.filter(([k])=>k==='t');
  if(timestamps.length!==1||!/^\d+$/.test(timestamps[0][1]))return false;
  const t=timestamps[0][1];if(Math.abs(now-Number(t))>300)return false;
  const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(secret),{name:'HMAC',hash:'SHA-256'},false,['verify']);
  for(const [scheme,value] of fields) {
    if(scheme!=='v1'||!/^([a-f0-9]{2}){32}$/i.test(value))continue;
    const bytes=Uint8Array.from(value.match(/../g),v=>parseInt(v,16));
    if(await crypto.subtle.verify('HMAC',key,bytes,new TextEncoder().encode(t+'.'+raw)))return true;
  }
  return false;
}
// Translate the calendar date in Italy, preserving local time across daylight-saving changes.
const romeOffset=new Intl.DateTimeFormat('en',{timeZone:'Europe/Rome',timeZoneName:'shortOffset'});
function offsetAt(ms) {
  const zone=romeOffset.formatToParts(ms).find(part=>part.type==='timeZoneName').value;
  return Number(zone.replace('GMT',''))*3600000;
}
function monthEnd(start) {
  const instant=start*1000;
  const local=new Date(instant+offsetAt(instant));
  const day=local.getUTCDate();
  local.setUTCDate(1);
  local.setUTCMonth(local.getUTCMonth()+2,0);
  local.setUTCDate(Math.min(day,local.getUTCDate()));
  const target=local.getTime();
  // Pick the earliest matching instant for an ambiguous hour and move forward across a gap.
  const early=target-offsetAt(target+86400000), late=target-offsetAt(target-86400000);
  const candidates=[early,late].sort((a,b)=>a-b);
  return (candidates.find(t=>t+offsetAt(t)===target) ?? candidates[1])/1000;
}
async function paymentCharge(env,paymentIntent) {
  const pi=await stripe(env,'/payment_intents/'+encodeURIComponent(paymentIntent),{'expand[]':'latest_charge'});
  let charge=pi.latest_charge;
  if(typeof charge==='string')charge=await stripe(env,'/charges/'+encodeURIComponent(charge));
  return {pi,charge};
}
const revokedCharge = c => c.refunded||c.amount_refunded>0||c.disputed;
async function verifiedPayment(env,paymentIntent) {
  if(!paymentIntent)return false;
  const {pi,charge}=await paymentCharge(env,paymentIntent);
  return pi.status==='succeeded'&&!!charge&&!revokedCharge(charge);
}
export async function fulfill(env,event) {
  const ref=event.data.object;
  if(['charge.refunded','charge.dispute.created'].includes(event.type)) {
    let pi=id(ref.payment_intent);
    if(!pi&&ref.charge)pi=id((await stripe(env,'/charges/'+encodeURIComponent(id(ref.charge)))).payment_intent);
    if(pi) {
      const affected=await env.ACCESS_DB.prepare('SELECT DISTINCT email FROM passes WHERE payment_intent=?').bind(pi).all();
      await env.ACCESS_DB.prepare('UPDATE passes SET revoked=1 WHERE payment_intent=?').bind(pi).run();
      for(const row of affected.results||[])await invalidateAccessCache(env,row.email);
    }
    return;
  }
  if(['checkout.session.expired','checkout.session.async_payment_failed'].includes(event.type)) {
    if(!ref.metadata?.gold_renewal_id)return;
    const session=await stripe(env,'/checkout/sessions/'+encodeURIComponent(ref.id));
    if(session.payment_status!=='unpaid')return;
    if(event.type==='checkout.session.expired'&&session.status!=='expired')return;
    if(event.type==='checkout.session.async_payment_failed') {
      const piId=id(session.payment_intent);
      if(session.status!=='complete'||!piId)return;
      const pi=await stripe(env,'/payment_intents/'+encodeURIComponent(piId));
      if(!['canceled','requires_payment_method'].includes(pi.status))return;
    }
    await closeFailedRenewal(env,session);
    return;
  }
  if(!['checkout.session.completed','checkout.session.async_payment_succeeded'].includes(event.type))return;
  if(event.type==='checkout.session.completed'&&ref.payment_status!=='paid')return;
  const s=await stripe(env,'/checkout/sessions/'+encodeURIComponent(ref.id),{'expand[]':'line_items'});
  if(s.mode!=='payment'||s.payment_status!=='paid'||s.status!=='complete')return;
  if(s.livemode !== (env.STRIPE_MODE==='live'))throw new Error('mode_mismatch');
  const rows=s.line_items?.data||[];
  if(rows.length!==1||s.line_items.has_more||rows[0].quantity!==1)return;
  const priceId=id(rows[0].price);
  if(!priceId)return;
  const entry=Object.entries({...plans(env),...plans(env,OLD_PLANS)}).find(([,p])=>p.price===priceId);
  if(!entry)return;
  const [plan,p]=entry;
  const order=await renewalOrder(env,s);
  if(order&&(order.plan!==plan||order.status==='expired'))throw new Error('invalid_renewal_plan');
  const expected=order?order.amount:p.amount;
  if(order&&(s.currency!=='eur'||s.amount_total!==expected||expected!==(order.promo?Math.round(p.amount*.9):p.amount)||s.amount_subtotal!==p.amount||s.total_details?.amount_discount!==p.amount-expected))throw new Error('invalid_renewal_amount');
  if(!order&&(s.currency!=='eur'||s.amount_total!==p.amount))return;
  const email=order?order.email:normalize(s.customer_details?.email);
  if(!email||!id(s.payment_intent))throw new Error('missing_customer');
  // Fetch authoritative charge state too: late/retried success must not restore refunded access.
  const {pi,charge}=await paymentCharge(env,id(s.payment_intent));
  if(pi.status!=='succeeded')return;
  if(!charge)throw new Error('charge_unavailable');
  const revoked=revokedCharge(charge)?1:0;
  const paidAt=Number(event.created);
  if(!Number.isFinite(paidAt)||paidAt<Number(s.created))throw new Error('invalid_payment_time');
  if(plan==='monthly')return;
  const isNew=Object.hasOwn(PLANS,plan);
  const previous=isNew ? await subscriptionAccess(env,email,paidAt) : null;
  const earliest=Math.max(paidAt,previous?.expires_at||0);
  // Compare and insert in one statement: a concurrent purchase forces a fresh calculation.
  // Ignore an isolated future pass when the account is already expired (for example after
  // a refund). Preserve active and contiguous time, including a payment delivered out of order.
  const anchor=order?`WITH RECURSIVE coverage(t) AS (
      SELECT ?1 UNION SELECT p.expires_at FROM coverage JOIN passes p
      ON p.email=?2 AND p.revoked=0 AND p.expires_at>t AND (p.starts_at<=t OR (p.starts_at<=?3 AND p.expires_at>?3))
    ) SELECT MAX(t) AS t FROM coverage`:
    'SELECT MAX(?1,COALESCE(MAX(expires_at),0)) AS t FROM passes WHERE email=?2 AND revoked=0 AND ?3';
  const anchorArgs=[earliest,email,order?Math.max(paidAt,Math.floor(Date.now()/1000)):Number(isNew)];
  for(let attempt=0;attempt<8;attempt++) {
    const current=await env.ACCESS_DB.prepare(anchor).bind(...anchorArgs).all();
    const start=current.results[0].t;
    const end=plan==='pass1m' ? monthEnd(start) : start+p.seconds;
    const sql=`INSERT INTO passes(checkout_id,email,plan,payment_intent,starts_at,expires_at,revoked)
      SELECT ?4,?2,?5,?6,?7,?8,?9 WHERE (?7=(${anchor})) ON CONFLICT(checkout_id) DO NOTHING`;
    await env.ACCESS_DB.prepare(sql).bind(...anchorArgs,s.id,plan,id(s.payment_intent),start,end,revoked).run();
    const saved=await env.ACCESS_DB.prepare('SELECT checkout_id FROM passes WHERE checkout_id=?').bind(s.id).all();
    if(saved.results.length) {
      await markRenewalPaid(env,order,s,paidAt);
      await invalidateAccessCache(env,email);
      return;
    }
  }
  throw new Error('concurrent_pass_purchase');
}
export async function access(env,email,now=Math.floor(Date.now()/1000)) {
  // Keep the existing low-cost authorization path for every private data read.
  if(VIPS.has(email))return {active:true,source:'vip',authentication:'legacy_email_only'};
  if(env.ACCESS_DB) {
    const result=await env.ACCESS_DB.prepare('SELECT expires_at,plan,payment_intent FROM passes WHERE email=? AND revoked=0 AND starts_at<=? AND expires_at>? ORDER BY expires_at DESC').bind(email,now,now).all();
    for(const row of result.results||[]) {
      if(await verifiedPayment(env,row.payment_intent))return {active:true,source:'stripe_pass',plan:row.plan,expires_at:row.expires_at,authentication:'legacy_email_only'};
    }
  }
  return subscriptionAccess(env,email,now);
}
async function accountAccess(env,email,now) {
  if(VIPS.has(email))return {active:true,source:'vip',expires_at:null,has_verified_payment:false,authentication:'legacy_email_only'};
  const sub=await subscriptionAccess(env,email,now,true);
  let result=sub, end=sub.active?sub.expires_at:now, history=sub.expires_at||0;
  let verified=sub.has_verified_payment===true;
  if(env.ACCESS_DB) {
    const rows=(await env.ACCESS_DB.prepare('SELECT starts_at,expires_at,plan,payment_intent FROM passes WHERE email=? AND revoked=0 ORDER BY starts_at').bind(email).all()).results||[];
    // Check relevant passes plus the most recent expired purchase for promotion history.
    const expired=rows.filter(r=>r.expires_at<=now).at(-1);
    for(const row of rows) {
      if(row.expires_at<=now&&row!==expired)continue;
      if(!await verifiedPayment(env,row.payment_intent))continue;
      verified=true;
      if(row.expires_at<=now)history=Math.max(history,row.expires_at);
      if(row.starts_at<=end&&row.expires_at>now) {
        if(row.starts_at<=now)result={active:true,source:'stripe_pass',plan:row.plan};
        end=Math.max(end,row.expires_at);
      }
    }
  }
  return {...result,expires_at:result.active?end:(history||null),has_verified_payment:verified,authentication:'legacy_email_only'};
}
async function subscriptionAccess(env,email,now,summary=false) {
  let latest=0,history=0,verified=false;
  const customers=await all(env,'/customers',{email});
  for(const customer of customers) {
    const subs=await all(env,'/subscriptions',{customer:customer.id,status:'all'});
    for(const sub of subs) {
      const valid=(sub.items?.data||[]).filter(item=>id(item.price)===subscriptionPrice(env) || (env.STRIPE_MODE==='live'&&LEGACY_MONTHLY.has(id(item.price))));
      if(!valid.length)continue;
      const expiry=sub.status==='trialing'?sub.trial_end:Math.max(...valid.map(i=>i.current_period_end||sub.current_period_end||sub.ended_at||0));
      const end=Math.min(expiry||0,sub.cancel_at||expiry||0,sub.ended_at||expiry||0);
      if(end<=now)history=Math.max(history,end);
      const active=['active','trialing'].includes(sub.status)&&end>now;
      if(!summary&&!active)continue;
      let invoice;
      if(sub.latest_invoice&&(sub.status==='active'||(summary&&!verified))) {
        invoice=await stripe(env,'/invoices/'+encodeURIComponent(id(sub.latest_invoice)),summary?{'expand[]':'payments'}:undefined);
        if(summary&&!verified&&invoice.status==='paid'&&invoice.amount_paid>0) {
          for(const p of invoice.payments?.data||[]) {
            if(p.status==='paid'&&p.amount_paid>0&&p.payment?.type==='payment_intent'&&await verifiedPayment(env,id(p.payment.payment_intent))) {verified=true;break;}
          }
        }
      }
      if(!active)continue;
      if(sub.status==='active'&&invoice?.status!=='paid')continue;
      latest=Math.max(latest,end);
    }
  }
  const entitlement=latest>now ? {active:true,source:'stripe_subscription',plan:'monthly',expires_at:latest,authentication:'legacy_email_only'} : {active:false};
  return summary?{...entitlement,expires_at:entitlement.expires_at??(history||null),has_verified_payment:verified}:entitlement;
}
export default {
  async scheduled(event,env) { if(env.AUTH_ENABLED==='true')await cleanupAuth(env); },
  async fetch(request,env) {
    const testLogin=goldTestLoginPage(request,env);
    if(testLogin)return testLogin;
    const trialResponse=await telegramTrialRoute(request,env,access,subscriptionPrice(env));
    if(trialResponse)return trialResponse;
    const authResponse=await authRoute(request,env,access,Math.floor(Date.now()/1000),{
      status:async(email,now,fresh=false)=>renewalStatus(env,email,await cachedAccess(env,email,now,()=>renewalEnabled(env)?accountAccess(env,email,now):access(env,email,now),fresh),now,plans(env)),
      checkout:(req,email,now)=>renewalCheckout(req,env,email,now,{access:accountAccess,catalog:plans(env),stripe})
    });
    if(authResponse)return authResponse;
    const u=new URL(request.url),origin=request.headers.get('Origin');
    const headers={'Content-Type':'application/json','Cache-Control':'no-store','Vary':'Origin'};
    if(ORIGINS.has(origin))Object.assign(headers,{'Access-Control-Allow-Origin':origin,'Access-Control-Allow-Methods':'GET, POST, OPTIONS','Access-Control-Allow-Headers':'Content-Type'});
    const reply=(body,status=200)=>Response.json(body,{status,headers});
    if(request.method==='OPTIONS')return new Response(null,{status:ORIGINS.has(origin)?204:403,headers});
    if(!['live','test'].includes(env.STRIPE_MODE))return reply({error:'Configurare STRIPE_MODE.'},503);
    try {
      if(u.pathname==='/api/stripe-webhook'&&request.method==='POST') {
        if(!env.ACCESS_DB||!env.STRIPE_WEBHOOK_SECRET)return reply({error:'Webhook non configurato.'},503);
        const raw=await request.text();
        if(raw.length>1048576)return reply({error:'Payload troppo grande.'},413);
        if(!await verifySignature(raw,request.headers.get('Stripe-Signature'),env.STRIPE_WEBHOOK_SECRET))return reply({error:'Firma non valida.'},400);
        let event;try{event=JSON.parse(raw);}catch{return reply({error:'JSON non valido.'},400);}
        if(event.livemode!==(env.STRIPE_MODE==='live'))return reply({error:'Ambiente errato.'},400);
        await fulfill(env,event);return reply({received:true});
      }
      if(origin&&!ORIGINS.has(origin))return reply({error:'Origine non consentita.'},403);
      if(u.pathname==='/api/create-checkout-session'&&request.method==='POST') {
        if(!ORIGINS.has(origin))return reply({error:'Origine richiesta.'},403);
        // Set only after complete deployment and sandbox checkout/expiry tests.
        if(env.CHECKOUT_ENABLED!=='true'||!env.ACCESS_DB||!env.STRIPE_WEBHOOK_SECRET)return reply({error:'Acquisti in preparazione. Contatta il supporto.'},503);
        let body;try{body=await request.json();}catch{return reply({error:'Richiesta non valida.'},400);}
        const p=Object.hasOwn(PLANS,body?.plan) && plans(env)[body.plan];if(!p?.price)return reply({error:'Piano non valido.'},400);
        const params={mode:'payment','line_items[0][price]':p.price,'line_items[0][quantity]':'1',
          success_url:'https://arabsniperbet.com/?success=true#piani',cancel_url:'https://arabsniperbet.com/?canceled=true#piani',
          'metadata[plan]':body.plan,integration_identifier:'arabsniper_web_qtvrnzkp'};
        params.customer_creation='always';
        const session=await stripe(env,'/checkout/sessions',params,'POST');
        if(session.livemode!==(env.STRIPE_MODE==='live')||!String(session.url).startsWith('https://checkout.stripe.com/'))throw new Error('invalid_checkout');
        return reply({url:session.url});
      }
      if((u.pathname==='/'||u.pathname==='/api/check-access')&&request.method==='GET') {
        if(env.AUTH_ENABLED==='true')return reply({active:false,error:'Usa il codice email per accedere.'},401);
        const email=normalize(u.searchParams.get('email'));
        if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)||email.length>254)return reply({active:false});
        return reply(await access(env,email));
      }
      return reply({error:'Percorso non disponibile.'},404);
    }catch{return reply({active:false,error:'Servizio pagamenti temporaneamente non disponibile.'},503);}
  }
};
