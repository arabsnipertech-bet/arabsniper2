// Production bundle: generated from worker.mjs and auth.mjs. Keep AUTH_ENABLED=false until cutover.
const {authRoute, cleanupAuth} = (() => {
// Staged OTP authentication. Activate only after email delivery and private data cutover.
const COOKIE = '__Host-gold_session_v2';
const SESSION_SECONDS = 3600;
const encoder = new TextEncoder();
const hex = bytes => Array.from(new Uint8Array(bytes), b=>b.toString(16).padStart(2,'0')).join('');
const token = () => hex(crypto.getRandomValues(new Uint8Array(32)));
const hash = async value => hex(await crypto.subtle.digest('SHA-256',encoder.encode(value)));
async function mac(secret,value) {
 const key=await crypto.subtle.importKey('raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
 return hex(await crypto.subtle.sign('HMAC',key,encoder.encode(value)));
}
function generateCode() {
 let n; do { n=crypto.getRandomValues(new Uint32Array(1))[0]; } while(n>=4294000000);
 return String(n%1000000).padStart(6,'0');
}
function reply(body,status=200,extra={}) {
 return Response.json(body,{status,headers:{'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',...extra}});
}
function cookie(value,seconds) {return `${COOKIE}=${value}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=${seconds}`;}
async function first(env,sql,...args) {
 return (await env.ACCESS_DB.prepare(sql).bind(...args).all()).results?.[0];
}
async function limit(env,key,max,seconds,now) {
 const bucket=Math.floor(now/seconds);
 const row=await first(env,`INSERT INTO gold_limits(key,hits,expires_at) VALUES (?,1,?)
 ON CONFLICT(key) DO UPDATE SET hits=hits+1 RETURNING hits`,`${key}:${bucket}`,now+seconds);
 return row.hits<=max;
}
async function sessionRecord(request,env,now=Math.floor(Date.now()/1000)) {
 const t=request.headers.get('Cookie')?.split(';').map(s=>s.trim()).find(s=>s.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);
 if(!t||!/^[a-f0-9]{64}$/.test(t))return null;
 const row=await first(env,'SELECT email,expires_at FROM gold_sessions WHERE digest=? AND expires_at>?',await hash(COOKIE+':'+t),now);
 return row||null;
}
async function sessionEmail(request,env,now=Math.floor(Date.now()/1000)) {
 return (await sessionRecord(request,env,now))?.email||null;
}
async function authRoute(request,env,access,now=Math.floor(Date.now()/1000)) {
 const url=new URL(request.url),path=url.pathname;
 if(!path.startsWith('/api/auth/')&&!path.startsWith('/api/gold-data/'))return null;
 if(env.AUTH_ENABLED!=='true'||!env.ACCESS_DB||!env.OTP_SECRET||!env.RESEND_API_KEY||!env.EMAIL_FROM||!env.GOLD_DATA)
   return reply({error:'Accesso con codice in preparazione.'},503);
 // Same-origin only: cookies must belong to the website, never workers.dev.
 if(url.origin!==env.AUTH_ORIGIN)return reply({error:'Dominio non valido.'},403);
 if(request.method==='POST'&&request.headers.get('Origin')!==url.origin)return reply({error:'Origine non valida.'},403);
 try {
  if(path==='/api/auth/session'&&request.method==='GET') {
   const session=await sessionRecord(request,env,now);
   if(!session)return reply({active:false},401,{'Set-Cookie':cookie('',0)});
   return reply({...await access(env,session.email,now),authentication:'email_otp',session_expires_at:session.expires_at});
  }
  if(path==='/api/auth/logout'&&request.method==='POST') {
   const t=request.headers.get('Cookie')?.split(';').map(s=>s.trim()).find(s=>s.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);
   if(t)await env.ACCESS_DB.prepare('DELETE FROM gold_sessions WHERE digest=?').bind(await hash(COOKIE+':'+t)).run();
   return reply({ok:true},200,{'Set-Cookie':cookie('',0)});
  }
  if(path.startsWith('/api/gold-data/')&&request.method==='GET') {
   const email=await sessionEmail(request,env,now);
   if(!email)return reply({error:'Accedi con il codice email.'},401);
   if(!(await access(env,email,now)).active)return reply({error:'Accesso Gold non attivo.'},403);
   const file=path.slice('/api/gold-data/'.length);
   if(!/^(arab_quote_day[1-5]\.json|arab_quote_prediction_audit\.json|arab_quote_model_research\.json|_audit_parts\/[a-f0-9]{64}\.json\.gz)$/.test(file))return reply({error:'File non disponibile.'},404);
   const object=await env.GOLD_DATA.get(file);
   if(!object)return reply({error:'Dati non disponibili.'},404);
   return new Response(object.body,{headers:{'Content-Type':file.endsWith('.gz')?'application/octet-stream':'application/json','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'}});
  }
  if(request.method!=='POST'||!['/api/auth/request-code','/api/auth/verify-code'].includes(path))return reply({error:'Percorso non disponibile.'},404);
  if(!request.headers.get('Content-Type')?.startsWith('application/json'))return reply({error:'Formato non valido.'},415);
  const raw=await request.text();if(raw.length>2048)return reply({error:'Richiesta troppo grande.'},413);
  let body;try{body=JSON.parse(raw);}catch{return reply({error:'Richiesta non valida.'},400);}
  const email=String(body.email||'').trim().toLowerCase();
  if(email.length>254||!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))return reply({error:'Email non valida.'},400);
  const ip=request.headers.get('CF-Connecting-IP');if(!ip)return reply({error:'Connessione non valida.'},403);
  const ipKey=await mac(env.OTP_SECRET,'ip:'+ip),emailKey=await mac(env.OTP_SECRET,'email:'+email);
  if(!await limit(env,'requests:'+ipKey,60,600,now))return reply({error:'Troppi tentativi. Riprova tra qualche minuto.'},429);
  if(path==='/api/auth/request-code') {
   if(!await limit(env,'send-ip:'+ipKey,10,3600,now)||!await limit(env,'send-email:'+emailKey,5,3600,now)||!await limit(env,'send-global',100,3600,now))return reply({error:'Troppi invii. Riprova più tardi.'},429);
   const code=generateCode(),challenge=token();
   const digest=await mac(env.OTP_SECRET,`${email}:${challenge}:${code}`);
   const row=await first(env,`INSERT INTO gold_codes(email,challenge,digest,expires_at,attempts,sent_at,consumed) VALUES (?,?,?,?,0,?,0)
    ON CONFLICT(email) DO UPDATE SET challenge=excluded.challenge,digest=excluded.digest,expires_at=excluded.expires_at,attempts=0,sent_at=excluded.sent_at,consumed=0
    WHERE gold_codes.sent_at<=? RETURNING challenge`,email,challenge,digest,now+600,now,now-60);
   if(!row)return reply({error:'Attendi un minuto prima di richiedere un nuovo codice.'},429);
   // Send to any valid address; never disclose subscription status before proof of mailbox ownership.
   const sent=await fetch('https://api.resend.com/emails',{method:'POST',headers:{Authorization:`Bearer ${env.RESEND_API_KEY}`,'Content-Type':'application/json'},body:JSON.stringify({from:env.EMAIL_FROM,to:[email],subject:'Il tuo codice di accesso Arab Sniper Gold',text:`Il tuo codice di accesso è ${code}.\n\nScade tra 10 minuti ed è utilizzabile una sola volta. Non condividerlo. Se non hai richiesto l’accesso, ignora questa email.`}),signal:AbortSignal.timeout(10000)});
   if(!sent.ok){await env.ACCESS_DB.prepare('DELETE FROM gold_codes WHERE email=? AND challenge=?').bind(email,challenge).run();return reply({error:'Invio email non riuscito. Riprova più tardi.'},503);}
   return reply({ok:true,challenge});
  }
  if(!/^\d{6}$/.test(String(body.code))||!/^[a-f0-9]{64}$/.test(String(body.challenge)))return reply({error:'Codice non valido.'},400);
  const digest=await mac(env.OTP_SECRET,`${email}:${body.challenge}:${body.code}`);
  // Atomic consume and attempt increment: concurrent requests cannot reuse a successful code.
  const row=await first(env,`UPDATE gold_codes SET attempts=attempts+1,consumed=CASE WHEN digest=? THEN 1 ELSE 0 END
   WHERE email=? AND challenge=? AND expires_at>? AND consumed=0 AND attempts<5 RETURNING consumed`,digest,email,body.challenge,now);
  if(!row?.consumed)return reply({error:'Codice errato, scaduto o già utilizzato.'},400);
  const entitlement=await access(env,email,now);
  if(!entitlement.active)return reply({error:'Email verificata. Acquista un piano per accedere a Gold.'},403);
  const t=token();await env.ACCESS_DB.prepare('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)').bind(await hash(COOKIE+':'+t),email,now+SESSION_SECONDS).run();
  return reply({...entitlement,authentication:'email_otp',session_expires_at:now+SESSION_SECONDS},200,{'Set-Cookie':cookie(t,SESSION_SECONDS)});
 }catch{return reply({error:'Servizio accessi temporaneamente non disponibile.'},503);}
}
async function cleanupAuth(env,now=Math.floor(Date.now()/1000)) {
 for(const table of ['gold_codes','gold_sessions','gold_limits'])await env.ACCESS_DB.prepare(`DELETE FROM ${table} WHERE expires_at<=?`).bind(now).run();
}

return {authRoute,cleanupAuth};
})();

// OTP remains disabled until its staged deployment requirements are satisfied.
const API_VERSION = '2026-08-26.dahlia';
const ORIGINS = new Set(['https://arabsniperbet.com','https://www.arabsniperbet.com']);
export const PLANS = {
  daily: {price:'price_1UTM5fH763HaPm5Ufge4HJC5', amount:500, seconds:86400},
  weekly:{price:'price_1UTM5mH763HaPm5UwzU6S0LG', amount:1500, seconds:604800},
  monthly:{price:'price_1UTM5yH763HaPm5Uo2hm0Ped', amount:2900}
};
const LEGACY_MONTHLY = new Set(['price_1T8fx8H763HaPm5UVjZgaawF','price_1T6ri6H763HaPm5UiG9xQY5F']);
const VIPS = new Set(['bruno.pirolo@gmail.com']);
const normalize = s => String(s||'').trim().toLowerCase();
const id = o => typeof o === 'string' ? o : o?.id;
function plans(env) {
  if (env.STRIPE_MODE === 'test') {
    return Object.fromEntries(Object.entries(PLANS).map(([k,v])=>[k,{...v,price:env['TEST_PRICE_'+k.toUpperCase()]}]));
  }
  return PLANS;
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
export async function fulfill(env,event) {
  const ref=event.data.object;
  if(['charge.refunded','charge.dispute.created'].includes(event.type)) {
    let pi=id(ref.payment_intent);
    if(!pi&&ref.charge)pi=id((await stripe(env,'/charges/'+encodeURIComponent(id(ref.charge)))).payment_intent);
    if(pi)await env.ACCESS_DB.prepare('UPDATE passes SET revoked=1 WHERE payment_intent=?').bind(pi).run();
    return;
  }
  if(!['checkout.session.completed','checkout.session.async_payment_succeeded'].includes(event.type))return;
  if(event.type==='checkout.session.completed'&&ref.payment_status!=='paid')return;
  const s=await stripe(env,'/checkout/sessions/'+encodeURIComponent(ref.id),{'expand[]':'line_items'});
  if(s.mode!=='payment'||s.payment_status!=='paid'||s.status!=='complete')return;
  if(s.livemode !== (env.STRIPE_MODE==='live'))throw new Error('mode_mismatch');
  const rows=s.line_items?.data||[];
  if(rows.length!==1||s.line_items.has_more||rows[0].quantity!==1)return;
  const entry=Object.entries(plans(env)).find(([,p])=>p.seconds&&p.price===id(rows[0].price));
  if(!entry)return;
  const [plan,p]=entry;
  if(s.currency!=='eur'||s.amount_total!==p.amount)return;
  const email=normalize(s.customer_details?.email);
  if(!email||!id(s.payment_intent))throw new Error('missing_customer');
  // Fetch authoritative charge state too: late/retried success must not restore refunded access.
  const pi=await stripe(env,'/payment_intents/'+encodeURIComponent(id(s.payment_intent)),{'expand[]':'latest_charge'});
  if(pi.status!=='succeeded')return;
  let charge=pi.latest_charge;
  if(typeof charge==='string')charge=await stripe(env,'/charges/'+encodeURIComponent(charge));
  if(!charge)throw new Error('charge_unavailable');
  const revoked=charge.refunded||charge.amount_refunded>0||charge.disputed ? 1 : 0;
  const paidAt=Number(event.created);
  if(!Number.isFinite(paidAt)||paidAt<Number(s.created))throw new Error('invalid_payment_time');
  // Unique Checkout ID prevents retries/concurrent delivery from extending the pass.
  await env.ACCESS_DB.prepare('INSERT INTO passes (checkout_id,email,plan,payment_intent,starts_at,expires_at,revoked) VALUES (?,?,?,?,?,?,?) ON CONFLICT(checkout_id) DO NOTHING')
    .bind(s.id,email,plan,id(s.payment_intent),paidAt,paidAt+p.seconds,revoked).run();
}
export async function access(env,email,now=Math.floor(Date.now()/1000)) {
  if(VIPS.has(email))return {active:true,source:'vip',authentication:'legacy_email_only'};
  if(env.ACCESS_DB) {
    const result=await env.ACCESS_DB.prepare('SELECT expires_at,plan,payment_intent FROM passes WHERE email=? AND revoked=0 AND expires_at>? ORDER BY expires_at DESC').bind(email,now).all();
    for(const row of result.results||[]) {
      // Revocation remains correct even if a refund webhook has not arrived yet.
      const pi=await stripe(env,'/payment_intents/'+encodeURIComponent(row.payment_intent),{'expand[]':'latest_charge'});
      let c=pi.latest_charge;if(typeof c==='string')c=await stripe(env,'/charges/'+encodeURIComponent(c));
      if(pi.status==='succeeded'&&c&&!c.refunded&&!c.amount_refunded&&!c.disputed)return {active:true,source:'stripe_pass',plan:row.plan,expires_at:row.expires_at,authentication:'legacy_email_only'};
    }
  }
  const customers=await all(env,'/customers',{email});
  for(const customer of customers) {
    const subs=await all(env,'/subscriptions',{customer:customer.id,status:'all'});
    for(const sub of subs) {
      if(!['active','trialing'].includes(sub.status))continue;
      const valid=(sub.items?.data||[]).filter(item=>id(item.price)===plans(env).monthly.price || (env.STRIPE_MODE==='live'&&LEGACY_MONTHLY.has(id(item.price))));
      if(!valid.length)continue;
      const expiry=sub.status==='trialing'?sub.trial_end:Math.max(...valid.map(i=>i.current_period_end||sub.current_period_end||0));
      if(!(expiry>now))continue;
      if(sub.status==='active') {
        if(!sub.latest_invoice)continue;
        const invoice=await stripe(env,'/invoices/'+encodeURIComponent(id(sub.latest_invoice)));
        if(invoice.status!=='paid')continue;
      }
      return {active:true,source:'stripe_subscription',plan:'monthly',expires_at:expiry,authentication:'legacy_email_only'};
    }
  }
  return {active:false};
}
export default {
  async scheduled(event,env) { if(env.AUTH_ENABLED==='true')await cleanupAuth(env); },
  async fetch(request,env) {
    const authResponse=await authRoute(request,env,access);
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
        const p=plans(env)[body.plan];if(!p?.price)return reply({error:'Piano non valido.'},400);
        const params={mode:p.seconds?'payment':'subscription','line_items[0][price]':p.price,'line_items[0][quantity]':'1',
          success_url:'https://arabsniperbet.com/?success=true#piani',cancel_url:'https://arabsniperbet.com/?canceled=true#piani',
          'metadata[plan]':body.plan,integration_identifier:'arabsniper_web_qtvrnzkp'};
        if(p.seconds)params.customer_creation='always';
        else params['subscription_data[metadata][plan]']='monthly';
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
