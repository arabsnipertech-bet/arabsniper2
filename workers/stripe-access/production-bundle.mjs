// Production bundle: generated from worker.mjs and auth.mjs. Keep AUTH_ENABLED=false until cutover.
const {authRoute, cleanupAuth} = (() => {
// Staged OTP authentication. Activate only after email delivery and private data cutover.
// Versioned digest rejects legacy seven-day tokens even if the cookie is renamed.
const COOKIE = '__Host-gold_session_24h';
const SESSION_SECONDS = 86400;
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
async function authRoute(request,env,access,now=Math.floor(Date.now()/1000),renewals=null) {
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
   const fresh=url.searchParams.get('refresh')==='1';
   if(fresh&&!await limit(env,'status:'+session.email,5,60,now))return reply({error:'Attendi un minuto prima di aggiornare ancora.'},429);
   const status=renewals?await renewals.status(session.email,now,fresh):{...await access(env,session.email,now),email:session.email,authenticated:true,server_time:now};
   return reply({...status,authentication:'email_otp',session_expires_at:session.expires_at});
  }
  if(path==='/api/auth/renew'&&request.method==='POST') {
   const email=await sessionEmail(request,env,now);
   if(!email)return reply({error:'Accedi con il codice email per rinnovare.'},401);
   if(!renewals)return reply({error:'Rinnovi in preparazione.'},503);
   const result=await renewals.checkout(request,email,now);
   const {status,...body}=result;return reply(body,status||200);
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
  const status=renewals?await renewals.status(email,now,true):{...await access(env,email,now),email,authenticated:true,server_time:now};
  // A verified mailbox may renew expired access; private feeds still require an active entitlement.
  if(!status.active&&env.GOLD_RENEWAL_ENABLED!=='true')return reply({error:'Email verificata. Acquista un piano per accedere a Gold.'},403);
  const t=token();await env.ACCESS_DB.prepare('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)').bind(await hash(COOKIE+':'+t),email,now+SESSION_SECONDS).run();
  return reply({...status,authentication:'email_otp',session_expires_at:now+SESSION_SECONDS},200,{'Set-Cookie':cookie(t,SESSION_SECONDS)});
 }catch{return reply({error:'Servizio accessi temporaneamente non disponibile.'},503);}
}
async function cleanupAuth(env,now=Math.floor(Date.now()/1000)) {
 for(const table of ['gold_codes','gold_sessions','gold_limits'])await env.ACCESS_DB.prepare(`DELETE FROM ${table} WHERE expires_at<=?`).bind(now).run();
}



return {authRoute,cleanupAuth};
})();

const telegramTrialRoute = (() => {
/* Telegram DM -> verified email -> no-card, non-renewing Stripe 7-day Gold trial.
   Disabled unless explicitly enabled on the production Cloudflare Worker.
   Never activate trials from channel messages, unverified emails, or arbitrary web requests. */
const DAYS7=7*86400;
const CAMPAIGN='gold-engine-2026-11';
const campaignOn=env=>(env.TELEGRAM_TRIAL_CAMPAIGN??CAMPAIGN)===CAMPAIGN;
const EMAIL=/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const lower=s=>String(s||'').trim().toLowerCase();
const enc=new TextEncoder();
const nowSeconds=()=>Math.floor(Date.now()/1000);
async function mac(secret,value) {
  const key=await crypto.subtle.importKey('raw',enc.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  return Array.from(new Uint8Array(await crypto.subtle.sign('HMAC',key,enc.encode(value))),b=>b.toString(16).padStart(2,'0')).join('');
}
function otp() {
  let x;do{x=crypto.getRandomValues(new Uint32Array(1))[0]}while(x>=4294000000);
  return String(x%1000000).padStart(6,'0');
}
async function first(env,sql,...values){
  return (await env.ACCESS_DB.prepare(sql).bind(...values).all()).results?.[0]||null;
}
async function api(env,path,values,method='GET',idempotency) {
  const headers={'Authorization':'Bearer '+env.STRIPE_SECRET_KEY,'Stripe-Version':'2026-08-26.dahlia'};
  let url='https://api.stripe.com/v1'+path,body;
  if(method==='GET')url+='?'+new URLSearchParams(values||{});
  else {headers['Content-Type']='application/x-www-form-urlencoded';body=new URLSearchParams(values||{});}
  if(idempotency)headers['Idempotency-Key']=idempotency;
  const response=await fetch(url,{method,headers,body,signal:AbortSignal.timeout(12000)});
  if(!response.ok)throw Error('stripe_request_failed');
  return response.json();
}
async function sendTelegram(env,chat,text) {
  const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({chat_id:chat,text,disable_web_page_preview:true}),
    signal:AbortSignal.timeout(10000)});
  if(!r.ok)throw Error('telegram_send_failed');
}
async function sendEmail(env,email,code) {
  const r=await fetch('https://api.resend.com/emails',{
    method:'POST',headers:{Authorization:'Bearer '+env.RESEND_API_KEY,'Content-Type':'application/json'},
    body:JSON.stringify({from:env.EMAIL_FROM,to:[email],
      subject:'ArabSniperBet Gold · conferma i tuoi 7 giorni gratuiti',
      text:'Il tuo codice per la prova Gold gratuita di 7 giorni è: '+code+
           '\nScade tra 10 minuti. Non condividerlo. Non è richiesto alcun pagamento né un rinnovo automatico.'}),
    signal:AbortSignal.timeout(10000)});
  if(!r.ok)throw Error('resend_unavailable');
}
async function otpLimit(env,key,max,expiresAt) {
  const row=await first(env,
    'INSERT INTO telegram_trial_limits(key,hits,expires_at) VALUES (?,1,?) '+
    'ON CONFLICT(key) DO UPDATE SET hits=hits+1 RETURNING hits',key,expiresAt);
  return row&&row.hits<=max;
}
async function pages(env,path,params={}) {
  const rows=[];let after;
  for(let i=0;i<20;i++) {
    const page=await api(env,path,{...params,limit:'100',...(after?{starting_after:after}:{})});
    if(!Array.isArray(page.data))throw Error('invalid_stripe_page');
    rows.push(...page.data);
    if(!page.has_more)return rows;
    if(!page.data.length)throw Error('invalid_stripe_page');
    after=page.data.at(-1).id;
  }
  throw Error('stripe_page_limit');
}
async function campaignGrant(env,access,email,tgid,now,monthlyPrice) {
  let claim=await first(env,'SELECT * FROM telegram_campaign_claims WHERE campaign=? AND (email=? OR telegram_id=?) LIMIT 1',CAMPAIGN,email,tgid);
  if(claim&&(claim.email!==email||claim.telegram_id!==tgid))return {type:'already_claimed'};
  if(claim?.status==='granted')return {type:'granted',expires_at:claim.expires_at};
  if(!claim) {
    // Follow contiguous entitlements, including a pass followed by a longer trial.
    let horizon=now,complete=false;
    for(let i=0;i<100;i++) {
      const current=await access(env,email,horizon);
      if(!current.active){complete=true;break;}
      if(current.source==='vip'&&!current.expires_at)return {type:'unlimited'};
      if(!Number.isSafeInteger(current.expires_at)||current.expires_at<=horizon)throw Error('invalid_entitlement');
      horizon=current.expires_at;
    }
    if(!complete)throw Error('entitlement_chain_limit');
    await env.ACCESS_DB.prepare('INSERT OR IGNORE INTO telegram_campaign_claims (campaign,email,telegram_id,requested_at,base_expires_at,expires_at,status) VALUES (?,?,?,?,?,?,?)')
      .bind(CAMPAIGN,email,tgid,now,horizon,horizon+DAYS7,'reserved').run();
    claim=await first(env,'SELECT * FROM telegram_campaign_claims WHERE campaign=? AND email=? AND telegram_id=?',CAMPAIGN,email,tgid);
    if(!claim)return {type:'already_claimed'};
  }
  if(claim.status==='granted')return {type:'granted',expires_at:claim.expires_at};
  if(claim.status!=='reserved'||claim.expires_at<=now)throw Error('campaign_reservation_expired');
  const key=(await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,CAMPAIGN+':'+email+':'+tgid)).slice(0,32);
  const customers=(await pages(env,'/customers',{email})).filter(c=>lower(c.email)===email);
  const subscriptions=[];
  for(const c of customers)for(const s of await pages(env,'/subscriptions',{customer:c.id,status:'all'}))subscriptions.push({...s,_customer:c.id});
  // Recover successful remote writes even after Stripe's idempotency retention window.
  let subscription=subscriptions.find(s=>s.metadata?.gold_trial_campaign===CAMPAIGN&&s.metadata?.gold_trial_claim===key);
  if(!claim.action&&!subscription) {
    const trial=subscriptions.filter(s=>s.status==='trialing'&&s.trial_end>now&&s.items?.data?.some(i=>(typeof i.price==='string'?i.price:i.price?.id)===monthlyPrice))
      .sort((a,b)=>b.trial_end-a.trial_end)[0];
    let customer=trial?trial._customer:customers[0]?.id;
    if(!customer)customer=(await api(env,'/customers',{email,'metadata[trial_source]':'telegram_arabsniper'},'POST','as-campaign-customer-'+key)).id;
    if(!customer)throw Error('customer_not_created');
    await env.ACCESS_DB.prepare('UPDATE telegram_campaign_claims SET action=?,customer_id=?,subscription_id=? WHERE campaign=? AND email=? AND telegram_id=? AND action IS NULL AND status=\'reserved\'')
      .bind(trial?'extend':'create',customer,trial?.id||null,CAMPAIGN,email,tgid).run();
    claim=await first(env,'SELECT * FROM telegram_campaign_claims WHERE campaign=? AND email=? AND telegram_id=?',CAMPAIGN,email,tgid);
  }
  if(!subscription) {
    const params={trial_end:String(claim.expires_at),cancel_at:String(claim.expires_at),
      'trial_settings[end_behavior][missing_payment_method]':'cancel',
      'metadata[gold_trial_campaign]':CAMPAIGN,'metadata[gold_trial_claim]':key};
    if(claim.action==='extend') {
      const target=await api(env,'/subscriptions/'+encodeURIComponent(claim.subscription_id));
      if(target.status!=='trialing'||target.trial_end>claim.expires_at)throw Error('trial_changed_during_campaign');
      params.proration_behavior='none';
      subscription=await api(env,'/subscriptions/'+encodeURIComponent(claim.subscription_id),params,'POST','as-campaign-extend-'+key);
    } else if(claim.action==='create') {
      params.customer=claim.customer_id;params['items[0][price]']=monthlyPrice;
      params['metadata[trial_source]']='telegram_arabsniper';
      subscription=await api(env,'/subscriptions',params,'POST','as-campaign-create-'+key);
    } else throw Error('invalid_campaign_action');
  }
  if(subscription.status!=='trialing'||Number(subscription.trial_end)!==claim.expires_at||Number(subscription.cancel_at)!==claim.expires_at) {
    // Never leave an unexpectedly renewable subscription created by this promotion.
    if(claim.action==='create'&&subscription.id)await api(env,'/subscriptions/'+encodeURIComponent(subscription.id),{},'DELETE','as-campaign-unsafe-cancel-'+key);
    throw Error('campaign_safety_not_confirmed');
  }
  await env.ACCESS_DB.prepare('UPDATE telegram_campaign_claims SET status=\'granted\',subscription_id=?,granted_at=? WHERE campaign=? AND email=? AND telegram_id=? AND status=\'reserved\'')
    .bind(subscription.id,now,CAMPAIGN,email,tgid).run();
  // The status panel can have a short-lived cached entitlement after a grant.
  const hasCache=await first(env,"SELECT name FROM sqlite_schema WHERE type='table' AND name='gold_access_cache'");
  if(hasCache)await env.ACCESS_DB.prepare('DELETE FROM gold_access_cache WHERE email=?').bind(email).run();
  return {type:'granted',expires_at:claim.expires_at};
}
async function tryGrant(env,access,email,tgid,now,monthlyPrice) {
  if(campaignOn(env))return campaignGrant(env,access,email,tgid,now,monthlyPrice);
  // Never issue extra free weeks to accounts that already have an entitlement.
  if((await access(env,email,now)).active)return {type:'already_active'};
  const expiry=now+DAYS7;
  await env.ACCESS_DB.prepare(
    'INSERT OR IGNORE INTO telegram_trial_claims (email,telegram_id,started_at,expires_at,status) VALUES (?,?,?,?,?)'
  ).bind(email,tgid,now,expiry,'reserved').run();
  const claim=await first(env,'SELECT * FROM telegram_trial_claims WHERE email=? AND telegram_id=?',email,tgid);
  if(!claim)return {type:'already_claimed'};
  if(claim.status==='granted')return {type:'already_claimed'};
  if(claim.status!=='reserved')throw Error('invalid_trial_state');
  // Stable claim start and idempotency prevent retrying the webhook from extending the trial.
  const claimKey=await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,'trial:'+email+':'+tgid);
  const list=await api(env,'/customers',{email,limit:'20'});
  const customer=(list.data||[]).find(c=>lower(c.email)===email);
  let customerId=customer?.id;
  if(!customerId) {
    const created=await api(env,'/customers',{email,'metadata[trial_source]':'telegram_arabsniper'},'POST','as-tg-cust-'+claimKey.slice(0,32));
    customerId=created.id;
  }
  if(!customerId)throw Error('customer_not_created');
  const params={
    customer:customerId,
    'items[0][price]':monthlyPrice,
    trial_end:String(claim.expires_at),
    cancel_at:String(claim.expires_at),
    'trial_settings[end_behavior][missing_payment_method]':'cancel',
    'metadata[trial_source]':'telegram_arabsniper',
    'metadata[telegram_user_hash]':claimKey.slice(0,32)
  };
  const subscription=await api(env,'/subscriptions',params,'POST','as-tg-trial-'+claimKey.slice(0,32));
  if(subscription.status!=='trialing'||Number(subscription.trial_end)!==Number(claim.expires_at)||
     Number(subscription.cancel_at)!==Number(claim.expires_at)) {
    // Stripe returned an unexpected billing state: cancel immediately, never leave an
    // uncapped paid renewal behind. Failed cancellation needs operator intervention.
    if(subscription.id) {
      try {await api(env,'/subscriptions/'+encodeURIComponent(subscription.id),{},'DELETE',
        'as-tg-unsafe-cancel-'+claimKey.slice(0,32));}
      catch {throw Error('unsafe_subscription_manual_cancellation_required');}
    }
    throw Error('trial_safety_not_confirmed');
  }
  await env.ACCESS_DB.prepare(
    "UPDATE telegram_trial_claims SET status='granted',subscription_id=? WHERE email=? AND telegram_id=? AND status='reserved'"
  ).bind(subscription.id,email,tgid).run();
  return {type:'granted',expires_at:claim.expires_at};
}
async function processPrivateMessage(env,access,msg,monthlyPrice,now) {
  const chat=msg?.chat;
  if(chat?.type!=='private'||String(chat?.id)!==String(msg?.from?.id))return;
  const tgid=String(chat.id),text=String(msg.text||'').trim();
  if(!/^\d{1,18}$/.test(tgid)||text.length>254)return;
  if(/^\/(start|prova)(?:@\w+)?(?:\s.*)?$/i.test(text)) {
    await sendTelegram(env,tgid,'🎁 ArabSniperBet Gold · 7 giorni gratis!\n\n' +
      'Inviami qui in privato la tua email. Riceverai un codice per confermarla.\n' +
      (campaignOn(env)?'Nuovo motore: altri 7 giorni anche se hai già provato Gold! Una sola attivazione per questa campagna. Se hai un accesso attivo, i giorni si aggiungono alla scadenza.\n':'Una sola prova per persona/email.\n') +
      'Nessuna carta, nessun addebito e nessun rinnovo automatico della prova.\n' +
      'Accesso al sito: https://arabsniperbet.com/gold-dashboard-live.html');
    return;
  }
  if(EMAIL.test(text)&&text.length<=254) {
    const email=lower(text);
    const existing=campaignOn(env)
      ?await first(env,'SELECT * FROM telegram_campaign_claims WHERE campaign=? AND (email=? OR telegram_id=?) LIMIT 1',CAMPAIGN,email,tgid)
      :await first(env,'SELECT status FROM telegram_trial_claims WHERE email=? OR telegram_id=? LIMIT 1',email,tgid);
    if(existing&&(!campaignOn(env)||existing.status==='granted'||existing.email!==email||existing.telegram_id!==tgid)){
      await sendTelegram(env,tgid,campaignOn(env)?'Questa email o questo account Telegram ha già richiesto i 7 giorni di questa campagna.':'Questa email o questo account Telegram ha già richiesto la prova gratuita. Per assistenza contatta il supporto ArabSniperBet.');
      return;
    }
    const previous=await first(env,'SELECT sent_at FROM telegram_trial_codes WHERE telegram_id=?',tgid);
    if(previous&&previous.sent_at>now-120){
      await sendTelegram(env,tgid,'Attendi almeno due minuti prima di richiedere un altro codice.');
      return;
    }
    const hour=Math.floor(now/3600),day=Math.floor(now/86400);
    const emailHash=(await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,'limit-email:'+email)).slice(0,24);
    if(!await otpLimit(env,'chat:'+tgid+':'+hour,5,(hour+1)*3600)||
       !await otpLimit(env,'email:'+emailHash+':'+day,3,(day+1)*86400)||
       !await otpLimit(env,'global:'+hour,200,(hour+1)*3600)) {
      await sendTelegram(env,tgid,'Limite di richieste raggiunto. Riprova più tardi o contatta il supporto.');
      return;
    }
    const code=otp();
    const digest=await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,(campaignOn(env)?CAMPAIGN+':':'')+tgid+':'+email+':'+code);
    await env.ACCESS_DB.prepare(
      'INSERT INTO telegram_trial_codes (telegram_id,email,digest,expires_at,sent_at,attempts) VALUES (?,?,?,?,?,0) '+
      'ON CONFLICT(telegram_id) DO UPDATE SET email=excluded.email,digest=excluded.digest,expires_at=excluded.expires_at,sent_at=excluded.sent_at,attempts=0'
    ).bind(tgid,email,digest,now+600,now).run();
    try {await sendEmail(env,email,code)}
    catch(e) {
      await env.ACCESS_DB.prepare('DELETE FROM telegram_trial_codes WHERE telegram_id=? AND digest=?').bind(tgid,digest).run();
      throw e;
    }
    await sendTelegram(env,tgid,'📩 Codice inviato alla tua email. Scrivi qui le 6 cifre ricevute entro 10 minuti. Non inoltrarle ad altri.');
    return;
  }
  if(/^\d{6}$/.test(text)) {
    const record=await first(env,'SELECT * FROM telegram_trial_codes WHERE telegram_id=?',tgid);
    if(!record||record.expires_at<=now||record.attempts>=5){
      await sendTelegram(env,tgid,'Codice assente o scaduto. Inviami nuovamente la tua email per richiederne uno.');
      return;
    }
    const digest=await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,(campaignOn(env)?CAMPAIGN+':':'')+tgid+':'+record.email+':'+text);
    // Atomic comparison and increment makes an incorrect code one of five attempts.
    const result=await first(env,
      'UPDATE telegram_trial_codes SET attempts=attempts+1 WHERE telegram_id=? AND digest=? AND expires_at>? AND attempts<5 RETURNING email',
      tgid,digest,now);
    if(!result) {
      await env.ACCESS_DB.prepare('UPDATE telegram_trial_codes SET attempts=attempts+1 WHERE telegram_id=? AND expires_at>?').bind(tgid,now).run();
      await sendTelegram(env,tgid,'Codice errato o scaduto. Verifica le 6 cifre ricevute.');
      return;
    }
    try {
      const grant=await tryGrant(env,access,record.email,tgid,now,monthlyPrice);
      if(grant.type==='granted') {
        await env.ACCESS_DB.prepare('DELETE FROM telegram_trial_codes WHERE telegram_id=?').bind(tgid).run();
        await sendTelegram(env,tgid,
          '✅ Prova Gold attivata per 7 giorni!\n\n' +
          'Accedi con questa email: '+record.email+'\n' +
          'Scadenza: '+new Date(grant.expires_at*1000).toLocaleString('it-IT',{timeZone:'Europe/Rome'})+' (ora italiana)\n'+
          'https://arabsniperbet.com/gold-dashboard-live.html\n\n' +
          'Usa il codice email richiesto dal sito per accedere. Nessun addebito o rinnovo automatico.');
      } else if(grant.type==='unlimited') {
        await sendTelegram(env,tgid,'Il tuo accesso Gold è già senza scadenza: puoi provare subito il nuovo motore dal sito.');
      } else {
        await sendTelegram(env,tgid,grant.type==='already_active'?
          'La tua email ha già un accesso Gold attivo. Accedi dal sito con il codice email.':
          'La prova di 7 giorni è già stata richiesta per questa email o account Telegram.');
      }
    }catch {
      await sendTelegram(env,tgid,'Non riesco a completare la prova al momento. Riprova inserendo lo stesso codice prima della scadenza o contatta il supporto.');
    }
    return;
  }
  await sendTelegram(env,tgid,'Per la prova Gold gratuita scrivi /prova, poi invia la tua email e conferma il codice ricevuto.');
}
async function telegramTrialRoute(request,env,access,monthlyPrice,now=nowSeconds()) {
  if(new URL(request.url).pathname!=='/api/telegram-webhook')return null;
  if(request.method!=='POST')return new Response('Method not allowed',{status:405});
  if(env.TELEGRAM_TRIAL_ENABLED!=='true'||!env.ACCESS_DB||!env.TELEGRAM_BOT_TOKEN||
     !env.TELEGRAM_WEBHOOK_SECRET||!env.TELEGRAM_TRIAL_HMAC_SECRET||
     !env.STRIPE_SECRET_KEY||!env.RESEND_API_KEY||!env.EMAIL_FROM||!monthlyPrice)
    return new Response('Unavailable',{status:503});
  if(request.headers.get('X-Telegram-Bot-Api-Secret-Token')!==env.TELEGRAM_WEBHOOK_SECRET)
    return new Response('Forbidden',{status:403});
  const raw=await request.text();
  if(raw.length>16384)return new Response('Too large',{status:413});
  let update;try{update=JSON.parse(raw)}catch{return new Response('Invalid',{status:400})}
  // Never act on channel updates or on externally supplied email without a private chat.
  try {if(update.message)await processPrivateMessage(env,access,update.message,monthlyPrice,now)}
  catch {return new Response('Retry',{status:503})}
  return new Response('OK',{status:200});
}

return telegramTrialRoute;
})();

const goldTestLoginPage = (() => {
/** Minimal same-origin test-only Gold OTP harness; no secrets in HTML. */
function goldTestLoginPage(request,env) {
  const url=new URL(request.url);
  if(url.pathname!=='/test-login')return null;
  if(request.method!=='GET')return new Response('Method not allowed',{status:405});
  if(env.STRIPE_MODE!=='test'||url.origin!==env.AUTH_ORIGIN)
    return new Response('Not found',{status:404});
  const html=`<!doctype html><html lang="it"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ArabSniperBet · Test login Gold</title>
<style>
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,sans-serif;background:#0b1b2a;color:#fff;padding:30px 16px}
main{max-width:560px;margin:30px auto;padding:30px;background:#142d3a;border:1px solid #c59c3b;border-radius:16px}
h1{color:#f1c75d;font-size:26px;margin:0 0 12px}p{line-height:1.5;color:#d2dce3}
label{display:block;margin:18px 0 6px}input{width:100%;border-radius:8px;border:1px solid #acc2cc;background:white;color:#111;padding:13px;font-size:16px}
button{margin:14px 0;padding:13px 17px;border:0;border-radius:8px;background:#f1c75d;color:#142d3a;font-weight:750;cursor:pointer}
button:disabled{opacity:.6}#result{white-space:pre-wrap;overflow-wrap:anywhere;padding:16px;background:#081c23;border-radius:9px}
small{color:#b9c8d0}
</style></head><body><main><h1>ArabSniperBet · Collaudo Gold</h1>
<p>Area di test. Questo controllo usa lo stesso login con codice email e la stessa sessione dell'Area Gold, senza acquistare nulla.</p>
<label for="email">Email usata nella prova Telegram</label><input type="email" id="email" autocomplete="email" required>
<button id="request">Ricevi un nuovo codice</button>
<section id="verify" hidden><label for="code">Codice email (6 cifre)</label>
<input id="code" inputmode="numeric" pattern="[0-9]{6}" maxlength="6" autocomplete="one-time-code">
<button id="confirm">Verifica e controlla accesso</button></section>
<p id="result" role="status">Pronto. Richiedi un codice email.</p>
<small>Test su Stripe Sandbox. Non inoltrare né condividere il codice OTP. La prova gratuita scade automaticamente alla data configurata in Stripe.</small>
</main><script>
let challenge='';
const el=id=>document.getElementById(id),status=text=>{el('result').textContent=text;};
async function call(path,payload) {
 const response=await fetch(path,{method:'POST',credentials:'same-origin',
 headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
 const data=await response.json();
 if(!response.ok)throw Error(data.error||'Risposta non valida ('+response.status+')');
 return data;
}
el('request').onclick=async()=>{
 const email=el('email').value.trim().toLowerCase();
 if(!/^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$/.test(email)){status('Inserisci un indirizzo email valido.');return;}
 el('request').disabled=true;status('Richiesta codice in corso...');
 try {
   const data=await call('/api/auth/request-code',{email});
   if(!data.ok||!data.challenge)throw Error('Codice non richiesto');
   challenge=data.challenge;el('verify').hidden=false;
   status('Codice inviato. Inserisci le sei cifre ricevute nella tua casella email.');
 } catch(e){status('Errore: '+e.message);}
 finally{el('request').disabled=false;}
};
el('confirm').onclick=async()=>{
 const email=el('email').value.trim().toLowerCase();
 const code=el('code').value.trim();
 if(!/^[0-9]{6}$/.test(code)){status('Il codice deve avere 6 cifre.');return;}
 el('confirm').disabled=true;status('Verifica in corso...');
 try {
   const grant=await call('/api/auth/verify-code',{email,code,challenge});
   if(!grant.active){status('Email verificata, ma nessun accesso Gold attivo.');return;}
   const session=await fetch('/api/auth/session',{credentials:'same-origin'});
   const entitlement=await session.json();
   if(!session.ok||!entitlement.active)throw Error('Sessione non attiva');
   const exp=Number(entitlement.expires_at);
   const expiry=Number.isFinite(exp)&&exp>0?new Date(exp*1000).toLocaleString('it-IT'):'Non comunicata';
   status('✅ ACCESSO GOLD ATTIVO\\nFonte: '+entitlement.source+'\\nPiano: '+entitlement.plan+'\\nScadenza: '+expiry+'\\nSessione OTP verificata.');
 }catch(e){status('Errore: '+e.message);}
 finally{el('confirm').disabled=false;el('code').value='';}
};
</script></body></html>`;
  return new Response(html,{headers:{
    'Content-Type':'text/html; charset=utf-8',
    'Cache-Control':'no-store',
    'X-Content-Type-Options':'nosniff',
    'Content-Security-Policy':"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'",
    'Referrer-Policy':'no-referrer'
  }});
}


return goldTestLoginPage;
})();

const {renewalEnabled, cachedAccess, invalidateAccessCache, renewalStatus, renewalCheckout, renewalOrder, markRenewalPaid, closeFailedRenewal} = (() => {
// Session-bound renewals. No identity, price or entitlement is accepted from the client.
const renewalEnabled = env => env.GOLD_RENEWAL_ENABLED==='true';
async function first(env,sql,...args) {
 return (await env.ACCESS_DB.prepare(sql).bind(...args).all()).results?.[0]||null;
}
async function invalidateAccessCache(env,email) {
 if(renewalEnabled(env))await env.ACCESS_DB.prepare('DELETE FROM gold_access_cache WHERE email=?').bind(email).run();
}
async function cachedAccess(env,email,now,load,fresh=false) {
 if(!renewalEnabled(env))return load();
 if(!fresh) {
  const cached=await first(env,'SELECT payload FROM gold_access_cache WHERE email=? AND valid_until>?',email,now);
  if(cached)return JSON.parse(cached.payload);
 }
 const value=await load();
 // The expiry is an absolute upper bound; never cache access beyond it.
 const until=Math.min(now+300,value.active&&value.expires_at?value.expires_at:now+300);
 await env.ACCESS_DB.prepare('INSERT INTO gold_access_cache(email,payload,valid_until) VALUES (?,?,?) ON CONFLICT(email) DO UPDATE SET payload=excluded.payload,valid_until=excluded.valid_until')
  .bind(email,JSON.stringify(value),until).run();
 return value;
}
function promoConfigured(env) {return renewalEnabled(env)&&env.GOLD_PROMO_ENABLED==='true'&&!!env.GOLD_RENEWAL_COUPON_ID;}
function checkoutReady(env,catalog) {
 return renewalEnabled(env)&&env.CHECKOUT_ENABLED==='true'&&['live','test'].includes(env.STRIPE_MODE)&&!!env.STRIPE_WEBHOOK_SECRET&&!!env.STRIPE_SECRET_KEY&&Object.values(catalog).every(p=>p.price);
}
function adminAllowed(env,email) {
 try{return JSON.parse(env.GOLD_PROMO_ALLOW_EMAILS||'[]').some(e=>String(e).trim().toLowerCase()===email);}catch{return false;}
}
function promoEligible(env,email,value,now) {
 const left=Number(value.expires_at)-now;
 return promoConfigured(env)&&value.active&&left>0&&left<=48*3600&&(value.has_verified_payment===true||adminAllowed(env,email));
}
async function renewalStatus(env,email,value,now,catalog) {
 const ready=checkoutReady(env,catalog);
 let eligible=ready&&promoEligible(env,email,value,now);
 if(eligible) {
  const used=await first(env,"SELECT id FROM gold_renewals WHERE email=? AND cycle_expires_at=? AND promo=1 AND status!='expired'",email,value.expires_at);
  eligible=!used;
 }
 const pending=ready?await first(env,"SELECT checkout_url,expires_at FROM gold_renewals WHERE email=? AND status='open' AND expires_at>?",email,now):null;
 const {has_verified_payment,...entitlement}=value;
 return {
  ...entitlement,email,authenticated:true,server_time:now,
  renewal:{enabled:ready,promo_eligible:eligible,promo_percent:eligible?10:0,checkout_lifetime_seconds:3600,
   pending_checkout:pending?{url:pending.checkout_url,expires_at:pending.expires_at}:null,
   plans:Object.entries(catalog).map(([key,p])=>({key,label:{pass3d:'Gold 3 giorni',pass14d:'Gold 14 giorni',pass1m:'Gold mensile'}[key],amount:p.amount,payable_amount:eligible?Math.round(p.amount*.9):p.amount}))}
 };
}
async function renewalCheckout(request,env,email,now,{access,catalog,stripe}) {
 const answer=(error,status)=>({error,status});
 if(!checkoutReady(env,catalog))return answer('Rinnovi in preparazione. Contatta l’assistenza.',503);
 if(!request.headers.get('Content-Type')?.startsWith('application/json'))return answer('Formato non valido.',415);
 const raw=await request.text();if(raw.length>1024)return answer('Richiesta troppo grande.',413);
 let body;try{body=JSON.parse(raw);}catch{return answer('Richiesta non valida.',400);}
 const p=Object.hasOwn(catalog,body?.plan)&&catalog[body.plan];
 if(!p?.price||typeof body.use_promo!=='boolean')return answer('Piano non valido.',400);
 const limit=await first(env,`INSERT INTO gold_limits(key,hits,expires_at) VALUES (?,1,?)
  ON CONFLICT(key) DO UPDATE SET hits=hits+1 RETURNING hits`,'renew:'+email+':'+Math.floor(now/600),now+600);
 if(limit.hits>20)return answer('Troppi tentativi. Riprova tra qualche minuto.',429);
 // A live pending order is reused. Changing packages first expires the old Stripe session.
 let pending=await first(env,"SELECT * FROM gold_renewals WHERE email=? AND status IN ('creating','open')",email);
 if(pending?.checkout_id&&(pending.expires_at<=now||pending.plan!==body.plan||!!pending.promo!==body.use_promo)) {
  const path='/checkout/sessions/'+encodeURIComponent(pending.checkout_id);
  let s=await stripe(env,path);
  if(s.status==='open'&&s.payment_status==='unpaid')s=await stripe(env,path+'/expire',{},'POST');
  if(s.status!=='expired'||s.payment_status!=='unpaid')return answer('Pagamento in verifica. Attendi la conferma e aggiorna lo stato.',409);
  await env.ACCESS_DB.prepare("UPDATE gold_renewals SET status='expired' WHERE id=? AND status IN ('creating','open')").bind(pending.id).run();
  pending=null;
 }
 if(pending&&(pending.plan!==body.plan||!!pending.promo!==body.use_promo))return answer('Un Checkout è in preparazione. Riprova con lo stesso pacchetto.',409);
 if(pending?.checkout_id)return {url:pending.checkout_url,expires_at:pending.expires_at};
 if(!pending) {
  // Force fresh Stripe-backed entitlement before reserving any promotion.
  const value=await access(env,email,now);
  if(body.use_promo&&!promoEligible(env,email,value,Math.floor(Date.now()/1000)))return answer('Promozione non disponibile per questo account.',403);
  const promo=body.use_promo?1:0;
  if(promo) {
   const used=await first(env,"SELECT id FROM gold_renewals WHERE email=? AND cycle_expires_at=? AND promo=1 AND status!='expired'",email,value.expires_at);
   if(used)return answer('Promozione già utilizzata per questa scadenza.',409);
   const coupon=await stripe(env,'/coupons/'+encodeURIComponent(env.GOLD_RENEWAL_COUPON_ID));
   // Restrictive/mismatched coupons fail closed; standard prices remain untouched.
   if(!coupon.valid||coupon.percent_off!==10||coupon.amount_off||coupon.duration!=='once'||coupon.applies_to?.products?.length||coupon.livemode!==(env.STRIPE_MODE==='live'))
    return answer('Promozione temporaneamente non disponibile. Scegli il prezzo ordinario.',503);
  }
  const created=Math.floor(Date.now()/1000);
  if(promo&&!promoEligible(env,email,value,created))return answer('La finestra promozionale è terminata. Aggiorna lo stato.',409);
  const orderId=crypto.randomUUID(),until=created+3600;
  const origin=env.AUTH_ORIGIN;
  const params={mode:'payment','line_items[0][price]':p.price,'line_items[0][quantity]':'1',customer_email:email,customer_creation:'always','adaptive_pricing[enabled]':'false',
   success_url:origin+'/gold-dashboard-live.html?renewal=success',cancel_url:origin+'/gold-dashboard-live.html?renewal=canceled',
   client_reference_id:orderId,'metadata[gold_renewal_id]':orderId,'metadata[plan]':body.plan,expires_at:String(until),integration_identifier:'arabsniper_gold_qtvrnzkp'};
  if(promo)params['discounts[0][coupon]']=env.GOLD_RENEWAL_COUPON_ID;
  await env.ACCESS_DB.prepare(`INSERT OR IGNORE INTO gold_renewals(id,email,plan,cycle_expires_at,promo,amount,created_at,expires_at,stripe_params,status)
   VALUES (?,?,?,?,?,?,?,?,?,'creating')`).bind(orderId,email,body.plan,value.expires_at||0,promo,promo?Math.round(p.amount*.9):p.amount,created,until,JSON.stringify(params)).run();
  pending=await first(env,"SELECT * FROM gold_renewals WHERE email=? AND status IN ('creating','open')",email);
  if(!pending)return answer('Promozione già prenotata. Aggiorna lo stato.',409);
  if(pending.plan!==body.plan||!!pending.promo!==body.use_promo)return answer('Un altro Checkout è in preparazione. Aggiorna lo stato.',409);
 }
 // Preserve the exact parameters and key on retries, including ambiguous network failures.
 // Never retry a creation after Stripe's minimum 24h idempotency retention has elapsed.
 if(now-pending.created_at>=23*3600)return answer('Pagamento da verificare con l’assistenza.',503);
 const s=await stripe(env,'/checkout/sessions',JSON.parse(pending.stripe_params),'POST','gold-renewal-'+pending.id);
 if(s.livemode!==(env.STRIPE_MODE==='live')||!s.id||s.client_reference_id!==pending.id||s.expires_at!==pending.expires_at)throw Error('invalid_checkout');
 const windowClosed=pending.promo&&!(Number.isInteger(s.created)&&s.created<pending.cycle_expires_at);
 if(windowClosed||s.mode!=='payment'||s.status!=='open'||s.currency!=='eur'||s.amount_total!==pending.amount||s.amount_subtotal!==p.amount||s.total_details?.amount_discount!==p.amount-pending.amount||!String(s.url).startsWith('https://checkout.stripe.com/')) {
  // Do not redirect to a Checkout whose displayed amount differs from the offer.
  if(s.status==='open'&&s.payment_status==='unpaid') {
   await stripe(env,'/checkout/sessions/'+encodeURIComponent(s.id)+'/expire',{},'POST');
   await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='expired' WHERE id=? AND status='creating'").bind(s.id,pending.id).run();
  }
  return windowClosed?answer('La finestra promozionale è terminata. Aggiorna lo stato.',409):answer('Importo del Checkout non confermato. Riprova o contatta l’assistenza.',503);
 }
 await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,checkout_url=?,status=CASE WHEN status='creating' THEN 'open' ELSE status END WHERE id=?")
  .bind(s.id,s.url,pending.id).run();
 if(pending.expires_at<=now)return answer('Il Checkout precedente è scaduto. Riprova per aprirne uno nuovo.',409);
 return {url:s.url,expires_at:pending.expires_at};
}
async function renewalOrder(env,session) {
 const orderId=session.metadata?.gold_renewal_id;
 if(!orderId)return null;
 const row=await first(env,'SELECT * FROM gold_renewals WHERE id=?',orderId);
 if(!row||session.client_reference_id!==row.id||(row.checkout_id&&row.checkout_id!==session.id)||session.created<row.created_at||session.expires_at!==row.expires_at)throw Error('invalid_renewal_order');
 return row;
}
async function markRenewalPaid(env,order,session,paidAt) {
 if(!order)return;
 await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='fulfilled',payment_intent=?,fulfilled_at=? WHERE id=? AND status!='fulfilled'")
  .bind(session.id,typeof session.payment_intent==='string'?session.payment_intent:session.payment_intent.id,paidAt,order.id).run();
}
async function closeFailedRenewal(env,session) {
 const order=await renewalOrder(env,session);
 if(order)await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='expired' WHERE id=? AND status IN ('creating','open')")
  .bind(session.id,order.id).run();
}


return {renewalEnabled,cachedAccess,invalidateAccessCache,renewalStatus,renewalCheckout,renewalOrder,markRenewalPaid,closeFailedRenewal};
})();




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

