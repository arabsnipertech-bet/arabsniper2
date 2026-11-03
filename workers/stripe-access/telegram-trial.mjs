/* Telegram DM -> verified email -> no-card, non-renewing Stripe 7-day Gold trial.
   Disabled unless explicitly enabled on the production Cloudflare Worker.
   Never activate trials from channel messages, unverified emails, or arbitrary web requests. */
const DAYS7=7*86400;
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
async function tryGrant(env,access,email,tgid,now,monthlyPrice) {
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
     Number(subscription.cancel_at)!==Number(claim.expires_at))
    throw Error('trial_safety_not_confirmed');
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
      'Una sola prova per persona/email. Nessuna carta, nessun addebito e nessun rinnovo automatico.\n' +
      'Accesso al sito: https://arabsniperbet.com/gold-dashboard-live.html');
    return;
  }
  if(EMAIL.test(text)&&text.length<=254) {
    const email=lower(text);
    const existing=await first(env,'SELECT status FROM telegram_trial_claims WHERE email=? OR telegram_id=? LIMIT 1',email,tgid);
    if(existing){
      await sendTelegram(env,tgid,'Questa email o questo account Telegram ha già richiesto la prova gratuita. Per assistenza contatta il supporto ArabSniperBet.');
      return;
    }
    const previous=await first(env,'SELECT sent_at FROM telegram_trial_codes WHERE telegram_id=?',tgid);
    if(previous&&previous.sent_at>now-120){
      await sendTelegram(env,tgid,'Attendi almeno due minuti prima di richiedere un altro codice.');
      return;
    }
    const code=otp();
    const digest=await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,tgid+':'+email+':'+code);
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
    const digest=await mac(env.TELEGRAM_TRIAL_HMAC_SECRET,tgid+':'+record.email+':'+text);
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
          'https://arabsniperbet.com/gold-dashboard-live.html\n\n' +
          'Usa il codice email richiesto dal sito per accedere. Nessun addebito o rinnovo automatico.');
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
export async function telegramTrialRoute(request,env,access,monthlyPrice,now=nowSeconds()) {
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
