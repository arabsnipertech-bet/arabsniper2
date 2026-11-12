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
// Campaign grant history is a permanent eligibility ledger; never purge on expiry.
// The current campaign keeps its existing retry/idempotency handling.
async function freePromotionUsed(env,email,tgid) {
  return !!await first(env,
    "SELECT campaign FROM telegram_campaign_claims WHERE status='granted' AND (email=? OR telegram_id=?) AND (?='' OR campaign<>?) LIMIT 1",
    email,tgid,campaignOn(env)?CAMPAIGN:'',campaignOn(env)?CAMPAIGN:'');
}
const FREE_USED='Hai già usufruito della promozione gratuita Gold. Non sono disponibili ulteriori prove gratuite. Accedi alla tua area Gold per le offerte di rinnovo disponibili: https://arabsniperbet.com/gold-dashboard-live.html';
async function tryGrant(env,access,email,tgid,now,monthlyPrice) {
  if(await freePromotionUsed(env,email,tgid))return {type:'free_promotion_used'};
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
      (campaignOn(env)?'Nuovo motore: altri 7 giorni anche se hai già provato Gold! Una sola attivazione; chi ne usufruisce non potrà aderire a future prove gratuite. Se hai un accesso attivo, i giorni si aggiungono alla scadenza.\n':'Una sola prova per persona/email.\n') +
      'Nessuna carta, nessun addebito e nessun rinnovo automatico della prova.\n' +
      'Accesso al sito: https://arabsniperbet.com/gold-dashboard-live.html');
    return;
  }
  if(EMAIL.test(text)&&text.length<=254) {
    const email=lower(text);
    if(await freePromotionUsed(env,email,tgid)){await sendTelegram(env,tgid,FREE_USED);return;}
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
      } else if(grant.type==='free_promotion_used') {
        await sendTelegram(env,tgid,FREE_USED);
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
