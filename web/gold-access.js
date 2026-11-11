/* Account status is a server snapshot. The browser countdown never grants access. */
(() => {
 'use strict';
 const $=id=>document.getElementById(id);
 let value=null,at=0,timer=null,busy=false,refreshing=false,wasLocked=false,expiryChecked=false,windowChecked=false;
 const money=amount=>new Intl.NumberFormat('it-IT',{style:'currency',currency:'EUR'}).format(amount/100);
 const serverNow=()=>value.server_time+(performance.now()-at)/1000;
 function message(text,error=false){$('goldAccessMessage').textContent=text;$('goldAccessMessage').dataset.error=String(error);}
 function lock(locked) {
  $('app').classList.toggle('gold-access-locked',locked);$('goldAccessLockNote').hidden=!locked;
  if(locked&&!wasLocked)window.dispatchEvent(new Event('gold-access-expired'));
  if(!locked&&wasLocked)window.dispatchEvent(new Event('gold-access-restored'));
  wasLocked=locked;
 }
 function tick() {
  if(!value)return;
  const now=serverNow(),left=Math.max(0,Math.ceil(Number(value.expires_at)-now));
  if(value.session_expires_at&&now>=value.session_expires_at) {
   stop();window.dispatchEvent(new Event('gold-session-expired'));return;
  }
  const active=value.active&&(!value.expires_at||left>0);
  const tone=!active?'expired':!value.expires_at||left>172800?'green':left>43200?'yellow':'red';
  $('goldAccess').dataset.tone=tone;
  $('goldAccessState').textContent=!active?'SCADUTO':tone==='green'?'ATTIVO':'IN SCADENZA';
  $('goldAccessRemaining').textContent=!active?'Accesso terminato':!value.expires_at?'Senza scadenza':`${Math.floor(left/86400)} g · ${Math.floor(left%86400/3600)} h · ${Math.floor(left%3600/60)} min`;
  $('goldRenewalSummary').textContent=active?'Rinnova il tuo accesso':'Acquista un nuovo pacchetto';
  $('goldAccessPromo').hidden=!(active&&left<=172800&&value.renewal?.promo_eligible);
  lock(!active);
  if(value.active&&value.expires_at&&!left&&!expiryChecked){expiryChecked=true;void refresh(true);}
  if(active&&left>0&&left<=172800&&value.expires_at-value.server_time>172800&&!windowChecked){windowChecked=true;void refresh();}
 }
 function paintPlans() {
  const select=$('goldRenewalPlan'),selected=select.value;
  select.replaceChildren();
  const promo=value?.renewal?.promo_eligible&&$('goldRenewalDiscount').checked;
  for(const p of value?.renewal?.plans||[]) {
   const option=document.createElement('option');option.value=p.key;
   option.textContent=p.label+' · '+money(promo?p.payable_amount:p.amount)+(promo?' (−10%)':'');
   select.append(option);
  }
  if([...select.options].some(p=>p.value===selected))select.value=selected;
 }
 function accept(data) {
  // Older Worker responses still permit the existing login while rollout is staged.
  if(!data?.email||!Number.isFinite(data.server_time))return data?.active===true;
  const wasEligible=value?.renewal?.promo_eligible;
  value=data;at=performance.now();expiryChecked=false;windowChecked=false;
  $('goldAccess').hidden=false;$('goldAccessEmail').textContent=data.email;
  $('goldAccessExpiry').textContent=data.expires_at?new Intl.DateTimeFormat('it-IT',{timeZone:'Europe/Rome',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(data.expires_at*1000).replace(',',''):data.active?'Senza scadenza':'—';
  $('goldAccessZone').textContent=data.expires_at?'Europe/Rome':'';
  const renew=data.renewal;
  $('goldRenewalOptions').hidden=!renew?.enabled;
  $('goldRenewalUnavailable').hidden=!!renew?.enabled;
  $('goldRenewalDiscountLabel').hidden=!renew?.promo_eligible;
  if(!wasEligible||!renew?.promo_eligible)$('goldRenewalDiscount').checked=!!renew?.promo_eligible;
  paintPlans();
  $('goldCheckoutResume').hidden=true;
  if(renew?.pending_checkout?.url?.startsWith('https://checkout.stripe.com/')) {
   $('goldCheckoutResume').href=renew.pending_checkout.url;$('goldCheckoutResume').hidden=false;
  }
  clearInterval(timer);tick();if(value)timer=setInterval(tick,1000);
  return data.active&&(!data.expires_at||data.expires_at>data.server_time);
 }
 async function request(path,body) {
  const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),20000);
  try {
   const res=await fetch('/api/auth/'+path,{method:body?'POST':'GET',credentials:'same-origin',cache:'no-store',signal:controller.signal,
    headers:body?{'Content-Type':'application/json'}:{},...(body?{body:JSON.stringify(body)}:{})});
   const data=await res.json();
   if(!res.ok){const err=new Error(data.error||'Stato non disponibile. Riprova.');err.status=res.status;throw err;}
   return data;
  }finally{clearTimeout(timeout);}
 }
 async function refresh(fresh=false) {
  if(refreshing||!value)return;
  refreshing=true;
  try {accept(await request('session'+(fresh?'?refresh=1':'')));}
  catch(error) {
   message(error.name==='AbortError'?'Connessione lenta. Premi Aggiorna stato per riprovare.':error.message,true);
   if(error.status===401){stop();window.dispatchEvent(new Event('gold-session-expired'));}
  }finally{refreshing=false;}
 }
 async function checkout() {
  if(busy||!value)return;
  busy=true;$('goldRenewalPay').disabled=true;message('Apertura del pagamento sicuro…');
  try {
   const data=await request('renew',{plan:$('goldRenewalPlan').value,use_promo:!!value.renewal?.promo_eligible&&$('goldRenewalDiscount').checked});
   if(!data.url?.startsWith('https://checkout.stripe.com/'))throw new Error('Pagamento non disponibile. Riprova.');
   window.location.assign(data.url);
  }catch(error){message(error.name==='AbortError'?'Conferma in ritardo. Riprova: il pagamento già creato verrà riutilizzato.':error.message,true);}
  finally{busy=false;$('goldRenewalPay').disabled=false;}
 }
 function deny(){if(!value)return false;value={...value,active:false};tick();return true;}
 function stop(){clearInterval(timer);timer=null;value=null;wasLocked=false;$('goldAccess').hidden=true;$('goldAccessEmail').textContent='';}
 window.GoldAccess={accept,refresh,deny,stop};
 $('goldRenewalPay').addEventListener('click',checkout);
 $('goldRenewalDiscount').addEventListener('change',paintPlans);
 $('goldAccessRefresh').addEventListener('click',()=>refresh(true));
 setInterval(()=>{if(!document.hidden)void refresh();},300000);
 window.addEventListener('focus',()=>{if(value&&performance.now()-at>30000)void refresh();});
 document.addEventListener('visibilitychange',()=>{if(!document.hidden&&value)void refresh();});
 const returned=new URLSearchParams(location.search).get('renewal');
 if(returned==='success') {
  message('Rientro da Stripe. Se il pagamento è ancora in verifica, la scadenza si aggiornerà alla conferma.');
  // Bounded polling reads status only; the success URL has no fulfillment authority.
  for(const ms of [5000,10000,15000])setTimeout(()=>refresh(true),ms);
 } else if(returned==='canceled')message('Pagamento annullato. Puoi scegliere un pacchetto e riprovare.');
})();
