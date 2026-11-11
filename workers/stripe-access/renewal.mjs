// Session-bound renewals. No identity, price or entitlement is accepted from the client.
export const renewalEnabled = env => env.GOLD_RENEWAL_ENABLED==='true';
async function first(env,sql,...args) {
 return (await env.ACCESS_DB.prepare(sql).bind(...args).all()).results?.[0]||null;
}
export async function invalidateAccessCache(env,email) {
 if(renewalEnabled(env))await env.ACCESS_DB.prepare('DELETE FROM gold_access_cache WHERE email=?').bind(email).run();
}
export async function cachedAccess(env,email,now,load,fresh=false) {
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
export async function renewalStatus(env,email,value,now,catalog) {
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
export async function renewalCheckout(request,env,email,now,{access,catalog,stripe}) {
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
  if(body.use_promo&&!promoEligible(env,email,value,now))return answer('Promozione non disponibile per questo account.',403);
  const promo=body.use_promo?1:0;
  if(promo) {
   const used=await first(env,"SELECT id FROM gold_renewals WHERE email=? AND cycle_expires_at=? AND promo=1 AND status!='expired'",email,value.expires_at);
   if(used)return answer('Promozione già utilizzata per questa scadenza.',409);
   const coupon=await stripe(env,'/coupons/'+encodeURIComponent(env.GOLD_RENEWAL_COUPON_ID));
   // Restrictive/mismatched coupons fail closed; standard prices remain untouched.
   if(!coupon.valid||coupon.percent_off!==10||coupon.amount_off||coupon.duration!=='once'||coupon.applies_to?.products?.length||coupon.livemode!==(env.STRIPE_MODE==='live'))
    return answer('Promozione temporaneamente non disponibile. Scegli il prezzo ordinario.',503);
  }
  const orderId=crypto.randomUUID(),until=now+3600;
  const origin=env.AUTH_ORIGIN;
  const params={mode:'payment','line_items[0][price]':p.price,'line_items[0][quantity]':'1',customer_email:email,customer_creation:'always','adaptive_pricing[enabled]':'false',
   success_url:origin+'/gold-dashboard-live.html?renewal=success',cancel_url:origin+'/gold-dashboard-live.html?renewal=canceled',
   client_reference_id:orderId,'metadata[gold_renewal_id]':orderId,'metadata[plan]':body.plan,expires_at:String(until),integration_identifier:'arabsniper_gold_qtvrnzkp'};
  if(promo)params['discounts[0][coupon]']=env.GOLD_RENEWAL_COUPON_ID;
  await env.ACCESS_DB.prepare(`INSERT OR IGNORE INTO gold_renewals(id,email,plan,cycle_expires_at,promo,amount,created_at,expires_at,stripe_params,status)
   VALUES (?,?,?,?,?,?,?,?,?,'creating')`).bind(orderId,email,body.plan,value.expires_at||0,promo,promo?Math.round(p.amount*.9):p.amount,now,until,JSON.stringify(params)).run();
  pending=await first(env,"SELECT * FROM gold_renewals WHERE email=? AND status IN ('creating','open')",email);
  if(!pending)return answer('Promozione già prenotata. Aggiorna lo stato.',409);
  if(pending.plan!==body.plan||!!pending.promo!==body.use_promo)return answer('Un altro Checkout è in preparazione. Aggiorna lo stato.',409);
 }
 // Preserve the exact parameters and key on retries, including ambiguous network failures.
 // Never retry a creation after Stripe's minimum 24h idempotency retention has elapsed.
 if(now-pending.created_at>=23*3600)return answer('Pagamento da verificare con l’assistenza.',503);
 const s=await stripe(env,'/checkout/sessions',JSON.parse(pending.stripe_params),'POST','gold-renewal-'+pending.id);
 if(s.livemode!==(env.STRIPE_MODE==='live')||!s.id||s.client_reference_id!==pending.id||s.expires_at!==pending.expires_at)throw Error('invalid_checkout');
 if(s.mode!=='payment'||s.status!=='open'||s.currency!=='eur'||s.amount_total!==pending.amount||s.amount_subtotal!==p.amount||s.total_details?.amount_discount!==p.amount-pending.amount||!String(s.url).startsWith('https://checkout.stripe.com/')) {
  // Do not redirect to a Checkout whose displayed amount differs from the offer.
  if(s.status==='open'&&s.payment_status==='unpaid') {
   await stripe(env,'/checkout/sessions/'+encodeURIComponent(s.id)+'/expire',{},'POST');
   await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='expired' WHERE id=? AND status='creating'").bind(s.id,pending.id).run();
  }
  return answer('Importo del Checkout non confermato. Riprova o contatta l’assistenza.',503);
 }
 await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,checkout_url=?,status=CASE WHEN status='creating' THEN 'open' ELSE status END WHERE id=?")
  .bind(s.id,s.url,pending.id).run();
 if(pending.expires_at<=now)return answer('Il Checkout precedente è scaduto. Riprova per aprirne uno nuovo.',409);
 return {url:s.url,expires_at:pending.expires_at};
}
export async function renewalOrder(env,session) {
 const orderId=session.metadata?.gold_renewal_id;
 if(!orderId)return null;
 const row=await first(env,'SELECT * FROM gold_renewals WHERE id=?',orderId);
 if(!row||session.client_reference_id!==row.id||(row.checkout_id&&row.checkout_id!==session.id)||session.created<row.created_at||session.expires_at!==row.expires_at)throw Error('invalid_renewal_order');
 return row;
}
export async function markRenewalPaid(env,order,session,paidAt) {
 if(!order)return;
 await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='fulfilled',payment_intent=?,fulfilled_at=? WHERE id=? AND status!='fulfilled'")
  .bind(session.id,typeof session.payment_intent==='string'?session.payment_intent:session.payment_intent.id,paidAt,order.id).run();
}
export async function closeFailedRenewal(env,session) {
 const order=await renewalOrder(env,session);
 if(order)await env.ACCESS_DB.prepare("UPDATE gold_renewals SET checkout_id=?,status='expired' WHERE id=? AND status IN ('creating','open')")
  .bind(session.id,order.id).run();
}
