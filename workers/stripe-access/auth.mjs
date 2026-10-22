// Staged OTP authentication. Activate only after email delivery and private data cutover.
const COOKIE = '__Host-gold_session';
const encoder = new TextEncoder();
const hex = bytes => Array.from(new Uint8Array(bytes), b=>b.toString(16).padStart(2,'0')).join('');
const token = () => hex(crypto.getRandomValues(new Uint8Array(32)));
const hash = async value => hex(await crypto.subtle.digest('SHA-256',encoder.encode(value)));
async function mac(secret,value) {
 const key=await crypto.subtle.importKey('raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
 return hex(await crypto.subtle.sign('HMAC',key,encoder.encode(value)));
}
export function generateCode() {
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
export async function sessionEmail(request,env,now=Math.floor(Date.now()/1000)) {
 const t=request.headers.get('Cookie')?.split(';').map(s=>s.trim()).find(s=>s.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);
 if(!t||!/^[a-f0-9]{64}$/.test(t))return null;
 const row=await first(env,'SELECT email FROM gold_sessions WHERE digest=? AND expires_at>?',await hash(t),now);
 return row?.email||null;
}
export async function authRoute(request,env,access,now=Math.floor(Date.now()/1000)) {
 const url=new URL(request.url),path=url.pathname;
 if(!path.startsWith('/api/auth/')&&!path.startsWith('/api/gold-data/'))return null;
 if(env.AUTH_ENABLED!=='true'||!env.ACCESS_DB||!env.OTP_SECRET||!env.RESEND_API_KEY||!env.EMAIL_FROM||!env.GOLD_DATA)
   return reply({error:'Accesso con codice in preparazione.'},503);
 // Same-origin only: cookies must belong to the website, never workers.dev.
 if(url.origin!==env.AUTH_ORIGIN)return reply({error:'Dominio non valido.'},403);
 if(request.method==='POST'&&request.headers.get('Origin')!==url.origin)return reply({error:'Origine non valida.'},403);
 try {
  if(path==='/api/auth/session'&&request.method==='GET') {
   const email=await sessionEmail(request,env,now);
   if(!email)return reply({active:false},401);
   return reply({...await access(env,email,now),authentication:'email_otp'});
  }
  if(path==='/api/auth/logout'&&request.method==='POST') {
   const t=request.headers.get('Cookie')?.split(';').map(s=>s.trim()).find(s=>s.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);
   if(t)await env.ACCESS_DB.prepare('DELETE FROM gold_sessions WHERE digest=?').bind(await hash(t)).run();
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
  const t=token();await env.ACCESS_DB.prepare('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)').bind(await hash(t),email,now+604800).run();
  return reply({...entitlement,authentication:'email_otp'},200,{'Set-Cookie':cookie(t,604800)});
 }catch{return reply({error:'Servizio accessi temporaneamente non disponibile.'},503);}
}
export async function cleanupAuth(env,now=Math.floor(Date.now()/1000)) {
 for(const table of ['gold_codes','gold_sessions','gold_limits'])await env.ACCESS_DB.prepare(`DELETE FROM ${table} WHERE expires_at<=?`).bind(now).run();
}
