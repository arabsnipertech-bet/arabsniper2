/** Minimal same-origin test-only Gold OTP harness; no secrets in HTML. */
export function goldTestLoginPage(request,env) {
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
