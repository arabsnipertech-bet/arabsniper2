// Archive-only routes: no fetch, collector, prediction engine or scheduler.
const FILTERS=new Set(['team','league','market','oddMin','oddMax','probMin','evMin','lowMin','ociMin','reliability','category','official','period','from','to','diversify']);
const json=(value,status=200)=>Response.json(value,{status,headers:{'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'}});
const digest=s=>typeof s==='string'&&/^[a-f0-9]{64}$/.test(s);
const first=async(db,sql,...values)=>(await db.prepare(sql).bind(...values).all()).results?.[0];
export function filters(value){
 if(!value||typeof value!=='object'||Array.isArray(value))throw Error('filters');
 for(const [k,v] of Object.entries(value))if(!FILTERS.has(k)||!['string','number','boolean'].includes(typeof v)||String(v).length>160||typeof v==='number'&&!Number.isFinite(v))throw Error('filters');
 return value;
}
async function body(request){
 if(!request.headers.get('Content-Type')?.startsWith('application/json'))throw Error('body');
 const reader=request.body?.getReader();if(!reader)throw Error('body');
 const chunks=[];let size=0;
 for(;;){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>8192){await reader.cancel();throw Error('body');}chunks.push(value);}
 const bytes=new Uint8Array(size);let offset=0;for(const c of chunks){bytes.set(c,offset);offset+=c.length;}
 return JSON.parse(new TextDecoder().decode(bytes));
}
async function archive(env,key){
 const item=await env.GOLD_DATA.get('explorer/'+key);if(!item)return null;
 if(key.startsWith('objects/')){
  const bytes=await item.arrayBuffer();const actual=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
  if(actual!==key.slice(8,-5))throw Error('checksum');
  return JSON.parse(new TextDecoder().decode(bytes));
 }
 return item.json();
}
export async function explorerRoute(request,env,{sessionEmail,access},now=Math.floor(Date.now()/1000)){
 const u=new URL(request.url),path=u.pathname.replace(/^\/api\/auth\/explorer\//,'/api/explorer/');
 if(!path.startsWith('/api/explorer/'))return null;
 if(env.AUTH_ENABLED!=='true'||env.GOLD_EXPLORER_ENABLED!=='true'||!env.ACCESS_DB||!env.GOLD_DATA)return json({error:'Explorer in preparazione.'},503);
 if(u.origin!==env.AUTH_ORIGIN||request.method!=='GET'&&request.headers.get('Origin')!==u.origin)return json({error:'Origine non valida.'},403);
 try{
  const email=await sessionEmail(request,env,now);if(!email)return json({error:'Accedi dalla Gold con il codice email.'},401);
  if(!(await access(env,email,now)).active)return json({error:'Accesso Gold non attivo.'},403);
  const db=env.ACCESS_DB;
  if(request.method==='GET'){
   let file;const day=path.match(/^\/api\/explorer\/days\/(\d{4}-\d{2}-\d{2})$/),object=path.match(/^\/api\/explorer\/objects\/([a-f0-9]{64})$/);
   if(path==='/api/explorer/current')file='current.json';if(path==='/api/explorer/catalog')file='catalog.json';
   if(day)file='days/'+day[1]+'.json';if(object)file='objects/'+object[1]+'.json';
   if(file){const data=await archive(env,file);return data?json(data):json({error:'Archivio non ancora disponibile.'},404);}
   if(path==='/api/explorer/searches'){
    const items=(await db.prepare('SELECT id,name,filters,updated_at FROM explorer_searches WHERE email=? ORDER BY updated_at DESC,id').bind(email).all()).results||[];
    return json({items:items.map(r=>({...r,filters:JSON.parse(r.filters)}))});
   }
   if(path==='/api/explorer/favorites'){
    const offset=Number(u.searchParams.get('offset')||0);if(!Number.isSafeInteger(offset)||offset<0||offset>500)return json({error:'Pagina non valida.'},400);
    const saved=(await db.prepare('SELECT id,snapshot,saved_at FROM explorer_favorites WHERE email=? ORDER BY saved_at DESC,id LIMIT 21 OFFSET ?').bind(email,offset).all()).results||[];
    const items=saved.slice(0,20).map(r=>({...r,snapshot:JSON.parse(r.snapshot)}));
    const days=new Map(await Promise.all([...new Set(items.map(r=>r.snapshot.date))].map(async d=>[d,await archive(env,'days/'+d+'.json')])));
    return json({items:items.map(r=>({...r,result:days.get(r.snapshot.date)?.results?.[r.snapshot.fixture_id]||null,
      references:days.get(r.snapshot.date)?.market_references?.[r.id]||{}})),next:saved.length>20?offset+20:null});
   }
  }
  if(request.method==='POST'){
   const count=await first(db,'INSERT INTO gold_limits(key,hits,expires_at) VALUES (?,1,?) ON CONFLICT(key) DO UPDATE SET hits=hits+1 RETURNING hits','explorer:'+email+':'+Math.floor(now/60),now+60);
   if(count.hits>30)return json({error:'Attendi un minuto prima di salvare ancora.'},429);
   let input;try{input=await body(request);}catch{return json({error:'Richiesta non valida o troppo grande.'},400);}
   if(path==='/api/explorer/searches'){
    const name=typeof input.name==='string'?input.name.trim():'';if(!name||name.length>80)return json({error:'Nome richiesto, massimo 80 caratteri.'},400);
    let validated;try{validated=filters(input.filters);}catch{return json({error:'Filtri non validi.'},400);}
    const id=input.id||crypto.randomUUID();if(!/^[a-f0-9-]{36}$/.test(id))return json({error:'Identificativo non valido.'},400);
    await db.prepare(`INSERT INTO explorer_searches(email,id,name,filters,updated_at) SELECT ?,?,?,?,?
     WHERE (SELECT COUNT(*) FROM explorer_searches WHERE email=?)<50 OR EXISTS(SELECT 1 FROM explorer_searches WHERE email=? AND id=?)
     ON CONFLICT(email,id) DO UPDATE SET name=excluded.name,filters=excluded.filters,updated_at=excluded.updated_at`)
     .bind(email,id,name,JSON.stringify(validated),now,email,email,id).run();
    return await first(db,'SELECT id FROM explorer_searches WHERE email=? AND id=?',email,id)?json({id}):json({error:'Limite di 50 ricerche raggiunto.'},409);
   }
   if(path==='/api/explorer/favorites'){
    if(!digest(input.id)||!digest(input.object))return json({error:'Selezione non valida.'},400);
    const object=await archive(env,'objects/'+input.object+'.json'),row=object?.rows?.find(r=>r.id===input.id);
    if(!row)return json({error:'Selezione non disponibile. Aggiorna i dati.'},404);
    // Never accept probabilities, odds or account identity supplied by the client.
    await db.prepare(`INSERT INTO explorer_favorites(email,id,snapshot,saved_at) SELECT ?,?,?,?
     WHERE (SELECT COUNT(*) FROM explorer_favorites WHERE email=?)<500 ON CONFLICT(email,id) DO NOTHING`)
     .bind(email,row.id,JSON.stringify({...row,object:input.object}),now,email).run();
    return await first(db,'SELECT id FROM explorer_favorites WHERE email=? AND id=?',email,row.id)?json({id:row.id}):json({error:'Limite di 500 preferiti raggiunto.'},409);
   }
   if(path==='/api/explorer/delete'){
    const table={favorite:'explorer_favorites',search:'explorer_searches'}[input.type];
    if(!table||typeof input.id!=='string'||input.id.length>64)return json({error:'Richiesta non valida.'},400);
    await db.prepare(`DELETE FROM ${table} WHERE email=? AND id=?`).bind(email,input.id).run();return json({ok:true});
   }
  }
  return json({error:'Percorso non disponibile.'},404);
 }catch{return json({error:'Explorer temporaneamente non disponibile. Riprova.'},503);}
}
