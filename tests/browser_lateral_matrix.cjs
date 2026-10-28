// End-to-end UI on synthetic backend output. No account or provider calls.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict'),cp=require('node:child_process');
async function verifyCachedRelease(browser,payload){
 const http=require('node:http'),requests=[];
 const html=fs.readFileSync('web/gold-dashboard-live.html','utf8');
 const script=html.match(/<script src="(\/lateral-matrix\.js[^\"]*)"><\/script>/)[1];
 const assets=new Set(['statistics-first.js','statistics-first.css','lateral-matrix.js','lateral-matrix.css','live-scores.js']);
 const oldScript='/lateral-matrix.js?v=5.10';
 const server=http.createServer((req,res)=>{
  requests.push(req.url);
  const url=new URL(req.url,'http://127.0.0.1');
  res.setHeader('Cache-Control','no-store');
  if(url.pathname==='/prime-cached-v6'){
   res.setHeader('Content-Type','text/html; charset=utf-8');
   return res.end('<!doctype html><script src="'+oldScript+'"></script>');
  }
  if(req.url===oldScript){
   res.setHeader('Cache-Control','public, max-age=31536000');
   res.setHeader('Content-Type','text/javascript');
   return res.end('window.ArabMatrix={POLICY:"gold-goals-v6",SCOPE:"gold-senior-whitelist-v2"};');
  }
  if(url.pathname==='/gold-dashboard-live'){
   res.setHeader('Content-Type','text/html; charset=utf-8');return res.end(html);
  }
  if(url.pathname==='/api/auth/session'){
   res.setHeader('Content-Type','application/json');return res.end('{"active":false}');
  }
  const file=url.pathname.slice(1);
  if(assets.has(file)){
   res.setHeader('Content-Type',file.endsWith('.css')?'text/css':'text/javascript');
   if(file==='lateral-matrix.js')res.setHeader('Cache-Control','no-cache, must-revalidate');
   return res.end(fs.readFileSync('web/'+file));
  }
  res.statusCode=404;res.end();
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const origin='http://127.0.0.1:'+server.address().port,page=await browser.newPage();
 try{
  // Real HTTP cache, with no route interception or direct script injection.
  await page.goto(origin+'/prime-cached-v6');
  await page.goto(origin+'/prime-cached-v6?second=1');
  assert.equal(requests.filter(url=>url===oldScript).length,1,'Previous bundle is genuinely cached');
  assert.equal(await page.evaluate(()=>ArabMatrix.POLICY),'gold-goals-v6');
  await page.goto(origin+'/gold-dashboard-live');
  assert.equal(await page.evaluate(()=>ArabMatrix.POLICY),'gold-goals-v6.1','Production HTML must replace the cached client');
  assert.equal(await page.evaluate(()=>ArabMatrix.SCOPE),payload.gold_scope.policy_id);
  assert.equal(requests.filter(url=>url===script).length,1,'Current content-versioned bundle requested');
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  assert.equal(await page.locator('.mx-card').count(),payload.matches.length,'Current whitelist fixtures remain visible');
  assert((await page.locator('.mx-counters').textContent()).includes('Proposte su 5 mercati gol · Combo PT+ST'));
  assert(!(await page.locator('.mx-dashboard').textContent()).includes('Dati in aggiornamento alla whitelist'));
  fs.mkdirSync('test-artifacts',{recursive:true});
  await page.screenshot({path:'test-artifacts/matrix-cache-rollout.png'});
  console.log('PASS Gold release: cached v6 bundle replaced by versioned v6.1, current whitelist fixtures visible');
 }finally{await page.close();await new Promise(resolve=>server.close(resolve));}
}
(async()=>{
 const raw=cp.execFileSync('python',['-c',`
import sys,json
sys.path[:0]=['src','tests']
from test_statistics_first import match,NOW
from test_lateral_matrix import payload
from statistics_first import statistical_analysis
from lateral_matrix import build_matrix
from matrix_quotes import attach
from copy import deepcopy
from test_gold_scope_goals import example
from opportunity_policy import annotate
from test_opportunity_policy import row
from lateral_matrix import specification as S, label
from gold_competitions import SCOPE_ID
a=build_matrix(statistical_analysis(match(),NOW),NOW)
variants=[]
for first,last in [(2.5,2.1),(2.1,2.6)]:
 b=deepcopy(a);state={}
 attach(b,payload('2026-10-26T06:00:00Z',first,1.8),state,match()['fixture'],'2026-10-26T06:00:00Z')
 attach(b,payload('2026-10-26T07:00:00Z',(first+last)/2,1.8),state,match()['fixture'],'2026-10-26T07:00:00Z')
 attach(b,payload(NOW,last,1.8),state,match()['fixture'],NOW)
 variants.append(b)
contexts=[]
for args in [('home','1',None),('away','2',None),('away','1',None),('home','2',None),('home','1',.4)]:
 b=deepcopy(a);context=example(*args)
 b['inputs']=context['inputs']
 for source in context['matrix']['rows']:
  target=next(r for r in b['matrix']['rows'] if r['spec']==source['spec'])
  target.update({k:v for k,v in source.items() if k not in ('id','label')})
 annotate(b['matrix'],NOW,b)
 contexts.append(b)
caps=[]
for spec,price in [(S('btts','ht','yes'),5.5),(S('total','ft','over',3.5),4.5),
                   (S('asian_total','ft','over',3.5),4.5),(S('total','ht','over',1.5),3.5),
                   (S('total','ft','over',2.5),2.6),(S('total','ht','over',1.5),3.5001),
                   (S('total','ft','over',3.5),4.5001)]:
 b=deepcopy(a)
 b['matrix']['rows']=[row(spec=spec,period=spec['period'],label=label(spec),odd=price,statistical_pct=60)]
 annotate(b['matrix'],NOW)
 caps.append(b)
combo=deepcopy(next(r for r in a['matrix']['rows'] if r['spec']['kind']=='halves_total'))
reference=deepcopy(next(r for r in a['matrix']['rows'] if r['spec']==S('total','ft','over',2.5)))
reference['oci']=20
caps[0]['matrix']['rows'].extend([reference,combo])
annotate(caps[0]['matrix'],NOW,caps[0])
print(json.dumps({'variants':variants,'contexts':contexts,'caps':caps,'scope':SCOPE_ID}))
`],{encoding:'utf8',maxBuffer:10*1024*1024});
 const {variants,contexts,caps,scope}=JSON.parse(raw),now=Date.now();
 Object.assign(caps[4].matrix.rows[0],{quote_status:'cached_stable',cache_source_at:new Date(now-2*3600000).toISOString(),cache_expires_at:new Date(now+10*3600000).toISOString()});
 const payload={gold_scope:{policy_id:scope},updated_at:new Date(now).toISOString(),multimarket_method:{model_id:'lateral-matrix-v5.0'},matches:Array.from({length:9},(_,i)=>({gold_scope:{policy_id:scope,allowed:true},fixture:{fixture_id:String(i+1),home:'Squadra casa',away:'Squadra ospite',
  kickoff:new Date(now+7200000+i*60000).toISOString(),status_short:'NS',time:'20:45',country:'Italia',league:'DATI SINTETICI DI VERIFICA',match:i===8?'Nome squadra molto lungo per verificare la visualizzazione da telefono – Avversaria':'Squadra '+(i+1)+' – Avversaria'},multimarket:structuredClone(variants[i%2])}))};
 // UI-only price fixtures exercise all three result cells and movement colors.
 for(const m of payload.matches)for(const row of m.multimarket.matrix.rows){
  if(row.spec.kind==='result'&&row.period==='ft')Object.assign(row,{quote_status:'recent',first_odd:2.5,odd:2.2,delta_pct:-12,bookmaker:'OPERATORE SINTETICO',oci:55,paired_books:3,agreeing_books:3,margin_pct:4,margin_delta_pp:-1,margin_band:'low',ev_pct:-1,ev_low_pct:-2});
 }
 const nearCombo=payload.matches[3].multimarket.matrix.rows.find(r=>r.spec.kind==='halves_total');
 Object.assign(nearCombo,{statistical_pct:55,fair_odd:100/55});
 Object.assign(nearCombo.combo,{active:false,reasons:['combo_probability','combo_rates'],lambda_ht:1.12,lambda_sh:1.8,over25_ft_oci:0});
 const browser=await chromium.launch({headless:true});
 try{
  await verifyCachedRelease(browser,payload);
  const page=await browser.newPage();await page.route('**/*',r=>r.abort());
  await page.setContent(fs.readFileSync('web/gold-dashboard-live.html','utf8'));
  await page.addStyleTag({content:fs.readFileSync('web/lateral-matrix.css','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/lateral-matrix.js','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/live-scores.js','utf8')});
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  fs.mkdirSync('test-artifacts',{recursive:true});
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844],['small-mobile',360,800]]){
   await page.setViewportSize({width,height});
   assert.equal(await page.locator('.mx-card').count(),9);
   assert.equal(await page.locator('.mx-heading > b').textContent(),'GOLD v6.1');
   assert((await page.locator('.mx-card footer').first().textContent()).includes('Regole gold-goals-v6.1 · Modello statistico lateral-matrix-v5.0'));
   assert.equal(await page.locator('.mx-1x2 .mx-quote-cell').count(),27);
   assert.equal(await page.locator('.mx-best').count(),9);
   const summaries=await page.locator('.mx-best').allTextContents();
   assert(summaries.every(t=>t.includes('Over 0,5 PT + Over 0,5 ST')&&t.includes('Quota book n.d.')));
   assert.equal(summaries.filter(t=>t.includes('★ Combo Consigliata')).length,5);
   assert.equal(summaries.filter(t=>t.includes('Combo non attiva')&&t.includes('OCI -92,5')).length,3);
   assert.equal(summaries.filter(t=>t.includes('Combo vicina · non attiva')).length,1);
   assert(summaries.every(t=>t.includes('Over 2,5 PT · Stima')));
   assert(summaries.every(t=>!t.includes('1,85–2,60')&&!t.includes('Soglie statistiche e controlli')));
   assert.equal(await page.locator('.mx-quote-legend').count(),0);
   const tones=await page.locator('.mx-best [class^="mx-combo-"]').evaluateAll(nodes=>nodes.map(n=>({tone:n.className,color:getComputedStyle(n).color,bold:+getComputedStyle(n).fontWeight})));
   assert.equal(tones.filter(t=>t.tone==='mx-combo-active'&&t.color==='rgb(129, 225, 180)'&&t.bold>=700).length,5);
   assert.equal(tones.filter(t=>t.tone==='mx-combo-near'&&t.color==='rgb(255, 224, 128)').length,1);
   assert.equal(tones.filter(t=>t.tone==='mx-combo-inactive'&&t.color==='rgb(255, 157, 144)').length,3);
   assert(!summaries.some(t=>t.includes('Confronto dei quattro mercati')));
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5);
   const proposals=await page.locator('.mx-best[data-opportunity-id]').evaluateAll(nodes=>nodes.map(n=>({odd:+n.dataset.opportunityOdd,max:+n.dataset.opportunityMaxOdd,p:+n.dataset.opportunityProbability,text:n.textContent})));
   assert(proposals.every(r=>r.odd>=1.85&&r.odd<=r.max&&[2.6,3.5,4.5,5.5,7.5].includes(r.max)&&r.p>=18));
   assert(proposals.some(r=>r.text.includes('Convergenza di Trend')));
   assert(!proposals.some(r=>r.text.includes('Contrarian sano')));
   assert.equal(await page.locator('.mx-1x2 .mx-net-red').count(),27);
   assert((await page.locator('.mx-1x2').first().textContent()).includes('Apertura'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
   await page.screenshot({path:`test-artifacts/matrix-${name}.png`});
   await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-proposal-${name}.png`});
   await page.locator('.mx-best').nth(3).scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-combo-near-${name}.png`});
   await page.locator('.mx-best').nth(1).scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-combo-inactive-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert(await page.locator('dialog.mx-modal').isVisible(),'Matrix must be a native modal');
   assert.equal(await page.locator('.mx-market-row').count(),6,'Five goal targets and the half-time combo open by default');
   const comboRow=page.locator('.mx-market-row').filter({hasText:'Over 0,5 PT + Over 0,5 ST'});
   assert((await comboRow.textContent()).includes('Combo Attiva'));
   assert((await comboRow.textContent()).includes('EV non calcolabile senza quota reale'));
   assert.equal(await page.locator('.mx-card .mx-detail').count(),0,'No inline expansion');
   await page.keyboard.press('Shift+Tab');
   assert(await page.evaluate(()=>!!document.activeElement.closest('dialog.mx-modal')),'Native modal focus trap');
   for(const [section,count] of [['ft',8],['ht',6],['result',3],['asian',40],['targets',6]]){
    await page.locator('[data-mx-section="'+section+'"]').click();
    assert.equal(await page.locator('.mx-market-row').count(),count);
    assert.equal(await page.locator('.mx-modal-scroll').evaluate(e=>e.scrollTop),0,'Changing section starts at its first row');
    if(section==='asian')await page.locator('.mx-modal-scroll').evaluate(e=>{e.scrollTop=e.scrollHeight});
   }
   const text=await page.locator('dialog.mx-modal').textContent();
   assert(!/Risultato esatto|Over 5,5|Under 5,5|Multigol/.test(text),'Dead markets must be absent');
   assert(await page.evaluate(()=>{const e=document.querySelector('dialog.mx-modal');return e.scrollWidth<=e.clientWidth}),'Modal horizontal overflow');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Matrix overflow '+name);
   await page.locator('[data-mx-close]').focus();
   await page.screenshot({path:`test-artifacts/matrix-details-${name}.png`});
   await page.keyboard.press('Escape');
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0);
   assert(await page.locator('[data-mx-open]').first().evaluate(e=>e===document.activeElement),'Focus restored after Escape');
   await page.locator('[data-mx-open]').first().click();
   await page.locator('[data-mx-section="ht"]').click();
   await page.evaluate(()=>renderMatchResearch(currentPayload));
   assert(await page.locator('dialog.mx-modal').isVisible(),'Fast render preserves modal');
   assert.equal(await page.locator('[data-mx-section="ht"]').getAttribute('aria-pressed'),'true');
   assert.equal(await page.locator('.mx-market-row').count(),6);
   await page.locator('[data-mx-close]').click();
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0);
   await page.locator('[data-mx-open]').first().click();
   await page.mouse.click(1,1);
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0,'Backdrop closes modal');
   assert(await page.evaluate(()=>document.body.style.overflow!=='hidden'),'Body scrolling restored');
  }
  await page.locator('[data-mx-category="divergence"]').click();
  assert.equal(await page.locator('.mx-card').count(),4);
  await page.locator('[data-mx-category="all"]').click();
  await page.locator('[data-mx-filter="family"]').selectOption('asian');
  await page.locator('[data-mx-filter="period"]').selectOption('ht');
  assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5,'Matrix filters must not replace the match proposal');
  await page.locator('[data-mx-open]').first().click();
  await page.locator('[data-mx-section="ht"]').click();
  assert.equal(await page.locator('.mx-market-row').count(),6);
  assert((await page.locator('.mx-market-row').first().textContent()).includes('PT'));
  await page.locator('[data-mx-close]').click();
  await page.locator('[data-mx-filter="family"]').selectOption('all');
  await page.locator('[data-mx-filter="period"]').selectOption('all');
  await page.locator('[data-mx-category="no_quote"]').click();
  assert.equal(await page.locator('.mx-card').count(),9);
  await page.locator('[data-mx-category="all"]').click();
  await page.evaluate(()=>{currentPayload.matches[0].fixture.kickoff=new Date(Date.now()-3600000).toISOString();currentPayload.matches[0].fixture.status_short='HT';currentPayload.matches[0].fixture.live={status:'HT',elapsed:45,goals:{home:1,away:1},halftime:{home:1,away:1},observed_at:new Date().toISOString()};renderMatchResearch(currentPayload)});
  const frozen=await page.evaluate(()=>{window.savedMatrixCard=document.querySelector('.mx-card');return JSON.stringify(currentPayload)});
  await page.evaluate(()=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'1':{fixture_id:'1',status:'2H',elapsed:62,goals:{home:2,away:1},halftime:{home:1,away:1},observed_at:new Date(Date.now()+1000).toISOString()}}});window.dispatchEvent(new Event('arabsniper-live-update'))});
  assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozen);
  assert(await page.evaluate(()=>savedMatrixCard===document.querySelector('.mx-card')));
  assert((await page.locator('.mx-card').first().textContent()).includes('2–1'));
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Live overflow');
  const timed={...payload,matches:contexts.map((a,i)=>({...payload.matches[i],multimarket:a}))};
  await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},timed);
  const timing=await page.locator('.mx-timing:not(.mx-first-half)').allTextContents();
  assert(timing[0].includes('Ordine di valutazione: Over 1,5 PT')&&timing[0].includes('Favorita in casa'));
  assert(timing[1].includes('Ordine di valutazione: Over 1,5 PT')&&timing[1].includes('Favorita in trasferta'));
  assert(timing[2].includes('Ordine di valutazione: Goal PT')&&timing[3].includes('Ordine di valutazione: Goal PT'));
  assert(timing[0].includes('Precedenza tra idonei'));
  assert(timing[4].includes('Quote bilanciate')&&timing[4].includes('0,40'));
  assert((await page.locator('.mx-best h4').allTextContents())[2].includes('Goal'));
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Drought priority overflow');
  await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
  await page.screenshot({path:'test-artifacts/matrix-drought-priority-mobile.png'});
  const capped={...payload,matches:caps.map((a,i)=>({...payload.matches[i],multimarket:a}))};
  await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},capped);
  for(const [name,width] of [['desktop',1280],['mobile',390],['small-mobile',360]]){
   await page.setViewportSize({width,height:844});
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5);
   assert.equal(await page.locator('.mx-best[data-opportunity-odd="5.5"]').count(),1);
   assert.equal(await page.locator('.mx-best-empty').count(),2);
   assert((await page.locator('.mx-best').nth(4).textContent()).includes('Quota stabile da cache'));
   assert((await page.locator('.mx-best').first().textContent()).includes('minimo 26'));
   assert(!(await page.locator('.mx-best').first().textContent()).includes('2,10–3,50'));
   assert.equal(await page.locator('.mx-first-half').count(),2);
   assert((await page.locator('.mx-best').first().textContent()).includes('★ Combo Consigliata'));
   assert((await page.locator('.mx-first-half').first().textContent()).includes('xG PT non disponibili'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Market caps overflow '+name);
   await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-market-caps-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert((await page.locator('.mx-market-row').first().textContent()).includes('Idoneo'));
   assert(await page.evaluate(()=>{const e=document.querySelector('dialog.mx-modal');return e.scrollWidth<=e.clientWidth}),'Market caps modal overflow');
   await page.locator('[data-mx-close]').click();
  }
  console.log('PASS Gold v6.1 browser: concise cards, green/yellow/red combos, versions, quote coverage, unchanged desktop/390/360 grid, modal and immutable live overlays');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});

