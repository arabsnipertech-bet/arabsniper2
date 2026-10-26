// End-to-end UI on synthetic backend output. No account or provider calls.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict'),cp=require('node:child_process');
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
a=build_matrix(statistical_analysis(match(),NOW),NOW)
variants=[]
for first,last in [(2.5,2.1),(2.1,2.6)]:
 b=deepcopy(a);state={}
 attach(b,payload('2026-10-26T06:00:00Z',first,1.6),state,match()['fixture'],'2026-10-26T06:00:00Z')
 attach(b,payload('2026-10-26T07:00:00Z',(first+last)/2,1.6),state,match()['fixture'],'2026-10-26T07:00:00Z')
 attach(b,payload(NOW,last,1.6),state,match()['fixture'],NOW)
 variants.append(b)
print(json.dumps(variants))
`],{encoding:'utf8',maxBuffer:10*1024*1024});
 const variants=JSON.parse(raw),now=Date.now();
 const payload={updated_at:new Date(now).toISOString(),multimarket_method:{model_id:'lateral-matrix-v5.0'},matches:Array.from({length:9},(_,i)=>({fixture:{fixture_id:String(i+1),home:'Squadra casa',away:'Squadra ospite',
  kickoff:new Date(now+7200000+i*60000).toISOString(),status_short:'NS',time:'20:45',country:'Italia',league:'DATI SINTETICI DI VERIFICA',match:i===8?'Nome squadra molto lungo per verificare la visualizzazione da telefono – Avversaria':'Squadra '+(i+1)+' – Avversaria'},multimarket:structuredClone(variants[i%2])}))};
 // UI-only price fixtures exercise all three result cells and movement colors.
 for(const m of payload.matches)for(const row of m.multimarket.matrix.rows){
  if(row.spec.kind==='result'&&row.period==='ft')Object.assign(row,{quote_status:'recent',first_odd:2.5,odd:2.2,delta_pct:-12,bookmaker:'OPERATORE SINTETICO',oci:55,paired_books:3,agreeing_books:3,margin_pct:4,margin_delta_pp:-1,margin_band:'low',ev_pct:-1,ev_low_pct:-2});
 }
 const browser=await chromium.launch({headless:true});
 try{
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
   assert.equal(await page.locator('.mx-1x2 .mx-quote-cell').count(),27);
   assert.equal(await page.locator('.mx-best').count(),9);
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),9);
   const proposals=await page.locator('.mx-best[data-opportunity-id]').evaluateAll(nodes=>nodes.map(n=>({odd:+n.dataset.opportunityOdd,p:+n.dataset.opportunityProbability,text:n.textContent})));
   assert(proposals.every(r=>r.odd>=1.4&&r.odd<=3.5&&r.p>=35));
   assert(proposals.some(r=>r.text.includes('Convergenza di Trend')));
   assert(proposals.some(r=>r.text.includes('Opportunità Contrarian')));
   assert.equal(await page.locator('.mx-1x2 .mx-net-red').count(),27);
   assert((await page.locator('.mx-1x2').first().textContent()).includes('Apertura*'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
   await page.screenshot({path:`test-artifacts/matrix-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert((await page.locator('.mx-market-row').count())>250,'Full matrix must remain accessible');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Matrix overflow '+name);
   await page.locator('.mx-detail').scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-details-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
  }
  await page.locator('[data-mx-category="divergence"]').click();
  assert.equal(await page.locator('.mx-card').count(),4);
  await page.locator('[data-mx-category="all"]').click();
  await page.locator('[data-mx-filter="family"]').selectOption('combo');
  await page.locator('[data-mx-filter="period"]').selectOption('ht');
  assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),9,'Matrix filters must not replace the match proposal');
  await page.locator('[data-mx-open]').first().click();
  assert((await page.locator('.mx-market-row').count())>10);
  assert((await page.locator('.mx-market-row').first().textContent()).includes('PT'));
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
  console.log('PASS V5 browser: full palinsesto, 297-market matrices, desktop/390/360, contrarian and missing-price filters, immutable live overlays');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
