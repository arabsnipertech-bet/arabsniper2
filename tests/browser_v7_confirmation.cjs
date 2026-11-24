// Presentation regressions on synthetic backend fields. All network is blocked.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||process.env.CHROMIUM_PATH});
 try{
  for(const width of [1280,390,360]){
   const page=await browser.newPage({viewport:{width,height:1000}}),errors=[],external=[];
   page.on('pageerror',e=>errors.push(e.message));
   await page.route('**/*',r=>{external.push(r.request().url());return r.abort();});
   await page.setContent('<!doctype html><meta name="viewport" content="width=device-width"><style>body{margin:0;padding:12px;box-sizing:border-box;background:#101720;color:white}*{box-sizing:border-box}</style><main id="qa"><p>QA — DATI SINTETICI</p><div id="results"></div></main>');
   await page.addStyleTag({content:fs.readFileSync('web/lateral-matrix.css','utf8')});
   await page.addStyleTag({content:fs.readFileSync('web/gold-explorer-qualification.css','utf8')});
   await page.addScriptTag({content:fs.readFileSync('web/competition-scope.js','utf8')});
   await page.addScriptTag({content:fs.readFileSync('web/gold-explorer-qualification.js','utf8')});
   await page.addScriptTag({content:fs.readFileSync('web/lateral-matrix.js','utf8')});
   await page.evaluate(()=>{
    const spec={kind:'total',period:'ht',side:'over',line:1.5},key=JSON.stringify(spec);
    const origin={origin_badge:'Rialzo senza conferma',origin_tooltip:'Prima osservata 2,25 → 2,50. OCI attuale 0: neutro. Solo osservazione.'};
    const signal={market_id:key,source_market_id:key,tier:'BLU',badge:'◆ BLU',horizon_label:'[1° TEMPO]',odd:2.5,probability:.377,study_only:true,observation_only:true,...origin};
    const record={source_market_id:key,label:'Over 1,5 PT',calibrated:.377,model_fair_odd:2.66,odd:2.5,ev:-.059,ev_low:-.064,features:{oci:0,opening_delta_pct:11.1,delta_pct:-2},confirmation:{qualification_type:'study',qualification_reasons:['ev_low_negative','study_only'],observation_only:true},...origin};
    const source={id:key,spec,odd:2.5,first_odd:2.25,quote_status:'recent',quote_executable:true};
    const view={v7:{fixture_id:'123',triad:{recommended_alternative:signal}},triad:{recommended_alternative:{signal,record,source}}};
    document.querySelector('#results').innerHTML=ArabMatrix.triadPanel(view);
    const saved={origin_badge:'Trend Quota +5,5%',origin_tooltip:'Motivazione storica congelata'};
    if(ArabMatrix.marketOrigin(source,{record:saved}).origin_badge!==saved.origin_badge)throw Error('Historical explanation rewritten');
   });
   const text=await page.locator('#results').textContent();
   assert(text.includes('Osservazione · Consigliata'));
   assert(text.includes('Rialzo senza conferma'));assert(!text.includes('Trend Quota'));
   assert(text.includes('scenario prudente negativo'));
   assert.equal(await page.locator('[data-explorer-fixture="123"]').getAttribute('href'),'/gold-explorer.html?fixture=123');
   assert.equal(await page.locator('[data-v7-slot="recommended_alternative"] .mx-origin-badge').getAttribute('data-mx-origin-tooltip'),'Prima osservata 2,25 → 2,50. OCI attuale 0: neutro. Solo osservazione.');
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),true);
   assert.deepEqual(errors,[]);assert.deepEqual(external,[]);
   fs.mkdirSync('test-artifacts',{recursive:true});await page.screenshot({path:'test-artifacts/v7-confirmation-'+width+'.png',fullPage:true});
   await page.close();
  }
 }finally{await browser.close();}
 console.log('V7 confirmation: desktop, 390px, 360px; no network; immutable historical explanation.');
})().catch(e=>{console.error(e);process.exit(1);});
