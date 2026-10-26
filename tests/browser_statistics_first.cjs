// Synthetic renderer test; no account, premium feed, or external API is used.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true});
 try{
  const page=await browser.newPage();
  await page.route('**/*',r=>r.abort());
  await page.setContent(fs.readFileSync('web/gold-dashboard-live.html','utf8'));
  await page.addStyleTag({content:fs.readFileSync('web/statistics-first.css','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/statistics-first.js','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/live-scores.js','utf8')});
  const profile={mean:1.25,last:0,without_last:1.43,rate:1.72,xg_mean:2.1,xg_sample:6,
    drought:1,sample:8,last_state:'below',under_conversion:true,peak_reduction:0};
  const mk=(id,selected,confirmation)=>({fixture:{fixture_id:String(id),kickoff:new Date(Date.now()+7200000).toISOString(),
    status_short:'NS',match:id===1?'Squadra casa – Squadra ospite':'Altra squadra – Avversaria',time:'20:45',league:'Campionato · DATI DI PROVA',country:'Italia'},
    multimarket:{model_id:'statistics-first-v4.0',captured_at:new Date().toISOString(),config_id:'synthetic',
      h2h:{used:2,overlapping_excluded:1},profiles:{home:{for_ft:profile,against_ft:profile,for_ht:profile,against_ht:profile},away:{for_ft:profile,against_ft:profile,for_ht:profile,against_ht:profile}},
      markets:['over25_ft','over15_ht','over35_ft','gg_ht'].map((market,i)=>({market,selection:i===3?'yes':'over',
        statistical_pct:68-i*10,statistical_score:64-i*10,statistical_candidate:true,published_selection:selected,published_rank:1,
        odd:i===1?null:2.1,quote_status:i===1?'missing':'recent',quote:i===1?null:{bookmaker:'Operatore di prova',movement:{first_odds:{over:2.4,yes:2.4}}},
        league_probability_pct:48-i*10,sensitivity:{low_pct:64-i*10,high_pct:71-i*10},
        market_confirmation:{status:i===3?'context':confirmation,patterns:i===0?['over25_from_above_2']:[]},
        movement:{agreeing_books:3,paired_books:3,amplitude_pct:12.5,direction:'down',oci:46,median_overround_delta_pp:-.4},
        pros:['Potenziale offensivo sostenuto dalla difesa avversaria.','Occasioni xG superiori ai gol realizzati.'],
        cons:['Campione limitato: stima ancora in verifica.'],selection_reasons:[],
        price_assessment:{reference_fair_odd:1.47,estimated_return_pct:42.8}}))}});
  const payload={updated_at:new Date().toISOString(),multimarket_method:{model_id:'statistics-first-v4.0'},matches:[mk(1,true,'confirmed'),mk(2,false,'neutral')]};
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  fs.mkdirSync('test-artifacts',{recursive:true});
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844],['small-mobile',360,800]]){
    await page.setViewportSize({width,height});
    assert.equal(await page.locator('.sf-card').count(),1);
    assert((await page.locator('.sf-badge').textContent()).includes('ORO + AZZURRO'));
    assert.equal(await page.locator('[data-sf-market]').count(),4);
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
    await page.screenshot({path:`test-artifacts/statistics-${name}.png`,fullPage:true});
    await page.locator('.sf-detail>summary').click();
    assert(await page.locator('.sf-team-grid').isVisible());
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Details overflow '+name);
    await page.screenshot({path:`test-artifacts/statistics-details-${name}.png`,fullPage:true});
    await page.locator('.sf-detail>summary').click();
  }
  await page.locator('[data-sf-market="over15_ht"]').click();
  assert((await page.locator('.sf-price').textContent()).includes('Quota non disponibile'));
  await page.locator('[data-sf-market="gg_ht"]').click();
  assert((await page.locator('.sf-badge').textContent()).includes('SCENARIO QUOTE'));
  await page.locator('[data-sf-filter="signal"]').selectOption('confirmed');
  assert.equal(await page.locator('.sf-card').count(),0);
  await page.locator('[data-sf-action="all"]').click();
  assert.equal(await page.locator('.sf-card').count(),2);
  await page.locator('[data-sf-filter="scope"]').selectOption('selected');
  await page.locator('[data-sf-market="over25_ft"]').click();
  await page.evaluate(()=>{currentPayload.matches[0].fixture.kickoff=new Date(Date.now()-3600000).toISOString();currentPayload.matches[0].fixture.status_short='HT';currentPayload.matches[0].fixture.live={status:'HT',elapsed:45,goals:{home:1,away:1},halftime:{home:1,away:1},observed_at:new Date().toISOString()};ArabStatistics.state.phase='live';renderMatchResearch(currentPayload)});
  await page.locator('.sf-detail>summary').click();
  const frozen=await page.evaluate(()=>{window.savedCard=document.querySelector('.sf-card');return JSON.stringify(currentPayload)});
  await page.evaluate(()=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'1':{fixture_id:'1',status:'2H',elapsed:63,goals:{home:2,away:1},halftime:{home:1,away:1},observed_at:new Date(Date.now()+1000).toISOString()}}});window.dispatchEvent(new Event('arabsniper-live-update'))});
  assert((await page.locator('.cx-score').textContent()).includes('2–1'));
  assert(await page.evaluate(()=>savedCard===document.querySelector('.sf-card')&&document.querySelector('.sf-detail').open));
  assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozen);
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Live overflow');
  await page.screenshot({path:'test-artifacts/statistics-live-mobile.png',fullPage:true});
  console.log('PASS V4 browser: desktop/mobile, four lists, filters, empty states, details, quote scope and frozen live updates');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
