// Browser-level mobile/desktop verification with synthetic, explicitly labelled data.
const {chromium}=require('playwright');
const fs=require('node:fs');const path=require('node:path');const assert=require('node:assert/strict');
(async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    const page=await browser.newPage();
    await page.route('**/*',r=>r.abort());
    await page.setContent(fs.readFileSync(path.join(__dirname,'../web/gold-dashboard-live.html'),'utf8'));
    await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'../web/live-scores.js'),'utf8')});
    const now=new Date().toISOString();
    const mk=(market,selection,label,p)=>({market,selection,label,probability_pct:p,statistical_pct:p,book_probability_pct:p-3,
      source:'statistica e bookmaker',source_reason:'Dati sintetici per verifica interfaccia.',quote:{bookmaker:'Test',provider_updated_at:now},odd:2,
      pros:['Produzione nel contesto confermata dai dati di prova.'],cons:['Campione di prova limitato.'],missing:['Trend PT di prova non disponibile.'],movement:{confirmed:false}});
    const rows=[mk('over15_ht','over','Over 1,5 PT',44),mk('gg_ht','yes','GG PT sì',29),mk('over25_ft','over','Over 2,5 FT',61),mk('over35_ft','over','Over 3,5 FT',38)];
    rows.push(mk('1x2_ft','1','Casa',24),mk('1x2_ft','x','Pareggio',26),mk('1x2_ft','2','Ospite',50));
    rows[6].odd=1.65; rows[6].movement={confirmed:true,color:'red',direction:'down',arrow:'↓',favorite_switch:{confirmed:true,from:'1',to:'2',agreeing_books:2,paired_books:2},amplitude_pct:10,agreeing_books:2,paired_books:2,operators:[{bookmaker:'Test',initial:1.85,current:1.65,sequence:[1.85,1.65]}]};
    rows.push(mk('over25_ht','over','Over 2,5 PT',18));
    rows[2].probabilities={statistics:.61};
    for(const [index,direction] of [[4,'up'],[2,'down'],[0,'down']])rows[index].movement={confirmed:true,direction,amplitude_pct:4,agreeing_books:3,paired_books:4};
    const primary={...rows[2],research_candidate:true,price_assessment:{reference_probability_pct:58,break_even_probability_pct:50,reference_fair_odd:100/58,estimated_return_pct:16,status:'unvalidated_positive_estimate',note:'Esempio sintetico non validato.'}};
    const payload={updated_at:now,matches:[{fixture:{fixture_id:'123456',kickoff:new Date(Date.now()+3600000).toISOString(),
      match:'Squadra casa – Squadra ospite <script>',time:'20:45',league:'Campionato di prova',country:'Italia'},
      multimarket:{model_id:'context-goals-v3.4',status:'experimental',captured_at:now,data_quality:{home:{sample:8,xg_for:4,shots_for:6,opportunity_matches:6},away:{sample:8,xg_for:0,shots_for:0,opportunity_matches:0}},profiles:{},weights:{},markets:rows,
        best_candidate:primary,ranked_hypotheses:rows.map(r=>r.market+'::'+r.selection)}}]};
    // Isolated synthetic renderer test: no network, premium account or auth change.
    await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;researchMarket='all';renderMatchResearch(p)},payload);
    fs.mkdirSync('test-artifacts',{recursive:true});
    for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844],['small-mobile',360,800]]){
      await page.setViewportSize({width,height});
      await page.evaluate(()=>contextBack());
      assert.equal(await page.locator('.cx-list-row').count(),1);
      assert((await page.locator('.cx-list-pick').textContent()).includes('Vantaggio teorico · da validare'));
      assert.equal(await page.locator('.cx-card').count(),0);
      assert(await page.locator('.cx-compact-odds .cx-favorite').isVisible());
      assert(await page.locator('.cx-compact-odds .cx-red').isVisible(),'Movement visible in mobile/desktop list');
      assert(await page.locator('.cx-red .cx-direction').isVisible());
      assert(await page.locator('.cx-legend').isVisible());
      assert((await page.locator('.cx-red').textContent()).includes('✓ 2/2 book'));
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
      await page.screenshot({path:`test-artifacts/list-${name}.png`,fullPage:true});
      assert.equal(await page.locator('.cx-list-row .cx-crossing').count(),1);
      assert(await page.locator('.cx-list-row .cx-crossing').first().isVisible());
      assert((await page.locator('.cx-list-row .cx-crossing').getAttribute('title')).includes('1 ↑ + Over 2,5 FT ↓'));
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Overview overflow at '+name);
      await page.screenshot({path:`test-artifacts/overview-${name}.png`,fullPage:true});
      await page.locator('.cx-open').click();
      assert.equal(await page.locator('.cx-card').count(),1);
      assert.equal(await page.locator('.cx-mini').count(),5);
      assert((await page.locator('.cx-card').textContent()).includes('Completezza dei dati'));
      assert.equal(await page.locator('.cx-odd-cell').count(),5);
      assert((await page.locator('.cx-odds-panel').textContent()).includes('1.85'));
      assert((await page.locator('.cx-odds-panel').textContent()).includes('1.65'));
      assert.equal(await page.locator('.cx-odds-panel .cx-red').count(),1);
      assert.equal(await page.locator('.cx-primary h4').textContent(),'Over 2,5 FT');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Horizontal overflow at '+name);
      await page.locator('.cx-scenario select').selectOption('gg_ht::yes');
      assert((await page.locator('.cx-scenario-title').textContent()).includes('GG PT sì'));
      assert.equal(await page.locator('.cx-primary h4').textContent(),'Over 2,5 FT');
      await page.screenshot({path:`test-artifacts/context-${name}.png`,fullPage:true});
    }
    // Started matches stay available with explicit prematch/live separation.
    await page.evaluate(()=>{
      const f=currentPayload.matches[0].fixture;
      f.kickoff=new Date(Date.now()-3600000).toISOString();
      f.status_short='HT';f.live={status:'HT',elapsed:45,goals:{home:1,away:1},halftime:{home:1,away:1},observed_at:new Date().toISOString()};
      contextBack();
    });
    assert.equal(await page.locator('.cx-list-row').count(),1);
    await page.locator('.cx-open').click();
    assert((await page.locator('.cx-card').textContent()).includes('non sono probabilità live'));
    assert((await page.locator('.cx-mini').filter({hasText:'GG PT sì'}).textContent()).includes('Verificato: riuscito'));
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Live mobile overflow');
    await page.screenshot({path:'test-artifacts/live-mobile.png',fullPage:true});
    assert.equal(await page.locator('.cx-card.cx-is-live').count(),1);
    const frozen=await page.evaluate(()=>{window.savedCard=document.querySelector('.cx-card');window.savedScenario=document.querySelector('.cx-scenario');document.querySelector('.cx-detail').open=true;return JSON.stringify(currentPayload)});
    await page.evaluate(()=>{
      ArabLive.accept({schema_version:1,enabled:true,fixtures:{'123456':{fixture_id:'123456',status:'2H',elapsed:63,goals:{home:2,away:1},halftime:{home:1,away:1},observed_at:new Date(Date.now()+1000).toISOString()}}});
      window.dispatchEvent(new Event('arabsniper-live-update'));
    });
    assert((await page.locator('.cx-score').textContent()).includes('2–1'));
    assert(await page.evaluate(()=>savedCard===document.querySelector('.cx-card')&&savedScenario===document.querySelector('.cx-scenario')&&document.querySelector('.cx-detail').open),'Live patch preserves card, controls and open details');
    assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozen,'Live must not mutate prematch data');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Score mobile overflow');
    await page.screenshot({path:'test-artifacts/live-score-mobile.png',fullPage:true});
    // Global market filter must not block exact per-fixture alternatives.
    await page.evaluate(()=>researchFilter('market','over25_ft::over'));
    await page.locator('.cx-mini').filter({hasText:'GG PT sì'}).click();
    assert((await page.locator('.cx-scenario-title').textContent()).includes('GG PT sì'));
    await page.evaluate(()=>{
      const rows=currentPayload.matches[0].multimarket.markets;
      rows.find(r=>r.market==='1x2_ft'&&r.selection==='1').odd=1.65;
      for(const [side,pct] of [['1',60],['x',23],['2',17]])rows.find(r=>r.market==='1x2_ft'&&r.selection===side).book_probability_pct=pct;
      contextBack();
    });
    await page.getByLabel('Segnale Gold',{exact:true}).selectOption('gold');
    assert.equal(await page.locator('.cx-list-row').count(),0);
    await page.getByLabel('Segnale Gold',{exact:true}).selectOption('dual');
    assert.equal(await page.locator('.cx-list-row').count(),1);
    await page.getByLabel('Fascia quota 1',{exact:true}).selectOption('band');
    assert.equal(await page.locator('.cx-list-row').count(),1);
    assert((await page.locator('.cx-home-band').textContent()).includes('FAVORITA CASA 1'));
    assert.equal(await page.locator('.cx-home-focus>span').count(),3);
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Favorite focus mobile overflow');
    await page.screenshot({path:'test-artifacts/favorite-focus-mobile.png',fullPage:true});
    console.log('Browser: desktop/mobile layout, no horizontal overflow, exact markets and stable primary passed.');
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
