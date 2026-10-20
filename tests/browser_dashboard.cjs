// Browser-level mobile/desktop verification with synthetic, explicitly labelled data.
const {chromium}=require('playwright');
const fs=require('node:fs');const path=require('node:path');const assert=require('node:assert/strict');
(async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    const page=await browser.newPage();
    await page.route('**/*',r=>r.abort());
    await page.setContent(fs.readFileSync(path.join(__dirname,'../web/gold-dashboard-live.html'),'utf8'));
    const now=new Date().toISOString();
    const mk=(market,selection,label,p)=>({market,selection,label,probability_pct:p,statistical_pct:p,book_probability_pct:p-3,
      source:'statistica e bookmaker',source_reason:'Dati sintetici per verifica interfaccia.',quote:{bookmaker:'Test',provider_updated_at:now},odd:2,
      pros:['Produzione nel contesto confermata dai dati di prova.'],cons:['Campione di prova limitato.'],missing:['Trend PT di prova non disponibile.'],movement:{confirmed:false}});
    const rows=[mk('over15_ht','over','Over 1,5 PT',44),mk('gg_ht','yes','GG PT sì',29),mk('over25_ft','over','Over 2,5 FT',61),mk('over35_ft','over','Over 3,5 FT',38)];
    rows.push(mk('1x2_ft','1','Casa',24),mk('1x2_ft','x','Pareggio',26),mk('1x2_ft','2','Ospite',50));
    rows[6].odd=1.65; rows[6].movement={confirmed:true,color:'red',arrow:'↓',amplitude_pct:10,agreeing_books:2,paired_books:2,operators:[{bookmaker:'Test',initial:1.85,current:1.65,sequence:[1.85,1.65]}]};
    const primary={...rows[2],research_candidate:true};
    const payload={updated_at:now,matches:[{fixture:{fixture_id:'demo',kickoff:new Date(Date.now()+3600000).toISOString(),
      match:'Squadra casa – Squadra ospite <script>',time:'20:45',league:'Campionato di prova',country:'Italia'},
      multimarket:{model_id:'context-goals-v3',status:'experimental',captured_at:now,profiles:{},weights:{},markets:rows,
        best_candidate:primary,ranked_hypotheses:rows.map(r=>r.market+'::'+r.selection)}}]};
    // Isolated synthetic renderer test: no network, premium account or auth change.
    await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;researchMarket='all';renderMatchResearch(p)},payload);
    fs.mkdirSync('test-artifacts',{recursive:true});
    for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844]]){
      await page.setViewportSize({width,height});
      assert.equal(await page.locator('.cx-card').count(),1);
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
    // Global market filter must not block exact per-fixture alternatives.
    await page.evaluate(()=>researchFilter('market','over25_ft::over'));
    await page.locator('.cx-mini').filter({hasText:'GG PT sì'}).click();
    assert((await page.locator('.cx-scenario-title').textContent()).includes('GG PT sì'));
    console.log('Browser: desktop/mobile layout, no horizontal overflow, exact markets and stable primary passed.');
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
