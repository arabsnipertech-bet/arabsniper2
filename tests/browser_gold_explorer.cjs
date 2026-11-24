// Synthetic QA fixture; never used as production data or promotional evidence.
const {chromium}=require('playwright');
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{createHash}=require('node:crypto');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||process.env.CHROMIUM_PATH});
 try{
 const page=await browser.newPage(),errors=[],external=[],calls=[];page.on('pageerror',e=>errors.push(e.message));
 const now=Date.parse('2026-11-17T10:00:00Z');await page.clock.install({time:now});
 const objects={},add=value=>{const raw=JSON.stringify(value),id=createHash('sha256').update(raw).digest('hex');objects[id]=value;return id;};
 const rows=Array.from({length:1250},(_,i)=>({id:createHash('sha256').update(String(i)).digest('hex'),fixture_id:String(Math.floor(i/5)),episode:Math.floor(i/5)+'::k',home:'QA Casa '+Math.floor(i/5),away:'QA Ospite',league:'Campionato QA '+i%3,kickoff:'2026-11-17T14:00:00Z',date:'2026-11-17',market:'m'+i%5,label:['GG PT','GG FT','Over 2,5 FT','Under 3,5 FT','1X2 Casa'][i%5],probability:.6,odd:2.2,opening:2.1,ev:.32,ev_low:.1,oci:55,reliability:'supported',category:i%10?'alternative':'official',roles:i%10?[]:['most_probable'],validated:true,niche:false,quote_usable:true,quote_valid_until:'2026-11-17T12:00:00Z',ranking_roles:['recommended_alternative','high_odds_shot'],captured_at:'2026-11-17T09:30:00Z',published_at:'2026-11-17T10:00:00Z',forecast_sha256:'f'.repeat(64),quote:{bookmaker:'Fonte sintetica QA'}}));
 // Official records deliberately lack Explorer eligibility: this is a display regression.
 for(let i=0;i<rows.length;i+=5){rows[i].roles=['recommended_alternative'];rows[i].ranking_roles=[];rows[i].validated=false;rows[i].quote_usable=false;rows[i].official_references={recommended_alternative:{odd:2.05,captured_at:'2026-11-17T08:00:00Z'}};}
 for(const row of rows){row.confirmation={qualification_type:'documented_value',observation_only:false,value_qualified:true};row.origin_badge='Ritmo 1°T (λ 1,20)';row.origin_tooltip='Motivo originale Gold · proiezione statistica del primo tempo';row.match_odds={
  '1':{first_odd:2,odd:1.8,bookmaker:'Book QA',quote_usable:true,quote_valid_until:'2026-11-17T12:00:00Z'},
  x:{first_odd:3,odd:3.3,bookmaker:'Book QA',quote_usable:true,quote_valid_until:'2026-11-17T12:00:00Z'},
  '2':{first_odd:4,odd:4,first_observed_at:'2026-11-17T08:00:00Z',quote_observed_at:'2026-11-17T09:30:00Z',quote_usable:true,quote_valid_until:'2026-11-17T12:00:00Z'}};}
 // Put the observation on the first original-role page, independent of best ranking.
 const observedRow=rows.filter(r=>r.roles?.includes('recommended_alternative')).sort((a,b)=>a.id.localeCompare(b.id))[0];
 observedRow.observation_only=true;observedRow.oci=0;observedRow.confirmation={qualification_type:'study',qualification_reasons:['study_only'],observation_only:true};
 const opposedRow=rows.find(r=>r.label==='GG FT'&&!r.roles.length);
 opposedRow.roles=['recommended_alternative']; // Exercise the separate original-Gold-role view too.
 opposedRow.coherence_status='opposed';opposedRow.coherence_opposed_to=['No Goal FT'];
 const pages=[];for(let i=0;i<rows.length;i+=250)pages.push(add({version:1,rows:rows.slice(i,i+250)}));
 const futurePages=[add({version:1,rows:rows.slice(0,250).map(r=>r.fixture_id==='1'?{...r,kickoff:'2026-11-18T14:00:00Z',date:'2026-11-18'}:r)}),...pages.slice(1)];
 const boards={best:rows.slice(0,10).map(r=>r.id),probable:rows.slice(0,10).map(r=>r.id),recommended:rows.slice(0,10).map(r=>r.id),shots:rows.slice(0,10).map(r=>r.id),markets:{'GG FT':rows.filter(r=>r.label==='GG FT').slice(0,10).map(r=>r.id)}};
 const snapshot=add({pages,count:rows.length,boards}),hist={references:{},results:{},scans:[{sha256:snapshot,phase:'night',published_at:'2026-11-17T10:00:00Z'}],legacy_pages:[pages[0]]};
 for(const r of rows){hist.references[r.episode]={night:snapshot,final:snapshot,fallback_night:true};hist.results[r.fixture_id]={kickoff:r.kickoff,status:'settled',outcomes:{...hist.results[r.fixture_id]?.outcomes,[r.market]:{win:1,loss:0}}};}
 let searches=[],favorites=[],status=200,empty=false,fail=false,futureFixture=false;
 await page.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());if(url.origin!=='https://explorer.test'){external.push(url.href);return route.abort();}
  if(url.pathname==='/api/auth/session')return route.fulfill({contentType:'application/json',body:JSON.stringify({active:true,authenticated:true,email:'qa@example.test',server_time:now/1000,expires_at:now/1000+86400,session_expires_at:now/1000+86400})});
  if(url.pathname.startsWith('/api/gold-data/'))return route.fulfill({contentType:'application/json',body:JSON.stringify({date:'2026-11-17',matches:[]})});
  if(url.pathname.startsWith('/api/auth/explorer/')){
   calls.push(url.pathname);if(fail)return route.abort();if(status!==200)return route.fulfill({status,contentType:'application/json',body:JSON.stringify({error:status===401?'Accedi dalla Gold':'Errore temporaneo'})});
   const endpoint=url.pathname.slice('/api/auth/explorer/'.length),input=request.method()==='POST'?request.postDataJSON():null;let result={};
   if(endpoint==='current')result={pages:empty?[]:futureFixture?futurePages:pages,count:empty?0:rows.length,generated_at:new Date(now).toISOString()};
   else if(endpoint.startsWith('objects/'))result=objects[endpoint.slice(8)];
   else if(endpoint==='catalog')result={days:{'2026-11-17':{}},legacy_import_pending:0};
   else if(endpoint.startsWith('days/'))result=hist;
   else if(endpoint==='searches'){if(input){const id=input.id||'saved-1';searches=searches.filter(s=>s.id!==id);searches.push({...input,id});result={id};}else result={items:searches};}
   else if(endpoint==='favorites'){if(input){const row=objects[input.object].rows.find(r=>r.id===input.id);favorites.push({id:row.id,snapshot:{...row,object:input.object},saved_at:now/1000,result:hist.results[row.fixture_id],references:{}});result={id:row.id};}else result={items:favorites,next:null};}
   else if(endpoint==='delete'){if(input.type==='search')searches=searches.filter(s=>s.id!==input.id);else favorites=favorites.filter(s=>s.id!==input.id);result={ok:true};}
   return route.fulfill({contentType:'application/json',body:JSON.stringify(result)});
  }
  const file=path.join(__dirname,'../web',url.pathname);if(fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({body:fs.readFileSync(file),contentType:file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':'text/javascript'});
  return route.fulfill({status:404,body:'Not found'});
 });
 await page.goto('https://explorer.test/gold-explorer.html');await page.locator('h1').evaluate(el=>el.prepend('COLLAUDO QA — DATI SINTETICI · '));await page.locator('#message').filter({hasText:'Dati caricati.'}).waitFor();
 assert.equal(await page.locator('#results .selection').count(),10);const initial=calls.length;
 const first=page.locator('#results .selection').first();assert.equal(await first.locator('.selection-1x2>div').count(),3);
 assert.equal(await first.locator('[data-qualification="documented_value"]').count(),1);
 assert((await first.locator('[data-movement=down]').innerText()).includes('↓ -10,0%'));assert((await first.locator('[data-movement=up]').innerText()).includes('↑ +10,0%'));assert((await first.locator('[data-movement=flat]').innerText()).includes('→ 0,0%'));
 await first.locator('.selection-origin summary').click();assert(await first.getByText('Motivo originale Gold · proiezione statistica del primo tempo',{exact:true}).isVisible());assert.equal(calls.length,initial,'Reading the explanation adds no requests');
 for(const mode of ['probable','recommended','shots','markets','best']){await page.selectOption('#mode',mode);assert(await page.locator('#results .selection').count()>=10);assert.equal(await page.locator('#results [data-coherence]').count(),0,'Opposites excluded from '+mode);}
 await page.selectOption('#mode','markets');assert.equal(await page.locator('#marketIndex a').count(),4);
 await page.locator('#marketIndex a').filter({hasText:'Under 3,5 FT'}).click();assert((await page.evaluate(()=>document.activeElement.textContent)).startsWith('Under 3,5 FT'));
 await page.selectOption('#mode','recommended');assert.equal(await page.locator('#results .selection').count(),10);
 const observation=page.locator('#results .selection').filter({hasText:'Osservazione V7'});
 assert.equal(await observation.count(),1,'Unvalidated observations stay in their original-role view');
 assert((await observation.innerText()).includes('Osservazione · ruolo originale: Consigliata'));
 assert((await observation.innerText()).includes('OCI 0 · neutro'));
 assert((await page.locator('#results').innerText()).includes('Triade: Consigliata'),'Other original-role cards stay unchanged');
 await observation.locator('[data-detail]').click();assert((await page.locator('#detailBody').innerText()).includes('2,05'));await page.click('#closeDetail');await page.fill('[name=team]','impossibile');assert((await page.locator('#results').innerText()).includes('Il caricamento è riuscito'));await page.fill('[name=team]','');
 await page.selectOption('#mode','search');await page.fill('[name=team]','QA Casa 0');
 const opposite=page.locator('#results .selection').filter({has:page.locator('[data-coherence="opposed"]')});
 assert.equal(await opposite.count(),1);assert((await opposite.innerText()).includes('Scenario opposto alla triade Gold'));
 assert((await opposite.innerText()).includes('No Goal FT'));assert((await opposite.innerText()).includes('escluso dalle proposte principali'));
 await page.fill('[name=team]','');
 await page.selectOption('#mode','best');assert(await page.locator('#marketNavigation').isHidden());
 await page.click('#more');assert.equal(await page.locator('#results .selection').count(),20);
 await page.fill('[name=team]','impossibile');assert.equal(await page.locator('#results .selection').count(),0);await page.fill('[name=team]','');
 await page.fill('[name=probMin]','70');assert.equal(await page.locator('#results .selection').count(),0);await page.fill('[name=probMin]','');assert.equal(calls.length,initial,'Filtering must not issue requests');
 await page.selectOption('[name=market]','GG FT');await page.fill('#searchName','GG personale');await page.click('#saveSearch');await page.locator('#message').filter({hasText:'Ricerca salvata'}).waitFor();
 await page.selectOption('#savedSearches','saved-1');await page.locator('#message').filter({hasText:'appena aggiornati'}).waitFor();assert.equal(await page.inputValue('[name=market]'),'GG FT');
 await page.fill('#searchName','GG aggiornata');await page.click('#updateSearch');await page.locator('#message').filter({hasText:'Ricerca aggiornata'}).waitFor();assert.equal(searches[0].name,'GG aggiornata');
 await page.click('#deleteSearch');await page.locator('#message').filter({hasText:'Ricerca eliminata'}).waitFor();assert.equal(searches.length,0);
 await page.locator('#results [data-favorite]').first().click();await page.locator('#message').filter({hasText:'Selezione salvata'}).waitFor();await page.click('[data-tab=favorites]');await page.locator('#favoriteRows .selection').waitFor();assert.equal(await page.locator('#favoriteRows .won').count(),1);
 await page.locator('#favoriteRows [data-detail]').click();assert(await page.locator('#detail').isVisible());assert((await page.locator('#detailBody').innerText()).includes('Ultima quota Fast valida'));assert((await page.locator('#detailBody').innerText()).includes('Ruolo originale Gold'));await page.click('#closeDetail');
 await page.locator('#favoriteRows [data-remove]').click();await page.locator('#message').filter({hasText:'Preferito rimosso'}).waitFor();assert.equal(favorites.length,0);
 await page.click('[data-tab=history]');await page.click('#loadHistory');await page.locator('#message').filter({hasText:'Storico caricato'}).waitFor();assert.equal(await page.locator('#historyRows .selection').count(),10);assert((await page.locator('#comparisons').innerText()).includes('Calibrazione'));
 await page.selectOption('#historyReference','night');await page.selectOption('#historyCohort','top');assert.equal(await page.locator('#historyRows .selection').count(),2); // GG FT filter, exact stored top10
 await page.locator('#certificates').evaluate(el=>el.parentElement.open=true);await page.locator('#certificates button').click();assert.equal(await page.locator('#certificateRows .selection').count(),10);await page.selectOption('#certificateMode','market:GG FT');assert.equal(await page.locator('#certificateRows .selection').count(),10);await page.click('#closeDetail');
 await page.click('[data-tab=explore]');await page.selectOption('[name=market]','');await page.selectOption('#mode','best');fs.mkdirSync('test-artifacts',{recursive:true});
 for(const [name,width,height] of [['desktop',1440,1100],['tablet',768,1024],['mobile',390,844],['small-mobile',360,800]]){
  await page.setViewportSize({width,height});assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Overflow '+name);
  await page.screenshot({path:'test-artifacts/explorer-'+name+'.png',fullPage:true});
 }
 await page.clock.setFixedTime(new Date('2026-11-17T14:01:00Z'));await page.selectOption('#mode','probable');assert.equal(await page.locator('#results .selection').count(),0,'Started fixtures removed');
 await page.clock.setFixedTime(new Date(now));await page.selectOption('#mode','best');const retained=await page.locator('#results .selection').count();fail=true;await page.click('#refresh');await page.locator('#dataState').filter({hasText:'Aggiornamento non riuscito'}).waitFor();assert.equal(await page.locator('#results .selection').count(),retained);fail=false;
 empty=true;await page.click('#refresh');await page.locator('#message').filter({hasText:'Nessuna analisi'}).waitFor();assert.equal(await page.locator('#results .selection').count(),0);
 fail=true;await page.click('#refresh');await page.locator('#message').filter({hasText:/fetch|Connessione|network/i}).waitFor();assert((await page.locator('#dataState').innerText()).includes('Caricamento non riuscito'));fail=false;await page.click('#refresh');await page.locator('#message').filter({hasText:'Nessuna analisi'}).waitFor();assert.equal(await page.locator('#dataState').innerText(),'');status=401;await page.click('#refresh');await page.locator('#locked').waitFor();assert(await page.locator('#app').isHidden());
 status=200;empty=false;await page.goto('https://explorer.test/gold-dashboard-live.html');await page.locator('button').filter({hasText:'Gold Explorer'}).waitFor();const before=calls.length;assert.equal(await page.locator('iframe[src*="gold-explorer"]').count(),0);await page.getByRole('button',{name:'Gold Explorer ↗'}).click();await page.frameLocator('iframe[title="Gold Explorer"]').locator('#results .selection').first().waitFor();assert(calls.length>before);await page.getByRole('button',{name:'Chiudi ×',exact:true}).click();
 assert.deepEqual(errors,[]); // Gold page may make its own existing requests, blocked by this isolated mock.
 assert.equal(external.filter(u=>/api-football|api-sports|odds/.test(u)).length,0);
 status=200;empty=false;await page.goto('https://explorer.test/gold-explorer.html?fixture=1');
 await page.locator('#message').filter({hasText:'Dati caricati.'}).waitFor();
 assert(await page.locator('#fixtureFocus').isVisible());
 assert((await page.locator('#fixtureFocusText').innerText()).includes('QA Casa 1'));
 assert.equal(await page.locator('#results .selection').count(),5);
 const focusedCalls=calls.length;await page.click('#clearFixtureFocus');
 assert(await page.locator('#fixtureFocus').isHidden());
 assert.equal(await page.locator('#results .selection').count(),10);
 assert.equal(calls.length,focusedCalls,'Leaving the fixture filter adds no requests');
 futureFixture=true;await page.goto('https://explorer.test/gold-explorer.html?fixture=1');
 await page.locator('#message').filter({hasText:'Dati caricati.'}).waitFor();
 assert.equal(await page.inputValue('[name=period]'),'all','A fixture link must include future saved days');
 assert.equal(await page.locator('#results .selection').count(),5,'Tomorrow\'s selected fixture stays visible');
 assert((await page.locator('#fixtureFocusText').innerText()).includes('QA Casa 1'));
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({status:'PASS',rows:1250,viewports:4,explorer_requests:calls.length,page_errors:errors,football_requests:0,fixture:'synthetic QA only'}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
