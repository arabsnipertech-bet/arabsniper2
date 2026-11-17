/* Live deployment smoke. Short-lived owner session supplied only via a private temp file. */
const {chromium}=require('playwright'),fs=require('fs'),assert=require('node:assert/strict');
(async()=>{
 const secret=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||'/usr/bin/google-chrome',args:['--no-sandbox']});
 const context=await browser.newContext({viewport:{width:1440,height:1000}});
 await context.addCookies([{name:'__Host-gold_session_24h',value:secret.token,domain:'arabsniperbet.com',path:'/',secure:true,httpOnly:true,sameSite:'Lax'}]);
 const page=await context.newPage(),errors=[],forbidden=[];
 page.on('pageerror',e=>errors.push(e.name));
 await context.route('**/*',async route=>{const u=new URL(route.request().url());if(/api-football|api-sports|odds-api/i.test(u.hostname)){forbidden.push(u.hostname);return route.abort();}return route.continue();});
 const started=Date.now();
 await page.goto('https://arabsniperbet.com/gold-explorer.html',{waitUntil:'domcontentloaded'});
 await page.locator('#message').filter({hasText:'Dati caricati.'}).waitFor({timeout:180000});
 const loadMs=Date.now()-started;
 const count=await page.locator('#metrics .metric strong').nth(1).textContent();assert(Number(count.replace(/\D/g,''))>0);
 fs.mkdirSync('test-artifacts',{recursive:true});
 for(const [name,width,height] of [['desktop',1440,1000],['tablet',820,1180],['mobile',390,844]]){
  await page.setViewportSize({width,height});
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  await page.screenshot({path:'test-artifacts/explorer-live-'+name+'.png',fullPage:false});
 }
 let requests=0;page.on('request',r=>{if(new URL(r.url()).pathname.startsWith('/api/auth/explorer/'))requests++;});
 await page.locator('#mode').selectOption('search');await page.locator('[name=team]').fill('zzzz-no-match');
 await page.locator('#filterForm').dispatchEvent('input');
 assert.equal(requests,0,'Local filters unexpectedly fetched data');
 assert.equal(errors.length,0);assert.equal(forbidden.length,0);
 await browser.close();
 fs.writeFileSync('test-artifacts/explorer-live-browser.json',JSON.stringify({loadMs,rows:count,viewports:3,jsErrors:errors.length,footballRequests:forbidden.length,filterRequests:requests}));
})().catch(e=>{console.error('Live browser smoke failed:',e.name);process.exit(1);});
