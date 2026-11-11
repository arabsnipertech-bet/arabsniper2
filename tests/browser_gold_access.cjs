// Real browser, entirely synthetic OTP, entitlement and Stripe responses.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const web=path.join(__dirname,'../web');
const origin='https://gold-test.example', target='/gold-dashboard-live.html';
const now=Date.parse('2026-11-11T16:00:00Z')/1000, sessionEnd=now+86400;
const plans=[['pass3d','Gold 3 giorni',500,450],['pass14d','Gold 14 giorni',1500,1350],['pass1m','Gold mensile',2900,2610]];
module.exports=async function checkGoldAccess(browser) {
 const context=await browser.newContext({timezoneId:'America/Los_Angeles',viewport:{width:1280,height:920}});
 try {
  const page=await context.newPage();await page.clock.install({time:now*1000});
  let hours=72,promo=false,renewals=[],privateReads=0,authenticated=true,serverTime=now;
  function session() {return {email:'cliente.con.indirizzo.molto.lungo@example.test',authenticated:true,active:hours>0,
   expires_at:now+hours*3600,server_time:serverTime,session_expires_at:sessionEnd,
   renewal:{enabled:true,promo_eligible:promo,plans:plans.map(([key,label,amount,discount])=>({key,label,amount,payable_amount:promo?discount:amount}))}};}
  await page.route('**/*',async route=>{
   const request=route.request(),u=new URL(request.url());
   if(u.origin==='https://checkout.stripe.com')return route.fulfill({body:'Checkout di prova',contentType:'text/plain'});
   if(u.origin!==origin)return route.abort();
   if(u.pathname==='/api/auth/session')return route.fulfill(authenticated?{json:session()}:{status:401,json:{active:false}});
   if(u.pathname==='/api/auth/request-code')return route.fulfill({json:{ok:true,challenge:'a'.repeat(64)}});
   if(u.pathname==='/api/auth/verify-code'){authenticated=true;return route.fulfill({json:session()});}
   if(u.pathname==='/api/auth/renew'){
    renewals.push(request.postDataJSON());return route.fulfill({json:{url:'https://checkout.stripe.com/c/pay/cs_simulated'}});
   }
   if(u.pathname.startsWith('/api/gold-data/')){privateReads++;return route.fulfill({json:{matches:[],date:'2026-11-11'}});}
   const file=path.join(web,u.pathname),ext=path.extname(file);
   if(['.html','.js','.css'].includes(ext)&&fs.existsSync(file))return route.fulfill({path:file,contentType:{'.html':'text/html','.js':'application/javascript','.css':'text/css'}[ext]});
   return route.fulfill({status:404,body:''});
  });
  const panel=page.locator('#goldAccess'),state=page.locator('#goldAccessState'),workspace=page.locator('.gold-workspace');
  const visit=async(h,p=false,suffix='')=>{hours=h;promo=p;await page.goto(origin+target+suffix);await panel.waitFor({state:'visible'});};
  await visit(72);assert.equal(await state.textContent(),'ATTIVO');assert.equal(await panel.getAttribute('data-tone'),'green');
  assert.equal(await page.locator('#goldAccessExpiry').textContent(),'14/11/2026 17:00');
  const deskPanel=await panel.boundingBox(),deskBrand=await page.locator('.brand').boundingBox();assert(deskPanel.x>deskBrand.x+deskBrand.width,'Desktop panel at the top right');
  await visit(36,true);assert.equal(await state.textContent(),'IN SCADENZA');assert.equal(await panel.getAttribute('data-tone'),'yellow');
  assert.equal(await page.locator('#goldAccessExpiry').textContent(),'13/11/2026 05:00');
  assert(await page.locator('#goldAccessPromo').isVisible());
  const prior=await page.locator('#goldAccessRemaining').textContent();await page.clock.fastForward(61000);
  assert.notEqual(await page.locator('#goldAccessRemaining').textContent(),prior,'Countdown updates with no reload');
  assert.equal(page.url(),origin+target);
  fs.mkdirSync('test-artifacts',{recursive:true});
  await page.screenshot({path:'test-artifacts/gold-access-desktop.png',fullPage:true});
  for(const width of [390,360]){
   await page.setViewportSize({width,height:844});
   await visit(6,true);assert.equal(await panel.getAttribute('data-tone'),'red');assert.equal(await state.textContent(),'IN SCADENZA');
   await page.locator('#goldRenewalSummary').click();
   assert.match(await page.locator('#goldRenewalPlan option').nth(1).textContent(),/13,50/);
   await page.locator('#goldRenewalDiscount').uncheck();assert.match(await page.locator('#goldRenewalPlan option').nth(1).textContent(),/15,00/);
   await page.locator('#goldRenewalDiscount').check();
   const p=await panel.boundingBox(),w=await workspace.boundingBox();assert(p.width<=width&&p.y+p.height<=w.y,'Panel fits without covering the matches');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'No horizontal overflow at '+width);
   await page.screenshot({path:'test-artifacts/gold-access-mobile-'+width+'.png',fullPage:true});
  }
  await page.locator('#goldRenewalPlan').selectOption('pass14d');
  await Promise.all([page.waitForURL('https://checkout.stripe.com/**'),page.locator('#goldRenewalPay').click()]);
  assert.deepEqual(renewals,[{plan:'pass14d',use_promo:true}]);
  const count=privateReads;await visit(-1,false,'?renewal=success');
  assert.equal(await state.textContent(),'SCADUTO');assert.equal(await panel.getAttribute('data-tone'),'expired');
  assert.equal(await workspace.isVisible(),false);assert.equal(privateReads,count,'Return from Stripe cannot read private matches');
  assert.match(await page.locator('#goldAccessMessage').textContent(),/Rientro da Stripe/);
  await page.locator('#goldRenewalSummary').click();
  assert(await page.locator('#goldRenewalPay').isVisible());
  authenticated=false;await page.goto(origin+target);
  await page.locator('#loginEmail').fill('cliente@example.test');await page.locator('#loginButton').click();
  await page.locator('#loginCode').fill('123456');await page.locator('#loginButton').click();
  await panel.waitFor({state:'visible'});assert.equal(await state.textContent(),'SCADUTO');assert.equal(privateReads,count);
  // A visible active panel also locks the sports workspace at the exact boundary.
  await visit(1/3600,false);await workspace.waitFor({state:'visible'});
  hours=0;serverTime=now+2;await page.clock.runFor(2500);
  await workspace.waitFor({state:'hidden'});assert.equal(await state.textContent(),'SCADUTO');
  console.log('Gold access browser: Rome expiry, countdown, 3 states, Stripe redirect, expired OTP, desktop and two mobile widths passed.');
 } finally {await context.close();}
};
if(require.main===module){
 const {chromium}=require('playwright');
 (async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH});try{await module.exports(browser);}finally{await browser.close();}})().catch(err=>{console.error(err);process.exitCode=1;});
}
