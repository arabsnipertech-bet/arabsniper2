const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync('web/gold-dashboard-live.html','utf8');
const source=html.slice(html.indexOf('    let loginChallenge = null;'),html.indexOf('    function showApplication()'));
const nodes=new Map(),node=id=>{if(!nodes.has(id))nodes.set(id,{value:'',hidden:false,disabled:false,textContent:'',focus(){},addEventListener(){}});return nodes.get(id);};
let calls=[],opened=0,mode='session';
const context={document:{getElementById:node},STORAGE_KEY:'old',localStorage:{removeItem(){throw Error('blocked');}},sessionStorage:{getItem(){throw Error('blocked');},setItem(){throw Error('blocked');},removeItem(){throw Error('blocked');}},location:{search:''},URLSearchParams,AbortController,setTimeout,clearTimeout,setInterval:()=>1,clearInterval(){},showApplication(){opened++;},
fetch:async(url,options)=>{calls.push([url,options]);if(url.endsWith('session'))return Response.json({active:false},{status:401});if(url.endsWith('request-code'))return Response.json({challenge:'a'.repeat(64)});if(mode==='bad')return Response.json({error:'Codice errato, scaduto o già utilizzato.'},{status:400});return Response.json({active:true});}};
vm.createContext(context);vm.runInContext(source,context);
(async()=>{
 await context.initializeAccess();assert.equal(calls.length,1,'Blocked local storage cannot prevent session validation');assert.equal(node('loginError').textContent,'');
 node('loginEmail').value=' Test@example.com ';await context.submitLogin({preventDefault(){}});
 assert.equal(node('loginCode').hidden,false);assert(node('loginStatus').textContent.includes('test@example.com'));assert.equal(node('loginError').textContent,'');assert(node('resendCode').disabled);
 const sent=calls.length;await context.resendLoginCode();assert.equal(calls.length,sent,'Cooldown cannot send twice');
 node('loginCode').value='12';await context.submitLogin({preventDefault(){}});assert.equal(calls.length,sent);assert(node('loginError').textContent.includes('6 cifre'));
 mode='bad';node('loginCode').value='123456';await context.submitLogin({preventDefault(){}});assert.equal(opened,0);assert(node('loginError').textContent.includes('Codice errato'));assert(!node('loginButton').disabled);
 mode='valid';await context.submitLogin({preventDefault(){}});assert.equal(opened,1);assert.equal(node('loginCode').value,'');assert(node('loginCode').hidden);
 assert(!calls.some(([url])=>!url.startsWith('/api/auth/')),'No account or entitlement changes');
 console.log('PASS Login UI: blocked storage, separate success/error, code validation, resend cooldown and successful access');
})().catch(error=>{console.error(error);process.exitCode=1;});

{
 const elements={legacyDashboard:{hidden:true},matchesGrid:{innerHTML:'old'},matchCount:{textContent:''}};
 const isolated={document:{getElementById:id=>elements[id]},currentMatches:[{fixture:{id:1}}],filteredMatches:[],renderMatches(){throw Error('Hidden legacy cards must not render');}};
 vm.createContext(isolated);vm.runInContext(html.slice(html.indexOf('    function applyFilters()'),html.indexOf('    function relationClass')),isolated);
 isolated.applyFilters();assert.equal(elements.matchesGrid.innerHTML,'');assert.equal(elements.matchCount.textContent,'1 partite');
}
