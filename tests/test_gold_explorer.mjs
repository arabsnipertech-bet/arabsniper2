import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {explorerRoute} from '../workers/stripe-access/explorer.mjs';
import {sessionEmail} from '../workers/stripe-access/auth.mjs';
import {matches,rank,goalRows,outcome,statistics,historyRows,romeDay} from '../web/explorer-core.mjs';
const origin='https://arabsniperbet.com',now=1800000000;
const hash=s=>createHash('sha256').update(s).digest('hex');
function setup(){
 const db=new DatabaseSync(':memory:');for(const f of ['auth-schema.sql','explorer-schema.sql'])db.exec(readFileSync(new URL('../workers/stripe-access/'+f,import.meta.url),'utf8'));
 const objects=new Map(),reads=[],env={AUTH_ENABLED:'true',GOLD_EXPLORER_ENABLED:'true',AUTH_ORIGIN:origin,ACCESS_DB:{prepare(sql){return{bind(...args){return{all:async()=>({results:db.prepare(sql).all(...args)}),run:async()=>db.prepare(sql).run(...args)}}}}},GOLD_DATA:{get:async key=>{reads.push(key);assert(key.startsWith('explorer/'));const raw=objects.get(key);return raw?{json:async()=>JSON.parse(raw),arrayBuffer:async()=>new TextEncoder().encode(raw).buffer}:null;}}};
 const add=value=>{const raw=JSON.stringify(value),id=hash(raw);objects.set('explorer/objects/'+id+'.json',raw);return id;};
 function cookie(email){const token=hash(email);db.prepare('INSERT OR REPLACE INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)').run(hash('__Host-gold_session_24h:'+token),email,now+86400);return '__Host-gold_session_24h='+token;}
 let active=true,source='stripe_subscription';const access=async()=>({active,source});
 const call=(path,body,email='one@test.it',t=now,headers={})=>explorerRoute(new Request(origin+'/api/explorer/'+path,{method:body?'POST':'GET',headers:{Cookie:email?cookie(email):'',Origin:origin,'Content-Type':'application/json',...headers},...(body?{body:JSON.stringify(body)}:{})}),env,{sessionEmail,access},t);
 return{db,env,objects,reads,add,call,setActive:v=>active=v,setSource:v=>source=v};
}
globalThis.fetch=async()=>{throw Error('Outbound network forbidden in Explorer');};
test('Gold active and trial allowed; missing, expired, revoked sessions and disabled feature fail closed',async()=>{
 const s=setup();s.objects.set('explorer/current.json','{"count":0}');
 assert.equal((await s.call('current')).status,200);s.setSource('trial');assert.equal((await s.call('current')).status,200);
 assert.equal((await s.call('current',null,null)).status,401);assert.equal((await s.call('current',null,'one@test.it',now+86400)).status,401);
 s.setActive(false);assert.equal((await s.call('current')).status,403);s.env.GOLD_EXPLORER_ENABLED='false';assert.equal((await s.call('current')).status,503);
});
test('Personal searches isolated across accounts; CRUD whitelist CSRF size rate limits',async()=>{
 const s=setup();const response=await s.call('searches',{name:'GG',filters:{market:'GG FT',probMin:55,diversify:false}});assert.equal(response.status,200);const {id}=await response.json();
 assert.equal((await (await s.call('searches',null,'two@test.it')).json()).items.length,0);
 await s.call('delete',{type:'search',id},'two@test.it');assert.equal((await (await s.call('searches')).json()).items.length,1);
 await s.call('searches',{id,name:'Modificata',filters:{team:'Roma'}});assert.equal((await (await s.call('searches')).json()).items[0].name,'Modificata');
 assert.equal((await s.call('searches',{name:'x',filters:{url:'https://api-football.com'}})).status,400);
 assert.equal((await s.call('searches',{name:'x',filters:{}},'one@test.it',now,{Origin:'https://evil.test'})).status,403);
 assert.equal((await s.call('searches',{name:'x'.repeat(9000),filters:{}})).status,400);
 await s.call('delete',{type:'search',id});assert.equal((await (await s.call('searches')).json()).items.length,0);
 let response2;for(let i=0;i<35;i++)response2=await s.call('delete',{type:'search',id});assert.equal(response2.status,429);
});
test('Favorites take canonical server snapshot, immutable on repeat, private and paginated; tampered SHA rejected',async()=>{
 const s=setup(),row={id:'a'.repeat(64),date:'2026-11-17',fixture_id:'1',odd:2.5,probability:.5},object=s.add({rows:[row]});
 assert.equal((await s.call('favorites',{id:row.id,object,odd:99,email:'two@test.it'})).status,200);
 const updated=s.add({rows:[{...row,odd:3}]});await s.call('favorites',{id:row.id,object:updated});
 let list=await(await s.call('favorites')).json();assert.equal(list.items[0].snapshot.odd,2.5);assert.equal(list.items[0].saved_at,now);
 assert.equal((await(await s.call('favorites',null,'two@test.it')).json()).items.length,0);
 await s.call('delete',{type:'favorite',id:row.id},'two@test.it');assert.equal((await(await s.call('favorites')).json()).items.length,1);
 for(let i=0;i<21;i++)s.db.prepare('INSERT INTO explorer_favorites VALUES (?,?,?,?)').run('one@test.it',String(i),JSON.stringify(row),now);
 list=await(await s.call('favorites')).json();assert.equal(list.items.length,20);assert.equal(list.next,20);assert.equal((await(await s.call('favorites?offset=20')).json()).items.length,2);
 s.objects.set('explorer/objects/'+object+'.json','{}');assert.equal((await s.call('objects/'+object)).status,503);
 assert.equal(await s.call('objects/../../gold'),null);
});
const row={id:'a',fixture_id:'1',episode:'1::k',market:'m',label:'GG FT',home:'Roma',away:'Torino',league:'Serie A',kickoff:'2026-11-17T20:00:00Z',probability:.6,odd:2,ev:.2,ev_low:.05,oci:55,reliability:'supported',category:'alternative',roles:[],validated:true,niche:false,ranking_roles:['recommended_alternative']};
test('All filters combine, Rome dates and DST are stable; missing metrics are never zero',()=>{
 const t=Date.parse('2026-11-17T17:00:00Z');assert(matches(row,{team:'ROMA',league:'Serie A',market:'GG FT',oddMin:1.5,oddMax:2.5,probMin:60,evMin:20,lowMin:5,ociMin:50,reliability:'supported',category:'alternative',official:'no',period:'3'},t));
 for(const f of [{team:'Milan'},{league:'B'},{market:'Under'},{oddMin:3},{oddMax:1.5},{probMin:61},{evMin:21},{lowMin:6},{ociMin:56},{reliability:'limited'},{category:'official'},{official:'yes'},{period:'tomorrow'},{period:'custom',from:'2026-11-18'}])assert(!matches(row,f,t),JSON.stringify(f));
 assert(!matches({...row,ev:null},{evMin:0},t));assert(!matches(row,{},Date.parse(row.kickoff)));assert.equal(romeDay(Date.parse('2026-10-25T23:30:00Z')),'2026-10-26');
});
test('Ranking diversifies and obeys V7 order; unqualified never enter; all five boards derive unchanged rows',()=>{
 const rows=[row,{...row,id:'b',probability:.8,ev:.1},{...row,id:'c',fixture_id:'2',niche:true},{...row,id:'d',fixture_id:'3',validated:false,ev:5}];
 assert.deepEqual(rank(rows).map(r=>r.id),['c','a']);assert.deepEqual(rank(rows,'probable').map(r=>r.id),['b','c']);assert.equal(rank(rows,'best',false).length,3);assert.equal(rank(rows,'shots').length,0);assert.equal(rank(rows,'recommended').length,2);
});
test('Opposed alternatives stay searchable but cannot enter main rankings; legacy and compatible rows survive',()=>{
 const opposite={...row,id:'opposed',fixture_id:'2',ev:.8,coherence_status:'opposed'};
 const compatible={...row,id:'aligned',fixture_id:'3',coherence_status:'compatible'};
 const broken={...row,id:'broken',fixture_id:'4',coherence_status:'inconsistent_gold_triad'};
 const rows=[row,opposite,compatible,broken],before=JSON.stringify(rows);
 for(const mode of ['best','probable','recommended'])assert.deepEqual(new Set(rank(rows,mode).map(r=>r.id)),new Set(['a','aligned']));
 assert(matches(opposite,{},Date.parse('2026-11-17T17:00:00Z')),'Advanced search retains the original alternative');
 assert.equal(JSON.stringify(rows),before);
});
test('Goal browsing gives each familiar market space without changing prices, probabilities, roles or existing rankings',()=>{
 const proposal=(id,kind,period,side,line,ev,fixture=id)=>Object.freeze({...row,id,fixture_id:fixture,market:JSON.stringify({kind,period,side,...(line===undefined?{}:{line})}),ev});
 const over=proposal('ht','total','ht','over',1.5,.05,'shared');
 const ft=proposal('ft','total','ft','over',2.5,.20,'shared');
 const ftNext=proposal('ft-next','total','ft','over',2.5,.10);
 const gg=proposal('gg','btts','ft','yes',undefined,.30);
 const ggNext=proposal('gg-next','btts','ft','yes',undefined,.25);
 const ah=proposal('ah','handicap','ft','home',-.25,9);
 const noGoal=proposal('no-goal','btts','sh','no',undefined,8);
 const under=proposal('under','total','ft','under',2.5,7);
 const asian=proposal('asian','asian_total','ht','over',1.5,6);
 const bad={...over,id:'bad',fixture_id:'bad',validated:false,ev:5};
 const opposed={...over,id:'opposed',fixture_id:'opposed',coherence_status:'opposed',ev:4};
 const rows=Object.freeze([ah,noGoal,under,asian,ggNext,ft,gg,over,ftNext,bad,opposed]);
 const before=JSON.stringify(rows),originalOrder=rank(rows).map(r=>r.id);
 const selected=goalRows(rows);
 assert.deepEqual(selected.map(r=>r.id),['ht','ft-next','gg','gg-next']);
 assert.strictEqual(selected[0],over);assert.equal(new Set(selected.map(r=>r.fixture_id)).size,selected.length);
 assert.deepEqual(goalRows(rows,false).map(r=>r.id),['ht','ft','gg','ft-next','gg-next']);
 assert.deepEqual(rank(rows).map(r=>r.id),originalOrder);assert.equal(originalOrder[0],'ah');
 assert.equal(JSON.stringify(rows),before);
 assert.deepEqual(goalRows([ah,noGoal,under,asian,bad,opposed]),[],'No weaker or incompatible substitute is invented');
});
test('A canonical Asian specification cannot masquerade as a binary Over, while old exact goal labels remain readable',()=>{
 const exact={...row,id:'old',label:'Over 1,5 PT',market:'legacy-market',kind:'total',period:'ht'};
 const disguised={...exact,id:'asian',market:JSON.stringify({kind:'asian_total',period:'ht',side:'over',line:1.5})};
 const contradictory={...exact,id:'handicap',kind:'handicap'};
 assert.deepEqual(goalRows([exact,disguised,contradictory],false).map(r=>r.id),['old']);
});
test('Outcome, ROI, Brier and partial refunds; no result without matching kickoff, no imputed odds',()=>{
 const result={kickoff:row.kickoff,status:'settled',outcomes:{m:{win:1,loss:0}}},won={...row,outcome:outcome(row,result)},lost={...row,probability:.4,outcome:{win:0,loss:1,binary:true,profit:-1}};
 const s=statistics([won,lost,{...row,outcome:null}]);assert.equal(s.coverage,2/3);assert.equal(s.hitRate,.5);assert.equal(s.roi,0);assert(Math.abs(s.brier-.16)<1e-10);
 assert.equal(outcome(row,{...result,kickoff:'2026-11-18T20:00:00Z'}),null);assert.equal(outcome({...row,odd:null},result).profit,null);
 assert.equal(outcome(row,{...result,outcomes:{m:{win:.5,loss:0}}}).binary,false);
});
test('Certified cohorts use stored membership; official original probability unavailable is not invented',()=>{
 const official={...row,roles:['most_probable'],official_references:{most_probable:{odd:2.4,captured_at:'2026-11-17T10:00:00Z'}},captured_at:'2026-11-17T11:00:00Z'};
 const history={references:{'1::k':{night:'n',final:'f'}},results:{}},snapshots=new Map([['f',{rows:[official],boards:{best:[]}}],['n',{rows:[row],boards:{best:['a']}}]]);
 assert.equal(historyRows(history,snapshots,'final','top').length,0);assert.equal(historyRows(history,snapshots,'night','top').length,1);
 const saved=historyRows(history,snapshots,'final','official')[0];assert.equal(saved.odd,2.4);assert.equal(saved.probability,null);assert.equal(saved.ev,null);
});

test('Existing auth route alias enforces the same server authorization',async()=>{
 const s=setup();s.objects.set('explorer/current.json','{}');
 const req=new Request(origin+'/api/auth/explorer/current');
 assert.equal((await explorerRoute(req,s.env,{sessionEmail,access:async()=>({active:true})},now)).status,401);
 const allowed=await explorerRoute(req,s.env,{sessionEmail:async()=> 'authorized@test.it',access:async()=>({active:true})},now);
 assert.equal(allowed.status,200);
});
