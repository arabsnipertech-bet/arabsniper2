// Calendar lookup uses the published date, never a stale positional day number.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync('web/gold-dashboard-live.html', 'utf8');
const calendarSource = html.slice(html.indexOf('    function calendarDate('), html.indexOf('    let loginChallenge'));
const loadSource = html.slice(html.indexOf('    async function loadCalendarDay('), html.indexOf('    const RESEARCH_MARKET_OPTIONS'));
const calls = [], elements = new Map();
let feeds = {}, rendered = [];
const element = id => {
  if (!elements.has(id)) elements.set(id, {hidden:false, textContent:'', innerHTML:'', remove(){}});
  return elements.get(id);
};
const context = {
  Date, Intl, Set, console:{error(){}}, currentDay:1, dayLoadSequence:0,
  currentPayload:null, currentMatches:[], filteredMatches:[], window:{},
  document:{hidden:false, getElementById:element},
  formatDateIt:date=>date.slice(5).split('-').reverse().join('/'),
  formatTimestamp:x=>x, escapeHtml:String, closeDetail(){},
  renderMarketRankings(){}, renderMatchResearch:p=>rendered.push(p.date),
  loadModelResearch(){}, applyFilters(){}, setDefaultHourForDay(){},
  buildDayButtons(){context.calendarBase=context.calendarDate();},
  loadJson:async file=>{calls.push(file); return feeds[file];}
};
vm.createContext(context);
vm.runInContext(calendarSource + loadSource, context);
const day = date => ({date, updated_at:'2026-10-28T23:22:00Z', matches:[{untouched:true}], multimarket_method:{}});
const files = dates => Object.fromEntries(dates.map((date,i)=>[`arab_quote_day${i+1}.json`, day(date)]));
(async()=>{
  assert.equal(context.calendarDate(0,new Date('2026-10-28T23:01:00Z')),'2026-10-29');
  assert.equal(context.calendarDate(0,new Date('2026-10-24T22:01:00Z')),'2026-10-25');
  assert.equal(context.calendarDate(1,new Date('2026-10-24T22:01:00Z')),'2026-10-26');
  assert.equal(context.calendarDate(4,new Date('2026-10-28T23:01:00Z')),'2026-11-02');
  assert.equal(context.calendarDate(1,new Date('2028-02-28T12:00:00Z')),'2028-02-29');
  feeds=files(['2026-10-28','2026-10-29','2026-10-30','2026-10-31','2026-11-01']);
  const before=JSON.stringify(feeds);
  let result=await context.loadCalendarDay(1,'2026-10-29');
  assert.equal(result.file,'arab_quote_day2.json');
  assert.equal(result.payload.date,'2026-10-29');
  assert.equal(result.shifted,true);
  assert.equal(calls.length,2,'Only the expected slot and its date offset are downloaded');
  assert.equal(JSON.stringify(feeds),before,'Saved predictions and timestamps remain immutable');
  assert.equal((await context.loadCalendarDay(1,'2026-10-30')).file,'arab_quote_day3.json');
  assert.equal((await context.loadCalendarDay(4,'2026-11-01')).file,'arab_quote_day5.json');
  await assert.rejects(context.loadCalendarDay(5,'2026-11-02'),e=>e.code==='GOLD_DAY_PENDING');
  // Fast refresh has advanced only the first two slots; the later ones are older.
  feeds=files(['2026-10-29','2026-10-30','2026-10-30','2026-10-31','2026-11-01']);
  assert.equal((await context.loadCalendarDay(1,'2026-10-29')).shifted,false);
  assert.equal((await context.loadCalendarDay(3,'2026-10-31')).file,'arab_quote_day4.json');
  // Publication can move between GETs: fail visibly instead of using a wrong date.
  feeds=files(['2026-10-28','2026-10-30']);
  await assert.rejects(context.loadCalendarDay(1,'2026-10-29'),e=>e.code==='GOLD_DAY_PENDING');
  for (const invalid of [null,'2026-02-30','2026-10-29T00:00:00Z']) {
    feeds=files([invalid]);
    await assert.rejects(context.loadCalendarDay(1,'2026-10-29'),/Data della giornata non valida/);
  }
  // A slower previous selection must not overwrite the user's newer selection.
  const pending=[];
  context.loadCalendarDay=(slot,date)=>new Promise(resolve=>pending.push({slot,date,resolve}));
  context.currentDay=1; const first=context.loadCurrentDay();
  context.currentDay=2; const second=context.loadCurrentDay();
  pending[1].resolve({payload:day(pending[1].date),file:'arab_quote_day2.json',shifted:false});
  await second;
  pending[0].resolve({payload:day(pending[0].date),file:'arab_quote_day1.json',shifted:false});
  await first;
  assert.deepEqual(rendered,[pending[1].date]);
  assert.equal(context.currentPayload.date,pending[1].date);
  context.loadCalendarDay=async()=>{const e=new Error('Giornata non ancora pubblicata');e.code='GOLD_DAY_PENDING';throw e;};
  await context.loadCurrentDay();
  assert.equal(context.currentPayload,null);
  assert.equal(element('liveStatus').textContent,'Giornata in aggiornamento');
  assert.equal(element('matchCount').textContent,'0 partite');
  assert(element('matchResearch').innerHTML.includes('non ancora pubblicata'));
  console.log('Gold calendar: Rome midnight/DST, shifted/mixed feeds, missing dates, immutable forecasts and request races passed.');
})().catch(error=>{console.error(error);process.exitCode=1;});
