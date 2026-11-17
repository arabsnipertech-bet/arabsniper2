export const categories={official:'Ufficiale V7',alternative:'Alternativa Explorer',observation:'In osservazione',unqualified:'Non qualificata',legacy:'Archivio V7'};
export const modes={best:'Migliori del giorno',probable:'Più probabili',recommended:'Consigliate',shots:'Colpacci',markets:'Migliori per mercato',search:'Ricerca avanzata'};
export const text=v=>typeof v==='string'?v:typeof v==='number'?String(v):v?.name||'';
export const numeric=v=>typeof v==='number'&&Number.isFinite(v);
const formatter=new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Rome',year:'numeric',month:'2-digit',day:'2-digit'});
export const romeDay=t=>formatter.format(t);
export const fresh=(r,now=Date.now())=>r.quote_usable&&Date.parse(r.quote_valid_until)>now;
export function matches(r,f,now=Date.now(),historical=false){
 if(f.team&&!(text(r.home)+' '+text(r.away)).toLocaleLowerCase('it').includes(f.team.toLocaleLowerCase('it')))return false;
 for(const [key,field] of [['league','league'],['market','label'],['reliability','reliability'],['category','category']])if(f[key]&&text(r[field])!==f[key])return false;
 if(f.official==='yes'&&!r.roles?.length||f.official==='no'&&r.roles?.length)return false;
 for(const [key,field,scale] of [['oddMin','odd',1],['probMin','probability',100],['evMin','ev',100],['lowMin','ev_low',100],['ociMin','oci',1]])if(f[key]!=null&&f[key]!==''&&(!numeric(r[field])||r[field]*scale<Number(f[key])))return false;
 if(f.oddMax!=null&&f.oddMax!==''&&(!numeric(r.odd)||r.odd>Number(f.oddMax)))return false;
 if(historical)return true;
 const at=Date.parse(r.kickoff);if(!Number.isFinite(at)||at<=now)return false;
 const day=romeDay(at),today=romeDay(now),noon=Date.parse(today+'T12:00:00Z');
 if(day>romeDay(noon+4*86400000))return false;
 if(f.period==='today'&&day!==today||f.period==='tomorrow'&&day!==romeDay(noon+86400000))return false;
 if(['3','6'].includes(f.period)&&at>now+Number(f.period)*3600000)return false;
 if(f.period==='custom'&&(f.from&&day<f.from||f.to&&day>f.to))return false;
 return true;
}
export function rank(rows,mode='best',diversify=true){
 const role={recommended:'recommended_alternative',shots:'high_odds_shot'}[mode];
 const candidates=rows.filter(r=>r.validated&&(!role||r.ranking_roles?.includes(role)));
 const tie=(a,b)=>a.market===b.market?(a.id===b.id?0:a.id>b.id?-1:1):a.market>b.market?-1:1;
 candidates.sort((a,b)=>mode==='probable'?b.probability-a.probability||b.ev_low-a.ev_low||b.ev-a.ev||tie(a,b):Number(b.niche)-Number(a.niche)||b.ev-a.ev||b.probability-a.probability||tie(a,b));
 const seen=new Set();return candidates.filter(r=>{if(diversify&&seen.has(r.fixture_id))return false;seen.add(r.fixture_id);return true;});
}
export function outcome(row,result){
 if(!result||result.status!=='settled'||Date.parse(result.kickoff)!==Date.parse(row.kickoff))return null;
 const value=result.outcomes?.[row.market];if(!value||!numeric(value.win)||!numeric(value.loss))return null;
 return {...value,profit:numeric(row.odd)&&row.odd>1?value.win*(row.odd-1)-value.loss:null,
  binary:value.win+value.loss===1&&[0,1].includes(value.win),label:value.win===1?'Vinta':value.loss===1?'Persa':value.win===0&&value.loss===0?'Rimborsata':'Esito parziale'};
}
export function statistics(rows){
 const verified=rows.filter(r=>r.outcome),binary=verified.filter(r=>r.outcome.binary),priced=verified.filter(r=>numeric(r.outcome.profit)),probabilistic=binary.filter(r=>numeric(r.probability));
 const mean=xs=>xs.length?xs.reduce((a,b)=>a+b,0)/xs.length:null,won=binary.filter(r=>r.outcome.win===1).length;
 return {n:rows.length,verified:verified.length,won,lost:binary.length-won,hitRate:binary.length?won/binary.length:null,coverage:rows.length?verified.length/rows.length:null,
  roi:mean(priced.map(r=>r.outcome.profit)),roiN:priced.length,ev:mean(rows.map(r=>r.ev).filter(numeric)),brier:mean(probabilistic.map(r=>(r.probability-r.outcome.win)**2)),binaryN:probabilistic.length,
  bins:Array.from({length:10},(_,i)=>{const group=probabilistic.filter(r=>Math.min(9,Math.floor(r.probability*10))===i);return {range:`${i*10}–${(i+1)*10}%`,n:group.length,predicted:mean(group.map(r=>r.probability)),observed:mean(group.map(r=>r.outcome.win))};})};
}
export function historyRows(history,snapshots,reference='final',cohort='validated',board='best'){
 const result=[];
 for(const [episode,ref] of Object.entries(history.references||{})){
  const hash=ref[reference],snap=snapshots.get(hash);if(!snap)continue;
  if(!snap.byEpisode){snap.byEpisode=new Map();for(const r of snap.rows){if(!snap.byEpisode.has(r.episode))snap.byEpisode.set(r.episode,[]);snap.byEpisode.get(r.episode).push(r);}}
  const top=new Set((snap.boards?.[board]||[]).slice(0,10));
  for(const row of snap.byEpisode.get(episode)||[]){
   if(cohort==='validated'&&!row.validated||cohort==='top'&&!top.has(row.id)||cohort==='official'&&!row.roles?.length||cohort==='alternative'&&(!row.validated||row.roles?.length))continue;
   let selected=row;
   if(cohort==='official'){
    const original=Object.values(row.official_references||{})[0];
    if(original){const same=Date.parse(original.captured_at)===Date.parse(row.captured_at);selected={...row,odd:original.odd??null,probability:same?row.probability:null,ev:same?row.ev:null,ev_low:same?row.ev_low:null,official_original:true};}
   }
   result.push({...selected,snapshot:hash,reference,fallback_night:ref.fallback_night,outcome:outcome(selected,history.results?.[row.fixture_id])});
  }
 }
 return result;
}
