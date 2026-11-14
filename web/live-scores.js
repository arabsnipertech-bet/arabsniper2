/* Independent display overlay: never mutates the prematch payload. */
(function(root){
  const ACTIVE=new Set(['1H','HT','2H','ET','BT','P','LIVE']);
  const FINAL=new Set(['FT','AET','PEN']);
  const HALF_COMPLETE=new Set(['HT','2H','ET','BT','P','FT','AET','PEN']);
  const ADMIN={PST:'Rinviata',CANC:'Annullata',ABD:'Abbandonata',AWD:'Assegnata a tavolino',WO:'Walkover',SUSP:'Sospesa',INT:'Interrotta'};
  const validScore=s=>s&&Number.isInteger(s.home)&&Number.isInteger(s.away)&&s.home>=0&&s.away>=0;
  const finalScore=l=>FINAL.has(l?.status)&&(validScore(l.fulltime)||l.status==='FT'&&validScore(l.goals));
  const savedScore=r=>({home:r?.home_ft,away:r?.away_ft});
  const observationTime=l=>Date.parse(l?.observed_at)||0;
  let scores={},enabled=false,polling=false,lastPollAt=0,staleAfter=300000;
  function reconcile(previous,row) {
    if(!previous)return row;
    if(observationTime(row)<observationTime(previous)||finalScore(previous)&&!FINAL.has(row.status))return previous;
    const next={...row};
    if(HALF_COMPLETE.has(next.status)&&!validScore(next.halftime)&&HALF_COMPLETE.has(previous.status)&&validScore(previous.halftime)){
      next.halftime=previous.halftime;
      next.halftime_observed_at=previous.halftime_observed_at||previous.observed_at;
    }
    if(finalScore(previous)&&!finalScore(next)){
      next.status=previous.status;
      next.fulltime=validScore(previous.fulltime)?previous.fulltime:previous.goals;
      next.goals=previous.goals;
      next.fulltime_observed_at=previous.fulltime_observed_at||previous.observed_at;
    }
    return next;
  }
  function fixture(match) {
    const f=match.fixture||{},candidate=scores[String(f.fixture_id)];
    const r=f.result, saved=FINAL.has(r?.status)&&validScore(savedScore(r));
    let old=f.live;
    if(saved&&(!finalScore(old)||Date.parse(r.settled_at)>observationTime(old))){
      old={status:r.status,fulltime:savedScore(r),halftime:{home:r.home_ht,away:r.away_ht},observed_at:r.settled_at};
    }
    const live=candidate?reconcile(old,candidate):old;
    if(!live||live===f.live)return f;
    return {...f,live,status_short:live.status,elapsed:live.elapsed};
  }
  function view(match,now=Date.now()) {
    const f=fixture(match),l=f.live||{},status=l.status||f.status_short||f.result?.status;
    const observed=Date.parse(l.observed_at),fresh=Number.isFinite(observed)&&now-observed<=staleAfter&&now>=observed-30000&&!l.refresh_failed;
    const active=ACTIVE.has(status),finished=FINAL.has(status);
    const pair=status==='FT'&&validScore(l.fulltime)?l.fulltime:
      (active||finished||status==='SUSP'||status==='INT')&&validScore(l.goals)?l.goals:
      finished&&validScore(l.fulltime)?l.fulltime:null;
    const score=pair?`${pair.home}–${pair.away}`:null;
    const minute=Number.isFinite(l.elapsed)?`${l.elapsed}${Number.isFinite(l.extra)&&l.extra>0?'+'+l.extra:''}′`:'';
    const label=finished?'Terminata':ADMIN[status]|| (active?(fresh?(status==='HT'?'Intervallo':status==='BT'?'Pausa supplementari':status==='P'?'Rigori':'LIVE'):'Da aggiornare'):'Orario di inizio trascorso');
    return {active:active&&fresh,stale:active&&!fresh,finished,score,minute,label,observed_at:l.observed_at,
      visible:active||finished||Object.hasOwn(ADMIN,status)||Date.parse(f.kickoff)<=now};
  }
  function accept(data) {
    if(data?.schema_version!==1||!data.fixtures||typeof data.fixtures!=='object'||Array.isArray(data.fixtures)) throw new Error('Invalid live data');
    staleAfter=data.stale_after_seconds===360?360000:300000;
    enabled=data.enabled===true;
    if(!enabled){scores={};return;}
    const next={};
    for(const [id,row] of Object.entries(data.fixtures)) {
      if(!/^\d+$/.test(id)||String(row.fixture_id)!==id||typeof row.status!=='string'||!Number.isFinite(Date.parse(row.observed_at)))continue;
      next[id]=reconcile(scores[id],row);
    }
    scores=next;
  }
  function firstHalf(match,now=Date.now()) {
    const f=fixture(match),l=f.live||{},status=l.status||f.status_short||f.result?.status;
    if(status==='1H')return {status:'in_progress',score:validScore(l.goals)?`${l.goals.home}–${l.goals.away}`:null,stale:view(match,now).stale};
    if(!HALF_COMPLETE.has(status))return {status:'pending',score:null};
    const saved=match.fixture||{},r=f.result||{};
    const candidates=[l.halftime];
    if(FINAL.has(r.status||f.status_short))candidates.push({home:r.home_ht,away:r.away_ht});
    if(HALF_COMPLETE.has(saved.live?.status))candidates.push(saved.live.halftime);
    const half=candidates.find(validScore);
    return {status:'complete',score:half?`${half.home}–${half.away}`:null};
  }
  async function poll() {
    if(polling||root.document?.hidden||Date.now()-lastPollAt<15000)return;
    polling=true;lastPollAt=Date.now();
    const controller=new AbortController(), timeout=setTimeout(()=>controller.abort(),10000);
    try {
      const response=await root.fetch('/api/live-scores',{cache:'no-store',signal:controller.signal});
      if(!response.ok)throw new Error('Live unavailable');
      accept(await response.json());
    } catch(_) { /* Retain last observation; view() marks it stale after 5 min. */ }
    finally {clearTimeout(timeout);polling=false;root.dispatchEvent(new Event('arabsniper-live-update'));}
  }
  root.ArabLive={fixture,view,firstHalf,accept,poll};
  if(root.document){
    root.setInterval(poll,120000);
    root.document.addEventListener('visibilitychange',()=>{if(!root.document.hidden)poll();});
    root.addEventListener('pageshow',poll);
    root.addEventListener('focus',poll);
  }
})(typeof window==='undefined'?globalThis:window);

