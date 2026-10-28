/* Independent display overlay: never mutates the prematch payload. */
(function(root){
  const ACTIVE=new Set(['1H','HT','2H','ET','BT','P','LIVE']);
  const FINAL=new Set(['FT','AET','PEN']);
  const HALF_COMPLETE=new Set(['HT','2H','ET','BT','P','FT','AET','PEN']);
  const validScore=s=>s&&Number.isInteger(s.home)&&Number.isInteger(s.away)&&s.home>=0&&s.away>=0;
  let scores={},enabled=false,polling=false;
  function fixture(match) {
    const f=match.fixture||{},candidate=scores[String(f.fixture_id)];
    const old=f.live||{};
    if(!candidate||Date.parse(candidate.observed_at)<=Date.parse(old.observed_at||'1970-01-01'))return f;
    return {...f,live:candidate,status_short:candidate.status,elapsed:candidate.elapsed};
  }
  function view(match,now=Date.now()) {
    const f=fixture(match),l=f.live||{},status=l.status||f.status_short||f.result?.status;
    const observed=Date.parse(l.observed_at),fresh=Number.isFinite(observed)&&now-observed<=300000&&now>=observed-30000&&!l.refresh_failed;
    const score=Number.isFinite(l.goals?.home)&&Number.isFinite(l.goals?.away)?`${l.goals.home}–${l.goals.away}`:null;
    const active=ACTIVE.has(status),finished=FINAL.has(status);
    const minute=Number.isFinite(l.elapsed)?`${l.elapsed}${Number.isFinite(l.extra)&&l.extra>0?'+'+l.extra:''}′`:'';
    const label=finished?'Terminata':status==='SUSP'?'Sospesa':status==='INT'?'Interrotta':active?(fresh?(status==='HT'?'Intervallo':status==='BT'?'Pausa supplementari':status==='P'?'Rigori':'LIVE'):'Da aggiornare'):'Orario di inizio trascorso';
    return {active:active&&fresh,stale:active&&!fresh,finished,score,minute,label,observed_at:l.observed_at,
      visible:active||finished||['SUSP','INT'].includes(status)||Date.parse(f.kickoff)<=now};
  }
  function accept(data) {
    if(data?.schema_version!==1||!data.fixtures||typeof data.fixtures!=='object'||Array.isArray(data.fixtures)) throw new Error('Invalid live data');
    enabled=data.enabled===true;
    if(!enabled){scores={};return;}
    const next={};
    for(const [id,row] of Object.entries(data.fixtures)) {
      if(!/^\d+$/.test(id)||String(row.fixture_id)!==id||typeof row.status!=='string'||!Number.isFinite(Date.parse(row.observed_at)))continue;
      const previous=scores[id];
      // Keep an already confirmed half-time score when a later poll omits it.
      // Never carry forward a provisional 1H score or replace a newer correction.
      const preserve=HALF_COMPLETE.has(row.status)&&!validScore(row.halftime)&&
        HALF_COMPLETE.has(previous?.status)&&validScore(previous?.halftime)&&
        Date.parse(row.observed_at)>Date.parse(previous.observed_at);
      next[id]=preserve?{...row,halftime:{...previous.halftime},halftime_observed_at:previous.halftime_observed_at||previous.observed_at}:row;
    }
    scores=next;
  }
  function firstHalf(match,now=Date.now()) {
    const f=fixture(match),l=f.live||{},status=l.status||f.status_short||f.result?.status;
    if(status==='1H')return {status:'in_progress',score:validScore(l.goals)?`${l.goals.home}–${l.goals.away}`:null,stale:view(match,now).stale};
    if(!HALF_COMPLETE.has(status))return {status:'pending',score:null};
    const saved=match.fixture||{},r=f.result||{};
    const candidates=[l.halftime,{home:r.home_ht,away:r.away_ht}];
    if(HALF_COMPLETE.has(saved.live?.status))candidates.push(saved.live.halftime);
    const half=candidates.find(validScore);
    return {status:'complete',score:half?`${half.home}–${half.away}`:null};
  }
  async function poll() {
    if(polling||root.document?.hidden)return;
    polling=true;
    try {
      const response=await root.fetch('/api/live-scores',{cache:'no-store',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw new Error('Live unavailable');
      accept(await response.json());
    } catch(_) { /* Retain last observation; view() marks it stale after 5 min. */ }
    finally {polling=false;root.dispatchEvent(new Event('arabsniper-live-update'));}
  }
  root.ArabLive={fixture,view,firstHalf,accept,poll};
  if(root.document){
    root.setInterval(poll,120000);
    root.document.addEventListener('visibilitychange',()=>{if(!root.document.hidden)poll();});
    root.addEventListener('pageshow',poll);
    root.addEventListener('focus',poll);
  }
})(typeof window==='undefined'?globalThis:window);

