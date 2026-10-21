/* Independent display overlay: never mutates the prematch payload. */
(function(root){
  const ACTIVE=new Set(['1H','HT','2H','ET','BT','P','LIVE']);
  const FINAL=new Set(['FT','AET','PEN']);
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
      next[id]=row;
    }
    scores=next;
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
  root.ArabLive={fixture,view,accept,poll};
  if(root.document){
    root.setInterval(poll,120000);
    root.document.addEventListener('visibilitychange',()=>{if(!root.document.hidden)poll();});
  }
})(typeof window==='undefined'?globalThis:window);
