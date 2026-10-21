// One global poller. Public requests read stored data and NEVER call API-Sports.
const WATCH_URL = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/live_watchlist.json';
const API = 'https://v3.football.api-sports.io/';
const FINAL = new Set(['FT','AET','PEN','CANC','ABD','AWD','WO']);
const iso = ms => new Date(ms).toISOString();
const localDate = ms => new Intl.DateTimeFormat('en-CA', {timeZone:'Europe/Rome',year:'numeric',month:'2-digit',day:'2-digit'}).format(ms);
const json = value => Response.json(value, {headers:{'Cache-Control':'public, max-age=30','X-Content-Type-Options':'nosniff'}});
const number = x => Number.isFinite(x) && x >= 0 ? x : null;
const score = x => ({home:number(x?.home),away:number(x?.away)});
export function normalize(row, now) {
  const f=row?.fixture, s=f?.status;
  if(!Number.isInteger(f?.id)||typeof s?.short!=='string') return null;
  return {fixture_id:String(f.id),status:s.short,elapsed:number(s.elapsed),extra:number(s.extra),
    goals:score(row.goals),halftime:score(row.score?.halftime),fulltime:score(row.score?.fulltime),
    observed_at:iso(now),refresh_failed:false,source:'API-Football'};
}
export class ScoreStore {
  constructor(ctx,env) { this.ctx=ctx;this.env=env;this.pending=null; }
  async fetch(request) {
    if(new URL(request.url).pathname==='/refresh') {
      if(!this.pending) this.pending=this.refresh().finally(()=>{this.pending=null;});
      await this.pending;
      return new Response('ok');
    }
    const state=await this.ctx.storage.get('state');
    return json({schema_version:1,enabled:this.env.LIVE_ENABLED==='true',
      generated_at:state?.published_at||null,poll_interval_seconds:120,
      status:state?.status||'not_configured',fixtures:state?.scores||{}});
  }
  async refresh(now=Date.now()) {
    if(this.env.LIVE_ENABLED!=='true'||!this.env.API_SPORTS_KEY) return;
    const store=this.ctx.storage;
    let s=await store.get('state')||{scores:{},watch:[],calls:0};
    if(s.attempted_at && now-s.attempted_at<115000) return;
    s.attempted_at=now;
    const today=iso(now).slice(0,10);
    if(s.day!==today){s.day=today;s.calls=0;s.quota=null;}
    // Persist the interval before network I/O, including failed attempts.
    await store.put('state',s);
    try {
      if(!s.watch_at || now-s.watch_at>1800000) {
        const response=await fetch(WATCH_URL,{signal:AbortSignal.timeout(15000)});
        if(!response.ok) throw new Error('watchlist_unavailable');
        const payload=await response.json();
        if(!Array.isArray(payload.fixtures)||!Number.isFinite(Date.parse(payload.generated_at))) throw new Error('invalid_watchlist');
        const byId=new Map((s.watch||[]).map(x=>[String(x.fixture_id),x]));
        for(const f of payload.fixtures) if(/^\d+$/.test(String(f.fixture_id))&&Number.isFinite(Date.parse(f.kickoff))) byId.set(String(f.fixture_id),f);
        s.watch=[...byId.values()].filter(f=>Date.parse(f.kickoff)>now-48*3600000);
        s.watch_at=now;
      }
      const dates=[...new Set(s.watch.filter(f=> {
        const t=Date.parse(f.kickoff),last=s.scores[String(f.fixture_id)];
        return t<=now+600000 && t>now-24*3600000 && !FINAL.has(last?.status);
      }).map(f=>localDate(Date.parse(f.kickoff))))].slice(0,2);
      if(!dates.length){s.status='idle';s.published_at=iso(now);return;}
      const cap=Math.max(0,Math.min(1500,Number(this.env.MAX_CALLS_PER_DAY)||1500));
      const reserve=Math.max(1000,Number(this.env.RESERVE_REQUESTS)||1000);
      const api=async path=> {
        if(s.calls>=cap) throw new Error('live_daily_cap');
        s.calls++; await store.put('state',s);
        const response=await fetch(API+path,{headers:{'x-apisports-key':this.env.API_SPORTS_KEY},signal:AbortSignal.timeout(20000)});
        if(s.quota) {s.quota.current++;}
        const remaining=response.headers.get('x-ratelimit-requests-remaining');
        if(s.quota && remaining!==null && /^\d+$/.test(remaining)) s.quota.current=Math.max(0,s.quota.limit-Number(remaining));
        if(!response.ok) throw new Error('provider_unavailable');
        const value=await response.json();
        if(value.errors && Object.keys(value.errors).length) throw new Error('provider_error');
        return value;
      };
      if(!s.quota || now-s.quota.checked_at>3600000) {
        const q=(await api('status')).response;
        if(!Number.isInteger(q?.requests?.current)||!Number.isInteger(q?.requests?.limit_day)||q.subscription?.active!==true) throw new Error('quota_unverified');
        s.quota={current:q.requests.current,limit:q.requests.limit_day,checked_at:now};
      }
      const allowed=new Set(s.watch.map(f=>String(f.fixture_id)));
      for(const date of dates) {
        if(s.quota.limit-s.quota.current<=Math.max(reserve,Math.ceil(s.quota.limit*.1))) throw new Error('quota_reserve');
        const data=await api('fixtures?date='+date+'&timezone=Europe%2FRome');
        if(!Array.isArray(data.response)) throw new Error('invalid_response');
        const received=new Set();
        for(const row of data.response) {
          const live=normalize(row,now);
          if(live&&allowed.has(live.fixture_id)){s.scores[live.fixture_id]=live;received.add(live.fixture_id);}
        }
        for(const f of s.watch) if(localDate(Date.parse(f.kickoff))===date&&!received.has(String(f.fixture_id))&&s.scores[String(f.fixture_id)]) s.scores[String(f.fixture_id)].refresh_failed=true;
      }
      s.scores=Object.fromEntries(Object.entries(s.scores).filter(([id])=>allowed.has(id)));
      s.status='ok';s.published_at=iso(now);
    } catch(error) {
      // Error codes only: never publish upstream text, credentials or account data.
      s.status=['watchlist_unavailable','invalid_watchlist','live_daily_cap','provider_unavailable','provider_error','quota_unverified','quota_reserve','invalid_response'].includes(error.message)?error.message:'refresh_failed';
    } finally { await store.put('state',s); }
  }
}
export default {
  async fetch(request,env) {
    if(request.method!=='GET'||new URL(request.url).pathname!=='/api/live-scores') return new Response('Not found',{status:404});
    const stub=env.SCORES.get(env.SCORES.idFromName('global'));
    return stub.fetch('https://scores.internal/read');
  },
  async scheduled(event,env,ctx) {
    const stub=env.SCORES.get(env.SCORES.idFromName('global'));
    ctx.waitUntil(stub.fetch('https://scores.internal/refresh'));
  }
};
