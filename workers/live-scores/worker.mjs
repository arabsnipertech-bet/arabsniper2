const WATCH_URL = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/live_watchlist.json';
const API = 'https://v3.football.api-sports.io/';
const MINUTE = 60_000, HOUR = 60 * MINUTE, DAY = 24 * HOUR;
const FAST_WINDOW = 3 * HOUR, RETRY_WINDOW = 72 * HOUR, CACHE_WINDOW = 4 * DAY;
const OVERDUE = 135 * MINUTE;
const EXTENDED_LIVE_WINDOW = 4 * HOUR, SCHEDULED_RESULT_WINDOW = 48 * HOUR;
const PROVIDER_MIN_GAP_MS = 250;
const MAX_FIXTURE_BATCH = 20;
const MAX_RELAY_FIXTURES = 40;
const POLL_INTERVAL = 5 * MINUTE;
// One minute covers cron/import latency; observation timestamps never move without data.
const LIVE_FRESHNESS = POLL_INTERVAL + MINUTE;
const HALFTIME_RECHECK = 30 * MINUTE;
const START_STATUS_GRACE = 15 * MINUTE;
const FINISHED = new Set(['FT', 'AET', 'PEN']);
const LIVE = new Set(['1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE']);
const ADMIN = new Set(['CANC', 'ABD', 'AWD', 'WO']);
const HALF_COMPLETE = new Set(['HT', '2H', 'ET', 'BT', 'P', ...FINISHED]);
const STATUSES = new Set(['TBD', 'NS', '1H', 'HT', '2H', 'ET', 'BT', 'P', 'SUSP', 'INT', 'PST', 'LIVE',
  ...FINISHED, ...ADMIN]);
const KNOWN_ERRORS = new Set(['token', 'key', 'headers', 'access', 'ip', 'rateLimit', 'requests', 'subscription', 'plan', 'date', 'timezone']);
const rome = new Intl.DateTimeFormat('en-CA', {timeZone:'Europe/Rome', year:'numeric', month:'2-digit', day:'2-digit'});
const iso = ms => new Date(ms).toISOString();
const timestamp = ms => Number.isFinite(ms) ? iso(ms) : null;
const json = value => Response.json(value, {headers:{'Cache-Control':'no-store', 'X-Content-Type-Options':'nosniff'}});
const number = value => Number.isInteger(value) && value >= 0 ? value : null;
const score = value => ({home:number(value?.home), away:number(value?.away)});
const validScore = value => number(value?.home) !== null && number(value?.away) !== null;
const hasFinal = row => FINISHED.has(row?.status) && (validScore(row.fulltime) || row.status === 'FT' && validScore(row.goals));
const complete = row => hasFinal(row) && validScore(row.halftime);
const postponed = row => row?.status === 'PST' || ADMIN.has(row?.status);
const scheduled = row => row?.status === 'NS' || row?.status === 'TBD';
const inPlay = row => LIVE.has(row?.status);
const liveWindow = row => inPlay(row) ? EXTENDED_LIVE_WINDOW : FAST_WINDOW;
const freshScore = (row, now) => row?.source === 'API-Football' && !row.refresh_failed &&
  (scheduled(row) || validScore(row.goals) || hasFinal(row)) &&
  now - Date.parse(row.observed_at) >= -30_000 && now - Date.parse(row.observed_at) < POLL_INTERVAL;

export function normalize(row, now) {
  const f = row?.fixture, status = f?.status?.short;
  if (!Number.isSafeInteger(f?.id) || f.id <= 0 || !STATUSES.has(status)) return null;
  const goals = score(row.goals), fulltime = score(row.score?.fulltime);
  const halftime = score(row.score?.halftime);
  return {fixture_id:String(f.id), status, elapsed:number(f.status.elapsed), extra:number(f.status.extra),
    goals, halftime:status === 'HT' && !validScore(halftime) ? goals : halftime,
    fulltime:status === 'FT' && !validScore(fulltime) ? goals : fulltime,
    extratime:score(row.score?.extratime), penalties:score(row.score?.penalty),
    observed_at:iso(now), refresh_failed:false, source:'API-Football'};
}


export function relayRows(payload, now) {
  const at = Date.parse(payload?.captured_at);
  if (payload?.schema_version !== 1 || payload?.source !== 'API-Football' ||
      !Number.isFinite(at) || at > now + 30_000 || now - at > 7 * DAY ||
      !Array.isArray(payload.fixtures) || payload.fixtures.length > MAX_RELAY_FIXTURES) return [];
  return payload.fixtures.map(raw => normalize(raw, at)).filter(row =>
    row && (hasFinal(row) || now - at <= POLL_INTERVAL)).map(row =>
      ({...row, transport:'GitHub Actions/R2'}));
}

function reconcile(previous, next) {
  if (hasFinal(previous) && !FINISHED.has(next.status)) return previous;
  if (FINISHED.has(previous?.status) && !FINISHED.has(next.status) && !ADMIN.has(next.status)) return previous;
  const nextHalf = HALF_COMPLETE.has(next.status) && validScore(next.halftime);
  if (!nextHalf && HALF_COMPLETE.has(previous?.status) && validScore(previous.halftime)) {
    next.halftime = previous.halftime;
    next.halftime_observed_at = previous.halftime_observed_at || previous.observed_at;
  }
  if (hasFinal(previous) && !hasFinal(next)) {
    next.status = previous.status;
    next.goals = previous.goals;
    next.fulltime = validScore(previous.fulltime) ? previous.fulltime : previous.goals;
    next.fulltime_observed_at = previous.fulltime_observed_at || previous.observed_at;
  }
  if (hasFinal(next) && validScore(next.halftime) &&
      (next.halftime.home > next.fulltime.home || next.halftime.away > next.fulltime.away)) {
    next.halftime = score(null);
    delete next.halftime_observed_at;
  }
  for (const key of ['extratime', 'penalties']) {
    if (previous?.status === next.status && !validScore(next[key]) && validScore(previous[key])) next[key] = previous[key];
  }
  return next;
}

function retryDelay(now, kickoff, row, checked) {
  if (complete(row)) return checked === 1 ? HOUR : checked === 2 ? 8 * HOUR : DAY;
  if (hasFinal(row)) return now - kickoff >= DAY ? 2 * HOUR : HALFTIME_RECHECK;
  if (postponed(row)) return 6 * HOUR;
  if (scheduled(row)) {
    if (now - kickoff >= DAY) return 2 * HOUR;
    return now - kickoff >= FAST_WINDOW ? HOUR : START_STATUS_GRACE;
  }
  if (now - kickoff >= DAY) return 2 * HOUR;
  return now - kickoff >= FAST_WINDOW ? 30 * MINUTE : 10 * MINUTE;
}

function overdueWindow(row) {
  if (scheduled(row)) return SCHEDULED_RESULT_WINDOW;
  if (inPlay(row)) return EXTENDED_LIVE_WINDOW;
  return OVERDUE;
}

function retryAfter(headers, now) {
  const value = headers.get('retry-after');
  if (!value) return 0;
  return /^\d+$/.test(value) ? now + Number(value) * 1000 : Date.parse(value) || 0;
}

export class ScoreStore {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.pending = null;
  }

  async fetch(request) {
    if (new URL(request.url).pathname === '/refresh') {
      if (!this.pending) this.pending = this.refresh().finally(() => { this.pending = null; });
      await this.pending;
      return new Response('ok');
    }
    const s = await this.ctx.storage.get('state');
    const id = new URL(request.url).searchParams.get('id');
    if (id !== null) {
      if (!/^\d{1,16}$/.test(id)) return new Response('Invalid fixture ID', {status:400});
      const row = s?.scores[id] || await this.ctx.storage.get('final:' + id);
      return json({schema_version:1, enabled:this.env.LIVE_ENABLED === 'true', fixtures:row ? {[id]:row} : {}});
    }
    const now = Date.now(), enabled = this.env.LIVE_ENABLED === 'true';
    const last = s?.last_cron_at || s?.attempted_at;
    let status = s?.status || 'not_configured';
    if (!enabled) status = 'disabled';
    else if (last && now - last > 6 * MINUTE) status = 'cron_stale';
    const health = {...s?.health, relay_fixture_capacity:MAX_RELAY_FIXTURES,
      last_cron_at:timestamp(last), last_successful_poll_at:timestamp(s?.last_successful_poll_at),
      retry_after:timestamp(s?.retry_at), consecutive_errors:s?.consecutive_errors || 0,
      relay:s?.relay || null,
      usage:{day:s?.day, worker_requests:s?.calls || 0, provider:s?.provider_usage || null}};
    return json({schema_version:1, enabled, generated_at:timestamp(s?.published_at), poll_interval_seconds:300, stale_after_seconds:360,
      status, diagnostic:s?.diagnostic || null, health, fixtures:s?.scores || {}});
  }

  async refresh(now = Date.now()) {
    const store = this.ctx.storage;
    const s = await store.get('state') || {scores:{}, watch:[], calls:0};
    if (s.attempted_at && now - s.attempted_at < 55_000) return;
    s.attempted_at = s.last_cron_at = now;
    if (this.env.LIVE_ENABLED !== 'true' || !this.env.API_SPORTS_KEY) {
      s.status = this.env.LIVE_ENABLED === 'true' ? 'secret_missing' : 'disabled';
      await store.put('state', s);
      return;
    }
    const day = iso(now).slice(0, 10);
    if (s.day !== day) Object.assign(s, {day, calls:0, quota:null});
    // Upgrade old exponential pauses once, never shorten the provider's Retry-After.
    if (!s.minute_backoff_v2) {
      if (s.status === 'provider_rate_limit' && s.retry_at > now + 4 * MINUTE)
        s.retry_at = Math.max(s.provider_retry_after || 0, now + 4 * MINUTE);
      s.minute_backoff_v2 = true;
    }
    s.followups ||= {};
    s.pages ||= {};
    const cap = Math.max(1, Math.min(1500, Number(this.env.MAX_CALLS_PER_DAY) || 1500));
    const reserve = Math.max(1000, Number(this.env.RESERVE_REQUESTS) || 1000);
    const remaining = () => s.quota.limit - s.quota.current;
    const threshold = () => Math.max(reserve, Math.ceil(s.quota.limit * .1));
    const archive = new Map();
    // Retain already collected final scores when upgrading a running Durable Object.
    if (!s.final_archive_initialized) {
      for (const [id, row] of Object.entries(s.scores)) if (hasFinal(row)) archive.set(id, row);
    }
    await store.put('state', s);

    let lastProviderRequestAt = null;
    const api = async path => {
      if (s.calls >= cap) throw new Error('live_daily_cap');
      if (path !== 'status' && remaining() <= threshold()) throw new Error('quota_reserve');
      s.calls++;
      await store.put('state', s);
      const endpoint = path === 'status' ? 'status' : 'fixtures';
      const query = endpoint === 'status' ? 'status' : path.startsWith('fixtures?ids=') ? 'by_id' : 'by_date';
      if (lastProviderRequestAt !== null) {
        const next = lastProviderRequestAt + PROVIDER_MIN_GAP_MS;
        let wait = next - Date.now();
        while (wait > 0) {
          await new Promise(resolve => setTimeout(resolve, wait));
          wait = next - Date.now();
        }
      }
      const requestAt = Date.now();
      const requestGap = lastProviderRequestAt === null ? null : requestAt - lastProviderRequestAt;
      lastProviderRequestAt = requestAt;
      const response = await fetch(API + path, {headers:{'x-apisports-key':this.env.API_SPORTS_KEY,
        'Cache-Control':'no-cache'}, cache:'no-store', signal:AbortSignal.timeout(12_000)});
      const numericHeader = name => {
        const value = response.headers.get(name);
        return value !== null && /^\d+$/.test(value) ? Number(value) : null;
      };
      s.provider_usage = {observed_at:iso(now), endpoint, query, request_gap_ms:requestGap,
        daily_remaining:numericHeader('x-ratelimit-requests-remaining'),
        daily_limit:numericHeader('x-ratelimit-requests-limit'),
        minute_remaining:numericHeader('x-ratelimit-remaining'),
        minute_limit:numericHeader('x-ratelimit-limit'), response_age_seconds:numericHeader('age')};
      const daily = response.headers.get('x-ratelimit-requests-remaining');
      if (s.quota) {
        s.quota.current++;
        if (daily !== null && /^\d+$/.test(daily)) s.quota.current = Math.max(0, s.quota.limit - Number(daily));
      }
      const minute = response.headers.get('x-ratelimit-remaining');
      const body = response.status === 204 ? {response:[]} : await response.json().catch(() => null);
      const errors = Object.keys(body?.errors || {});
      if (!response.ok || !body || errors.length) {
        const categories = errors.filter(k => KNOWN_ERRORS.has(k));
        s.diagnostic = {endpoint, query, http_status:response.status, categories:categories.length ? categories : ['other'],
          observed_at:iso(now), quota:s.provider_usage,
          limit_scope:response.headers.get('x-ratelimit-requests-remaining') === '0' ? 'daily' :
            minute === '0' ? 'minute' : 'unconfirmed'};
        const limited = response.status === 429 || errors.includes('rateLimit') || minute === '0';
        s.provider_retry_after = retryAfter(response.headers, now);
        throw new Error(limited ? 'provider_rate_limit' : errors.length ? 'provider_error' : 'provider_unavailable');
      }
      if (minute !== null && /^\d+$/.test(minute) && Number(minute) <= 2) {
        s.retry_at = Math.max(s.retry_at || 0, now + MINUTE, retryAfter(response.headers, now));
      }
      return body;
    };

    let watchIssue = s.watch_issue || null;
    try {
      const watchAttempt = s.watch_attempted_at || s.watch_at;
      if (!watchAttempt || now - watchAttempt >= (watchIssue ? 5 : 15) * MINUTE) {
        s.watch_attempted_at = now;
        try {
          const response = await fetch(WATCH_URL, {signal:AbortSignal.timeout(10_000)});
          if (!response.ok) throw new Error('watchlist_unavailable');
          const payload = await response.json();
          if (!Array.isArray(payload.fixtures) || !Number.isFinite(Date.parse(payload.generated_at)))
            throw new Error('invalid_watchlist');
          const byId = new Map(s.watch.map(f => [String(f.fixture_id), f]));
          for (const f of payload.fixtures) {
            const id = String(f.fixture_id);
            if (!/^\d+$/.test(id) || !Number.isFinite(Date.parse(f.kickoff))) continue;
            const previous = byId.get(id);
            byId.set(id, previous?.kickoff === f.kickoff ? previous : {fixture_id:id, kickoff:f.kickoff});
          }
          s.watch = [...byId.values()];
          s.watch_at = now;
          s.watch_generated_at = Date.parse(payload.generated_at);
          watchIssue = null;
        } catch (error) {
          watchIssue = error.message === 'invalid_watchlist' ? 'invalid_watchlist' : 'watchlist_unavailable';
        }
      }
      if (!watchIssue && now - s.watch_generated_at > 4 * HOUR) watchIssue = 'watchlist_stale';
      if (!watchIssue && s.status === s.watch_issue) s.status = 'idle';
      s.watch_issue = watchIssue;
      const kickoff = f => Date.parse(f.provider_kickoff || f.kickoff);
      s.watch = s.watch.filter(f => kickoff(f) > now - CACHE_WINDOW);
      const tracked = new Map(s.watch.map(f => [String(f.fixture_id), f]));
      for (const [id, row] of Object.entries(s.scores)) {
        if (tracked.has(id) || !s.watch_at) continue;
        if (hasFinal(row)) archive.set(id, row);
        delete s.scores[id];
        delete s.followups[id];
      }
      if (this.env.OFFICIAL_RESULTS) {
        try {
          const object = await this.env.OFFICIAL_RESULTS.get('live-scores/provider-relay.json');
          if (object && object.size <= 200_000) {
            const relay = await object.json();
            let accepted = 0;
            for (const row of relayRows(relay, now)) {
              const id = row.fixture_id, old = s.scores[id];
              if (!tracked.has(id) || Date.parse(old?.observed_at) >= Date.parse(row.observed_at)) continue;
              const next = reconcile(old, row);
              if (next === old) continue;
              s.scores[id] = next;
              if (hasFinal(next)) archive.set(id, next);
              accepted++;
            }
            s.relay = {captured_at:relay.captured_at || null, accepted,
              provider_calls:relay.calls || 0, day:relay.day || null};
            if (accepted) s.published_at = now;
          }
        } catch {
          s.relay = {...s.relay, error:'relay_unavailable'};
        }
      }
      if (s.retry_at > now || (s.provider_cycle_at && now - s.provider_cycle_at < POLL_INTERVAL)) return;
      s.diagnostic = null;
      s.provider_retry_after = null;
      const active = s.watch.filter(f => {
        const t = kickoff(f), row = s.scores[f.fixture_id];
        return t <= now + 10 * MINUTE && now - t < liveWindow(row) &&
          !FINISHED.has(row?.status) && !postponed(row) && !freshScore(row, now) &&
          (!scheduled(row) || now - t < START_STATUS_GRACE);
      });
      const activeIds = new Set(active.map(f => String(f.fixture_id)));
      const followup = f => {
        const id = String(f.fixture_id), row = s.scores[id], t = kickoff(f), age = now - t, checked = s.followups[id];
        if (postponed(row)) {
          const rescheduled = f.provider_kickoff && f.provider_kickoff !== f.kickoff;
          return age >= (rescheduled ? -10 * MINUTE : 6 * HOUR) && age < RETRY_WINDOW && !(checked?.next_at > now);
        }
        if (age < 0 || age >= RETRY_WINDOW || checked?.next_at > now || freshScore(row, now)) return false;
        if (scheduled(row)) {
          return age >= START_STATUS_GRACE && (row.refresh_failed ||
            now - Date.parse(row.observed_at) >= retryDelay(now, t, row, 1));
        }
        if (hasFinal(row)) {
          const observed = Date.parse(row.observed_at);
          if (!complete(row)) return !Number.isFinite(observed) || now - observed >= retryDelay(now, t, row, 1);
          return now - observed >= HOUR || checked?.count > 0;
        }
        return age >= FAST_WINDOW || age >= 15 * MINUTE && (!row || row.status === 'NS' || row.status === 'TBD' ||
          row.refresh_failed || now - Date.parse(row.observed_at) > 6 * MINUTE);
      };
      if (!active.length && !s.watch.some(followup)) {
        s.status = watchIssue || 'idle';
        s.published_at = now;
        s.consecutive_errors = 0;
        return;
      }
      // This guard also applies to empty or partial provider responses.
      s.provider_cycle_at = now;
      if (!s.quota || now - s.quota.checked_at > HOUR) {
        const q = (await api('status')).response;
        if (!Number.isInteger(q?.requests?.current) || !Number.isInteger(q?.requests?.limit_day) ||
            q.requests.limit_day <= 0 || q.subscription?.active !== true) throw new Error('quota_unverified');
        s.quota = {current:q.requests.current, limit:q.requests.limit_day, checked_at:now};
      }
      let polled = false;
      const updatedThisCycle = new Set();
      const accept = (rows, eligible) => {
        if (!Array.isArray(rows)) throw new Error('invalid_response');
        const received = new Set();
        for (const raw of rows) {
          const row = normalize(raw, now), id = row?.fixture_id;
          if (!eligible.has(id) || !tracked.has(id)) continue;
          const f = tracked.get(id), old = s.scores[id];
          const reportedKickoff = Date.parse(raw.fixture.date);
          if (Number.isFinite(reportedKickoff) && (postponed(old) || row.status === 'PST')) {
            f.provider_kickoff = iso(reportedKickoff);
          }
          const next = reconcile(old, row);
          if (hasFinal(next) && (!hasFinal(old) || ['status', 'halftime', 'fulltime', 'extratime', 'penalties']
            .some(key => JSON.stringify(next[key]) !== JSON.stringify(old[key])))) archive.set(id, next);
          s.scores[id] = next;
          received.add(id);
          updatedThisCycle.add(id);
        }
        polled = true;
        s.last_successful_poll_at = now;
        return received;
      };
      const needsRecoverySlot = active.length === MAX_FIXTURE_BATCH && s.watch.some(f =>
        !activeIds.has(String(f.fixture_id)) && !complete(s.scores[f.fixture_id]) && followup(f));
      const pollDates = active.length > MAX_FIXTURE_BATCH || needsRecoverySlot;
      if (pollDates) {
        const dates = [...new Set(active.map(f => rome.format(kickoff(f))))];
        for (const date of dates) {
          if (s.retry_at > now) break;
          const page = s.pages[date] || 1;
          const query = 'fixtures?date=' + date + '&timezone=Europe%2FRome' + (page > 1 ? '&page=' + page : '');
          const data = await api(query);
          const eligible = new Set(s.watch.filter(f => rome.format(kickoff(f)) === date).map(f => String(f.fixture_id)));
          const received = accept(data.response, eligible);
          const pages = Number(data.paging?.total);
          s.pages[date] = Number.isInteger(pages) && pages > page && page < 3 ? page + 1 : 1;
          if (page === 1 && s.pages[date] === 1) {
            for (const id of activeIds) {
              if (eligible.has(id) && !received.has(id) && s.scores[id]) s.scores[id].refresh_failed = true;
            }
          }
        }
        s.pages = Object.fromEntries(dates.map(date => [date, s.pages[date] || 1]));
      } else {
        s.pages = {};
      }
      // With a small live schedule, one ID query covers live and recovery together.
      const due = new Set(s.watch.filter(f => !updatedThisCycle.has(String(f.fixture_id)) && followup(f)).sort((a,b) =>
        Number(hasFinal(s.scores[a.fixture_id])) - Number(hasFinal(s.scores[b.fixture_id])) ||
        (s.followups[a.fixture_id]?.next_at || 0) - (s.followups[b.fixture_id]?.next_at || 0) ||
        kickoff(a) - kickoff(b)));
      const selected = new Set(pollDates ? [] : active);
      for (const f of due) selected.add(f);
      const batch = [...selected].slice(0, MAX_FIXTURE_BATCH);
      if (batch.length && !(s.retry_at > now) && s.calls < cap && remaining() > threshold()) {
        const ids = batch.map(f => String(f.fixture_id));
        const data = await api('fixtures?ids=' + ids.join('-') + '&timezone=Europe%2FRome');
        const received = accept(data.response, new Set(ids));
        for (const f of batch) {
          const id = String(f.fixture_id), row = s.scores[id];
          if (row && !received.has(id) && !hasFinal(row)) row.refresh_failed = true;
          if (due.has(f)) {
            const count = (s.followups[id]?.count || 0) + 1;
            s.followups[id] = {count, next_at:now + retryDelay(now, kickoff(f), row, count)};
          }
        }
      }
      if (!polled && !(s.retry_at > now)) {
        if (s.calls >= cap) throw new Error('live_daily_cap');
        if (remaining() <= threshold()) throw new Error('quota_reserve');
      }
      s.status = watchIssue || (s.retry_at > now ? 'minute_quota_low' : 'ok');
      s.published_at = now;
      s.consecutive_errors = 0;
      if (!(s.retry_at > now)) s.retry_at = null;
    } catch (error) {
      const codes = ['live_daily_cap', 'provider_unavailable', 'provider_error', 'provider_rate_limit',
        'quota_unverified', 'quota_reserve', 'invalid_response'];
      s.status = codes.includes(error.message) ? error.message : 'refresh_failed';
      s.consecutive_errors = (s.consecutive_errors || 0) + 1;
      const backoff = Math.min((s.status === 'provider_rate_limit' ? 4 : 30) * MINUTE, 2 * MINUTE * 2 ** Math.min(4, s.consecutive_errors - 1));
      const reset = Date.parse(day + 'T00:00:00Z') + DAY;
      s.retry_at = Math.max(s.provider_retry_after || 0,
        s.status === 'live_daily_cap' ? reset : now + (s.status === 'quota_reserve' ? HOUR : backoff));
    } finally {
      let overdue = 0, missingHalftime = 0, staleLive = 0, trackingLive = 0, providerPending = 0;
      const overdueIds = [], staleLiveIds = [], trackingIds = [], providerPendingIds = [];
      for (const f of s.watch) {
        const id = String(f.fixture_id), row = s.scores[id];
        const age = now - Date.parse(f.provider_kickoff || f.kickoff);
        if (age >= 0 && age < liveWindow(row) && !hasFinal(row) && !postponed(row)) {
          // A confirmed not-started match leaves the live count after the kickoff grace.
          if (age < START_STATUS_GRACE || !scheduled(row)) {
            trackingLive++;
            trackingIds.push(id);
          }
        }
        if (age >= OVERDUE && !hasFinal(row) && !postponed(row)) {
          if (age < overdueWindow(row)) {
            providerPending++;
            if (providerPendingIds.length < 20) providerPendingIds.push(id);
          } else {
            overdue++;
            if (overdueIds.length < 20) overdueIds.push(id);
          }
        }
        const scoreAge = now - Date.parse(row?.observed_at);
        const missingLive = !row && age >= POLL_INTERVAL && age < FAST_WINDOW;
        if (missingLive || (age >= 0 && age < EXTENDED_LIVE_WINDOW && inPlay(row) &&
            (!validScore(row.goals) || row.refresh_failed || !(scoreAge >= -30_000 && scoreAge <= LIVE_FRESHNESS)))) {
          staleLive++;
          if (staleLiveIds.length < MAX_FIXTURE_BATCH) staleLiveIds.push(String(f.fixture_id));
        }
        if (hasFinal(row) && !validScore(row.halftime)) missingHalftime++;
      }
      if (overdue && (s.status === 'ok' || s.status === 'idle')) s.status = 'results_pending';
      s.health = {overdue_fixtures:overdue, overdue_fixture_ids:overdueIds, missing_halftime:missingHalftime,
        provider_pending_fixtures:providerPending, provider_pending_fixture_ids:providerPendingIds,
        stale_live_fixtures:staleLive, stale_live_fixture_ids:staleLiveIds,
        live_tracking_fixtures:trackingLive, live_tracking_fixture_ids:trackingIds,
        watchlist_generated_at:timestamp(s.watch_generated_at), watchlist_status:watchIssue};
      for (const [id, row] of archive) await store.put('final:' + id, row);
      s.final_archive_initialized = true;
      await store.put('state', s);
      if (s.status !== 'ok' && s.status !== 'idle' || overdue) {
        console.warn(JSON.stringify({event:'live_scores_health', status:s.status, overdue,
          consecutive_errors:s.consecutive_errors, diagnostic:s.diagnostic}));
      }
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method !== 'GET' || url.pathname !== '/api/live-scores') return new Response('Not found', {status:404});
    const stub = env.SCORES.get(env.SCORES.idFromName('global'));
    const response = await stub.fetch('https://scores.internal/read' + url.search);
    if (!env.CF_VERSION_METADATA?.id) return response;
    const headers = new Headers(response.headers);
    headers.set('X-Live-Scores-Version', env.CF_VERSION_METADATA.id);
    return new Response(response.body, {status:response.status, headers});
  },
  scheduled(_event, env, ctx) {
    const stub = env.SCORES.get(env.SCORES.idFromName('global'));
    ctx.waitUntil(stub.fetch('https://scores.internal/refresh'));
  }
};
