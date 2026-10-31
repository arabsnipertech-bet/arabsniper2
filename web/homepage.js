/* Public projection only. Gold access remains entirely in the existing dashboard. */
(() => {
  const URL = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/arab_quote_homepage.json';
  const zone = 'Europe/Rome';
  const dateKey = date => new Intl.DateTimeFormat('en-CA', {timeZone: zone, year:'numeric', month:'2-digit', day:'2-digit'}).format(date);
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const numeric = value => typeof value === 'number' && Number.isFinite(value);
  const pct = value => numeric(value) ? value.toLocaleString('it-IT', {maximumFractionDigits:1}) + '%' : 'n/d';
  const time = value => new Intl.DateTimeFormat('it-IT', {timeZone: zone, hour:'2-digit', minute:'2-digit'}).format(new Date(value));
  // The public feed carries the Gold decision. Revalidate its public fields
  // against the exact V6.1 market and price band before rendering a card.
  const gold61Markets = {
    'total/ht/over/1.5': ['over15_ht', 2.10, 3.50, 40],
    'total/ht/over/2.5': ['over25_ht', 4.50, 7.50, 18],
    'total/ft/over/2.5': ['over25_ft', 1.85, 2.60, 48],
    'total/ft/over/3.5': ['over35_ft', 2.10, 4.50, 35],
    'btts/ht/yes/': ['gg_ht', 3.50, 5.50, 26]
  };
  function gold61Visible(row) {
    const s = row.spec || {}, d = row.opportunity;
    const kind = s.kind === 'asian_total' ? 'total' : s.kind;
    if (s.extra != null || (s.line != null && !numeric(s.line))) return false;
    const rule = gold61Markets[[kind, s.period, s.side, s.line].join('/')];
    if (!rule || ![globalThis.ArabCompetitionScope.id, 'gold-senior-whitelist-v2.1'].includes(row.competition_policy)) return false;
    const [market, minOdd, maxOdd, minProbability] = rule;
    return d.eligible === true && d.goal_market === market && row.quote_executable === true
      && d.structural_supported === true && d.market_checks_passed === true
      && (s.period !== 'ht' || d.ht_history_supported === true)
      && d.odds_min === minOdd && d.odds_max === maxOdd && row.odd >= minOdd && row.odd <= maxOdd
      && numeric(d.min_probability_pct) && d.min_probability_pct >= minProbability
      && numeric(row.statistical_pct) && row.statistical_pct >= d.min_probability_pct && row.statistical_pct <= 100;
  }
  let busy = false;
  async function refresh() {
    if (busy) return;
    busy = true;
    const target = document.getElementById('daily-picks');
    const now = new Date();
    document.getElementById('preview-date').textContent = new Intl.DateTimeFormat('it-IT', {timeZone:zone, day:'numeric', month:'long', year:'numeric'}).format(now);
    try {
      const response = await fetch(URL + '?v=' + Date.now(), {cache:'no-store', signal:AbortSignal.timeout(15000)});
      if (!response.ok) throw new Error('feed');
      const payload = await response.json();
      const seen = new Set();
      const rows = payload.date === dateKey(now) && Array.isArray(payload.selections) ? payload.selections.filter(row => {
        const kickoff = new Date(row.kickoff);
        if (!row.fixture_id || seen.has(row.fixture_id) || !Number.isFinite(+kickoff) || +kickoff <= +now || dateKey(kickoff) !== dateKey(now) || !numeric(row.odd) || row.odd <= 1 || !Number.isFinite(Date.parse(row.captured_at))) return false;
        if (row.model_id === 'lateral-matrix-v5.0' && !globalThis.ArabCompetitionScope.allows(row.competition_policy,row.country,row.league)) return false;
        if (row.model_id === 'lateral-matrix-v5.0' && row.opportunity?.policy_id === 'gold-goals-v6.1') {
          if (!gold61Visible(row)) return false;
        } else if (row.model_id === 'lateral-matrix-v5.0') {
          const d = row.opportunity;
          const s = row.spec || {}, caps = {over15_ht:3.5,over25_ft:3.5,gg_ht:5.5,over35_ft:5.5};
          const key = s.extra!=null?null:s.kind==='btts'&&s.period==='ht'&&s.side==='yes'&&s.line==null?'gg_ht':['total','asian_total'].includes(s.kind)&&s.side==='over'?(s.period==='ht'&&s.line===1.5?'over15_ht':s.period==='ft'&&s.line===2.5?'over25_ft':s.period==='ft'&&s.line===3.5?'over35_ft':null):null;
          const current = ['gold-goals-v4','gold-goals-v5','gold-goals-v6'].includes(d?.policy_id), maximum = current ? caps[key] : 3.5;
          const dedicated = ['gold-goals-v5','gold-goals-v6'].includes(d?.policy_id);
          const minimum = d?.policy_id === 'gold-goals-v6' ? ({over15_ht:35,gg_ht:30,over25_ft:30,over35_ft:30}[key] ?? 30) : d?.policy_id === 'gold-goals-v5' ? ({over15_ht:45,gg_ht:40}[key] || 35) : 35;
          if (!['gold-goals-v2','gold-goals-v3','gold-goals-v4','gold-goals-v5','gold-goals-v6'].includes(d?.policy_id) || (current && (!key || d.odds_max !== maximum)) || !['gold-senior-whitelist-v1','gold-senior-whitelist-v2'].includes(row.competition_policy) || d.eligible !== true || row.odd < 1.4 || row.odd > maximum || !numeric(d.min_probability_pct) || !numeric(row.statistical_pct) || row.statistical_pct < Math.max(minimum, d.min_probability_pct) || row.statistical_pct > 100 || (dedicated && ['over15_ht','gg_ht'].includes(key) && d.ht_history_supported !== true)) return false;
        }
        seen.add(row.fixture_id); return true;
      }).slice(0,3) : [];
      target.innerHTML = rows.length ? rows.map((row, i) => `<article class="pick"><div class="pick-top"><span>ANALISI ${String(i+1).padStart(2,'0')}</span><time datetime="${escape(row.kickoff)}">${time(row.kickoff)} · ${Date.parse(row.kickoff)>+now?'In programma':'Orario di inizio trascorso'}</time></div><div class="league">${escape(row.league)}</div><h3>${escape(row.match)}</h3><div class="target"><strong>${escape(row.label)}</strong><div class="odd">${row.odd.toLocaleString('it-IT',{minimumFractionDigits:2,maximumFractionDigits:2})}<small>QUOTA PREMATCH</small></div></div><div class="probabilities"><span>Stima statistica<b>${pct(row.statistical_pct)}</b></span><span>Lettura mercato<b>${row.model_id==='lateral-matrix-v5.0'?({convergence:'Convergenza',contrarian:'Contrarian sano',divergence:'Contrarian',unconfirmed:'Flusso non confermato'}[row.market_confirmation]||'In osservazione'):row.model_id==='statistics-first-v4.0'?({confirmed:'Oro + azzurro',context:'Scenario indiretto',contrary:'In contrasto'}[row.market_confirmation]||'Da confermare'):'Versione precedente'}</b></span></div><span class="snapshot">Analisi delle ${time(row.captured_at)} · orari italiani</span></article>`).join('') : '<div class="empty">Nessuna selezione pubblicata per oggi con questi criteri. La dashboard Gold contiene tutte le analisi, anche quelle senza quote o con dati limitati.</div>';
    } catch (_) {
      target.innerHTML = '<div class="empty">Selezioni temporaneamente non disponibili. Riproviamo automaticamente tra un minuto.</div>';
    } finally { busy = false; }
  }
  handleUrlMessages();
  refresh();
  setInterval(refresh, 60000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
})();

