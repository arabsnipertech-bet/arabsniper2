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
    'total/ft/over/2.5': ['over25_ft', 1.70, 2.60, 48],
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
      && (d.odds_min === minOdd || (market === 'over25_ft' && d.odds_min === 1.85)) && row.odd >= d.odds_min && d.odds_max === maxOdd && row.odd >= minOdd && row.odd <= maxOdd
      && numeric(d.min_probability_pct) && d.min_probability_pct >= minProbability
      && numeric(row.statistical_pct) && row.statistical_pct >= d.min_probability_pct && row.statistical_pct <= 100;
  }

  function renderCockpit(rows) {
    const panel = document.getElementById('cruscotto');
    if (!panel) return;
    const selected = document.getElementById('preview-fixture')?.value;
    const row = rows.find(item => String(item.fixture_id) === selected) || rows[0];
    if (!row) {
      panel.innerHTML = '<span class="eyebrow">Il cruscotto di oggi</span><h2 id="match-title">In attesa della prossima analisi</h2><p>Le partite disponibili compariranno qui al prossimo aggiornamento.</p>';
      return;
    }
    const odd = value => numeric(value) && value > 1 ? value.toLocaleString('it-IT', {minimumFractionDigits:2, maximumFractionDigits:2}) : 'n/d';
    const first = numeric(row.first_odd) && row.first_odd > 1 ? row.first_odd : null;
    const delta = first === null ? null : (row.odd / first - 1) * 100;
    const direction = delta === null ? 'Apertura non disponibile' : Math.abs(delta) < .005 ? 'Quota invariata' : (delta < 0 ? '↓ Quota in calo' : '↑ Quota in salita');
    const change = delta === null ? '' : ' · ' + (delta > 0 ? '+' : '') + pct(delta);
    const yStart = delta === null || Math.abs(delta) < .005 ? 40 : delta < 0 ? 12 : 68;
    const yEnd = delta === null || Math.abs(delta) < .005 ? 40 : delta < 0 ? 68 : 12;
    panel.innerHTML = `<div class="match-top"><span class="eyebrow">Il cruscotto di oggi</span><span class="demo-tag">Anteprima reale</span></div>
      <div class="match-board"><div><span class="league">${escape(row.league)} · ${time(row.kickoff)}</span><h2 id="match-title">${escape(row.match)}</h2></div></div>
      ${rows.length > 1 ? `<label for="preview-fixture">Esplora un’altra partita</label><select id="preview-fixture">${rows.map(item => `<option value="${escape(item.fixture_id)}"${item === row ? ' selected' : ''}>${escape(item.match)}</option>`).join('')}</select>` : ''}
      <h3>${escape(row.label)}</h3><dl class="metrics"><div><dt>Quota apertura</dt><dd>${odd(first)}</dd></div><div><dt>Quota osservata</dt><dd>${odd(row.odd)}</dd></div><div><dt>Probabilità modello</dt><dd>${pct(row.statistical_pct)}</dd></div></dl>
      ${first === null ? '' : `<svg class="preview-quote" viewBox="0 0 420 80" preserveAspectRatio="none" aria-hidden="true"><line x1="8" y1="${yStart}" x2="412" y2="${yEnd}" stroke="currentColor" stroke-width="3"/><circle cx="8" cy="${yStart}" r="4" fill="currentColor"/><circle cx="412" cy="${yEnd}" r="4" fill="currentColor"/></svg>`}
      <div class="chart-key"><span>Apertura</span><b>${direction}${change}</b><span>Ultimo dato</span></div>
      <figcaption>Confronto tra quota di apertura e ultima osservazione${row.bookmaker ? ' · ' + escape(row.bookmaker) : ''}. Analisi delle ${time(row.captured_at)}.${Number.isFinite(Date.parse(row.provider_updated_at)) ? ' Quota bookmaker delle ' + time(row.provider_updated_at) + '.' : ''} Orari italiani.</figcaption>`;
    const selector = document.getElementById('preview-fixture');
    if (selector) selector.onchange = () => renderCockpit(rows);
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
      renderCockpit(rows);
      target.innerHTML = rows.length ? rows.map((row, i) => `<article class="pick"><div class="pick-top"><span>ANALISI ${String(i+1).padStart(2,'0')}</span><time datetime="${escape(row.kickoff)}">${time(row.kickoff)} · ${Date.parse(row.kickoff)>+now?'In programma':'Orario di inizio trascorso'}</time></div><div class="league">${escape(row.league)}</div><h3>${escape(row.match)}</h3><div class="target"><strong>${escape(row.label)}</strong><div class="odd">${row.odd.toLocaleString('it-IT',{minimumFractionDigits:2,maximumFractionDigits:2})}<small>QUOTA PREMATCH</small></div></div><div class="probabilities"><span>Stima statistica<b>${pct(row.statistical_pct)}</b></span><span>Lettura mercato<b>${row.model_id==='lateral-matrix-v5.0'?({convergence:'Convergenza',contrarian:'Contrarian sano',divergence:'Contrarian',unconfirmed:'Flusso non confermato'}[row.market_confirmation]||'In osservazione'):row.model_id==='statistics-first-v4.0'?({confirmed:'Oro + azzurro',context:'Scenario indiretto',contrary:'In contrasto'}[row.market_confirmation]||'Da confermare'):'Versione precedente'}</b></span></div><span class="snapshot">Analisi delle ${time(row.captured_at)} · orari italiani</span></article>`).join('') : '<div class="empty">Nessuna selezione pubblicata per oggi con questi criteri. La dashboard Gold contiene tutte le analisi, anche quelle senza quote o con dati limitati.</div>';
    } catch (_) {
      const panel = document.getElementById('cruscotto');
      if (panel) panel.innerHTML = '<h2 id="match-title">Anteprima temporaneamente non disponibile</h2><p>Nuovo tentativo automatico tra un minuto.</p>';
      target.innerHTML = '<div class="empty">Selezioni temporaneamente non disponibili. Riproviamo automaticamente tra un minuto.</div>';
    } finally { busy = false; }
  }
  handleUrlMessages();
  refresh();
  setInterval(refresh, 60000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
})();

