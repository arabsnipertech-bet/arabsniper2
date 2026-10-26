/* Public projection only. Gold access remains entirely in the existing dashboard. */
(() => {
  const URL = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/arab_quote_homepage.json';
  const zone = 'Europe/Rome';
  const dateKey = date => new Intl.DateTimeFormat('en-CA', {timeZone: zone, year:'numeric', month:'2-digit', day:'2-digit'}).format(date);
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const numeric = value => typeof value === 'number' && Number.isFinite(value);
  const pct = value => numeric(value) ? value.toLocaleString('it-IT', {maximumFractionDigits:1}) + '%' : 'n/d';
  const time = value => new Intl.DateTimeFormat('it-IT', {timeZone: zone, hour:'2-digit', minute:'2-digit'}).format(new Date(value));
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
        if (row.model_id === 'lateral-matrix-v5.0') {
          const d = row.opportunity;
          if (d?.policy_id !== 'playable-value-v1' || d.eligible !== true || row.odd < 1.4 || row.odd > 3.5 || !numeric(row.statistical_pct) || row.statistical_pct < Math.max(35, d.min_probability_pct) || row.statistical_pct > 100) return false;
        }
        seen.add(row.fixture_id); return true;
      }).slice(0,3) : [];
      target.innerHTML = rows.length ? rows.map((row, i) => `<article class="pick"><div class="pick-top"><span>ANALISI ${String(i+1).padStart(2,'0')}</span><time datetime="${escape(row.kickoff)}">${time(row.kickoff)} · ${Date.parse(row.kickoff)>+now?'In programma':'Orario di inizio trascorso'}</time></div><div class="league">${escape(row.league)}</div><h3>${escape(row.match)}</h3><div class="target"><strong>${escape(row.label)}</strong><div class="odd">${row.odd.toLocaleString('it-IT',{minimumFractionDigits:2,maximumFractionDigits:2})}<small>QUOTA PREMATCH</small></div></div><div class="probabilities"><span>Stima statistica<b>${pct(row.statistical_pct)}</b></span><span>Lettura mercato<b>${row.model_id==='lateral-matrix-v5.0'?({convergence:'Convergenza',contrarian:'Contrarian',divergence:'Contrarian',unconfirmed:'Flusso non confermato'}[row.market_confirmation]||'In osservazione'):row.model_id==='statistics-first-v4.0'?({confirmed:'Oro + azzurro',context:'Scenario indiretto',contrary:'In contrasto'}[row.market_confirmation]||'Da confermare'):'Versione precedente'}</b></span></div><span class="snapshot">Analisi delle ${time(row.captured_at)} · orari italiani</span></article>`).join('') : '<div class="empty">Nessuna selezione pubblicata per oggi con questi criteri. La dashboard Gold contiene tutte le analisi, anche quelle senza quote o con dati limitati.</div>';
    } catch (_) {
      target.innerHTML = '<div class="empty">Selezioni temporaneamente non disponibili. Riproviamo automaticamente tra un minuto.</div>';
    } finally { busy = false; }
  }
  handleUrlMessages();
  refresh();
  setInterval(refresh, 60000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
})();

