/* The editorial Publisher serves a public allowlist. Gold login is handled by the existing dashboard. */
(() => {
  const feedUrl = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/publisher-public-v71/homepage.json'
  const rome = 'Europe/Rome'
  const el = id => document.getElementById(id)
  const time = new Intl.DateTimeFormat('it-IT', { timeZone: rome, hour: '2-digit', minute: '2-digit' })
  const shortDate = new Intl.DateTimeFormat('it-IT', { timeZone: rome, day: 'numeric', month: 'short' })
  const shortClock = value => `${shortDate.format(Date.parse(value))} · ${time.format(Date.parse(value))}`
  const number = (value, digits = 1, suffix = '') => Number.isFinite(value)
    ? value.toLocaleString('it-IT', { minimumFractionDigits: digits, maximumFractionDigits: digits }) + suffix
    : '—'
  const validDate = value => Number.isFinite(Date.parse(value))
  const outcomes = { won: 'Corretta', lost: 'Errata', void: 'Rimborsata' }
  const cohorts = { public: 'BEST gratuite pubblicate', gold: 'BEST Gold documentate', internal: 'Ricerca interna' }
  const goldUrl = 'gold-dashboard-live.html'
  const motion = window.matchMedia('(prefers-reduced-motion: reduce)')
  let livePlaying = !motion.matches
  let liveRows = []
  let liveIndex = 0
  let reportSignature
  let saved
  let pending

  function node(tag, text, className) {
    const element = document.createElement(tag)
    if (className) element.className = className
    if (text != null) element.textContent = text
    return element
  }

  function triadList(proposals) {
    const list = node('ul', null, 'gold-preview-triad')
    for (const proposal of proposals) {
      const item = node('li')
      const model = `Stima V7 ${number(proposal.probability * 100, 1, '%')}`
      item.append(node('span', proposal.category, 'gold-preview-role'), node('strong', proposal.market))
      if (Number.isFinite(proposal.odd) && validDate(proposal.quote_at)) {
        const quote = `Quota ${number(proposal.odd, 2)} · ${shortClock(proposal.quote_at)}`
        item.append(node('small', `${model} · ${quote}${proposal.study_only ? ' · Studio' : ''}`))
      } else {
        item.append(node('small', `${model} · Quota bookmaker in attesa`))
      }
      list.append(item)
    }
    return list
  }

  function matchCard(row, free, proposals) {
    const card = node('article', null, free ? 'pick editorial-pick' : 'pick editorial-pick preview')
    const meta = node('div', null, 'pick-top')
    const start = node('time', shortClock(row.kickoff))
    start.dateTime = row.kickoff
    const label = free ? row.category : proposals ? 'Anteprima Gold' : `Anteprima Gold · ${row.category}`
    meta.append(node('span', label), start)
    card.append(meta, node('div', row.league, 'league'), node('h3', row.match))
    if (free) {
      const market = node('div', null, 'target')
      market.append(node('strong', row.market), node('b', number(row.odd, 2), 'odd'))
      card.append(market, node('p', `Probabilità del modello ${number(row.probability * 100, 1, '%')}`, 'model-number'),
        node('p', row.reason, 'muted'), node('small', `Quota rilevata alle ${time.format(Date.parse(row.quote_at))} · ora italiana`, 'snapshot'))
    }
    if (proposals) card.append(triadList(proposals))
    if (!free || proposals) {
      const link = node('a', 'Vedi l’analisi in Gold ↗', 'editorial-link')
      link.href = goldUrl
      card.append(link)
    }
    return card
  }

  function reportCards(report) {
    const target = el('results-content')
    if (!report?.cohorts) return
    const signature = JSON.stringify(report)
    if (reportSignature === signature) return
    reportSignature = signature
    const detailsOpen = target.querySelector('details')?.open
    const grid = node('div', null, 'editorial-report-grid')
    for (const [key, label] of Object.entries(cohorts)) {
      const data = report.cohorts[key]
      if (!data) continue
      const card = node('article', null, 'editorial-metric')
      card.append(node('h3', label), node('p', `${data.verified} verificate`, 'model-number'),
        node('p', `${data.won} corrette · ${data.lost} errate`),
        node('p', `${data.pending} pendenti · ${data.void} rimborsate`),
        node('p', `Successo ${number(data.hit_rate, 1, '%')} · quota media ${number(data.average_odd, 2)}`),
        node('p', `Profitto teorico ${number(data.profit, 2, ' u')} · ROI ${number(data.roi, 2, '%')}`))
      grid.append(card)
    }
    const when = new Date(report.date + 'T12:00:00Z')
    const partial = report.recovered?.pending
    const status = partial ? 'esiti verificati finora dall’Auditor' : 'certificata dall’Auditor'
    target.replaceChildren(node('p', `Giornata del ${shortDate.format(when)} · ${status}`, 'eyebrow'))
    if (partial) {
      const count = report.recovered.pending_fixtures
      target.append(node('p', count ? `Partite ancora in verifica · ${count}` : 'Esiti ancora in verifica', 'muted'))
    }
    let metrics = target
    if (Array.isArray(report.triad_examples)) {
      const highlights = report.triad_examples.slice(0, 4)
      if (highlights.length) {
        target.append(node('h3', 'Esempi dalle schede Gold'), node('p',
          'Una proposta corretta per partita, registrata prima del calcio d’inizio.', 'muted'))
        const list = node('ul', null, 'editorial-hits')
        for (const row of highlights) {
          const item = node('li')
          const header = node('div', null, 'editorial-hit-match')
          header.append(node('strong', row.match), node('b', row.score, 'editorial-score'))
          const details = `${row.category} · ${row.market}${row.study_only ? ' · Studio' : ''}${row.halftime ? ` · PT ${row.halftime}` : ''}`
          item.append(header, node('p', details, 'muted'))
          list.append(item)
        }
        target.append(list)
      }
      metrics = node('details', null, 'editorial-report-details')
      metrics.open = detailsOpen ?? !highlights.length
      metrics.append(node('summary', 'Dati completi del report'))
      target.append(metrics)
    }
    metrics.append(grid)
    if (report.examples?.length) {
      const examples = node('ul', null, 'editorial-examples')
      for (const row of report.examples) {
        examples.append(node('li', `${outcomes[row.outcome]} · ${row.match} · ${row.market} · quota ${number(row.odd, 2)}`))
      }
      metrics.append(examples)
    }
    metrics.append(node('small', 'Una unità per selezione. Il ROI include le rimborsate a profitto zero e lascia fuori gli esiti pendenti.', 'snapshot'))
  }

  function showLiveMatch() {
    const row = liveRows[liveIndex]
    el('publisher-live-nav').hidden = liveRows.length < 2
    el('publisher-live-gold').hidden = !row
    el('publisher-live-count').textContent = `${liveIndex + 1} / ${liveRows.length}`
    const target = el('publisher-live-match')
    if (!row) {
      target.replaceChildren(node('p', 'In attesa delle prossime partite in Gold.', 'muted'))
      return
    }
    const score = Number.isInteger(row.home) && Number.isInteger(row.away) ? ` · ${row.home}–${row.away}` : ''
    const status = row.status ? `${row.status}${score} · ${shortClock(row.observed_at)}`
      : `In programma · ${shortClock(row.kickoff)}`
    target.replaceChildren(node('p', `🔒 Gold · ${row.league}`, 'eyebrow'), node('h3', row.match),
      node('p', status, 'model-number'))
  }

  function moveLive(step) {
    if (liveRows.length < 2) return
    liveIndex = (liveIndex + step + liveRows.length) % liveRows.length
    showLiveMatch()
  }

  function renderLive(payload, now) {
    const live = payload?.live
    const rows = live && validDate(live.kickoff) && validDate(live.observed_at) &&
      now - Date.parse(live.observed_at) < 86400000 ? [live] : []
    for (const row of Array.isArray(payload?.gold_schedule) ? payload.gold_schedule.slice(0, 8) : []) {
      if (row.fixture_id !== rows[0]?.fixture_id && validDate(row.kickoff) && Date.parse(row.kickoff) > now) {
        rows.push(row)
      }
    }
    if (JSON.stringify(rows) === JSON.stringify(liveRows)) return
    const previous = liveRows[liveIndex]?.fixture_id
    liveRows = rows
    liveIndex = Math.max(0, liveRows.findIndex(row => row.fixture_id === previous))
    showLiveMatch()
  }

  function toggleLive(playing) {
    livePlaying = playing
    el('publisher-live-pause').textContent = livePlaying ? 'Pausa' : 'Riprendi'
  }

  function render(payload, now = Date.now()) {
    const groups = (Array.isArray(payload?.groups) ? payload.groups : []).filter(group => {
      const free = group.free
      return free && validDate(free.kickoff) && Date.parse(free.kickoff) > now &&
        validDate(free.expires_at) && Date.parse(free.expires_at) > now && validDate(free.quote_at)
    })
    const future = row => validDate(row.kickoff) && Date.parse(row.kickoff) > now
    const previews = (Array.isArray(payload?.gold_previews) ? payload.gold_previews : [])
      .filter(row => future(row) && row.proposals?.length === 3).slice(0, 2)
    const triads = new Map(previews.map(row => [row.fixture_id, row.proposals]))
    const cards = []
    const seen = new Set()
    for (const group of groups) {
      if (seen.has(group.free.fixture_id)) continue
      seen.add(group.free.fixture_id)
      cards.push(matchCard(group.free, true, triads.get(group.free.fixture_id)))
    }
    for (const preview of previews) {
      if (seen.has(preview.fixture_id)) continue
      seen.add(preview.fixture_id)
      cards.push(matchCard(preview, false, preview.proposals))
    }
    for (const group of groups) {
      for (const preview of Array.isArray(group.previews) ? group.previews : []) {
        if (seen.has(preview.fixture_id) || !future(preview)) continue
        seen.add(preview.fixture_id)
        cards.push(matchCard(preview, false))
      }
    }
    el('daily-picks').replaceChildren(...(cards.length ? cards : [
      node('p', 'Le anteprime compaiono quando una scheda Gold ha tutte e tre le proposte.', 'empty')
    ]))
    if (validDate(payload?.updated_at)) {
      const stamp = el('preview-date')
      stamp.textContent = `Bacheca aggiornata ${shortClock(payload.updated_at)}`
      stamp.dateTime = payload.updated_at
    }
    renderLive(payload, now)
    if (payload?.academy) {
      const lesson = payload.academy
      const content = [node('h3', lesson.title), node('p', lesson.body)]
      if (lesson.example) content.push(node('p', lesson.example, 'model-number'))
      el('publisher-academy').replaceChildren(...content)
    }
    reportCards(payload?.report)
    if (!payload?.report && payload?.historical) {
      reportSignature = undefined
      const previous = payload.historical
      const when = new Date(previous.date + 'T12:00:00Z')
      el('results-content').replaceChildren(
        node('p', 'Archivio verificato · edizione precedente', 'eyebrow'),
        node('h3', `Giornata del ${shortDate.format(when)}`),
        node('p', `${previous.won} corrette · ${previous.lost} errate`, 'model-number'),
        node('p', 'Il report V7.1 comparirà dopo il primo ciclo certificato.', 'muted'))
    }
    el('publisher-trial-copy').textContent = payload?.trial_verified === true
      ? 'Apri @ArabSniperBot, invia /prova e verifica l’email. La prova Gold dura 7 giorni, senza carta e senza rinnovo automatico.'
      : 'Apri il bot ufficiale per consultare le opzioni di prova Gold.'
  }

  async function refresh() {
    if (pending) return
    pending = true
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 6500)
    try {
      const response = await fetch(feedUrl, { signal: controller.signal })
      if (!response.ok) throw new Error('feed')
      const payload = await response.json()
      if (payload.schema_version !== 1 || payload.publisher !== 'v7.1') throw new Error('schema')
      saved = payload
      render(saved)
    } catch {
      render(saved)
    } finally {
      clearTimeout(timer)
      pending = false
    }
  }

  handleUrlMessages()
  el('publisher-live-back').addEventListener('click', () => moveLive(-1))
  el('publisher-live-next').addEventListener('click', () => moveLive(1))
  el('publisher-live-pause').addEventListener('click', () => toggleLive(!livePlaying))
  motion.addEventListener('change', event => toggleLive(!event.matches))
  toggleLive(livePlaying)
  refresh()
  setInterval(() => {
    if (livePlaying && !document.hidden && !el('publisher-live').matches(':hover, :focus-within')) moveLive(1)
  }, 8000)
  setInterval(() => { if (saved && !document.hidden) render(saved) }, 30000)
  setInterval(() => { if (!document.hidden) refresh() }, 120000)
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh() })
})()
