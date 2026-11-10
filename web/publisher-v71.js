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
  const cohorts = { public: 'Gratuite pubblicate', gold: 'Disponibili in Gold · documentate', internal: 'Ricerca interna' }
  let saved
  let pending

  function node(tag, text, className) {
    const element = document.createElement(tag)
    if (className) element.className = className
    if (text != null) element.textContent = text
    return element
  }

  function matchCard(row, free) {
    const card = node('article', null, free ? 'pick editorial-pick' : 'pick editorial-pick preview')
    const meta = node('div', null, 'pick-top')
    const start = node('time', shortClock(row.kickoff))
    start.dateTime = row.kickoff
    meta.append(node('span', free ? row.category : `Anteprima Gold · ${row.category}`), start)
    card.append(meta, node('div', row.league, 'league'), node('h3', row.match))
    if (!free) {
      const link = node('a', 'Vedi l’analisi in Gold ↗', 'editorial-link')
      link.href = 'gold-dashboard-live.html'
      card.append(link)
      return card
    }
    const market = node('div', null, 'target')
    market.append(node('strong', row.market), node('b', number(row.odd, 2), 'odd'))
    card.append(market, node('p', `Probabilità del modello ${number(row.probability * 100, 1, '%')}`, 'model-number'),
      node('p', row.reason, 'muted'), node('small', `Quota rilevata alle ${time.format(Date.parse(row.quote_at))} · ora italiana`, 'snapshot'))
    return card
  }

  function reportCards(report) {
    const target = el('results-content')
    if (!report?.cohorts) return
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
    target.replaceChildren(node('p', `Giornata del ${shortDate.format(when)} · certificata dall’Auditor`, 'eyebrow'), grid)
    if (report.examples?.length) {
      const examples = node('ul', null, 'editorial-examples')
      for (const row of report.examples) {
        examples.append(node('li', `${outcomes[row.outcome]} · ${row.match} · ${row.market} · quota ${number(row.odd, 2)}`))
      }
      target.append(examples)
    }
    target.append(node('small', 'Una unità per selezione. Il ROI include le rimborsate a profitto zero e lascia fuori gli esiti pendenti.', 'snapshot'))
  }

  function render(payload, now = Date.now()) {
    const groups = (Array.isArray(payload?.groups) ? payload.groups : []).filter(group => {
      const free = group.free
      return free && validDate(free.kickoff) && Date.parse(free.kickoff) > now &&
        validDate(free.expires_at) && Date.parse(free.expires_at) > now && validDate(free.quote_at)
    })
    const cards = []
    const seen = new Set()
    for (const group of groups) {
      if (seen.has(group.free.fixture_id)) continue
      seen.add(group.free.fixture_id)
      cards.push(matchCard(group.free, true))
      for (const preview of Array.isArray(group.previews) ? group.previews : []) {
        if (seen.has(preview.fixture_id) || !validDate(preview.kickoff) || Date.parse(preview.kickoff) <= now) continue
        seen.add(preview.fixture_id)
        cards.push(matchCard(preview, false))
      }
    }
    el('daily-picks').replaceChildren(...(cards.length ? cards : [
      node('p', 'Nessuna nuova BEST in programma al momento. La bacheca si aggiorna con le prossime analisi qualificate.', 'empty')
    ]))
    if (validDate(payload?.updated_at)) {
      const stamp = el('preview-date')
      stamp.textContent = `Bacheca aggiornata ${shortClock(payload.updated_at)}`
      stamp.dateTime = payload.updated_at
    }
    const live = payload?.live
    if (live && validDate(live.kickoff) && validDate(live.observed_at)) {
      const score = Number.isInteger(live.home) && Number.isInteger(live.away) ? ` · ${live.home}–${live.away}` : ''
      el('publisher-live').replaceChildren(node('p', live.league, 'eyebrow'), node('h3', live.match),
        node('p', `${live.status}${score} · ${shortClock(live.observed_at)}`, 'model-number'))
    } else {
      el('publisher-live').replaceChildren(node('p', 'Il prossimo Live comparirà dopo la pubblicazione di una selezione qualificata.', 'muted'))
    }
    if (payload?.academy) {
      const lesson = payload.academy
      const content = [node('h3', lesson.title), node('p', lesson.body)]
      if (lesson.example) content.push(node('p', lesson.example, 'model-number'))
      el('publisher-academy').replaceChildren(...content)
    }
    reportCards(payload?.report)
    if (payload?.trial_verified === true) {
      el('publisher-trial-copy').textContent = 'Apri @ArabSniperBot, invia /prova e verifica l’email. La prova Gold dura 7 giorni, senza carta e senza rinnovo automatico.'
    }
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
  refresh()
  setInterval(() => { if (saved && !document.hidden) render(saved) }, 30000)
  setInterval(() => { if (!document.hidden) refresh() }, 120000)
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh() })
})()
