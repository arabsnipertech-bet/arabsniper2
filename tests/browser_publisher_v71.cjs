const assert = require('node:assert/strict')
const { chromium } = require('playwright')

const feedPath = 'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/publisher-public-v71/homepage.json'
const later = minutes => new Date(Date.now() + minutes * 60000).toISOString()
const preview = { fixture_id: '2', match: 'Club Riservato – Ospiti', league: 'Campionato', kickoff: later(60), category: 'Colpaccio' }
const free = { ...preview, fixture_id: '1', match: 'Club Aperto – Ospiti', kickoff: later(40), category: 'Probabile',
  market: 'Over 2,5 finale', odd: 1.93, probability: .58, quote_at: later(-10), expires_at: later(20),
  reason: 'Snapshot verificato' }
const metrics = { verified: 2, won: 1, lost: 1, pending: 0, void: 0, hit_rate: 50, average_odd: 1.9, profit: -.2, roi: -10 }
const feed = { schema_version: 1, publisher: 'v7.1', updated_at: later(-1), trial_verified: true,
  groups: [{ free, previews: [preview, { ...preview, fixture_id: '3', match: 'Partita iniziata', kickoff: later(-4) }] }],
  live: { ...preview, status: 'In corso', home: 1, away: 0, observed_at: later(-1) },
  report: { date: '2026-11-09', cohorts: { public: metrics, gold: metrics, internal: metrics }, examples: [
    { ...free, outcome: 'lost' },
  ] },
  academy: { title: 'OCI e variazioni delle quote', body: 'Leggere l’orario della rilevazione.' },
}
const goldSchedule = [
  { fixture_id: '8', match: 'Casa Gold – Ospiti Gold', league: 'Campionato', kickoff: later(160) },
  { fixture_id: '9', match: 'Seconda Gara – Ospiti', league: 'Campionato', kickoff: later(180) },
]
const proposals = [
  { category: 'Più probabile', market: 'Over 2,5 finale', probability: .58, odd: 1.93, quote_at: later(-10) },
  { category: 'Consigliata', market: 'Over 1,5 primo tempo', probability: .38, study_only: true },
  { category: 'Colpaccio', market: 'Gol primo tempo', probability: .29, odd: 3.80, quote_at: later(-10) },
]
const newFeed = { ...feed, groups: [], live: null, gold_schedule: goldSchedule,
  gold_previews: goldSchedule.map(row => ({ ...row, proposals })),
  report: { ...feed.report, triad_examples: [1, 2, 3, 4].map(id => ({
    match: `Squadra ${id} – Ospiti`, score: '2–1', category: 'Consigliata',
    market: 'Over 1,5 primo tempo', halftime: '1–1', study_only: true,
  })) },
}

async function main() {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
  try {
    for (const width of [390, 1440]) {
      const page = await browser.newPage({ viewport: { width, height: 900 } })
      const errors = []
      let payload = feed
      page.on('pageerror', error => errors.push(error))
      await page.route('http://arabsniper.test/**', route => {
        const url = new URL(route.request().url())
        const file = url.pathname === '/' ? '/index.html' : url.pathname
        return route.fulfill({ path: 'web' + file })
      })
      await page.route(feedPath, route => route.fulfill({ json: payload, headers: { 'access-control-allow-origin': '*' } }))
      await page.goto('http://arabsniper.test/?canceled=true')
      await page.getByText('Club Aperto – Ospiti', { exact: true }).first().waitFor()
      const bacheca = page.locator('#daily-picks')
      assert.equal(await bacheca.locator('article').count(), 2)
      assert.equal(await bacheca.locator('.target').count(), 1)
      assert.match(await bacheca.innerText(), /Anteprima Gold · Colpaccio/)
      assert.doesNotMatch(await bacheca.innerText(), /Partita iniziata/)
      assert.match(await page.locator('#publisher-live').innerText(), /1–0/)
      assert.match(await page.locator('#publisher-academy').innerText(), /OCI/)
      assert.match(await page.locator('#results-content').innerText(), /Errata/)
      assert.match(await page.locator('#publisher-trial-copy').innerText(), /7 giorni/)
      assert.equal(await page.getByText('Accedi a Gold ↗').getAttribute('href'), 'gold-dashboard-live.html')
      assert.equal(await page.locator('#piani button').count(), 3)
      assert.match(await page.locator('#checkout-status').innerText(), /Pagamento annullato/)
      assert.equal(await page.locator('[href^="mailto:"]').count(), 1)
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), width)

      payload = newFeed
      await page.emulateMedia({ reducedMotion: 'reduce' })
      await page.clock.install()
      await page.reload()
      await bacheca.getByText('Casa Gold – Ospiti Gold', { exact: true }).waitFor()
      assert.equal(await bacheca.locator('.gold-preview-triad li').count(), 6)
      assert.match(await bacheca.innerText(), /Quota bookmaker in attesa/)
      const live = page.locator('#publisher-live')
      assert.match(await live.innerText(), /🔒 Gold/)
      assert.match(await live.innerText(), /Casa Gold – Ospiti Gold/)
      assert.match(await live.innerText(), /In programma/)
      assert.doesNotMatch(await live.innerText(), /Over|Colpaccio/)
      const resume = live.getByRole('button', { name: 'Riprendi' })
      await page.clock.runFor(8100)
      assert.match(await live.innerText(), /Casa Gold – Ospiti Gold/)
      await resume.click()
      await page.evaluate(() => document.activeElement.blur())
      await page.mouse.move(0, 0)
      await page.clock.runFor(8100)
      assert.match(await live.innerText(), /Seconda Gara – Ospiti/)
      await live.getByRole('button', { name: 'Partita precedente' }).click()
      assert.match(await live.innerText(), /Casa Gold – Ospiti Gold/)
      const recap = page.locator('#results-content')
      assert.equal(await recap.locator('.editorial-hits li').count(), 4)
      assert.match(await recap.innerText(), /PT 1–1/)
      assert.equal(await recap.locator('details').evaluate(node => node.open), false)
      await recap.getByText('Dati completi del report').click()
      assert.match(await recap.innerText(), /Ricerca interna/)
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), width)
      assert.deepEqual(errors, [])
      await page.close()
    }
  } finally {
    await browser.close()
  }
  console.log('PASS: public homepage on 390 px and 1440 px, full previews, locked rotation and certified recap')
}
main().catch(error => { console.error(error); process.exitCode = 1 })
