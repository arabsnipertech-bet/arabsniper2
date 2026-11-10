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

async function main() {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
  try {
    for (const width of [390, 1440]) {
      const page = await browser.newPage({ viewport: { width, height: 900 } })
      const errors = []
      page.on('pageerror', error => errors.push(error))
      await page.route('http://arabsniper.test/**', route => {
        const url = new URL(route.request().url())
        const file = url.pathname === '/' ? '/index.html' : url.pathname
        return route.fulfill({ path: 'web' + file })
      })
      await page.route(feedPath, route => route.fulfill({ json: feed }))
      await page.goto('http://arabsniper.test/?checkout=cancelled')
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
      assert.equal(await page.locator('[href^="mailto:"]').count(), 1)
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), width)
      assert.deepEqual(errors, [])
      await page.close()
    }
  } finally {
    await browser.close()
  }
  console.log('PASS: public V7.1 homepage on 390 px and 1440 px, PARTIAL, live, reports, Academy and Gold links')
}
main().catch(error => { console.error(error); process.exitCode = 1 })
