// Snapshot the existing presentation at its original time. No policy changes.
const fs = require('node:fs');
const matrix = require('../web/lateral-matrix.js');
const Clock = Date;
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const result = input.map(record => {
  const m = structuredClone(record);
  const at = Clock.parse(m._gold_history.captured_at);
  global.Date = class extends Clock {
    constructor(...args) { super(...(args.length ? args : [at])); }
    static now() { return at; }
  };
  delete m._gold_history;
  // Score overlays are not inputs to a prematch category reconstruction.
  delete m.fixture.result; delete m.fixture.live; m.fixture.status_short = 'NS';
  const tier = matrix.cockpitTier(m);
  const active = matrix.publishedView(m, at);
  if (active?.triad) {
    const picks = Object.values(active.triad).filter(Boolean);
    return {name:tier.name, tone:tier.tone, category:tier.category, badge:tier.badge || null,
      proposal_id:tier.row?.id || null,
      platinum_ids:picks.filter(p=>p.signal.tier==='PLATINUM').map(p=>p.source.id),
      radar_ids:picks.filter(p=>p.signal.tier==='BLU').map(p=>p.source.id)};
  }
  const rows = matrix.rowsOf(m);
  const options = {now:at, archived:false, kickoff:m.fixture.kickoff,
    monitoring:m.multimarket?.matrix?.quote_monitoring};
  const platinum = matrix.platinumSignals(rows, options);
  const radar = tier.tone === 'studio' ? matrix.marketSignals(rows, options) : [];
  return {name:radar.length ? 'RADAR BLU' : tier.name, tone:tier.tone,
    category:tier.category || (radar.length ? 'radar' : tier.tone), proposal_id:tier.row?.id || null,
    platinum_ids:platinum.map(r=>r.id), radar_ids:radar.map(r=>r.id)};
});
global.Date = Clock;
process.stdout.write(JSON.stringify(result));
