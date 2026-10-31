/* Shared competition guard for public previews and stored Gold dashboards. */
(() => {
  // Generated from gold_competitions.py. Run python src/gold_competitions.py --sync-browser after policy edits.
  const rules = {"aliases":{"czechia":"czech republic","korea republic":"south korea","republic of ireland":"ireland","the netherlands":"netherlands","turkiye":"turkey","united states":"usa","united states of america":"usa"},"id":"gold-senior-whitelist-v2.2","retired":{"austria":["regionalliga","regionalliga mitte","regionalliga north","regionalliga ost","regionalliga salzburg","regionalliga south","regionalliga tirol","regionalliga west"],"azerbaijan":["ii liqa","second league"],"bulgaria":["3rd league","third league","third league northeast","third league northwest","third league southeast","third league southwest","treta liga"],"croatia":["2 nl","second nl"],"czech republic":["3 liga","3 liga cfl a","3 liga cfl b","3 liga msfl","3 liga promotion play off","cfl","msfl"],"denmark":["2 division","2nd division","2nd division group 1","2nd division group 2"],"finland":["ykkonen"],"france":["championnat national","ligue 3","national"],"germany":["3 liga"],"ireland":["fai national league","national league"],"kazakhstan":["2 division","second division","second league"],"netherlands":["tweede divisie"],"northern ireland":["premier intermediate league"],"norway":["2 division","2 division group 1","2 division group 2","2 division play offs","2 divisjon","3 division","3 divisjon","postnord ligaen"],"poland":["2 liga","ii liga","ii liga east","ii liga west","iii liga","iv liga"],"portugal":["liga 3"],"romania":["liga 3","liga iii","liga iii play offs","liga iii serie 1","liga iii serie 10","liga iii serie 2","liga iii serie 3","liga iii serie 4","liga iii serie 5","liga iii serie 6","liga iii serie 7","liga iii serie 8","liga iii serie 9"],"scotland":["league one"],"serbia":["srpska liga belgrade","srpska liga east","srpska liga vojvodina","srpska liga west"],"slovakia":["2 liga","3 liga","3 liga bratislava","3 liga center","3 liga east","3 liga play offs","3 liga west","ii liga","iii liga"],"spain":["primera division rfef","primera division rfef group 1","primera division rfef group 2","primera division rfef group 3","primera division rfef group 4","primera division rfef group 5","primera division rfef play offs","primera federacion"],"sweden":["ettan","ettan norra","ettan relegation round","ettan sodra"],"switzerland":["1 liga promotion","promotion league"],"turkey":["2 lig","2 lig red","2 lig white"],"ukraine":["druha liga","druha liga group a","druha liga group b","druha liha","persha liga","persha liha"]}};
  const normalize = value => String(value || '').normalize('NFKD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
  const versions = new Set([rules.id, 'gold-senior-whitelist-v2.1', 'gold-senior-whitelist-v2', 'gold-senior-whitelist-v1']);
  const withoutCountry = [...new Set(Object.values(rules.retired).flat())];
  function allows(id, country, league) {
    if (!versions.has(id)) return false;
    const c = normalize(country), name = normalize(league);
    const families = c ? rules.retired[rules.aliases[c] || c] || [] : withoutCountry;
    return !families.some(f => name === f || name.startsWith(f+' '));
  }
  const api = {id: rules.id, accepts: id => versions.has(id), allows};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else globalThis.ArabCompetitionScope = api;
})();
