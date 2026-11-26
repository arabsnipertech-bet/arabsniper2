import { JSONParser } from '@streamparser/json';

export async function readBoard(bucket, number) {
  if (!Number.isInteger(number) || number < 1 || number > 5) throw Error('Invalid day');
  const obj = await bucket.get('arab_quote_day' + number + '.json');
  if (!obj) throw Error('Missing Gold board');
  const board = {matches: []};
  const parser = new JSONParser({paths:['$.matches.*','$.date','$.updated_at'], keepStack:false});
  parser.onValue = ({value, key, stack}) => {
    if (stack.length === 1) { board[key] = value; return; }
    const fixture = value.fixture;
    if (fixture?.status_short !== 'NS' || fixture.result) return;
    const matrix = value.multimarket?.matrix || {};
    const v7 = matrix.v7 || {};
    const sources = new Set(Object.values(v7.triad || {}).map(p => p?.source_market_id).filter(id => id != null));
    board.matches.push({fixture, multimarket:{matrix:{v7, rows:(matrix.rows || []).filter(r => sources.has(r.id))}}});
  };
  const reader = obj.body.getReader();
  try {
    for (;;) {
      const {value, done} = await reader.read();
      if (done) break;
      parser.write(value);
    }
    if (!parser.isEnded) parser.end();
  } finally { reader.releaseLock(); }
  if (!board.date || !Number.isFinite(Date.parse(board.updated_at))) throw Error('Invalid Gold revision');
  return board;
}
