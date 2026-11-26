// Local Worker prototype. The only input is a read-only copy of the Python outbox.
export async function preview(bucket, now = Date.now()) {
  const object = await bucket.get('publisher/v7.1/state.json');
  if (!object || object.size > 5_000_000) throw new Error('Missing or oversized ledger');
  const ledger = await object.json();
  if (ledger.schema !== 1 || !ledger.posts || typeof ledger.posts !== 'object' ||
      Array.isArray(ledger.posts)) throw new Error('Invalid ledger');
  const proposals = [];
  for (const [id, post] of Object.entries(ledger.posts)) {
    if (post?.channel !== 'telegram' || post.id !== id ||
        id !== [post.channel, post.kind, post.key].join('/') || post.receipt ||
        !['ready', 'retry'].includes(post.status)) continue;
    if (!Number.isInteger(post.attempts ?? 0) || (post.attempts ?? 0) < 0 ||
        (post.attempts ?? 0) >= 3 || (post.status === 'retry' && !post.retry_at)) continue;
    if (!(Date.parse(post.expires_at) > now) || (post.retry_at && !(Date.parse(post.retry_at) <= now))) continue;
    if (typeof post.text !== 'string' || !post.text.trim() || !post.marker ||
        !post.text.includes(post.marker)) continue;
    const edit = Boolean(post.edit_id);
    proposals.push({id, marker: post.marker, caption: post.text,
      method: edit ? 'editMessageMedia' : 'sendPhoto',
      media: edit && post.kind === 'live' && post.view?.animation ? 'animation' : 'photo',
      message_id: edit ? post.edit_id : null,
      needs_source_validation: true, needs_python_render: true});
  }
  return {dry_run: true, proposals};
}

// Deliberately no public route, scheduler, write binding calls or delivery transport.
export default {async fetch(request, env) {
  if (request.method !== 'GET' || new URL(request.url).pathname !== '/preview')
    return new Response(null, {status: 404});
  return Response.json(await preview(env.GOLD), {headers: {'Cache-Control': 'no-store'}});
}};
