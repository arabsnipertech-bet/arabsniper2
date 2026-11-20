// Replays on private local test data only. No Telegram transport or production ledger writes.
export async function claimShadow(bucket, postId, now, owner = 'shadow') {
  const key = 'offline-replay/ledger.json';
  const object = await bucket.get(key);
  if (!object) return 'missing';
  const ledger = await object.json();
  const post = ledger.posts[postId];
  if (owner !== 'shadow' || ledger.owner !== owner || post?.id !== postId ||
      post.channel !== 'telegram' || post.receipt || !['ready','retry'].includes(post.status) ||
      !Number.isInteger(post.attempts) || post.attempts < 0 || post.attempts >= 3 ||
      !(Date.parse(post.expires_at) > now) ||
      (post.status === 'retry' && !(Date.parse(post.retry_at) <= now))) return 'hold';
  post.status = 'sending';
  post.attempts++;
  const saved = await bucket.put(key,JSON.stringify(ledger),{onlyIf:{etagMatches:object.etag}});
  return saved ? 'claimed' : 'conflict';
}
