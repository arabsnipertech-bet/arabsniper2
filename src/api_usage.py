"""Sanitized API quota inspection and small scan metrics. Never prints credentials."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


def sanitize_status(payload):
    response = payload.get('response') or {}
    if payload.get('errors') or not isinstance(response, dict):
        raise ValueError('API status response unavailable')
    subscription, requests = response.get('subscription', {}), response.get('requests', {})
    current, limit = requests.get('current'), requests.get('limit_day')
    if not isinstance(current, int) or not isinstance(limit, int) or limit <= 0:
        raise ValueError('API quota values missing')
    return {'plan': subscription.get('plan'), 'active': subscription.get('active'),
            'requests_used_today': current, 'requests_limit_day': limit,
            'requests_remaining': max(0, limit-current), 'quota_timezone': 'UTC'}


def inspect():
    request = Request('https://v3.football.api-sports.io/status',
                      headers={'x-apisports-key': os.environ['API_SPORTS_KEY']})
    with urlopen(request, timeout=30) as response:
        result = sanitize_status(json.load(response))
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(result))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('### API quota (no credentials or account identity)\n```json\n'+json.dumps(result,indent=2)+'\n```\n')
    return result


if __name__ == '__main__':
    inspect()
