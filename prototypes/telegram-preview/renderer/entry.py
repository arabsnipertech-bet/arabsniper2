"""Loopback replay endpoint. No delivery transport, triggers, or external bindings."""
from datetime import datetime
from js import Uint8Array
from workers import Response, WorkerEntrypoint
from pyodide.ffi import to_js
import json

from publisher_v71 import editorial
from telegram_live_art import animation


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        if request.method != 'POST':
            return Response('Method not allowed', status=405)
        data = json.loads(await request.text())
        post = data['post']
        view = post['view']
        if data['mode'] == 'caption':
            caption = editorial.message(post['kind'], view, post['marker'], 'telegram',
                                        now=datetime.fromisoformat(post['created_at']),
                                        trial_ok=data['trial_ok'])
            return Response(caption)
        if data['mode'] == 'gif' and post['kind'] == 'live' and post.get('edit_id'):
            badge = f"{view['home']}–{view['away']} · {view['status'].upper()}"
            raw = animation(view['animation'], badge)
            mime = 'image/gif'
        elif data['mode'] == 'jpeg':
            raw = editorial.card(post['kind'], view)
            mime = 'image/jpeg'
        else:
            return Response('Invalid replay', status=400)
        return Response(Uint8Array.new(to_js(raw)), headers={'Content-Type': mime})
