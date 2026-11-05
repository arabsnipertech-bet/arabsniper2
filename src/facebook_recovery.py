"""Recover a confirmed Telegram slot on Facebook under the Page publication lock."""
from datetime import datetime
import json
import os
from pathlib import Path
from telegram_digest import ROME, artifact, gh
from telegram_slots import CHANNEL_SLOTS
from facebook_page import main as publish


def main():
    now = datetime.now(ROME)
    slot = os.environ.get('RECOVERY_SLOT')
    if slot not in CHANNEL_SLOTS:
        raise ValueError('Unknown recovery slot')
    due = datetime.fromisoformat(f'{now.date()}T{slot}').replace(tzinfo=ROME)
    if not 0 <= (now-due).total_seconds() < 30*60:
        raise ValueError('Facebook recovery window closed')
    name = f"tg-{now.date()}-{slot.replace(':', '')}-sent"
    saved = artifact(name)
    if not saved:
        raise ValueError('Confirmed Telegram receipt unavailable')
    Path('receipts').mkdir(exist_ok=True)
    gh('run', 'download', str(saved['workflow_run']['id']), '-n', name, '-D', 'receipts')
    receipt = json.loads(Path('receipts/post.json').read_text())
    if receipt.get('slot') != slot or receipt.get('day') != str(now.date()):
        raise ValueError('Receipt date or slot mismatch')
    publish('selections')


if __name__ == '__main__':
    main()
