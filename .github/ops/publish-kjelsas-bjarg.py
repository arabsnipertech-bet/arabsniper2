"""Publish the verified 14 November result once to each established channel."""
from datetime import datetime, timezone
import json
import os
import re
from time import sleep

import requests

from gold_browser_feed import read_triad_history
from gold_storage import client
from gold_triad_history import fixture_key
from lateral_matrix import market_id, specification
from multimarket_model import stamp
from publisher_v71.channels import CHANNEL, PAGE, DeliveryError, Facebook, Telegram
from publisher_v71.scores import public_cache
from publisher_v71.storage import Storage
from v7_decision import GOAL, OVER_CARRY


FIXTURE = '1648248'
KICKOFF = '2026-11-14T12:00:00+00:00'
PREFIX = 'spotlight/2026-11-14/' + FIXTURE
MATCH = 'Kjelsås–Bjarg'
HEADLINE = '🎯 Colpaccio in due tempi | ' + MATCH
EXACT = market_id(specification('halves_total', side='over', line=1.5, extra=2.5))
MARKETS = {EXACT: 'Over 1,5 PT + Over 2,5 ST',
           GOAL['over15_ht']: 'Over 1,5 PT', GOAL['over25_sh']: 'Over 2,5 ST',
           OVER_CARRY: 'Over 1,5 PT + Over 2,5 FT'}
ROLES = {'most_probable': 'Più probabile', 'recommended_alternative': 'Consigliata',
         'high_odds_shot': 'Colpaccio'}


def prematch(history):
    key = fixture_key({'fixture_id': FIXTURE, 'kickoff': KICKOFF})
    entry = history.get(key) or {}
    if stamp(entry.get('kickoff')) != stamp(KICKOFF):
        return []
    found = {}
    for role, label in ROLES.items():
        for item in (entry.get('slots') or {}).get(role, []):
            observed = stamp(item.get('captured_at'))
            market = item.get('market_id')
            if market in MARKETS and observed and observed < stamp(KICKOFF):
                found[market] = (label, MARKETS[market], item['captured_at'])
    if EXACT in found:
        return [found[EXACT]]
    return [found[key] for key in (GOAL['over15_ht'], GOAL['over25_sh'], OVER_CARRY) if key in found]


def final_score():
    for attempt in range(5):
        try:
            match = public_cache()['fixtures'].get(FIXTURE) or {}
        except (requests.RequestException, ValueError, KeyError):
            match = {}
        seen = stamp(match.get('observed_at'))
        if (match.get('fixture_id') == FIXTURE and match.get('status') == 'FT' and seen
                and seen > stamp(KICKOFF) and not match.get('refresh_failed')):
            ht = match.get('halftime') or {}
            ft = match.get('fulltime') or {}
            home, away = ft.get('home'), ft.get('away')
            if ht != {'home': 1, 'away': 1}:
                raise ValueError('Final feed disagrees with the verified halftime score')
            if type(home) is int and type(away) is int:
                if home < 2 or away < 3:
                    raise ValueError('Final feed disagrees with the verified goal sequence')
                return home, away
        if attempt < 4:
            sleep(55)
    return None


def message(picks, final):
    score = f'Finale {final[0]}–{final[1]}' if final else '2–3 al 69′'
    second = f'{sum(final) - 2} gol nella ripresa' if final else '3 gol nella ripresa entro il 69′'
    text = [HEADLINE, '', f'⚽ 1–1 all’intervallo · {score}',
            f'Due gol nel primo tempo, {second}. Il quinto gol è arrivato al 69′.']
    if picks:
        text += ['', 'Dalla scheda Gold prima del calcio d’inizio.']
        text += [f'• {role} · {market}' for role, market, _ in picks]
    text += ['', 'Al 69′ erano già stati raggiunti l’Over 1,5 nel primo tempo e l’Over 2,5 nel secondo.']
    return '\n'.join(text)


def telegram(post):
    result = Telegram(os.environ['BOT_TOKEN']).call('sendMessage', {'chat_id': '@' + CHANNEL, 'text': post})
    if result.get('chat', {}).get('username', '').lower() != CHANNEL or not result.get('message_id'):
        raise DeliveryError('uncertain', 'telegram_destination_unverified')
    return f"https://t.me/{CHANNEL}/{result['message_id']}"


def facebook(post):
    transport = Facebook(os.environ['META_TOKEN'])
    transport.identify()
    result = transport.request('POST', PAGE + '/feed', data={'message': post})
    post_id = result.get('id') or ''
    if not re.fullmatch(PAGE + r'_\d+', post_id):
        raise DeliveryError('uncertain', 'meta_missing_post_receipt')
    return 'https://www.facebook.com/' + post_id


def deliver(store, channel, send, post):
    key = f'{PREFIX}/{channel}'
    receipt, _ = store.read(f'evidence/{key}-receipt.json')
    if receipt:
        return receipt['url']
    if not store.freeze(f'{key}-claim.json', {'at': datetime.now(timezone.utc).isoformat()}):
        raise DeliveryError('uncertain', 'publication_already_attempted')
    url = send(post)
    print(channel, url, flush=True)
    store.freeze(f'{key}-receipt.json', {'url': url})
    return url


def main():
    s3 = client()
    bucket = os.environ['R2_BUCKET']
    store = Storage(s3, bucket)
    picks = prematch(read_triad_history(s3, bucket))
    final = final_score()
    post = message(picks, final)
    print(json.dumps({'fixture': FIXTURE, 'final': final, 'prematch': picks}, ensure_ascii=False), flush=True)
    for channel, send in (('Telegram', telegram), ('Facebook', facebook)):
        try:
            deliver(store, channel, send, post)
        except DeliveryError as error:
            print(channel, error.reason, flush=True)
    if any(not store.read(f'evidence/{PREFIX}/{channel}-receipt.json')[0] for channel in ('Telegram', 'Facebook')):
        raise SystemExit('At least one channel has no verified receipt')


if __name__ == '__main__':
    main()
