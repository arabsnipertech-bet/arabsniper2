"""Bounded 12–18 November campaign. Safe to retry; two Telegram posts per day."""
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import importlib.util
import sys

spec=importlib.util.spec_from_file_location('campaign_ops',Path(__file__).with_name('deploy-telegram-campaign.py'))
ops=importlib.util.module_from_spec(spec);spec.loader.exec_module(ops)
START=date(2026,11,12);END=date(2026,11,18)
ROME=ZoneInfo('Europe/Rome')
SLOTS=((12*60+45,'pranzo'),(20*60+45,'sera'))

def selected(now):
    local=now.astimezone(ROME)
    if not START<=local.date()<=END:return None
    minute=local.hour*60+local.minute
    for index,(start,label) in enumerate(SLOTS):
        if start<=minute<start+150:
            return local.date().isoformat(),index,label
    return None

def message(day,index):
    headings=[
        '🎁 HAI GIÀ PROVATO GOLD? PUOI TORNARE PER ALTRI 7 GIORNI',
        '⚽ SCOPRI IL NUOVO MOTORE ARABSNIPERBET CON 7 GIORNI GRATIS',
        '📊 UNA SETTIMANA PER ESPLORARE LE ANALISI GOLD',
        '💛 NUOVI ISCRITTI E VECCHI UTENTI: LA PROVA GOLD È APERTA A TUTTI',
    ]
    heading=headings[((date.fromisoformat(day)-START).days*2+index)%len(headings)]
    return (heading+'\n\n'
        'La nuova campagna offre 7 giorni gratuiti anche a chi ha già utilizzato una prova. '
        'Se hai accesso attivo, i giorni si aggiungono alla scadenza. Una richiesta per email e account Telegram in questa campagna.\n\n'
        '📲 Apri https://t.me/ArabSniperBot e scrivi /prova in PRIVATO. '
        'Invia la tua email e conferma il codice ricevuto. Nessuna carta richiesta.\n\n'
        '🏷️ Vuoi continuare? Nelle ultime 48 ore della tua prova, trovi in Gold il rinnovo con il 10% di sconto: '
        '3 giorni a 4,50 €, 14 giorni a 13,50 €, un mese a 26,10 €. '
        'L’offerta vale per un acquisto relativo a questa scadenza. Pagamento singolo, senza rinnovo automatico.\n\n'
        '🌐 https://arabsniperbet.com/gold-dashboard-live.html\n'
        '18+ · Analisi statistiche, nessuna garanzia di vincita.')

def run(now=None,verify_only=False):
    choice=selected(now or datetime.now(timezone.utc))
    if not choice and not verify_only:
        print('Outside campaign dates or publication windows; nothing sent');return
    # Fail closed if a subsequent Worker deployment removed campaign eligibility.
    import os, email.parser, email.policy
    raw,h=ops.request(ops.BASE+ops.WORKER+'/content/v2',os.environ['CF_TOKEN'])
    ct=h.get('Content-Type','')
    if 'multipart/' in ct:
        m=email.parser.BytesParser(policy=email.policy.default).parsebytes(('Content-Type: '+ct+'\r\n\r\n').encode()+raw)
        code='\n'.join(p.get_payload(decode=True).decode() for p in m.iter_parts() if p.get_payload(decode=True))
    else:code=raw.decode()
    expected=Path('workers/stripe-access/renewal.mjs').read_text().replace('export ','')
    assert code.count(expected)==1,'Campaign discount code is not deployed'
    settings=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    bindings={b['name']:b for b in settings['bindings']}
    for flag in ['GOLD_RENEWAL_ENABLED','GOLD_PROMO_ENABLED','CHECKOUT_ENABLED','TELEGRAM_TRIAL_ENABLED']:
        assert bindings.get(flag,{}).get('text')=='true','Required feature is disabled'
    assert bindings.get('GOLD_RENEWAL_COUPON_ID',{}).get('text')=='arabsniper_gold_renewal_10'
    assert bindings.get('TELEGRAM_TRIAL_CAMPAIGN',{}).get('text',ops.CAMPAIGN)==ops.CAMPAIGN
    bot=ops.health()
    channel=ops.telegram('getChat',chat_id='@arabsniper_signals')
    assert channel['type']=='channel' and channel['username'].lower()=='arabsniper_signals'
    member=ops.telegram('getChatMember',chat_id=channel['id'],user_id=bot['id'])
    assert member['status']=='administrator' and member.get('can_post_messages')
    if verify_only:
        print('Campaign advertising preflight: OK (no publication)');return
    day,index,label=choice;key=ops.CAMPAIGN+':'+day+':'+label
    existing=ops.query('SELECT status,message_id FROM telegram_campaign_announcements WHERE campaign=?',[key])
    if existing:
        assert existing[0]['status']=='sent','Prior send outcome uncertain; inspect channel before retrying'
        print('Already sent: https://t.me/arabsniper_signals/'+str(existing[0]['message_id']));return
    row=ops.query("INSERT OR IGNORE INTO telegram_campaign_announcements(campaign,status,created_at) VALUES (?,'sending',unixepoch()) RETURNING campaign",[key])
    assert row,'Another run owns this slot'
    sent=ops.telegram('sendMessage',chat_id=channel['id'],text=message(day,index),disable_web_page_preview=True)
    assert sent['chat']['id']==channel['id']
    ops.query("UPDATE telegram_campaign_announcements SET status='sent',message_id=? WHERE campaign=?",[sent['message_id'],key])
    print('Published '+key+': https://t.me/arabsniper_signals/'+str(sent['message_id']))

if __name__=='__main__':
    try:run(verify_only='--verify' in sys.argv)
    except Exception as e:
        print('STOP:',str(e) if isinstance(e,(AssertionError,RuntimeError)) else type(e).__name__)
        sys.exit(1)
