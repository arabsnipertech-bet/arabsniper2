"""Gold Classic public copy and reusable 1080 × 1350 card templates."""
from datetime import date
from functools import lru_cache
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from multimarket_model import stamp

from .selection import ROME

SITE = 'https://arabsniperbet.com'
BOT = 'https://t.me/ArabSniperBot'
CHANNEL = 'https://t.me/arabsniper_signals'
BACKGROUND = '#0B1424'
PANEL = '#15253D'
GOLD = '#D8B66D'
IVORY = '#F3F0E8'
MUTED = '#BAC4D2'

ACADEMY = (
    ('Leggere un riscontro', 'Successo e ROI misurano cose diverse. Il successo conta gli esiti corretti tra le selezioni con un esito. Il ROI teorico rapporta il risultato netto alle unità imputate, a quota osservata.'),
    ('OCI e variazioni delle quote', 'L’OCI sintetizza ampiezza, persistenza e consenso dei movimenti di prezzo. Un calo indica una quota più bassa rispetto alla rilevazione precedente. Per interpretarlo servono orario, fonte e contesto della partita.'),
    ('Quota fair, probabilità e aggio', 'La quota fair corrisponde all’inverso della probabilità stimata. Per stimare l’aggio si sommano le probabilità implicite di tutti gli esiti comparabili presso lo stesso operatore e si sottrae il 100%.'),
    ('Lambda, Poisson e gol attesi', 'Lambda rappresenta il numero medio di gol attesi per un intervallo. La distribuzione di Poisson usa quel valore per stimare le probabilità dei diversi conteggi. Primo tempo e finale hanno intervalli distinti.'),
    ('Dallo snapshot all’esito', 'Mercato, quota e probabilità si leggono insieme all’ora dello snapshot. Il risultato osservato va confrontato con il mercato originale. Un mercato del primo tempo si chiude all’intervallo.'),
    ('Le tre categorie delle BEST', 'Probabile, Plausibile e Colpaccio corrispondono ai tre ruoli della triade. I livelli Blu, Gold e Platinum descrivono la qualificazione interna. Una categoria editoriale non rappresenta una certezza.'),
    ('Bilancio e limiti del modello', 'Una settimana con poche selezioni può avere risultati molto variabili. Confronta periodi con criteri omogenei e separa le selezioni pubblicate dalle analisi interne.'),
)
OUTCOMES = {'won': 'Corretta', 'lost': 'Errata', 'pending': 'In attesa', 'void': 'Rimborsata'}
COHORTS = {'public': 'Gratuite pubblicate', 'gold': 'Gold documentate', 'internal': 'Ricerca interna'}


def amount(value, digits=1):
    return '—' if value is None else f'{value:.{digits}f}'.replace('.', ',')


def italian_time(value):
    return stamp(value).astimezone(ROME).strftime('%d/%m · %H:%M')


def academy(day, report=None, week=()):
    weekday = day.weekday()
    title, body = ACADEMY[weekday]
    examples = (report or {}).get('examples', [])
    if weekday in (0, 4) and examples:
        row = examples[0]
        example = f"{row['match']} · {row['market']} a {amount(row['odd'], 2)} · {OUTCOMES[row['outcome']].lower()}."
    elif weekday == 6 and week:
        total = sum(item['cohorts']['public']['verified'] for item in week)
        won = sum(item['cohorts']['public']['won'] for item in week)
        example = f'Nei {len(week)} report certificati della settimana, {won} esiti corretti su {total} verificati per le gratuite.'
    else:
        example = ''
    return {'date': str(day), 'title': title, 'body': body, 'example': example}


def best_copy(view, *, locked=False):
    free = view['free']
    prefix = '🔒 Gold' if locked else 'Gold'
    return [
        f"{free['match']} · {free['league']} · {italian_time(free['kickoff'])}",
        f"{free['category']} · {free['market']} · quota {amount(free['odd'], 2)} · modello {amount(free['probability'] * 100)}%",
        f"{free['reason']} Quota rilevata {italian_time(free['quote_at'])}.",
        *(f"{prefix} · {row['match']} · {row['league']} · {italian_time(row['kickoff'])} · {row['category']}"
          for row in view['previews']),
    ]


def studio_copy(view, *, locked=False):
    free = view['free']
    prefix = '🔒 Gold' if locked else 'Gold'
    details = f"{free['category']} · {free['market']} · stima V7 {amount(free['probability'] * 100)}%"
    if 'odd' in free:
        details += f"\nQuota osservata {amount(free['odd'], 2)} · {italian_time(free['quote_at'])}"
    return [
        f"{free['match']} · {free['league']} · {italian_time(free['kickoff'])}",
        details,
        *(f"{prefix} · {row['match']} · {italian_time(row['kickoff'])}" for row in view['previews']),
    ]


def campaign_active(day):
    return date(2026, 11, 12) <= day <= date(2026, 11, 18)


def gold_access(now, trial_ok):
    lines = [f"🔓 Dashboard Gold {SITE}/gold-dashboard-live.html"]
    if trial_ok and campaign_active(now.astimezone(ROME).date()):
        lines.append(f"🎁 7 giorni gratis dal 12 al 18/11, anche per chi ha già provato. "
                     f"Apri {BOT} e scrivi /prova in privato.")
        lines.append("🏷️ Rinnovo con il 10% di sconto nelle ultime 48 ore della prova per gli utenti idonei.")
    elif trial_ok:
        lines.append(f"🎁 Prova Gold 7 giorni · {BOT} · /prova in privato")
    else:
        lines.append(f"🎁 Verifica la disponibilità della prova · {BOT} · /prova in privato")
    return '\n'.join(lines)


def report_copy(view, *, compact=False):
    if 'triad_examples' in view:
        examples = view['triad_examples']
        lines = ['Esempi selezionati dalle schede Gold · verifica Auditor' if examples
                 else 'Bilancio verificato dall’Auditor']
        for row in examples:
            match, market = row['match'], row['market']
            if compact:
                match = match[:46].rstrip() + '…' if len(match) > 46 else match
                market = market[:46].rstrip() + '…' if len(market) > 46 else market
            halftime = f" (PT {row['halftime']})" if row.get('halftime') else ''
            study = ' · Studio' if row.get('study_only') else ''
            lines.append(f"{match} · {row['score']}{halftime}\n✓ {row['category']} · {market}{study}")
        summary = []
        for key, name in (('public', 'BEST gratuite'), ('gold', 'BEST Gold documentate'), ('internal', 'Ricerca')):
            value = view['cohorts'][key]
            decided = value['won'] + value['lost']
            summary.append(f"{name} {value['won']}/{decided}" if decided else f'{name} —')
        lines.append('Corrette su esiti definitivi\n' + ' · '.join(summary))
        return lines

    lines = []
    for cohort, value in view['cohorts'].items():
        lines.append(
            f"{COHORTS[cohort]} · {value['verified']} verificati · {value['won']} C / {value['lost']} E · "
            f"{value['pending']} P / {value['void']} R\n"
            f"Successo {amount(value['hit_rate'])}% · Quota {amount(value['average_odd'], 2)} · "
            f"{amount(value['profit'], 2)} u · ROI {amount(value['roi'], 2)}%"
        )
    return lines


def live_copy(view):
    score = f" · {view['home']}–{view['away']}" if 'home' in view else ''
    return [f"{view['match']} · {view['league']}", f"{italian_time(view['kickoff'])} · {view['status']}{score}",
            'Aggiornamenti dal risultato ufficiale.']


def presentation(kind, view):
    if kind == 'best':
        return 'BEST', view['free']['match'], best_copy(view)[1:]
    if kind == 'studio':
        return 'STUDIO', view['free']['match'], studio_copy(view)[1:]
    if kind == 'report':
        return 'REPORT', 'Riscontri del ' + date.fromisoformat(view['date']).strftime('%d/%m'), report_copy(view)
    if kind == 'academy':
        return 'ACADEMY', view['title'], [view['body'], view.get('example', '')]
    if kind == 'live':
        return 'LIVE', view['match'], live_copy(view)[1:]
    return 'GOLD', 'Sette giorni in Gold', ['Esplora il cruscotto delle analisi.', 'Apri @ArabSniperBot e invia /prova.',
                                            'Accesso per 7 giorni, senza carta e senza rinnovo.']


def message(kind, view, marker, channel, *, now=None, trial_ok=False):
    label, title, lines = presentation(kind, view)
    if kind == 'best':
        lines = best_copy(view, locked=True)
        title = 'La selezione aperta'
    elif kind == 'studio':
        lines = studio_copy(view, locked=True)
        title = 'Spunto dalle schede Gold'
    elif kind == 'report' and 'triad_examples' not in view:
        lines.extend(f"{OUTCOMES[r['outcome']]} · {r['match'][:45]} · {r['market'][:38]}"
                     for r in view['examples'])
    elif kind == 'live':
        lines = live_copy(view)[1:]
    heading = f'ArabSniperBet · {label}\n{title}'
    site = f'Dati completi {SITE}/#risultati' if kind == 'report' and 'triad_examples' in view else SITE
    footer = f'{site} · 18+ · Stime sperimentali\n{marker}'
    if kind == 'gold':
        footer = f'{BOT}\n{CHANNEL}\n{footer}'
    elif kind in ('best', 'studio'):
        footer = f'{gold_access(now, trial_ok and now is not None)}\n18+ · Stime sperimentali\n{marker}'
    caption = '\n\n'.join([heading, *filter(None, lines), footer])
    if channel == 'telegram' and len(caption.encode('utf-16-le')) // 2 > 1024:
        if kind == 'best':
            lines[2] = lines[2][:100]
        elif kind == 'report':
            if 'triad_examples' in view:
                lines = report_copy(view, compact=True)
            else:
                lines = [*report_copy(view), *(line[:70] for line in lines[3:])]
        caption = '\n\n'.join([heading, *filter(None, lines), footer])
        if len(caption.encode('utf-16-le')) // 2 > 1024:
            budget = 1024 - len((heading + footer).encode('utf-16-le')) // 2 - 8
            body = '\n\n'.join(filter(None, lines))
            body = body.encode('utf-16-le')[:budget * 2].decode('utf-16-le', errors='ignore').rstrip() + '…'
            caption = '\n\n'.join([heading, body, footer])
    return caption


@lru_cache(maxsize=64)
def font(size, style='regular'):
    names = {'regular': 'DejaVuSans.ttf', 'bold': 'DejaVuSans-Bold.ttf',
             'serif': 'DejaVuSerif.ttf'}
    path = Path('/usr/share/fonts/truetype/dejavu') / names[style]
    return ImageFont.truetype(str(path), size)


def wrap(draw, text, face, width):
    output = []
    for paragraph in text.splitlines():
        line = ''
        for word in paragraph.split():
            possible = f'{line} {word}'.strip()
            if line and draw.textlength(possible, font=face) > width:
                output.append(line)
                line = word
            else:
                line = possible
        if line:
            output.append(line)
    return output


def block(draw, text, x, y, width, size, color, style='regular', max_lines=4):
    for point in range(size, 22, -2):
        face = font(point, style)
        lines = wrap(draw, text, face, width)
        if len(lines) <= max_lines and all(draw.textlength(line, font=face) <= width for line in lines):
            break
    step = int(point * 1.36)
    for line in lines:
        draw.text((x, y), line, fill=color, font=face)
        y += step
    return y


def card(kind, view):
    label, title, lines = presentation(kind, view)
    image = Image.new('RGB', (1080, 1350), BACKGROUND)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((42, 42, 1038, 1308), 32, outline='#41516A', width=2)
    draw.arc((765, 91, 981, 307), 12, 336, fill='#36465D', width=2)
    draw.arc((791, 117, 955, 281), 45, 348, fill='#3E4D63', width=2)
    draw.ellipse((856, 182, 890, 216), fill=GOLD)
    draw.line((873, 159, 873, 238), fill=GOLD, width=2)
    draw.line((834, 199, 912, 199), fill=GOLD, width=2)
    draw.text((91, 91), 'ARABSNIPERBET', fill=IVORY, font=font(32, 'bold'))
    draw.text((91, 145), 'GOLD CLASSIC  /  V7.1', fill=MUTED, font=font(22))
    draw.rounded_rectangle((91, 224, 91 + 37 + draw.textlength(label, font=font(26, 'bold')), 274),
                           12, fill=GOLD)
    draw.text((108, 232), label, fill=BACKGROUND, font=font(26, 'bold'))
    bottom = block(draw, title, 89, 318, 900, 72, IVORY, 'serif', max_lines=3)
    top = max(bottom + 35, 510)
    draw.rounded_rectangle((86, top, 994, 1164), 23, fill=PANEL)
    y = top + 32
    if kind in ('best', 'studio'):
        free = view['free']
        y = block(draw, f"{free['league']}   /   {italian_time(free['kickoff'])}", 120, y, 839, 28, MUTED, max_lines=1) + 17
    for index, line in enumerate(filter(None, lines)):
        if y > 1125:
            break  # Complete text remains in the caption.
        featured = index == 0 or kind == 'report'
        y = block(draw, line, 120, y, 839, 32 if kind != 'report' else 29,
                  GOLD if featured else IVORY, 'bold' if index == 0 else 'regular',
                  max_lines=3 if kind in ('report', 'best') else 5)
        y += 17
    draw.line((91, 1215, 989, 1215), fill=GOLD, width=2)
    draw.text((91, 1240), 'ARABSNIPERBET.COM', fill=GOLD, font=font(23, 'bold'))
    draw.text((942, 1240), '18+', fill=MUTED, font=font(23))
    output = BytesIO()
    image.save(output, 'JPEG', quality=89, optimize=True)
    return output.getvalue()
