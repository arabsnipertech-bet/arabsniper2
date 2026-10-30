"""Semantic discovery of football odds; raw IDs are provenance, never admission.

Internal canonical IDs select existing settlement adapters. Flexible labels do
not license mixing goals with corners/cards/team totals or different periods.
"""
from copy import deepcopy
from hashlib import sha256
import re
import unicodedata

VERSION = 'football-market-discovery-v2'
CANONICAL = {
    ('total','ft'): (5,'Goals Over/Under'), ('total','ht'): (6,'Goals Over/Under First Half'),
    ('total','sh'): (26,'Goals Over/Under - Second Half'),
    ('asian_total','ft'): (50,'Goal Line'), ('asian_total','ht'): (72,'Goal Line (1st Half)'),
    ('btts','ft'): (8,'Both Teams Score'), ('btts','ht'): (34,'Both Teams Score - First Half'),
    ('btts','sh'): (35,'Both Teams To Score - Second Half'),
    ('result','ft'): (1,'Match Winner'), ('result','ht'): (13,'First Half Winner'),
    ('result','sh'): (3,'Second Half Winner'),
    ('dnb','ft'): (2,'Home/Away'), ('dnb','ht'): (109,'Draw No Bet (1st Half)'),
    ('dnb','sh'): (182,'Draw No Bet (2nd Half)'),
    ('handicap','ft'): (4,'Asian Handicap'), ('handicap','ht'): (19,'Asian Handicap First Half'),
    ('handicap','sh'): (104,'Asian Handicap (2nd Half)'),
    ('halves_total','ft'): (184,'To Score in Both Halves'),
    ('team_home','ft'): (16,'Total - Home'), ('team_away','ft'): (17,'Total - Away'),
}
PERIODS = {
    'ht': r'\b(?:1st\s*half|first\s*half|half\s*1|half\s*time|1\s*h|h\s*1|primo\s*tempo|1[oa]?\s*tempo|1[oa]?\s*parte|pt|ht|1t)\b',
    'sh': r'\b(?:2nd\s*half|second\s*half|half\s*2|2\s*h|h\s*2|secondo\s*tempo|2[oa]?\s*tempo|2[oa]?\s*parte|st|2t)\b',
    'ft': r'\b(?:full\s*(?:time|match)|(?:regular|regulation)\s*time|ft|finale|tempo\s*regolamentare|90\s*(?:min|minutes|minuti))\b',
}
EXCLUDED = re.compile(r'\b(?:corners?|corner\s*kicks?|angoli|calci\s*d\s*angolo|cards?|cartellini|bookings?|'
    r'offsides?|fuorigioco|shots?|tiri|fouls?|falli|possession|possesso|saves?|parate|throw\s*ins?|'
    r'penalt(?:y|ies)|rigori|extra\s*time|supplementari|overtime|aet|et|120|minutes?|minuti|'
    r'first\s*goal|last\s*goal|next\s*goal|prossimo\s*gol|primo\s*gol|ultimo\s*gol|scorer|marcatore|'
    r'correct\s*score|exact\s*score|risultato\s*esatto|range|ranges|multigoal|multigol|odd|even|pari|dispari|'
    r'double\s*chance|doppia\s*chance|result\s*total|result\s*goals|highest\s*scoring|most\s*goals|'
    r'clean\s*sheet|win\s*to\s*nil|race\s*to|fantasy|virtual|esports)\b')
TEAM = re.compile(r'\b(?:home|away|host|hosts|visitor|visitors|casa|ospite|ospiti|team|teams|squadra|squadre)\b')
TOTAL_WORDS = frozenset('goals goal gol reti totale totali total totals over under o u ou asian asiatic asiatico asiatica asiatici line linea lines match partita game number of numero di the nel nella incontro'.split())
NEUTRAL_WORDS = frozenset('main alternative alt regular regulation time standard markets market full'.split())


def without_qualifiers(text):
    # A two-way label describes the partition, not the numeric goal line.
    text = re.sub(r'\b2\s*way\b', ' ', text)
    return ' '.join(word for word in text.split() if word not in NEUTRAL_WORDS)


def clean(value):
    value = re.sub(r'([a-z])([A-Z])',r'\1 \2',str(value or ''))
    value = ''.join(c for c in unicodedata.normalize('NFKD',value) if not unicodedata.combining(c))
    value = value.lower().replace(',', '.')
    value = re.sub(r'(?<=[a-z])\.(?=[a-z])', '', value)
    value = re.sub(r'(?<!\d)\.|\.(?!\d)', ' ', value)
    return re.sub(r'[^a-z0-9.]+', ' ', value).strip()


def identify(name):
    """Require semantic identity and period; reject unsupported qualifiers."""
    text = clean(name)
    if not text: return None
    # Remove the explicit regulation-time qualifier before rejecting minutes.
    text = re.sub(r'\b90\s*(?:min|minutes|minuti)\b','ft',text)
    if EXCLUDED.search(text): return None
    both_halves = bool(re.search(r'\b(?:both|each)\s*(?:halves|half)\b|\b(?:entrambi|ogni)\s*(?:i\s*)?(?:tempi|tempo)\b',text))
    if both_halves:
        if TEAM.search(text) or re.search(r'\bbtts\b|\bwin\b|\bvinc',text): return None
        numbers=re.findall(r'\d+(?:\.\d+)?',text)
        if numbers and (any(float(n)!=.5 for n in numbers) or 'over' not in text): return None
        words={w for w in text.split() if not re.fullmatch(r'\d+(?:\.\d+)?',w)}
        allowed=set('to score scored in both each halves half goals goal gol rete reti over under yes no entrambi i tempi ogni tempo almeno un una di del della partita'.split())
        if words <= allowed and re.search(r'\b(?:score|scored|goal|goals|gol|rete|reti)\b|\bover\s*0\.5\b',text):
            return {'kind':'halves_total','period':'ft','canonical_id':184,'canonical_name':CANONICAL['halves_total','ft'][1]}
        return None
    periods = {period for period,pattern in PERIODS.items() if re.search(pattern,text)}
    if len(periods)>1: return None
    period = next(iter(periods), 'ft')
    base = text
    for pattern in PERIODS.values(): base = re.sub(pattern,' ',base)
    base = without_qualifiers(base)
    kind = None
    if re.search(r'\b(?:both\s*teams?\s*(?:to\s*)?scor(?:e|ing)|btts|gg\s*ng|goal\s*no\s*goal|gol\s*no\s*gol|entrambe\s*(?:le\s*)?squadre\s*(?:a\s*segno|segnano))\b|^gg$',base):
        if re.search(r'\b(?:result|winner|and|con|over|under|home|away|handicap)\b',base): return None
        kind = 'btts'
    elif re.search(r'\b(?:draw\s*no\s*bet|dnb|rimborso\s*(?:in\s*caso\s*di\s*)?pareggio|senza\s*pareggio)\b',base) or base in ('home away','casa ospite'):
        kind = 'dnb'
    elif re.search(r'\basian\s*handicap\b|\bhandicap\s*asiatico\b|^ah$',base): kind='handicap'
    elif re.fullmatch(r'(?:match\s*|game\s*)?(?:winner|result)|1\s*x\s*2|esito(?:\s*(?:partita|incontro))?|risultato\s*(?:partita|incontro)',base): kind='result'
    elif re.search(r'\b(?:over|under|o\s*u|ou|total|totals|totale|totali|goals?|gol)\b',base):
        team = TEAM.search(base)
        if team:
            # Preserve known team totals as their own family, never match goals.
            if period == 'ft' and re.fullmatch(r'(?:total|totals|total goals|goals total) (?:home|away)|(?:home|away) (?:total|totals|total goals)',base):
                kind='team_home' if 'home' in base else 'team_away'
            else: return None
        else:
            words={w for w in base.split() if not re.fullmatch(r'\d+(?:\.\d+)?',w)}
            if not words <= TOTAL_WORDS: return None
            if len(set(re.findall(r'\d+(?:\.\d+)?',base)))>1: return None
            kind='asian_total' if re.search(r'\basian\b|\basiatic\w*\b|\bgoal\s*line\b',base) else 'total'
            if period=='sh' and kind=='asian_total': return None
    if (kind,period) not in CANONICAL: return None
    vocab={
        'btts':'both team teams to score scoring btts gg ng goal no gol entrambe le squadre a segno segnano yes',
        'dnb':'draw no bet dnb rimborso in caso di pareggio senza home away casa ospite',
        'handicap':'asian handicap asiatico ah',
    }
    if kind in vocab:
        words={w for w in base.split() if kind!='handicap' or not re.fullmatch(r'\d+(?:\.\d+)?',w)}
        if not words <= set(vocab[kind].split()): return None
    bid,label=CANONICAL[kind,period]
    return {'kind':kind,'period':period,'canonical_id':bid,'canonical_name':label}


def normalize_values(bet, identity):
    kind=identity['kind']; result=[]
    name=clean(bet.get('name'))
    for pattern in PERIODS.values(): name=re.sub(pattern,' ',name)
    name=without_qualifiers(name)
    named_lines=re.findall(r'(?<![\w.])(\d+(?:\.\d+)?)(?![\w.])',name)
    named_line=float(named_lines[0]) if len(named_lines)==1 and kind in ('total','asian_total','team_home','team_away') else None
    for value in bet.get('values', []):
        raw=clean(value.get('value')); normalized=None
        if kind in ('total','asian_total','team_home','team_away'):
            raw=re.sub(r'\b(?:piu\s*di|piu)\b','over',raw)
            raw=re.sub(r'\b(?:meno\s*di|meno)\b','under',raw)
            raw=re.sub(r'\s+(?:goals?|gol|reti)$','',raw)
            pattern=r'(over|under|o|u)\s*(\d+(?:\.\d+)?)'
            match=re.fullmatch(pattern,raw)
            reverse=re.fullmatch(r'(\d+(?:\.\d+)?)\s*(over|under|o|u)',raw)
            line=None
            if match: side,line=match[1],float(match[2])
            elif reverse: side,line=reverse[2],float(reverse[1])
            elif raw in ('over','under','o','u'):
                side=raw
                explicit=[value.get(k) for k in ('line','handicap','total') if value.get(k) is not None]
                try:
                    parsed={float(str(v).replace(',','.')) for v in explicit}
                    line=next(iter(parsed)) if len(parsed)==1 else named_line if not explicit else None
                except (ValueError,TypeError): line=None
            if line is not None and 0 <= line <= 15 and abs(line*4-round(line*4))<1e-8 and (named_line is None or line==named_line):
                normalized=f'{"Over" if side in ("over","o") else "Under"} {line:g}'
        elif kind in ('btts','halves_total'):
            normalized={'yes':'Yes','si':'Yes','no':'No','gg':'Yes','ng':'No','goal':'Yes','gol':'Yes','no goal':'No','no gol':'No'}.get(raw)
        elif kind in ('result','dnb'):
            normalized={'home':'Home','1':'Home','casa':'Home','away':'Away','2':'Away','ospite':'Away',
                        'draw':'Draw','x':'Draw','pareggio':'Draw'}.get(raw)
        elif kind=='handicap':
            # Keep signs: API-Football's outcome labels use the home anchor.
            signed=str(value.get('value','')).strip().lower().replace(',','.')
            m=re.fullmatch(r'(home|away|casa|ospite|1|2)\s*([+-]?\d+(?:\.\d+)?)',signed)
            if m: normalized=f'{"Home" if m[1] in ("home","casa","1") else "Away"} {m[2]}'
        # Invalid values stay explicit so downstream adapters can reject an
        # ambiguous pair instead of silently deleting a conflicting outcome.
        result.append({**deepcopy(value),'value':normalized if normalized is not None else '__unmapped__:'+str(value.get('value',''))})
    return result


def discover_payload(payload):
    if (payload or {}).get('_market_discovery_version')==VERSION: return payload
    result=deepcopy(payload or {}); result['_market_discovery_version']=VERSION
    for record in result.get('response',[]):
        for book in record.get('bookmakers',[]):
            for index,bet in enumerate(book.get('bets',[])):
                identity=identify(bet.get('name'))
                if identity is None:
                    bet['_discovery_rejected']=True
                    continue
                source_id,source_name=bet.get('id'),bet.get('name')
                token=str(source_id) if source_id is not None else 'name:'+clean(source_name)
                bid=identity['canonical_id']
                # Preserve equivalent historical catalogue identity 336.
                if str(source_id)=='336' and (identity['kind'],identity['period'])==('asian_total','ft'): bid=336
                metadata={'version':VERSION,'kind':'market_discovery','source_bet_id':source_id,
                    'source_bet_name':source_name,'source_token':sha256(token.encode()).hexdigest()[:16],
                    'market_kind':identity['kind'],'period':identity['period'],'canonical_id':bid}
                book['bets'][index]={**bet,'id':bid,'name':identity['canonical_name'],
                    'values':normalize_values(bet,identity),'_market_discovery':metadata}
    return result
