#!/usr/bin/env python3
"""
ARAB SNIPER QUOTE ENGINE V1
===========================

Motore autonomo per:
- estrazione fixture e quote API-Football / API-Sports;
- storico quota iniziale, attuale e variazioni nel tempo;
- consenso tra bookmaker tramite mediana;
- aggio, payout, carico del margine e OCI-AS;
- profilo statistico squadre, casa/trasferta, forma e scontri diretti;
- lambda FT e primo tempo;
- probabilita' ArabSniper per 1-X-2, Over 2.5 FT,
  Over 1.5 primo tempo e GG primo tempo;
- indicatori grafici pronti per il futuro frontend HTML:
    cerchio  = scelta ArabSniper
    quadrato = scelta book
    doppio   = concordanza ArabSniper + book

Il motore NON usa piu' etichette GOLD, MARKET, BOOST, PROBE ecc.
Produce dati grezzi per audit e una sezione display semplificata.

Esempi:
    python arab_quote_engine.py --today
    python arab_quote_engine.py --night --days 5
    python arab_quote_engine.py --date 2026-08-22
    python arab_quote_engine.py --today --github

Variabili ambiente:
    API_SPORTS_KEY   obbligatoria
    GITHUB_TOKEN     richiesta solo con --github
    ARAB_REPO_NAME   opzionale, default arabsnipertech-bet/arabsniper2
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import requests

try:
    from zoneinfo import ZoneInfo

    ROME_TZ = ZoneInfo("Europe/Rome")
except Exception:  # pragma: no cover
    ROME_TZ = timezone(timedelta(hours=1))

try:
    from github import Github
except Exception:  # pragma: no cover
    Github = None


# =========================================================
# CONFIGURAZIONE
# =========================================================
VERSION = "1.0.1"
API_BASE_URL = "https://v3.football.api-sports.io"

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent if BASE_DIR.name.lower() == "src" else BASE_DIR
DATA_DIR = PROJECT_ROOT / "data"

STATE_FILE = DATA_DIR / "arab_quote_state.json"
OUTPUT_FILE = DATA_DIR / "arab_quote_dashboard.json"
BET_CATALOG_FILE = DATA_DIR / "arab_bet_catalog.json"

REPO_NAME = os.getenv("ARAB_REPO_NAME", "arabsnipertech-bet/arabsniper2")
REMOTE_OUTPUT_FILE = "data/arab_quote_dashboard.json"
REMOTE_STATE_FILE = "data/arab_quote_state.json"
REMOTE_BET_CATALOG_FILE = "data/arab_bet_catalog.json"

API_MIN_INTERVAL = 0.14
API_RETRIES = (0, 2, 5)
MAX_HISTORY_POINTS = 120
MAX_TEAM_MATCHES = 14
MAX_H2H_MATCHES = 5
MAX_LEAGUE_MATCHES = 50

DEFAULT_EXCLUDED_COUNTRIES = {
    "Thailand", "Indonesia", "India", "Kenya", "Morocco", "Rwanda",
    "Nigeria", "Oman", "Algeria", "UAE", "Russia", "South-Africa",
    "Ethiopia", "Iran", "Bangladesh", "Vietnam", "Uganda", "Tanzania",
    "Zambia", "Egypt", "Myanmar",
}

LEAGUE_BLACKLIST = (
    "u19", "u20", "u21", "u23", "youth", "women", "friendly",
    "reserve", "amateur", "national 2", "national 3", "npl",
    "carioca", "paulista", "mineiro", "gaucho",
)

MARKET_1X2 = "1x2_ft"
MARKET_O25 = "over25_ft"
MARKET_O15HT = "over15_ht"
MARKET_GGHT = "gg_ht"

MARKET_SELECTIONS: Dict[str, Tuple[str, ...]] = {
    MARKET_1X2: ("1", "x", "2"),
    MARKET_O25: ("over", "under"),
    MARKET_O15HT: ("over", "under"),
    MARKET_GGHT: ("yes", "no"),
}

DISPLAY_MARKET_NAMES = {
    MARKET_1X2: "1-X-2",
    MARKET_O25: "Over 2,5 FT",
    MARKET_O15HT: "Over 1,5 PT",
    MARKET_GGHT: "GG primo tempo",
}


# =========================================================
# HELPERS GENERALI
# =========================================================
def now_rome() -> datetime:
    return datetime.now(ROME_TZ)


def iso_now() -> str:
    return now_rome().isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{now_rome().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", ".")
        if text in {"", "-", "None", "null", "nan"}:
            return default
        return float(text)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, safe_float(value, low)))


def round3(value: Any) -> float:
    return round(safe_float(value, 0.0), 3)


def round4(value: Any) -> float:
    return round(safe_float(value, 0.0), 4)


def mean(values: Iterable[float], default: float = 0.0) -> float:
    vals = [safe_float(v) for v in values]
    return sum(vals) / len(vals) if vals else default


def median(values: Iterable[float], default: float = 0.0) -> float:
    vals = [safe_float(v) for v in values if safe_float(v) > 1.0]
    return float(statistics.median(vals)) if vals else default


def weighted_mean(values: Sequence[float], weights: Sequence[float]) -> float:
    if not values:
        return 0.0
    if len(values) != len(weights):
        return mean(values)
    denominator = sum(max(safe_float(w), 0.0) for w in weights)
    if denominator <= 0:
        return mean(values)
    return sum(safe_float(v) * max(safe_float(w), 0.0) for v, w in zip(values, weights)) / denominator


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        log(f"⚠️ Lettura fallita {path}: {exc}")
        return default


def normalized_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("-", " ").split())


def fixture_datetime_rome(fixture: Mapping[str, Any]) -> Optional[datetime]:
    try:
        timestamp = fixture.get("timestamp")
        if timestamp:
            return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).astimezone(ROME_TZ)
    except Exception:
        pass

    try:
        raw = str(fixture.get("date", "")).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(ROME_TZ)
    except Exception:
        return None


def parse_iso(value: Any) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ROME_TZ)
        return parsed.astimezone(ROME_TZ)
    except Exception:
        return None


def is_blacklisted_league(name: Any) -> bool:
    text = normalized_text(name)
    return any(token in text for token in LEAGUE_BLACKLIST)


def consecutive_count(rows: Sequence[Mapping[str, Any]], key: str, expected: bool = True) -> int:
    count = 0
    for row in rows:
        if bool(row.get(key)) is expected:
            count += 1
        else:
            break
    return count


# =========================================================
# API CLIENT
# =========================================================
@dataclass
class APIClient:
    api_key: str
    min_interval: float = API_MIN_INTERVAL

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.last_call_ts = 0.0
        self.cache: Dict[str, Any] = {}

    def _throttle(self) -> None:
        elapsed = time.time() - self.last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call_ts = time.time()

    @staticmethod
    def _cache_key(path: str, params: Mapping[str, Any]) -> str:
        return f"{path}?" + "&".join(f"{key}={params[key]}" for key in sorted(params))

    def get(self, path: str, params: Mapping[str, Any], use_cache: bool = True) -> Optional[Dict[str, Any]]:
        key = self._cache_key(path, params)
        if use_cache and key in self.cache:
            cached = self.cache[key]
            return json.loads(json.dumps(cached))

        headers = {"x-apisports-key": self.api_key}
        for delay in API_RETRIES:
            if delay:
                time.sleep(delay)
            try:
                self._throttle()
                response = self.session.get(
                    f"{API_BASE_URL}/{path}",
                    headers=headers,
                    params=dict(params),
                    timeout=25,
                )
                if response.status_code != 200:
                    log(f"⚠️ API {path} HTTP {response.status_code}: {response.text[:180]}")
                    continue
                payload = response.json()
                if not isinstance(payload, dict):
                    continue
                errors = payload.get("errors") or {}
                if errors:
                    log(f"⚠️ API {path} errors: {errors}")
                    if isinstance(errors, dict) and errors.get("rateLimit"):
                        continue
                if "response" not in payload:
                    continue
                if use_cache:
                    self.cache[key] = payload
                return payload
            except Exception as exc:
                log(f"⚠️ API {path} exception: {exc}")
        return None


# =========================================================
# CATALOGO MERCATI API
# =========================================================
def fetch_bet_catalog(client: APIClient) -> Dict[str, Any]:
    payload = client.get("odds/bets", {}, use_cache=False)
    rows = payload.get("response", []) if payload else []
    catalog = {
        "updated_at": iso_now(),
        "bets": [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "normalized_name": normalized_text(row.get("name")),
            }
            for row in rows
            if isinstance(row, dict)
        ],
    }
    atomic_write_json(BET_CATALOG_FILE, catalog)
    return catalog


# =========================================================
# ESTRAZIONE QUOTE MULTI-BOOKMAKER
# =========================================================
def _contains_first_half(text: str) -> bool:
    return any(token in text for token in (
        "1st half", "first half", "half time", "halftime", "1h", "1° tempo",
    ))


def _is_btts_market(text: str) -> bool:
    return any(token in text for token in (
        "both teams score", "both teams to score", "btts", "gg",
    ))


def _parse_market_values(bet: Mapping[str, Any]) -> Dict[str, float]:
    parsed: Dict[str, float] = {}
    for item in bet.get("values", []) or []:
        value = normalized_text(item.get("value"))
        odd = safe_float(item.get("odd"), 0.0)
        if odd <= 1.0:
            continue
        parsed[value] = odd
    return parsed


def _find_odd(values: Mapping[str, float], predicates: Sequence[str]) -> float:
    """Trova una quota privilegiando la corrispondenza esatta.

    La vecchia ricerca solo per sottostringa poteva agganciare valori di
    mercati alternativi. Prima cerchiamo quindi il nome normalizzato esatto
    e usiamo la sottostringa soltanto come fallback controllato.
    """
    normalized_predicates = tuple(normalized_text(item) for item in predicates)

    for predicate in normalized_predicates:
        if predicate in values:
            return safe_float(values.get(predicate), 0.0)

    for key, odd in values.items():
        if any(predicate in key for predicate in normalized_predicates):
            return odd
    return 0.0


def _is_team_specific_goal_market(bet_name: str) -> bool:
    """Esclude totali goal riferiti a una sola squadra.

    API-Football usa denominazioni diverse a seconda del bookmaker. Non basta
    quindi cercare soltanto la stringa ``team total``.
    """
    return any(token in bet_name for token in (
        "team total",
        "home team",
        "away team",
        "home total",
        "away total",
        "total home",
        "total away",
        "home goals",
        "away goals",
        "goals home",
        "goals away",
        "home over under",
        "away over under",
    ))


def extract_bookmaker_markets(odds_payload: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Restituisce quote complete per bookmaker e mercato."""
    output: Dict[str, Dict[str, Any]] = {}

    for response_row in odds_payload.get("response", []) or []:
        for bookmaker in response_row.get("bookmakers", []) or []:
            bookmaker_id = str(bookmaker.get("id", ""))
            bookmaker_name = str(bookmaker.get("name", "N/D")).strip() or "N/D"
            book_key = bookmaker_id or bookmaker_name

            book_record = output.setdefault(book_key, {
                "bookmaker_id": bookmaker.get("id"),
                "bookmaker_name": bookmaker_name,
                "markets": {},
            })

            for bet in bookmaker.get("bets", []) or []:
                bet_id = safe_int(bet.get("id"), 0)
                bet_name = normalized_text(bet.get("name"))
                values = _parse_market_values(bet)
                first_half = _contains_first_half(bet_name)

                # 1-X-2 finale
                if bet_id == 1 or ("match winner" in bet_name and not first_half):
                    q1 = _find_odd(values, ("home", "1"))
                    qx = _find_odd(values, ("draw", "x"))
                    q2 = _find_odd(values, ("away", "2"))
                    if q1 > 1 and qx > 1 and q2 > 1:
                        book_record["markets"][MARKET_1X2] = {"1": q1, "x": qx, "2": q2}
                    continue

                # GG primo tempo
                if first_half and _is_btts_market(bet_name):
                    yes = _find_odd(values, ("yes", "si", "sì"))
                    no = _find_odd(values, ("no",))
                    if yes > 1:
                        market = {"yes": yes}
                        if no > 1:
                            market["no"] = no
                        book_record["markets"][MARKET_GGHT] = market
                    continue

                # Totali goal.
                #
                # IMPORTANTE: per l'Over/Under 2.5 FT accettiamo esclusivamente
                # il mercato principale API-Football con bet_id == 5. La vecchia
                # logica accettava qualunque mercato il cui nome contenesse
                # "goals" e poteva quindi sovrascrivere il totale partita con
                # il totale goal della sola squadra di casa/trasferta.
                if any(token in bet_name for token in ("corner", "card", "booking")):
                    continue

                team_specific = _is_team_specific_goal_market(bet_name)

                if bet_id == 5 and not first_half and not team_specific:
                    over25 = _find_odd(values, ("over 2.5", "over 2,5"))
                    under25 = _find_odd(values, ("under 2.5", "under 2,5"))
                    if over25 > 1 and under25 > 1:
                        book_record["markets"][MARKET_O25] = {
                            "over": over25,
                            "under": under25,
                        }
                    continue

                # Over 1.5 primo tempo: accetta soltanto un totale globale del
                # primo tempo, mai il totale di una singola squadra. Non
                # sovrascrive una coppia già trovata per lo stesso bookmaker.
                is_total_goal_market = any(
                    token in bet_name for token in ("total", "over under", "goals")
                )
                if (
                    first_half
                    and is_total_goal_market
                    and not team_specific
                    and MARKET_O15HT not in book_record["markets"]
                ):
                    over15 = _find_odd(values, ("over 1.5", "over 1,5"))
                    under15 = _find_odd(values, ("under 1.5", "under 1,5"))
                    if over15 > 1 and under15 > 1:
                        book_record["markets"][MARKET_O15HT] = {
                            "over": over15,
                            "under": under15,
                        }

    return output


def complete_market_books(
    bookmaker_markets: Mapping[str, Mapping[str, Any]],
    market_key: str,
) -> Dict[str, Dict[str, float]]:
    required = MARKET_SELECTIONS[market_key]
    complete: Dict[str, Dict[str, float]] = {}
    for book_key, book in bookmaker_markets.items():
        odds = (book.get("markets") or {}).get(market_key) or {}
        if market_key == MARKET_GGHT:
            # Per visualizzare il Sì è sufficiente; per l'aggio servono Sì e No.
            if safe_float(odds.get("yes")) > 1:
                complete[book_key] = {k: safe_float(v) for k, v in odds.items() if safe_float(v) > 1}
            continue
        if all(safe_float(odds.get(selection)) > 1 for selection in required):
            complete[book_key] = {selection: safe_float(odds[selection]) for selection in required}
    return complete


def market_math(odds: Mapping[str, float], selections: Sequence[str]) -> Dict[str, Any]:
    raw_probabilities = {
        selection: (1.0 / safe_float(odds.get(selection)))
        for selection in selections
        if safe_float(odds.get(selection)) > 1.0
    }
    total = sum(raw_probabilities.values())
    if total <= 0:
        return {
            "raw_probabilities": {}, "fair_probabilities": {},
            "overround": 0.0, "payout": 0.0,
            "margin_load_pp": {}, "margin_load_share": {},
        }

    fair = {selection: probability / total for selection, probability in raw_probabilities.items()}
    overround = total - 1.0
    margin_load = {
        selection: (raw_probabilities[selection] - fair[selection]) * 100.0
        for selection in fair
    }
    positive_total = sum(max(value, 0.0) for value in margin_load.values())
    margin_share = {
        selection: (max(value, 0.0) / positive_total if positive_total > 0 else 0.0)
        for selection, value in margin_load.items()
    }

    return {
        "raw_probabilities": {k: round4(v) for k, v in raw_probabilities.items()},
        "fair_probabilities": {k: round4(v) for k, v in fair.items()},
        "overround": round4(overround),
        "overround_pct": round3(overround * 100.0),
        "payout": round4(1.0 / total),
        "payout_pct": round3((1.0 / total) * 100.0),
        "margin_load_pp": {k: round3(v) for k, v in margin_load.items()},
        "margin_load_share": {k: round4(v) for k, v in margin_share.items()},
    }


def build_market_consensus(
    bookmaker_markets: Mapping[str, Mapping[str, Any]],
    market_key: str,
) -> Optional[Dict[str, Any]]:
    books = complete_market_books(bookmaker_markets, market_key)
    if not books:
        return None

    selections = MARKET_SELECTIONS[market_key]
    if market_key == MARKET_GGHT and not all("no" in odds for odds in books.values()):
        # Il mercato GG HT rimane estraibile anche senza No, ma niente aggio completo.
        selections_for_math = ("yes", "no") if any("no" in odds for odds in books.values()) else ("yes",)
    else:
        selections_for_math = selections

    consensus_odds: Dict[str, float] = {}
    dispersion: Dict[str, float] = {}
    for selection in selections:
        values = [odds.get(selection, 0.0) for odds in books.values() if safe_float(odds.get(selection)) > 1]
        if values:
            consensus_odds[selection] = round3(median(values))
            dispersion[selection] = round3(statistics.pstdev(values) if len(values) > 1 else 0.0)

    if market_key != MARKET_GGHT and not all(selection in consensus_odds for selection in selections):
        return None
    if market_key == MARKET_GGHT and "yes" not in consensus_odds:
        return None

    if market_key == MARKET_GGHT and "no" not in consensus_odds:
        math_pack = {
            "raw_probabilities": {},
            "fair_probabilities": {},
            "overround": 0.0,
            "overround_pct": 0.0,
            "payout": 0.0,
            "payout_pct": 0.0,
            "margin_load_pp": {},
            "margin_load_share": {},
            "note": "GG HT disponibile solo lato Si: aggio e OCI non calcolati.",
        }
    else:
        math_pack = market_math(consensus_odds, selections_for_math)
    return {
        "market": market_key,
        "odds": consensus_odds,
        "bookmakers_count": len(books),
        "dispersion": dispersion,
        "math": math_pack,
        "bookmakers": books,
    }


# =========================================================
# STORICO QUOTE, MOVIMENTO, AGGIO E OCI-AS
# =========================================================
def default_state() -> Dict[str, Any]:
    return {
        "version": VERSION,
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "fixtures": {},
    }


def odds_changed(previous: Mapping[str, Any], current: Mapping[str, Any], tolerance: float = 0.001) -> bool:
    keys = set(previous) | set(current)
    return any(abs(safe_float(previous.get(key)) - safe_float(current.get(key))) > tolerance for key in keys)


def _book_fair_probability(odds: Mapping[str, float], selection: str, market_key: str) -> float:
    selections = MARKET_SELECTIONS[market_key]
    if not all(safe_float(odds.get(item)) > 1 for item in selections):
        return 0.0
    return safe_float(market_math(odds, selections).get("fair_probabilities", {}).get(selection), 0.0)


def bookmaker_consensus_ratio(
    open_books: Mapping[str, Mapping[str, float]],
    current_books: Mapping[str, Mapping[str, float]],
    market_key: str,
    selection: str,
    movement_sign: int,
) -> Dict[str, Any]:
    common = sorted(set(open_books) & set(current_books))
    considered = 0
    concordant = 0
    for book_key in common:
        open_probability = _book_fair_probability(open_books[book_key], selection, market_key)
        current_probability = _book_fair_probability(current_books[book_key], selection, market_key)
        if open_probability <= 0 or current_probability <= 0:
            continue
        delta = current_probability - open_probability
        if abs(delta) < 0.001:
            continue
        considered += 1
        if (delta > 0 and movement_sign > 0) or (delta < 0 and movement_sign < 0):
            concordant += 1
    return {
        "concordant": concordant,
        "considered": considered,
        "ratio": round4(concordant / considered) if considered else 0.0,
    }


def movement_level(delta_fair_pp: float, consensus_ratio: float) -> Dict[str, str]:
    magnitude = abs(delta_fair_pp)
    if magnitude < 0.50:
        level, color = "stable", "gray"
    elif magnitude < 1.50:
        level, color = "low", "yellow"
    elif magnitude < 3.00:
        level, color = "medium", "orange"
    else:
        level, color = "strong", "red"

    # Se il movimento non è condiviso, retrocede di un livello.
    if consensus_ratio and consensus_ratio < 0.45:
        downgrade = {
            "strong": ("medium", "orange"),
            "medium": ("low", "yellow"),
            "low": ("low", "yellow"),
        }
        level, color = downgrade.get(level, (level, color))
    return {"level": level, "color": color}


def history_persistence(history: Sequence[Mapping[str, Any]], selection: str) -> float:
    if len(history) < 3:
        return 0.0
    points = history[-4:]
    directions: List[int] = []
    for previous, current in zip(points, points[1:]):
        p1 = safe_float((previous.get("math") or {}).get("fair_probabilities", {}).get(selection))
        p2 = safe_float((current.get("math") or {}).get("fair_probabilities", {}).get(selection))
        delta = p2 - p1
        if abs(delta) >= 0.001:
            directions.append(1 if delta > 0 else -1)
    if not directions:
        return 0.0
    dominant = max(directions.count(1), directions.count(-1))
    return round4(dominant / len(directions))


def calculate_oci_as(
    delta_fair_pp: float,
    consensus_ratio: float,
    bookmakers_count: int,
    history: Sequence[Mapping[str, Any]],
    selection: str,
    first_seen_at: str,
    current_at: str,
) -> Dict[str, Any]:
    if abs(delta_fair_pp) < 0.10:
        return {
            "value": 0.0,
            "absolute": 0.0,
            "direction": "stable",
            "components": {
                "magnitude": 0.0, "consensus": 0.0, "velocity": 0.0,
                "persistence": 0.0, "coverage": 0.0,
            },
        }

    start = parse_iso(first_seen_at)
    end = parse_iso(current_at)
    hours = max(((end - start).total_seconds() / 3600.0), 0.25) if start and end else 1.0
    velocity_pp_h = abs(delta_fair_pp) / hours
    persistence = history_persistence(history, selection)

    magnitude_component = min(abs(delta_fair_pp) / 4.0, 1.0) * 40.0
    consensus_component = clamp(consensus_ratio, 0.0, 1.0) * 25.0
    velocity_component = min(velocity_pp_h / 1.50, 1.0) * 10.0
    persistence_component = persistence * 15.0
    coverage_component = min(bookmakers_count / 8.0, 1.0) * 10.0

    absolute = clamp(
        magnitude_component + consensus_component + velocity_component +
        persistence_component + coverage_component,
        0.0,
        100.0,
    )
    sign = 1 if delta_fair_pp > 0 else -1
    return {
        "value": round3(absolute * sign),
        "absolute": round3(absolute),
        "direction": "toward" if sign > 0 else "away",
        "velocity_pp_per_hour": round3(velocity_pp_h),
        "components": {
            "magnitude": round3(magnitude_component),
            "consensus": round3(consensus_component),
            "velocity": round3(velocity_component),
            "persistence": round3(persistence_component),
            "coverage": round3(coverage_component),
        },
    }


def aggio_status(market_key: str, overround_pct: float) -> str:
    if overround_pct <= 0:
        return "unavailable"
    if market_key == MARKET_1X2:
        if overround_pct <= 5.0:
            return "low"
        if overround_pct <= 8.0:
            return "normal"
        return "high"
    if overround_pct <= 4.0:
        return "low"
    if overround_pct <= 7.0:
        return "normal"
    return "high"


def update_market_state(
    market_state: MutableMapping[str, Any],
    current: Mapping[str, Any],
    timestamp: str,
) -> Dict[str, Any]:
    if not market_state:
        market_state.update({
            "first_seen_at": timestamp,
            "last_seen_at": timestamp,
            "open": {
                "odds": dict(current.get("odds", {})),
                "math": dict(current.get("math", {})),
                "bookmakers_count": current.get("bookmakers_count", 0),
            },
            "current": {},
            "bookmakers_open": dict(current.get("bookmakers", {})),
            "bookmakers_current": {},
            "history": [],
        })

    market_state["last_seen_at"] = timestamp
    market_state["current"] = {
        "odds": dict(current.get("odds", {})),
        "math": dict(current.get("math", {})),
        "bookmakers_count": current.get("bookmakers_count", 0),
        "dispersion": dict(current.get("dispersion", {})),
    }
    market_state["bookmakers_current"] = dict(current.get("bookmakers", {}))

    point = {
        "ts": timestamp,
        "odds": dict(current.get("odds", {})),
        "math": dict(current.get("math", {})),
        "bookmakers_count": current.get("bookmakers_count", 0),
    }
    history = market_state.setdefault("history", [])
    if not history or odds_changed(history[-1].get("odds", {}), point["odds"]):
        history.append(point)
    else:
        history[-1] = point
    market_state["history"] = history[-MAX_HISTORY_POINTS:]

    return analyze_market_state(market_state, current.get("market", ""))


def analyze_market_state(market_state: Mapping[str, Any], market_key: str) -> Dict[str, Any]:
    open_pack = market_state.get("open", {}) or {}
    current_pack = market_state.get("current", {}) or {}
    open_odds = open_pack.get("odds", {}) or {}
    current_odds = current_pack.get("odds", {}) or {}
    open_math = open_pack.get("math", {}) or {}
    current_math = current_pack.get("math", {}) or {}
    open_fair = open_math.get("fair_probabilities", {}) or {}
    current_fair = current_math.get("fair_probabilities", {}) or {}
    open_margin = open_math.get("margin_load_pp", {}) or {}
    current_margin = current_math.get("margin_load_pp", {}) or {}

    selection_analysis: Dict[str, Any] = {}
    for selection in MARKET_SELECTIONS[market_key]:
        if selection not in current_odds:
            continue
        delta_odd = safe_float(current_odds.get(selection)) - safe_float(open_odds.get(selection))
        delta_fair_pp = (
            safe_float(current_fair.get(selection)) - safe_float(open_fair.get(selection))
        ) * 100.0
        movement_sign = 1 if delta_fair_pp > 0 else -1 if delta_fair_pp < 0 else 0
        consensus = bookmaker_consensus_ratio(
            market_state.get("bookmakers_open", {}) or {},
            market_state.get("bookmakers_current", {}) or {},
            market_key,
            selection,
            movement_sign,
        )
        level = movement_level(delta_fair_pp, safe_float(consensus.get("ratio")))
        oci = calculate_oci_as(
            delta_fair_pp=delta_fair_pp,
            consensus_ratio=safe_float(consensus.get("ratio")),
            bookmakers_count=safe_int(current_pack.get("bookmakers_count")),
            history=market_state.get("history", []) or [],
            selection=selection,
            first_seen_at=str(market_state.get("first_seen_at", "")),
            current_at=str(market_state.get("last_seen_at", "")),
        )
        selection_analysis[selection] = {
            "open_odd": round3(open_odds.get(selection)),
            "current_odd": round3(current_odds.get(selection)),
            "delta_odd": round3(delta_odd),
            "open_fair_probability": round4(open_fair.get(selection)),
            "current_fair_probability": round4(current_fair.get(selection)),
            "delta_fair_pp": round3(delta_fair_pp),
            "movement": level,
            "bookmaker_consensus": consensus,
            "oci_as": oci,
            "margin_load_open_pp": round3(open_margin.get(selection)),
            "margin_load_current_pp": round3(current_margin.get(selection)),
            "margin_load_delta_pp": round3(
                safe_float(current_margin.get(selection)) - safe_float(open_margin.get(selection))
            ),
        }

    # Direzione book: selezione con OCI positivo più alto.
    positive = sorted(
        (
            (selection, safe_float(data.get("oci_as", {}).get("value")))
            for selection, data in selection_analysis.items()
            if safe_float(data.get("oci_as", {}).get("value")) > 0
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    book_pick = positive[0][0] if positive and positive[0][1] >= 22 else None
    book_score = positive[0][1] if book_pick else 0.0

    if book_score >= 70:
        book_strength, book_color = "strong", "red"
    elif book_score >= 45:
        book_strength, book_color = "medium", "orange"
    elif book_score >= 22:
        book_strength, book_color = "low", "yellow"
    else:
        book_strength, book_color = "none", "gray"

    protection_candidates = sorted(
        (
            (selection, safe_float(data.get("margin_load_delta_pp")))
            for selection, data in selection_analysis.items()
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    protection_pick = protection_candidates[0][0] if protection_candidates and protection_candidates[0][1] > 0.02 else None

    current_overround_pct = safe_float(current_math.get("overround_pct"))
    open_overround_pct = safe_float(open_math.get("overround_pct"))

    return {
        "market": market_key,
        "open": open_pack,
        "current": current_pack,
        "selections": selection_analysis,
        "aggio": {
            "open_pct": round3(open_overround_pct),
            "current_pct": round3(current_overround_pct),
            "delta_pp": round3(current_overround_pct - open_overround_pct),
            "status": aggio_status(market_key, current_overround_pct),
            "payout_pct": round3(current_math.get("payout_pct")),
        },
        "book": {
            "pick": book_pick,
            "score": round3(book_score),
            "strength": book_strength,
            "color": book_color,
            "marker_shape": "square" if book_pick else "none",
            "protection_pick": protection_pick,
        },
    }


# =========================================================
# DATI PARTITE RECENTI E SCONTRI DIRETTI
# =========================================================
def _match_row(fixture_row: Mapping[str, Any], team_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    fixture = fixture_row.get("fixture", {}) or {}
    status = fixture.get("status", {}) or {}
    if status.get("short") not in {"FT", "AET", "PEN"}:
        return None

    teams = fixture_row.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    goals = fixture_row.get("goals", {}) or {}
    score = fixture_row.get("score", {}) or {}
    halftime = score.get("halftime", {}) or {}

    gh = safe_int(goals.get("home"), 0)
    ga = safe_int(goals.get("away"), 0)
    hth = safe_int(halftime.get("home"), 0)
    hta = safe_int(halftime.get("away"), 0)
    dt = fixture_datetime_rome(fixture)

    row: Dict[str, Any] = {
        "fixture_id": fixture.get("id"),
        "date": dt.date().isoformat() if dt else str(fixture.get("date", ""))[:10],
        "league_id": (fixture_row.get("league", {}) or {}).get("id"),
        "league": (fixture_row.get("league", {}) or {}).get("name"),
        "home_id": home.get("id"),
        "away_id": away.get("id"),
        "home": home.get("name"),
        "away": away.get("name"),
        "home_goals": gh,
        "away_goals": ga,
        "home_ht_goals": hth,
        "away_ht_goals": hta,
        "total_ft": gh + ga,
        "total_ht": hth + hta,
        "over25_ft": gh + ga >= 3,
        "over15_ft": gh + ga >= 2,
        "over15_ht": hth + hta >= 2,
        "gg_ft": gh > 0 and ga > 0,
        "gg_ht": hth > 0 and hta > 0,
    }

    if team_id is not None:
        is_home = str(home.get("id")) == str(team_id)
        is_away = str(away.get("id")) == str(team_id)
        if not is_home and not is_away:
            return None
        row.update({
            "side": "home" if is_home else "away",
            "team_goals_ft": gh if is_home else ga,
            "team_conceded_ft": ga if is_home else gh,
            "team_goals_ht": hth if is_home else hta,
            "team_conceded_ht": hta if is_home else hth,
            "team_goals_2h": max((gh - hth) if is_home else (ga - hta), 0),
            "team_conceded_2h": max((ga - hta) if is_home else (gh - hth), 0),
            "result_points": 3 if ((gh > ga and is_home) or (ga > gh and is_away)) else 1 if gh == ga else 0,
        })
    return row


def get_team_recent_matches(client: APIClient, team_id: int, last: int = MAX_TEAM_MATCHES) -> List[Dict[str, Any]]:
    payload = client.get("fixtures", {"team": team_id, "last": last, "status": "FT"})
    rows: List[Dict[str, Any]] = []
    for fixture_row in (payload or {}).get("response", []) or []:
        if is_blacklisted_league((fixture_row.get("league", {}) or {}).get("name")):
            continue
        parsed = _match_row(fixture_row, team_id=team_id)
        if parsed:
            rows.append(parsed)
    rows.sort(key=lambda row: row.get("date", ""), reverse=True)
    return rows


def get_h2h_matches(client: APIClient, home_id: int, away_id: int, last: int = MAX_H2H_MATCHES) -> List[Dict[str, Any]]:
    payload = client.get("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": last})
    rows: List[Dict[str, Any]] = []
    for fixture_row in (payload or {}).get("response", []) or []:
        if is_blacklisted_league((fixture_row.get("league", {}) or {}).get("name")):
            continue
        parsed = _match_row(fixture_row)
        if parsed:
            rows.append(parsed)
    rows.sort(key=lambda row: row.get("date", ""), reverse=True)
    return rows[:last]


def get_league_baseline(client: APIClient, league_id: int, season: int, last: int = MAX_LEAGUE_MATCHES) -> Dict[str, Any]:
    payload = client.get("fixtures", {"league": league_id, "season": season, "last": last, "status": "FT"})
    rows = [
        parsed
        for fixture_row in (payload or {}).get("response", []) or []
        if (parsed := _match_row(fixture_row)) is not None
    ]
    if not rows:
        return {
            "sample": 0,
            "avg_home_goals": 1.45,
            "avg_away_goals": 1.15,
            "avg_total_ft": 2.60,
            "avg_home_ht": 0.68,
            "avg_away_ht": 0.52,
            "avg_total_ht": 1.20,
            "over25_rate": 0.50,
            "over15ht_rate": 0.30,
            "gg_ht_rate": 0.12,
            "draw_rate": 0.27,
            "source": "fallback",
        }

    return {
        "sample": len(rows),
        "avg_home_goals": round3(mean(row["home_goals"] for row in rows)),
        "avg_away_goals": round3(mean(row["away_goals"] for row in rows)),
        "avg_total_ft": round3(mean(row["total_ft"] for row in rows)),
        "avg_home_ht": round3(mean(row["home_ht_goals"] for row in rows)),
        "avg_away_ht": round3(mean(row["away_ht_goals"] for row in rows)),
        "avg_total_ht": round3(mean(row["total_ht"] for row in rows)),
        "over25_rate": round4(mean(1.0 if row["over25_ft"] else 0.0 for row in rows)),
        "over15ht_rate": round4(mean(1.0 if row["over15_ht"] else 0.0 for row in rows)),
        "gg_ht_rate": round4(mean(1.0 if row["gg_ht"] else 0.0 for row in rows)),
        "draw_rate": round4(mean(1.0 if row["home_goals"] == row["away_goals"] else 0.0 for row in rows)),
        "source": "api_recent_league",
    }


def recency_weights(rows: Sequence[Mapping[str, Any]]) -> List[float]:
    weights: List[float] = []
    today = now_rome().date()
    for index, row in enumerate(rows):
        try:
            match_date = date.fromisoformat(str(row.get("date")))
            age = max((today - match_date).days, 0)
        except Exception:
            age = index * 7
        order_weight = 1.30 if index < 2 else 1.12 if index < 5 else 1.0
        age_weight = 1.0 if age <= 14 else 0.85 if age <= 30 else 0.65 if age <= 60 else 0.45
        weights.append(order_weight * age_weight)
    return weights


def summarize_team_matches(rows: Sequence[Mapping[str, Any]], expected_side: str) -> Dict[str, Any]:
    all_rows = list(rows)
    side_rows = [row for row in all_rows if row.get("side") == expected_side]
    if len(side_rows) >= 5:
        context_weight = 0.62
    elif len(side_rows) == 4:
        context_weight = 0.55
    elif len(side_rows) == 3:
        context_weight = 0.46
    elif len(side_rows) == 2:
        context_weight = 0.33
    elif len(side_rows) == 1:
        context_weight = 0.20
    else:
        context_weight = 0.0

    def summarize(sample: Sequence[Mapping[str, Any]]) -> Dict[str, float]:
        if not sample:
            return defaultdict(float)
        weights = recency_weights(sample)

        def wavg(key: str) -> float:
            return weighted_mean([safe_float(row.get(key)) for row in sample], weights)

        return {
            "goals_scored_ft": wavg("team_goals_ft"),
            "goals_conceded_ft": wavg("team_conceded_ft"),
            "goals_scored_ht": wavg("team_goals_ht"),
            "goals_conceded_ht": wavg("team_conceded_ht"),
            "goals_scored_2h": wavg("team_goals_2h"),
            "goals_conceded_2h": wavg("team_conceded_2h"),
            "total_ft": wavg("total_ft"),
            "total_ht": wavg("total_ht"),
            "points_per_game": wavg("result_points"),
            "over25_rate": mean(1.0 if row.get("over25_ft") else 0.0 for row in sample),
            "over15ht_rate": mean(1.0 if row.get("over15_ht") else 0.0 for row in sample),
            "gg_ht_rate": mean(1.0 if row.get("gg_ht") else 0.0 for row in sample),
            "scored_ht_rate": mean(1.0 if safe_int(row.get("team_goals_ht")) > 0 else 0.0 for row in sample),
            "conceded_ht_rate": mean(1.0 if safe_int(row.get("team_conceded_ht")) > 0 else 0.0 for row in sample),
            "clean_sheet_rate": mean(1.0 if safe_int(row.get("team_conceded_ft")) == 0 else 0.0 for row in sample),
        }

    all_summary = summarize(all_rows)
    side_summary = summarize(side_rows)

    keys = set(all_summary) | set(side_summary)
    blended = {
        key: round3(
            safe_float(all_summary.get(key)) * (1.0 - context_weight) +
            safe_float(side_summary.get(key, all_summary.get(key))) * context_weight
        )
        for key in keys
    }

    recent5 = all_rows[:5]
    baseline_rows = all_rows[5:] if len(all_rows) > 7 else all_rows
    recent_scoring = mean(safe_float(row.get("team_goals_ft")) for row in recent5)
    baseline_scoring = mean(safe_float(row.get("team_goals_ft")) for row in baseline_rows)
    scoring_gap = recent_scoring - baseline_scoring

    # Piccolo indice di regressione basato solo sui goal, non su xG.
    # Non applica la fallacia del ritardo: corregge al massimo del 6%.
    regression_factor = clamp(1.0 - scoring_gap * 0.035, 0.94, 1.06)

    return {
        "sample_all": len(all_rows),
        "sample_context": len(side_rows),
        "context_weight": round3(context_weight),
        "expected_side": expected_side,
        "metrics": blended,
        "all_metrics": {key: round3(value) for key, value in all_summary.items()},
        "context_metrics": {key: round3(value) for key, value in side_summary.items()},
        "form": {
            "recent5_points_per_game": round3(mean(safe_float(row.get("result_points")) for row in recent5)),
            "recent5_goals_scored": round3(recent_scoring),
            "baseline_goals_scored": round3(baseline_scoring),
            "scoring_gap": round3(scoring_gap),
            "regression_factor_goals_only": round3(regression_factor),
            "method": "goals_only_no_xg",
        },
        "streaks": {
            "over25_ft": consecutive_count(all_rows, "over25_ft"),
            "over15_ht": consecutive_count(all_rows, "over15_ht"),
            "gg_ht": consecutive_count(all_rows, "gg_ht"),
            "no_over25_ft": consecutive_count(all_rows, "over25_ft", expected=False),
            "no_over15_ht": consecutive_count(all_rows, "over15_ht", expected=False),
            "no_gg_ht": consecutive_count(all_rows, "gg_ht", expected=False),
        },
        "last_matches": all_rows,
    }


def summarize_h2h(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "sample": 0,
            "avg_total_ft": 0.0,
            "avg_total_ht": 0.0,
            "over25_rate": 0.0,
            "over15ht_rate": 0.0,
            "gg_ht_rate": 0.0,
            "weight_cap": 0.05,
            "matches": [],
        }
    weights = [1.0, 0.82, 0.66, 0.52, 0.40][:len(rows)]
    return {
        "sample": len(rows),
        "avg_total_ft": round3(weighted_mean([row["total_ft"] for row in rows], weights)),
        "avg_total_ht": round3(weighted_mean([row["total_ht"] for row in rows], weights)),
        "over25_rate": round4(weighted_mean([1.0 if row["over25_ft"] else 0.0 for row in rows], weights)),
        "over15ht_rate": round4(weighted_mean([1.0 if row["over15_ht"] else 0.0 for row in rows], weights)),
        "gg_ht_rate": round4(weighted_mean([1.0 if row["gg_ht"] else 0.0 for row in rows], weights)),
        "weight_cap": 0.05,
        "matches": list(rows),
    }


# =========================================================
# LAMBDA E PROBABILITA' ARABSNIPER
# =========================================================
def shrink_rate(value: float, sample: int, baseline: float, full_sample: int = 8) -> float:
    weight = clamp(sample / full_sample, 0.0, 1.0)
    return value * weight + baseline * (1.0 - weight)


def build_lambdas(
    home_profile: Mapping[str, Any],
    away_profile: Mapping[str, Any],
    league: Mapping[str, Any],
    h2h: Mapping[str, Any],
) -> Dict[str, Any]:
    hm = home_profile.get("metrics", {}) or {}
    am = away_profile.get("metrics", {}) or {}

    league_home = max(safe_float(league.get("avg_home_goals"), 1.45), 0.60)
    league_away = max(safe_float(league.get("avg_away_goals"), 1.15), 0.50)
    league_home_ht = max(safe_float(league.get("avg_home_ht"), 0.68), 0.25)
    league_away_ht = max(safe_float(league.get("avg_away_ht"), 0.52), 0.20)

    home_scored = shrink_rate(
        safe_float(hm.get("goals_scored_ft"), league_home),
        safe_int(home_profile.get("sample_context")),
        league_home,
    )
    away_conceded = shrink_rate(
        safe_float(am.get("goals_conceded_ft"), league_home),
        safe_int(away_profile.get("sample_context")),
        league_home,
    )
    away_scored = shrink_rate(
        safe_float(am.get("goals_scored_ft"), league_away),
        safe_int(away_profile.get("sample_context")),
        league_away,
    )
    home_conceded = shrink_rate(
        safe_float(hm.get("goals_conceded_ft"), league_away),
        safe_int(home_profile.get("sample_context")),
        league_away,
    )

    home_attack_strength = home_scored / league_home
    away_defence_weakness = away_conceded / league_home
    away_attack_strength = away_scored / league_away
    home_defence_weakness = home_conceded / league_away

    lambda_home_ft = league_home * home_attack_strength * away_defence_weakness
    lambda_away_ft = league_away * away_attack_strength * home_defence_weakness

    home_scored_ht = shrink_rate(
        safe_float(hm.get("goals_scored_ht"), league_home_ht),
        safe_int(home_profile.get("sample_context")),
        league_home_ht,
    )
    away_conceded_ht = shrink_rate(
        safe_float(am.get("goals_conceded_ht"), league_home_ht),
        safe_int(away_profile.get("sample_context")),
        league_home_ht,
    )
    away_scored_ht = shrink_rate(
        safe_float(am.get("goals_scored_ht"), league_away_ht),
        safe_int(away_profile.get("sample_context")),
        league_away_ht,
    )
    home_conceded_ht = shrink_rate(
        safe_float(hm.get("goals_conceded_ht"), league_away_ht),
        safe_int(home_profile.get("sample_context")),
        league_away_ht,
    )

    lambda_home_ht = league_home_ht * (home_scored_ht / league_home_ht) * (away_conceded_ht / league_home_ht)
    lambda_away_ht = league_away_ht * (away_scored_ht / league_away_ht) * (home_conceded_ht / league_away_ht)

    # Forma / regressione contenuta.
    lambda_home_ft *= safe_float(home_profile.get("form", {}).get("regression_factor_goals_only"), 1.0)
    lambda_away_ft *= safe_float(away_profile.get("form", {}).get("regression_factor_goals_only"), 1.0)

    home_form_ppg = safe_float(home_profile.get("form", {}).get("recent5_points_per_game"), 1.3)
    away_form_ppg = safe_float(away_profile.get("form", {}).get("recent5_points_per_game"), 1.3)
    form_gap = clamp((home_form_ppg - away_form_ppg) / 3.0, -0.12, 0.12)
    lambda_home_ft *= 1.0 + form_gap * 0.18
    lambda_away_ft *= 1.0 - form_gap * 0.18

    # H2H massimo 5%: compatibilita', non evento "dovuto".
    if safe_int(h2h.get("sample")) >= 2:
        league_total = max(safe_float(league.get("avg_total_ft"), 2.60), 1.50)
        h2h_total = safe_float(h2h.get("avg_total_ft"), league_total)
        h2h_factor = clamp(h2h_total / league_total, 0.95, 1.05)
        lambda_home_ft *= h2h_factor
        lambda_away_ft *= h2h_factor

        league_total_ht = max(safe_float(league.get("avg_total_ht"), 1.20), 0.60)
        h2h_total_ht = safe_float(h2h.get("avg_total_ht"), league_total_ht)
        h2h_ht_factor = clamp(h2h_total_ht / league_total_ht, 0.95, 1.05)
        lambda_home_ht *= h2h_ht_factor
        lambda_away_ht *= h2h_ht_factor

    lambda_home_ft = clamp(lambda_home_ft, 0.15, 3.60)
    lambda_away_ft = clamp(lambda_away_ft, 0.10, 3.20)
    lambda_home_ht = clamp(lambda_home_ht, 0.03, 1.90)
    lambda_away_ht = clamp(lambda_away_ht, 0.02, 1.70)

    return {
        "home_ft": round3(lambda_home_ft),
        "away_ft": round3(lambda_away_ft),
        "total_ft": round3(lambda_home_ft + lambda_away_ft),
        "home_ht": round3(lambda_home_ht),
        "away_ht": round3(lambda_away_ht),
        "total_ht": round3(lambda_home_ht + lambda_away_ht),
        "components": {
            "home_attack_strength": round3(home_attack_strength),
            "away_defence_weakness": round3(away_defence_weakness),
            "away_attack_strength": round3(away_attack_strength),
            "home_defence_weakness": round3(home_defence_weakness),
            "form_gap": round3(form_gap),
        },
    }


def poisson_pmf(goals: int, lam: float) -> float:
    try:
        return math.exp(-lam) * (lam ** goals) / math.factorial(goals)
    except Exception:
        return 0.0


def score_matrix(lambda_home: float, lambda_away: float, max_goals: int = 9) -> List[List[float]]:
    return [
        [poisson_pmf(home_goals, lambda_home) * poisson_pmf(away_goals, lambda_away)
         for away_goals in range(max_goals + 1)]
        for home_goals in range(max_goals + 1)
    ]


def probabilities_from_lambdas(lambdas: Mapping[str, Any]) -> Dict[str, Any]:
    matrix_ft = score_matrix(safe_float(lambdas.get("home_ft")), safe_float(lambdas.get("away_ft")))
    matrix_ht = score_matrix(safe_float(lambdas.get("home_ht")), safe_float(lambdas.get("away_ht")), max_goals=6)

    p1 = px = p2 = over25 = over15ft = 0.0
    for home_goals, row in enumerate(matrix_ft):
        for away_goals, probability in enumerate(row):
            if home_goals > away_goals:
                p1 += probability
            elif home_goals == away_goals:
                px += probability
            else:
                p2 += probability
            if home_goals + away_goals >= 3:
                over25 += probability
            if home_goals + away_goals >= 2:
                over15ft += probability

    over15ht = 0.0
    gg_ht = 0.0
    for home_goals, row in enumerate(matrix_ht):
        for away_goals, probability in enumerate(row):
            if home_goals + away_goals >= 2:
                over15ht += probability
            if home_goals >= 1 and away_goals >= 1:
                gg_ht += probability

    total_1x2 = p1 + px + p2
    if total_1x2 > 0:
        p1, px, p2 = p1 / total_1x2, px / total_1x2, p2 / total_1x2

    return {
        MARKET_1X2: {"1": round4(p1), "x": round4(px), "2": round4(p2)},
        MARKET_O25: {"over": round4(over25), "under": round4(1.0 - over25)},
        MARKET_O15HT: {"over": round4(over15ht), "under": round4(1.0 - over15ht)},
        MARKET_GGHT: {"yes": round4(gg_ht), "no": round4(1.0 - gg_ht)},
        "over15_ft_extra": {"over": round4(over15ft), "under": round4(1.0 - over15ft)},
    }


def model_reliability(home_profile: Mapping[str, Any], away_profile: Mapping[str, Any], league: Mapping[str, Any]) -> float:
    home_sample = min(safe_int(home_profile.get("sample_all")) / 12.0, 1.0)
    away_sample = min(safe_int(away_profile.get("sample_all")) / 12.0, 1.0)
    home_context = min(safe_int(home_profile.get("sample_context")) / 5.0, 1.0)
    away_context = min(safe_int(away_profile.get("sample_context")) / 5.0, 1.0)
    league_sample = min(safe_int(league.get("sample")) / 40.0, 1.0)
    reliability = (
        (home_sample + away_sample) * 0.20 +
        (home_context + away_context) * 0.20 +
        league_sample * 0.20
    )
    return round3(clamp(reliability, 0.0, 1.0) * 100.0)


def model_strength(
    market_key: str,
    pick: str,
    probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    reliability_pct: float,
) -> Dict[str, Any]:
    probability = safe_float(probabilities.get(pick))
    sorted_probabilities = sorted((safe_float(value) for value in probabilities.values()), reverse=True)
    gap = sorted_probabilities[0] - sorted_probabilities[1] if len(sorted_probabilities) > 1 else probability

    market_probability = 0.0
    if market_analysis:
        market_probability = safe_float(
            (market_analysis.get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}).get(pick)
        )
    edge_pp = (probability - market_probability) * 100.0 if market_probability > 0 else 0.0

    score = 0.0
    if market_key == MARKET_1X2:
        score += min(probability / 0.65, 1.0) * 45
        score += min(gap / 0.18, 1.0) * 25
    else:
        certainty = abs(probability - 0.5) * 2.0
        score += min(certainty, 1.0) * 45
        score += min(abs(edge_pp) / 8.0, 1.0) * 25 if market_probability else 8
    score += clamp(reliability_pct / 100.0, 0.0, 1.0) * 30

    if score >= 72:
        strength, color = "strong", "red"
    elif score >= 52:
        strength, color = "medium", "orange"
    else:
        strength, color = "low", "yellow"

    return {
        "probability": round4(probability),
        "probability_pct": round3(probability * 100.0),
        "market_fair_probability": round4(market_probability),
        "market_fair_probability_pct": round3(market_probability * 100.0),
        "edge_pp": round3(edge_pp),
        "score": round3(score),
        "strength": strength,
        "color": color,
        "marker_shape": "circle",
        "probability_gap": round4(gap),
    }


def build_market_markers(
    market_key: str,
    probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    reliability_pct: float,
) -> Dict[str, Any]:
    # Per GG primo tempo il prodotto seguito e' il lato Si.
    # Non cerchiamo automaticamente il No solo perche' matematicamente piu' frequente.
    if market_key == MARKET_GGHT:
        yes_probability = safe_float(probabilities.get("yes"))
        yes_odd = safe_float(
            ((market_analysis or {}).get("current", {}) or {}).get("odds", {}).get("yes")
        )
        raw_implied = (1.0 / yes_odd) if yes_odd > 1 else 0.0
        yes_edge = yes_probability - raw_implied if raw_implied > 0 else 0.0
        model_pick = "yes" if (yes_edge >= 0.0 and yes_probability >= 0.12) else None
        if model_pick:
            # Pack dedicato: il confronto e' contro la quota singola, poiche' il No
            # puo' non essere coperto dall'API per questo mercato.
            score = min(max(yes_probability * 100.0, 0.0), 45.0)
            score += min(max(yes_edge * 100.0, 0.0) / 8.0, 1.0) * 30.0
            score += clamp(reliability_pct / 100.0, 0.0, 1.0) * 25.0
            if score >= 70:
                strength, color = "strong", "red"
            elif score >= 50:
                strength, color = "medium", "orange"
            else:
                strength, color = "low", "yellow"
            model_pack = {
                "probability": round4(yes_probability),
                "probability_pct": round3(yes_probability * 100.0),
                "market_fair_probability": round4(raw_implied),
                "market_fair_probability_pct": round3(raw_implied * 100.0),
                "edge_pp": round3(yes_edge * 100.0),
                "score": round3(score),
                "strength": strength,
                "color": color,
                "marker_shape": "circle",
                "probability_gap": 0.0,
            }
        else:
            model_pack = {
                "probability": round4(yes_probability),
                "probability_pct": round3(yes_probability * 100.0),
                "market_fair_probability": round4(raw_implied),
                "market_fair_probability_pct": round3(raw_implied * 100.0),
                "edge_pp": round3(yes_edge * 100.0),
                "score": 0.0,
                "strength": "none",
                "color": "none",
                "marker_shape": "none",
                "probability_gap": 0.0,
            }
    else:
        model_pick = max(probabilities, key=lambda key: safe_float(probabilities[key]))
        model_pack = model_strength(market_key, model_pick, probabilities, market_analysis, reliability_pct)

    book_pack = (market_analysis or {}).get("book", {}) or {}
    book_pick = book_pack.get("pick")

    selections: Dict[str, Any] = {}
    for selection in MARKET_SELECTIONS[market_key]:
        model_here = selection == model_pick
        book_here = selection == book_pick
        if model_here and book_here:
            shape = "circle_square"
            agreement = True
            # Il colore congiunto segue la conferma più prudente.
            colors = {"yellow": 1, "orange": 2, "red": 3}
            reverse = {1: "yellow", 2: "orange", 3: "red"}
            combined_color = reverse[min(
                colors.get(model_pack.get("color"), 1),
                colors.get(book_pack.get("color"), 1),
            )]
        elif model_here:
            shape = "circle"
            agreement = False
            combined_color = model_pack.get("color", "yellow")
        elif book_here:
            shape = "square"
            agreement = False
            combined_color = book_pack.get("color", "yellow")
        else:
            shape = "none"
            agreement = False
            combined_color = "none"

        selections[selection] = {
            "shape": shape,
            "color": combined_color,
            "model": model_here,
            "book": book_here,
            "agreement": agreement,
        }

    if model_pick and book_pick and model_pick == book_pick:
        relation = "confirmed"
    elif model_pick and not book_pick:
        relation = "model_only"
    elif book_pick and model_pick != book_pick:
        relation = "contrast"
    else:
        relation = "neutral"

    return {
        "model": {"pick": model_pick, **model_pack},
        "book": book_pack,
        "relation": relation,
        "selections": selections,
    }


# =========================================================
# COSTRUZIONE OUTPUT PARTITA
# =========================================================
def fixture_meta(fixture_row: Mapping[str, Any]) -> Dict[str, Any]:
    fixture = fixture_row.get("fixture", {}) or {}
    league = fixture_row.get("league", {}) or {}
    teams = fixture_row.get("teams", {}) or {}
    dt = fixture_datetime_rome(fixture)
    return {
        "fixture_id": str(fixture.get("id")),
        "kickoff": dt.isoformat(timespec="seconds") if dt else fixture.get("date"),
        "date": dt.date().isoformat() if dt else str(fixture.get("date", ""))[:10],
        "time": dt.strftime("%H:%M") if dt else str(fixture.get("date", ""))[11:16],
        "league_id": league.get("id"),
        "league": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home_id": (teams.get("home", {}) or {}).get("id"),
        "home": (teams.get("home", {}) or {}).get("name"),
        "away_id": (teams.get("away", {}) or {}).get("id"),
        "away": (teams.get("away", {}) or {}).get("name"),
        "match": f"{(teams.get('home', {}) or {}).get('name', 'N/D')} - {(teams.get('away', {}) or {}).get('name', 'N/D')}",
    }


def simplified_market_display(
    market_key: str,
    market_analysis: Optional[Mapping[str, Any]],
    markers: Mapping[str, Any],
    probabilities: Mapping[str, float],
) -> Dict[str, Any]:
    current_odds = ((market_analysis or {}).get("current", {}) or {}).get("odds", {}) or {}
    open_odds = ((market_analysis or {}).get("open", {}) or {}).get("odds", {}) or {}
    selection_rows = ((market_analysis or {}).get("selections", {}) or {})

    selections = {}
    for selection in MARKET_SELECTIONS[market_key]:
        movement = selection_rows.get(selection, {}) or {}
        selections[selection] = {
            "open_odd": round3(open_odds.get(selection)),
            "current_odd": round3(current_odds.get(selection)),
            "model_probability_pct": round3(safe_float(probabilities.get(selection)) * 100.0),
            "delta_odd": round3(movement.get("delta_odd")),
            "delta_fair_pp": round3(movement.get("delta_fair_pp")),
            "movement_color": (movement.get("movement", {}) or {}).get("color", "gray"),
            "movement_level": (movement.get("movement", {}) or {}).get("level", "stable"),
            "oci_as": round3((movement.get("oci_as", {}) or {}).get("value")),
            "marker": (markers.get("selections", {}) or {}).get(selection, {}),
        }

    return {
        "name": DISPLAY_MARKET_NAMES[market_key],
        "selections": selections,
        "model_pick": (markers.get("model", {}) or {}).get("pick"),
        "book_pick": (markers.get("book", {}) or {}).get("pick"),
        "relation": markers.get("relation"),
        "aggio_pct": round3(((market_analysis or {}).get("aggio", {}) or {}).get("current_pct")),
        "aggio_status": ((market_analysis or {}).get("aggio", {}) or {}).get("status"),
        "protection_pick": ((market_analysis or {}).get("book", {}) or {}).get("protection_pick"),
    }


def build_fixture_output(
    client: APIClient,
    fixture_row: Mapping[str, Any],
    state: MutableMapping[str, Any],
    league_cache: MutableMapping[str, Any],
) -> Optional[Dict[str, Any]]:
    meta = fixture_meta(fixture_row)
    fixture_id = meta["fixture_id"]

    odds_payload = client.get("odds", {"fixture": fixture_id}, use_cache=False)
    if not odds_payload or not odds_payload.get("response"):
        return None

    bookmaker_markets = extract_bookmaker_markets(odds_payload)
    consensuses = {
        market_key: consensus
        for market_key in MARKET_SELECTIONS
        if (consensus := build_market_consensus(bookmaker_markets, market_key)) is not None
    }
    if MARKET_1X2 not in consensuses:
        return None

    timestamp = iso_now()
    fixture_state = state.setdefault("fixtures", {}).setdefault(fixture_id, {
        "meta": meta,
        "markets": {},
    })
    fixture_state["meta"] = meta

    market_analyses: Dict[str, Any] = {}
    for market_key, consensus in consensuses.items():
        market_state = fixture_state.setdefault("markets", {}).setdefault(market_key, {})
        market_analyses[market_key] = update_market_state(market_state, consensus, timestamp)

    home_matches = get_team_recent_matches(client, safe_int(meta["home_id"]))
    away_matches = get_team_recent_matches(client, safe_int(meta["away_id"]))
    if len(home_matches) < 3 or len(away_matches) < 3:
        return None

    home_profile = summarize_team_matches(home_matches, expected_side="home")
    away_profile = summarize_team_matches(away_matches, expected_side="away")

    league_key = f"{meta['league_id']}::{meta['season']}"
    if league_key not in league_cache:
        league_cache[league_key] = get_league_baseline(
            client,
            safe_int(meta["league_id"]),
            safe_int(meta["season"]),
        )
    league_profile = league_cache[league_key]

    h2h_rows = get_h2h_matches(client, safe_int(meta["home_id"]), safe_int(meta["away_id"]))
    h2h_profile = summarize_h2h(h2h_rows)

    lambdas = build_lambdas(home_profile, away_profile, league_profile, h2h_profile)
    model_probabilities = probabilities_from_lambdas(lambdas)
    reliability = model_reliability(home_profile, away_profile, league_profile)

    markers: Dict[str, Any] = {}
    display_markets: Dict[str, Any] = {}
    for market_key in MARKET_SELECTIONS:
        probabilities = model_probabilities.get(market_key, {}) or {}
        market_analysis = market_analyses.get(market_key)
        markers[market_key] = build_market_markers(
            market_key,
            probabilities,
            market_analysis,
            reliability,
        )
        display_markets[market_key] = simplified_market_display(
            market_key,
            market_analysis,
            markers[market_key],
            probabilities,
        )

    # Scelta principale: miglior combinazione probabilita', edge, affidabilita' e conferma book.
    candidates: List[Dict[str, Any]] = []
    for market_key, marker_pack in markers.items():
        model = marker_pack.get("model", {}) or {}
        pick = model.get("pick")
        current_odd = safe_float(
            ((market_analyses.get(market_key, {}) or {}).get("current", {}) or {}).get("odds", {}).get(pick)
        )
        probability = safe_float(model.get("probability"))
        expected_value = probability * current_odd - 1.0 if current_odd > 1 else 0.0
        agreement_bonus = 15.0 if marker_pack.get("relation") == "confirmed" else 0.0
        ranking_score = safe_float(model.get("score")) + agreement_bonus + clamp(expected_value * 100.0, -10, 20)
        candidates.append({
            "market": market_key,
            "selection": pick,
            "odd": round3(current_odd),
            "probability_pct": round3(probability * 100.0),
            "expected_value_pct": round3(expected_value * 100.0),
            "relation": marker_pack.get("relation"),
            "ranking_score": round3(ranking_score),
            "marker": (marker_pack.get("selections", {}) or {}).get(pick, {}),
        })
    candidates.sort(key=lambda item: item["ranking_score"], reverse=True)

    return {
        "fixture": meta,
        "display": {
            "primary_choice": candidates[0] if candidates else None,
            "ranked_choices": candidates,
            "markets": display_markets,
            "legend": {
                "circle": "Scelta ArabSniper",
                "square": "Scelta del book",
                "circle_square": "ArabSniper e book concordano",
                "red": "Conferma forte",
                "orange": "Conferma media",
                "yellow": "Conferma bassa",
            },
        },
        "model": {
            "reliability_pct": reliability,
            "lambdas": lambdas,
            "probabilities": model_probabilities,
            "markers": markers,
        },
        "book": {
            "markets": market_analyses,
        },
        "statistics": {
            "home": home_profile,
            "away": away_profile,
            "league": league_profile,
            "h2h": h2h_profile,
            "notes": {
                "xg": "Non disponibile direttamente nelle fixture base; predisposto per una fase deep successiva.",
                "streaks": "Le serie non generano eventi dovuti: sono usate solo come continuita' o regressione contenuta.",
                "h2h": "Peso massimo 5%, mai usato come contatore di Over mancanti.",
            },
        },
    }


# =========================================================
# SCAN E PUBBLICAZIONE
# =========================================================
def get_target_dates(start_date: date, days: int) -> List[str]:
    return [(start_date + timedelta(days=offset)).isoformat() for offset in range(days)]


def prune_state(state: MutableMapping[str, Any], keep_days_after_kickoff: int = 2) -> None:
    cutoff = now_rome() - timedelta(days=keep_days_after_kickoff)
    fixtures = state.get("fixtures", {}) or {}
    to_remove = []
    for fixture_id, record in fixtures.items():
        kickoff = parse_iso((record.get("meta", {}) or {}).get("kickoff"))
        if kickoff and kickoff < cutoff:
            to_remove.append(fixture_id)
    for fixture_id in to_remove:
        fixtures.pop(fixture_id, None)


def upload_json_to_github(filename: str, payload: Any, message: str) -> str:
    if Github is None:
        return "PYGITHUB_NOT_AVAILABLE"
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        return "MISSING_TOKEN"
    try:
        repo = Github(token).get_repo(REPO_NAME)
        content = json.dumps(payload, ensure_ascii=False, indent=2)
        try:
            current = repo.get_contents(filename)
            repo.update_file(current.path, message, content, current.sha)
        except Exception:
            repo.create_file(filename, message, content)
        return "SUCCESS"
    except Exception as exc:
        return f"GITHUB_ERROR: {exc}"


def scan_dates(
    client: APIClient,
    target_dates: Sequence[str],
    excluded_countries: Sequence[str],
) -> Dict[str, Any]:
    state = read_json(STATE_FILE, default_state())
    if not isinstance(state, dict):
        state = default_state()

    # La versione 1.0.1 corregge l'identificazione del mercato Over 2.5 FT.
    # Le aperture salvate dalla 1.0.0 possono appartenere ai team totals e non
    # devono essere riutilizzate per delta, aggio e OCI. Al primo avvio della
    # nuova versione lo storico viene quindi rigenerato automaticamente.
    if str(state.get("version", "")) != VERSION:
        log(
            f"♻️ Reset stato quote: versione {state.get('version', 'N/D')} -> {VERSION}"
        )
        state = default_state()

    state.setdefault("fixtures", {})

    league_cache: Dict[str, Any] = {}
    output_days: List[Dict[str, Any]] = []
    excluded = set(excluded_countries)

    for day_index, target_date in enumerate(target_dates, start=1):
        log(f"📅 Scan day{day_index}: {target_date}")
        fixtures_payload = client.get("fixtures", {"date": target_date, "timezone": "Europe/Rome"}, use_cache=False)
        fixtures = []
        for fixture_row in (fixtures_payload or {}).get("response", []) or []:
            fixture = fixture_row.get("fixture", {}) or {}
            league = fixture_row.get("league", {}) or {}
            if fixture.get("status", {}).get("short") != "NS":
                continue
            if league.get("country") in excluded:
                continue
            if is_blacklisted_league(league.get("name")):
                continue
            fixtures.append(fixture_row)

        log(f"⚽ Fixture pre-match valide: {len(fixtures)}")
        day_matches: List[Dict[str, Any]] = []
        for index, fixture_row in enumerate(fixtures, start=1):
            meta = fixture_meta(fixture_row)
            log(f"  [{index}/{len(fixtures)}] {meta['match']}")
            try:
                result = build_fixture_output(client, fixture_row, state, league_cache)
                if result:
                    day_matches.append(result)
            except Exception as exc:
                log(f"  ❌ Fixture {meta.get('fixture_id')} saltata: {exc}")

        day_matches.sort(key=lambda item: (item["fixture"].get("time", "99:99"), item["fixture"].get("match", "")))
        day_payload = {
            "day": day_index,
            "date": target_date,
            "updated_at": iso_now(),
            "matches": day_matches,
        }
        output_days.append(day_payload)
        atomic_write_json(DATA_DIR / f"arab_quote_day{day_index}.json", day_payload)

    state["version"] = VERSION
    state["updated_at"] = iso_now()
    prune_state(state)
    atomic_write_json(STATE_FILE, state)

    output = {
        "engine": "ArabSniper Quote Engine",
        "version": VERSION,
        "generated_at": iso_now(),
        "timezone": "Europe/Rome",
        "days": output_days,
        "method": {
            "book": "mediana multi-bookmaker + probabilita fair + aggio + OCI-AS",
            "model": "goal model Poisson con casa/trasferta, forma, campionato e H2H max 5%",
            "markers": "cerchio ArabSniper, quadrato book, doppio bordo concordanza",
            "legacy_labels_removed": ["GOLD", "MARKET", "BOOST", "PROBE", "OVER_TAG", "PT_TAG"],
        },
    }
    atomic_write_json(OUTPUT_FILE, output)
    return output


# =========================================================
# CLI
# =========================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ArabSniper Quote Engine V1")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--today", action="store_true", help="Scansiona il giorno corrente")
    mode.add_argument("--night", action="store_true", help="Scansione rolling da oggi")
    mode.add_argument("--date", type=str, help="Data iniziale YYYY-MM-DD")
    mode.add_argument("--catalog", action="store_true", help="Aggiorna solo il catalogo odds/bets")

    parser.add_argument("--days", type=int, default=5, help="Numero giorni per --night/--date")
    parser.add_argument("--github", action="store_true", help="Pubblica output e stato su GitHub")
    parser.add_argument("--include-country", action="append", default=[], help="Rimuove un paese dalla blacklist")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = str(os.getenv("API_SPORTS_KEY", "")).strip()
    if not api_key:
        log("❌ API_SPORTS_KEY mancante")
        raise SystemExit(2)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = APIClient(api_key=api_key)

    if args.catalog:
        catalog = fetch_bet_catalog(client)
        log(f"✅ Catalogo mercati aggiornato: {len(catalog.get('bets', []))} voci")
        if args.github:
            status = upload_json_to_github(REMOTE_BET_CATALOG_FILE, catalog, "Update ArabSniper bet catalog")
            log(f"GitHub catalog: {status}")
        return

    if args.today or args.night:
        start = now_rome().date()
    else:
        try:
            start = date.fromisoformat(args.date)
        except Exception:
            log("❌ Formato --date non valido. Usa YYYY-MM-DD")
            raise SystemExit(2)

    days = 1 if args.today else max(1, min(args.days, 7))
    excluded = sorted(DEFAULT_EXCLUDED_COUNTRIES - set(args.include_country))
    output = scan_dates(client, get_target_dates(start, days), excluded)

    log(f"✅ Output scritto: {OUTPUT_FILE}")
    log(f"✅ Stato quote scritto: {STATE_FILE}")

    if args.github:
        output_status = upload_json_to_github(REMOTE_OUTPUT_FILE, output, "Update ArabSniper quote dashboard")
        state_payload = read_json(STATE_FILE, {})
        state_status = upload_json_to_github(REMOTE_STATE_FILE, state_payload, "Update ArabSniper quote state")
        log(f"GitHub output: {output_status}")
        log(f"GitHub state: {state_status}")


if __name__ == "__main__":
    main()
