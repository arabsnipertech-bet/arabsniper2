#!/usr/bin/env python3
"""
ARAB SNIPER QUOTE ENGINE V1.1
============================

Motore autonomo per:
- estrazione fixture e quote API-Football / API-Sports;
- storico quota iniziale, attuale e variazioni nel tempo;
- consenso tra bookmaker tramite mediana;
- aggio, payout, carico del margine e OCI-AS;
- profilo statistico squadre, casa/trasferta, forma e scontri diretti;
- lambda FT e primo tempo;
- probabilita' ArabSniper pure e calibrate per 1-X-2, Over 2.5 FT,
  Over 1.5 primo tempo e GG primo tempo;
- layer xG reali quando forniti dalle statistiche API;
- validazione rigida e audit dei mercati quota;
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
VERSION = "1.1.4"
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
        self.request_count = 0
        self.last_error: Optional[Dict[str, Any]] = None

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
                self.request_count += 1
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
                    # API-Sports usa chiavi diverse a seconda del problema
                    # (rateLimit, requests, token, subscription, plan...).
                    # Qualunque errore con response vuota NON è un risultato valido.
                    self.last_error = {
                        "path": path,
                        "params": dict(params),
                        "errors": errors,
                        "status_code": response.status_code,
                        "at": iso_now(),
                    }
                    log(f"⚠️ API {path} errors: {errors}")
                    continue
                if "response" not in payload:
                    self.last_error = {
                        "path": path,
                        "params": dict(params),
                        "errors": "response_missing",
                        "status_code": response.status_code,
                        "at": iso_now(),
                    }
                    continue
                self.last_error = None
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
        if not competition_allowed(fixture_row.get("league", {}) or {}, ()):
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
        if not competition_allowed(fixture_row.get("league", {}) or {}, ()):
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
        "status_short": (fixture.get("status", {}) or {}).get("short"),
        "status_long": (fixture.get("status", {}) or {}).get("long"),
        "elapsed": (fixture.get("status", {}) or {}).get("elapsed"),
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


def _existing_day_payload(day_index: int, target_date: str) -> Optional[Dict[str, Any]]:
    payload = read_json(DATA_DIR / f"arab_quote_day{day_index}.json", {})
    if not isinstance(payload, dict):
        return None
    if str(payload.get("date", "")) != str(target_date):
        return None
    matches = payload.get("matches")
    if not isinstance(matches, list):
        return None
    return payload


def _fixture_id_from_output(item: Mapping[str, Any]) -> str:
    return str(((item.get("fixture", {}) or {}).get("fixture_id")) or "")


def _preserve_existing_match(
    item: Mapping[str, Any],
    status_row: Optional[Mapping[str, Any]],
    reason: str,
) -> Dict[str, Any]:
    preserved = json.loads(json.dumps(item))
    fixture = preserved.setdefault("fixture", {})
    if status_row:
        status = ((status_row.get("fixture", {}) or {}).get("status", {}) or {})
        fixture["status_short"] = status.get("short")
        fixture["status_long"] = status.get("long")
        fixture["elapsed"] = status.get("elapsed")
    fixture["data_freshness"] = "preserved"
    fixture["preserved_reason"] = reason
    fixture["preserved_at"] = iso_now()
    return preserved


def scan_dates(
    client: APIClient,
    target_dates: Sequence[str],
    excluded_countries: Sequence[str],
) -> Dict[str, Any]:
    state = read_json(STATE_FILE, default_state())
    if not isinstance(state, dict):
        state = default_state()

    current_version = str(state.get("version", ""))
    current_family = ".".join(current_version.split(".")[:2])
    target_family = ".".join(VERSION.split(".")[:2])
    if current_version and current_family != target_family:
        log(f"♻️ Reset stato quote per cambio schema: {current_version} -> {VERSION}")
        state = default_state()
    elif current_version != VERSION:
        # Le patch 1.1.x mantengono aperture, storico e fixture già analizzate.
        log(f"🧩 Aggiornamento compatibile stato quote: {current_version or 'N/D'} -> {VERSION}")
        state["version"] = VERSION

    state.setdefault("fixtures", {})
    league_cache: Dict[str, Any] = {}
    output_days: List[Dict[str, Any]] = []
    excluded = set(excluded_countries)

    for day_index, target_date in enumerate(target_dates, start=1):
        log(f"📅 Scan day{day_index}: {target_date}")
        day_path = DATA_DIR / f"arab_quote_day{day_index}.json"
        existing_payload = _existing_day_payload(day_index, target_date)
        existing_matches = list((existing_payload or {}).get("matches", []) or [])
        existing_by_id = {
            _fixture_id_from_output(item): item
            for item in existing_matches
            if _fixture_id_from_output(item)
        }

        fixtures_payload = client.get(
            "fixtures",
            {"date": target_date, "timezone": "Europe/Rome"},
            use_cache=False,
        )

        # Mai cancellare dati buoni per un errore API, rate limit o risposta temporaneamente vuota.
        if not fixtures_payload:
            if existing_matches:
                log(f"🛡️ API fixtures non disponibile: preservati {len(existing_matches)} match day{day_index}")
                preserved = [
                    _preserve_existing_match(item, None, "fixtures_api_unavailable")
                    for item in existing_matches
                ]
                day_payload = {
                    "day": day_index,
                    "date": target_date,
                    "updated_at": iso_now(),
                    "matches": preserved,
                    "refresh_status": "preserved_api_error",
                }
                output_days.append(day_payload)
                atomic_write_json(day_path, day_payload)
                continue
            log(f"⚠️ Nessun payload fixture e nessun dato precedente per day{day_index}")
            day_payload = {
                "day": day_index,
                "date": target_date,
                "updated_at": iso_now(),
                "matches": [],
                "refresh_status": "api_error_no_backup",
            }
            output_days.append(day_payload)
            atomic_write_json(day_path, day_payload)
            continue

        response_rows = list((fixtures_payload or {}).get("response", []) or [])
        status_rows = {
            str(((row.get("fixture", {}) or {}).get("id"))): row
            for row in response_rows
            if ((row.get("fixture", {}) or {}).get("id")) is not None
        }

        fixtures: List[Dict[str, Any]] = []
        for fixture_row in response_rows:
            fixture = fixture_row.get("fixture", {}) or {}
            league = fixture_row.get("league", {}) or {}
            if fixture.get("status", {}).get("short") != "NS":
                continue
            if not competition_allowed(league, excluded):
                continue
            fixtures.append(fixture_row)

        log(f"⚽ Fixture pre-match valide: {len(fixtures)}")
        refreshed_matches: List[Dict[str, Any]] = []
        refreshed_ids: set[str] = set()
        failed_ids: set[str] = set()

        for index, fixture_row in enumerate(fixtures, start=1):
            meta = fixture_meta(fixture_row)
            fid = str(meta.get("fixture_id") or "")
            log(f"  [{index}/{len(fixtures)}] {meta['match']}")
            try:
                result = build_fixture_output(client, fixture_row, state, league_cache)
                if result:
                    result.setdefault("fixture", {})["data_freshness"] = "fresh"
                    refreshed_matches.append(result)
                    refreshed_ids.add(fid)
                else:
                    failed_ids.add(fid)
                    log(f"  ⚠️ Fixture {fid} non aggiornata: dati incompleti, preservo eventuale versione precedente")
            except Exception as exc:
                failed_ids.add(fid)
                log(f"  ❌ Fixture {fid} saltata: {exc}")

        # Mantiene in pagina le partite già analizzate quando iniziano/finiscono.
        # Mantiene anche il dato precedente se il refresh di una singola fixture fallisce.
        preserved_count = 0
        for fid, old_item in existing_by_id.items():
            if fid in refreshed_ids:
                continue
            old_fixture = old_item.get("fixture", {}) or {}
            if not competition_allowed({"name": old_fixture.get("league"), "country": old_fixture.get("country")}, excluded):
                log(f"  🧹 Rimossa fixture {fid}: competizione non ammessa dal filtro qualità")
                continue
            status_row = status_rows.get(fid)
            current_status = str((((status_row or {}).get("fixture", {}) or {}).get("status", {}) or {}).get("short") or "")
            if fid in failed_ids:
                reason = "fixture_refresh_failed"
            elif current_status and current_status != "NS":
                reason = f"fixture_{current_status.lower()}"
            elif fid not in status_rows:
                reason = "fixture_missing_from_response"
            else:
                reason = "fixture_not_refreshed"
            refreshed_matches.append(_preserve_existing_match(old_item, status_row, reason))
            preserved_count += 1

        # Se l'API risponde ma tutte le elaborazioni falliscono, non sovrascrivere un day valido con zero.
        if not refreshed_matches and existing_matches:
            log(f"🛡️ Refresh senza risultati: preservati {len(existing_matches)} match precedenti day{day_index}")
            refreshed_matches = [
                _preserve_existing_match(item, status_rows.get(_fixture_id_from_output(item)), "empty_refresh_guard")
                for item in existing_matches
                if competition_allowed({
                    "name": ((item.get("fixture", {}) or {}).get("league")),
                    "country": ((item.get("fixture", {}) or {}).get("country")),
                }, excluded)
            ]
            preserved_count = len(refreshed_matches)

        refreshed_matches.sort(
            key=lambda item: (
                (item.get("fixture", {}) or {}).get("time", "99:99"),
                (item.get("fixture", {}) or {}).get("match", ""),
            )
        )
        day_payload = {
            "day": day_index,
            "date": target_date,
            "updated_at": iso_now(),
            "matches": refreshed_matches,
            "refresh_status": "ok",
            "fresh_matches": len(refreshed_ids),
            "preserved_matches": preserved_count,
            "prematch_fixtures_found": len(fixtures),
            "api_fixtures_found": len(response_rows),
        }
        output_days.append(day_payload)
        atomic_write_json(day_path, day_payload)

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
            "retention": "le partite analizzate restano visibili dopo il calcio d'inizio; errori API non svuotano i JSON",
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


# =========================================================
# ENHANCEMENT V1.1 — xG, MARKET AUDIT E CALIBRAZIONE BOOK
# =========================================================
# Questo blocco sovrascrive in modo compatibile alcune funzioni V1.0.1.
# Il runner continua a usare gli stessi comandi: --catalog, --today,
# --night, --date. Si aggiunge soltanto --audit-markets.

XG_CACHE_FILE = DATA_DIR / "arab_quote_xg_cache.json"
MARKET_AUDIT_FILE = DATA_DIR / "arab_quote_market_audit.json"
REMOTE_XG_CACHE_FILE = "data/arab_quote_xg_cache.json"
REMOTE_MARKET_AUDIT_FILE = "data/arab_quote_market_audit.json"
REMOTE_DAY_FILES = {day: f"data/arab_quote_day{day}.json" for day in range(1, 8)}

XG_ENABLED = str(os.getenv("ARAB_XG_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
XG_MATCHES_PER_TEAM = max(0, min(safe_int(os.getenv("ARAB_XG_MATCHES_PER_TEAM", 4), 4), MAX_TEAM_MATCHES))
XG_MAX_CALLS_PER_RUN = max(0, safe_int(os.getenv("ARAB_XG_MAX_CALLS_PER_RUN", 20), 20))
MARKET_MIN_BOOKS = max(1, safe_int(os.getenv("ARAB_MARKET_MIN_BOOKS", 2), 2))

_XG_STORE: Dict[str, Any] = read_json(XG_CACHE_FILE, {"version": VERSION, "fixtures": {}})
if not isinstance(_XG_STORE, dict):
    _XG_STORE = {"version": VERSION, "fixtures": {}}
_XG_STORE.setdefault("fixtures", {})
_XG_RUNTIME_CALLS = 0
_XG_DIRTY = False
_MARKET_RULES_CACHE: Optional[Dict[str, Any]] = None


def _bool_env(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "si", "sì"}


def _xg_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace("%", "").replace(",", ".")
    number = safe_float(value, -1.0)
    return round3(number) if number >= 0 else None


def _is_xg_stat_type(stat_type: Any) -> bool:
    name = normalized_text(stat_type).replace("_", " ").strip()
    exact = {
        "expected goals", "expected goal", "xg", "expected goals xg",
    }
    return name in exact or name.startswith("expected goals ")


def parse_fixture_xg_statistics(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Estrae xG reali dal payload /fixtures/statistics senza stimarli dai tiri."""
    teams: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("response", []) or []:
        team = row.get("team", {}) or {}
        team_id = str(team.get("id", ""))
        if not team_id:
            continue
        xg: Optional[float] = None
        matched_type = None
        for stat in row.get("statistics", []) or []:
            if _is_xg_stat_type(stat.get("type")):
                parsed = _xg_value(stat.get("value"))
                if parsed is not None:
                    xg = parsed
                    matched_type = stat.get("type")
                    break
        teams[team_id] = {
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "xg": xg,
            "stat_type": matched_type,
        }
    available = sum(1 for row in teams.values() if row.get("xg") is not None) >= 2
    return {"available": available, "teams": teams}


def fetch_fixture_xg(
    client: APIClient,
    fixture_id: Any,
    home_id: Any,
    away_id: Any,
) -> Dict[str, Any]:
    global _XG_RUNTIME_CALLS, _XG_DIRTY
    key = str(fixture_id)
    cached = (_XG_STORE.get("fixtures", {}) or {}).get(key)
    if isinstance(cached, dict):
        return cached

    empty = {
        "fixture_id": key,
        "available": False,
        "home_xg": None,
        "away_xg": None,
        "source": "unavailable",
        "checked_at": iso_now(),
    }
    if not XG_ENABLED or _XG_RUNTIME_CALLS >= XG_MAX_CALLS_PER_RUN:
        empty["source"] = "disabled" if not XG_ENABLED else "run_budget_exhausted"
        return empty

    _XG_RUNTIME_CALLS += 1
    payload = client.get("fixtures/statistics", {"fixture": fixture_id}, use_cache=True)
    parsed = parse_fixture_xg_statistics(payload or {})
    teams = parsed.get("teams", {}) or {}
    home_xg = (teams.get(str(home_id), {}) or {}).get("xg")
    away_xg = (teams.get(str(away_id), {}) or {}).get("xg")
    record = {
        "fixture_id": key,
        "available": home_xg is not None and away_xg is not None,
        "home_xg": home_xg,
        "away_xg": away_xg,
        "source": "api_fixture_statistics" if home_xg is not None and away_xg is not None else "api_no_xg",
        "checked_at": iso_now(),
        "raw_team_types": {
            team_key: team_row.get("stat_type")
            for team_key, team_row in teams.items()
            if team_row.get("stat_type")
        },
    }
    _XG_STORE.setdefault("fixtures", {})[key] = record
    _XG_DIRTY = True
    return record


def enrich_team_matches_with_xg(
    client: APIClient,
    rows: Sequence[Mapping[str, Any]],
    team_id: int,
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = [dict(row) for row in rows]
    for index, row in enumerate(enriched):
        if index >= XG_MATCHES_PER_TEAM:
            break
        fixture_id = row.get("fixture_id")
        if not fixture_id:
            continue
        xg = fetch_fixture_xg(client, fixture_id, row.get("home_id"), row.get("away_id"))
        home_xg = xg.get("home_xg")
        away_xg = xg.get("away_xg")
        row["home_xg"] = home_xg
        row["away_xg"] = away_xg
        row["xg_available"] = bool(xg.get("available"))
        row["xg_source"] = xg.get("source")
        is_home = str(row.get("home_id")) == str(team_id)
        if xg.get("available"):
            row["team_xg_for"] = home_xg if is_home else away_xg
            row["team_xg_against"] = away_xg if is_home else home_xg
    return enriched


def save_xg_cache() -> None:
    global _XG_DIRTY
    if not _XG_DIRTY:
        return
    _XG_STORE["version"] = VERSION
    _XG_STORE["updated_at"] = iso_now()
    _XG_STORE["runtime_calls_last_run"] = _XG_RUNTIME_CALLS
    atomic_write_json(XG_CACHE_FILE, _XG_STORE)
    _XG_DIRTY = False


def xg_weight_from_sample(sample: int) -> float:
    if sample <= 0:
        return 0.0
    if sample <= 3:
        return 0.20
    if sample <= 6:
        return 0.35
    if sample <= 10:
        return 0.50
    return 0.60


def summarize_team_matches(rows: Sequence[Mapping[str, Any]], expected_side: str) -> Dict[str, Any]:
    """Profilo squadra V1.1: goal + xG reali quando disponibili."""
    all_rows = list(rows)
    side_rows = [row for row in all_rows if row.get("side") == expected_side]
    context_weight = (
        0.62 if len(side_rows) >= 5 else
        0.55 if len(side_rows) == 4 else
        0.46 if len(side_rows) == 3 else
        0.33 if len(side_rows) == 2 else
        0.20 if len(side_rows) == 1 else 0.0
    )

    def summarize(sample: Sequence[Mapping[str, Any]]) -> Dict[str, float]:
        if not sample:
            return defaultdict(float)
        weights = recency_weights(sample)

        def wavg(key: str) -> float:
            return weighted_mean([safe_float(row.get(key)) for row in sample], weights)

        def xg_wavg(key: str) -> Tuple[float, int]:
            vals: List[float] = []
            ws: List[float] = []
            for row, weight in zip(sample, weights):
                value = row.get(key)
                if value is None:
                    continue
                parsed = safe_float(value, -1.0)
                if parsed < 0:
                    continue
                vals.append(parsed)
                ws.append(weight)
            return (weighted_mean(vals, ws), len(vals)) if vals else (0.0, 0)

        xgf, xgf_sample = xg_wavg("team_xg_for")
        xga, xga_sample = xg_wavg("team_xg_against")
        xg_sample = min(xgf_sample, xga_sample)
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
            "xg_for_ft": xgf,
            "xg_against_ft": xga,
            "xg_sample": float(xg_sample),
        }

    all_summary = summarize(all_rows)
    side_summary = summarize(side_rows)
    keys = set(all_summary) | set(side_summary)
    blended: Dict[str, float] = {}
    for key in keys:
        if key == "xg_sample":
            blended[key] = float(max(safe_int(all_summary.get(key)), safe_int(side_summary.get(key))))
            continue
        blended[key] = round3(
            safe_float(all_summary.get(key)) * (1.0 - context_weight) +
            safe_float(side_summary.get(key, all_summary.get(key))) * context_weight
        )

    recent5 = all_rows[:5]
    baseline_rows = all_rows[5:] if len(all_rows) > 7 else all_rows
    recent_scoring = mean(safe_float(row.get("team_goals_ft")) for row in recent5)
    baseline_scoring = mean(safe_float(row.get("team_goals_ft")) for row in baseline_rows)
    scoring_gap = recent_scoring - baseline_scoring

    recent_xg_rows = [row for row in recent5 if row.get("team_xg_for") is not None]
    recent_xg = mean(safe_float(row.get("team_xg_for")) for row in recent_xg_rows) if recent_xg_rows else 0.0
    recent_goal_for_xg = mean(safe_float(row.get("team_goals_ft")) for row in recent_xg_rows) if recent_xg_rows else 0.0
    finishing_gap = recent_goal_for_xg - recent_xg if recent_xg_rows else 0.0

    if len(recent_xg_rows) >= 3:
        regression_factor = clamp(1.0 - finishing_gap * 0.045, 0.92, 1.08)
        regression_method = "real_xg"
    else:
        regression_factor = clamp(1.0 - scoring_gap * 0.035, 0.94, 1.06)
        regression_method = "goals_fallback"

    xg_sample_all = safe_int(all_summary.get("xg_sample"))
    xg_sample_context = safe_int(side_summary.get("xg_sample"))
    xg_layer_weight = xg_weight_from_sample(max(xg_sample_context, min(xg_sample_all, 10)))

    return {
        "sample_all": len(all_rows),
        "sample_context": len(side_rows),
        "context_weight": round3(context_weight),
        "expected_side": expected_side,
        "metrics": blended,
        "all_metrics": {key: round3(value) for key, value in all_summary.items()},
        "context_metrics": {key: round3(value) for key, value in side_summary.items()},
        "xg": {
            "available": xg_sample_all > 0,
            "sample_all": xg_sample_all,
            "sample_context": xg_sample_context,
            "weight_candidate": round3(xg_layer_weight),
            "xg_for_ft": round3(blended.get("xg_for_ft", 0.0)),
            "xg_against_ft": round3(blended.get("xg_against_ft", 0.0)),
            "source": "api_fixture_statistics" if xg_sample_all > 0 else "goals_fallback",
        },
        "form": {
            "recent5_points_per_game": round3(mean(safe_float(row.get("result_points")) for row in recent5)),
            "recent5_goals_scored": round3(recent_scoring),
            "baseline_goals_scored": round3(baseline_scoring),
            "scoring_gap": round3(scoring_gap),
            "recent_xg_for": round3(recent_xg),
            "finishing_gap_goals_minus_xg": round3(finishing_gap),
            "regression_factor": round3(regression_factor),
            "regression_factor_goals_only": round3(regression_factor),
            "method": regression_method,
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


def _goal_lambdas_base(
    home_profile: Mapping[str, Any],
    away_profile: Mapping[str, Any],
    league: Mapping[str, Any],
) -> Dict[str, float]:
    hm = home_profile.get("metrics", {}) or {}
    am = away_profile.get("metrics", {}) or {}
    league_home = max(safe_float(league.get("avg_home_goals"), 1.45), 0.60)
    league_away = max(safe_float(league.get("avg_away_goals"), 1.15), 0.50)
    league_home_ht = max(safe_float(league.get("avg_home_ht"), 0.68), 0.25)
    league_away_ht = max(safe_float(league.get("avg_away_ht"), 0.52), 0.20)

    hs = shrink_rate(safe_float(hm.get("goals_scored_ft"), league_home), safe_int(home_profile.get("sample_context")), league_home)
    ac = shrink_rate(safe_float(am.get("goals_conceded_ft"), league_home), safe_int(away_profile.get("sample_context")), league_home)
    as_ = shrink_rate(safe_float(am.get("goals_scored_ft"), league_away), safe_int(away_profile.get("sample_context")), league_away)
    hc = shrink_rate(safe_float(hm.get("goals_conceded_ft"), league_away), safe_int(home_profile.get("sample_context")), league_away)
    home_ft = league_home * (hs / league_home) * (ac / league_home)
    away_ft = league_away * (as_ / league_away) * (hc / league_away)

    hsh = shrink_rate(safe_float(hm.get("goals_scored_ht"), league_home_ht), safe_int(home_profile.get("sample_context")), league_home_ht)
    ach = shrink_rate(safe_float(am.get("goals_conceded_ht"), league_home_ht), safe_int(away_profile.get("sample_context")), league_home_ht)
    ash = shrink_rate(safe_float(am.get("goals_scored_ht"), league_away_ht), safe_int(away_profile.get("sample_context")), league_away_ht)
    hch = shrink_rate(safe_float(hm.get("goals_conceded_ht"), league_away_ht), safe_int(home_profile.get("sample_context")), league_away_ht)
    home_ht = league_home_ht * (hsh / league_home_ht) * (ach / league_home_ht)
    away_ht = league_away_ht * (ash / league_away_ht) * (hch / league_away_ht)

    home_ft *= safe_float(home_profile.get("form", {}).get("regression_factor"), 1.0)
    away_ft *= safe_float(away_profile.get("form", {}).get("regression_factor"), 1.0)
    home_ppg = safe_float(home_profile.get("form", {}).get("recent5_points_per_game"), 1.3)
    away_ppg = safe_float(away_profile.get("form", {}).get("recent5_points_per_game"), 1.3)
    form_gap = clamp((home_ppg - away_ppg) / 3.0, -0.12, 0.12)
    home_ft *= 1.0 + form_gap * 0.18
    away_ft *= 1.0 - form_gap * 0.18
    return {
        "home_ft": home_ft, "away_ft": away_ft,
        "home_ht": home_ht, "away_ht": away_ht,
        "form_gap": form_gap,
        "league_home": league_home, "league_away": league_away,
    }


def build_lambdas(
    home_profile: Mapping[str, Any],
    away_profile: Mapping[str, Any],
    league: Mapping[str, Any],
    h2h: Mapping[str, Any],
) -> Dict[str, Any]:
    goal = _goal_lambdas_base(home_profile, away_profile, league)
    hm = home_profile.get("metrics", {}) or {}
    am = away_profile.get("metrics", {}) or {}
    league_home = goal["league_home"]
    league_away = goal["league_away"]

    home_xg_sample = min(safe_int(home_profile.get("xg", {}).get("sample_all")), 12)
    away_xg_sample = min(safe_int(away_profile.get("xg", {}).get("sample_all")), 12)
    pair_sample = min(home_xg_sample, away_xg_sample)
    xg_weight = xg_weight_from_sample(pair_sample)

    xg_home_ft = goal["home_ft"]
    xg_away_ft = goal["away_ft"]
    xg_available = False
    if xg_weight > 0:
        home_xgf = shrink_rate(safe_float(hm.get("xg_for_ft"), league_home), home_xg_sample, league_home)
        away_xga = shrink_rate(safe_float(am.get("xg_against_ft"), league_home), away_xg_sample, league_home)
        away_xgf = shrink_rate(safe_float(am.get("xg_for_ft"), league_away), away_xg_sample, league_away)
        home_xga = shrink_rate(safe_float(hm.get("xg_against_ft"), league_away), home_xg_sample, league_away)
        if min(home_xgf, away_xga, away_xgf, home_xga) > 0:
            xg_home_ft = league_home * (home_xgf / league_home) * (away_xga / league_home)
            xg_away_ft = league_away * (away_xgf / league_away) * (home_xga / league_away)
            xg_available = True
        else:
            xg_weight = 0.0

    home_ft = goal["home_ft"] * (1.0 - xg_weight) + xg_home_ft * xg_weight
    away_ft = goal["away_ft"] * (1.0 - xg_weight) + xg_away_ft * xg_weight

    # L'xG API e' FT: sul primo tempo applichiamo soltanto una correzione lieve.
    goal_total = max(goal["home_ft"] + goal["away_ft"], 0.10)
    final_total = max(home_ft + away_ft, 0.10)
    ht_xg_factor = clamp(math.sqrt(final_total / goal_total), 0.94, 1.06)
    home_ht = goal["home_ht"] * ht_xg_factor
    away_ht = goal["away_ht"] * ht_xg_factor

    h2h_ft_factor = 1.0
    h2h_ht_factor = 1.0
    if safe_int(h2h.get("sample")) >= 2:
        league_total = max(safe_float(league.get("avg_total_ft"), 2.60), 1.50)
        h2h_ft_factor = clamp(safe_float(h2h.get("avg_total_ft"), league_total) / league_total, 0.95, 1.05)
        league_total_ht = max(safe_float(league.get("avg_total_ht"), 1.20), 0.60)
        h2h_ht_factor = clamp(safe_float(h2h.get("avg_total_ht"), league_total_ht) / league_total_ht, 0.95, 1.05)
        home_ft *= h2h_ft_factor
        away_ft *= h2h_ft_factor
        home_ht *= h2h_ht_factor
        away_ht *= h2h_ht_factor

    home_ft = clamp(home_ft, 0.15, 3.60)
    away_ft = clamp(away_ft, 0.10, 3.20)
    home_ht = clamp(home_ht, 0.03, 1.90)
    away_ht = clamp(away_ht, 0.02, 1.70)
    return {
        "home_ft": round3(home_ft), "away_ft": round3(away_ft), "total_ft": round3(home_ft + away_ft),
        "home_ht": round3(home_ht), "away_ht": round3(away_ht), "total_ht": round3(home_ht + away_ht),
        "goals_only": {
            "home_ft": round3(goal["home_ft"]), "away_ft": round3(goal["away_ft"]),
            "home_ht": round3(goal["home_ht"]), "away_ht": round3(goal["away_ht"]),
        },
        "xg_only": {
            "available": xg_available,
            "home_ft": round3(xg_home_ft), "away_ft": round3(xg_away_ft),
            "sample_pair": pair_sample, "weight": round3(xg_weight),
        },
        "components": {
            "form_gap": round3(goal["form_gap"]),
            "h2h_ft_factor": round3(h2h_ft_factor),
            "h2h_ht_factor": round3(h2h_ht_factor),
            "ht_xg_factor": round3(ht_xg_factor),
            "xg_weight": round3(xg_weight),
        },
    }


def model_reliability(home_profile: Mapping[str, Any], away_profile: Mapping[str, Any], league: Mapping[str, Any]) -> float:
    home_sample = min(safe_int(home_profile.get("sample_all")) / 12.0, 1.0)
    away_sample = min(safe_int(away_profile.get("sample_all")) / 12.0, 1.0)
    home_context = min(safe_int(home_profile.get("sample_context")) / 5.0, 1.0)
    away_context = min(safe_int(away_profile.get("sample_context")) / 5.0, 1.0)
    league_sample = min(safe_int(league.get("sample")) / 40.0, 1.0)
    xg_home = min(safe_int(home_profile.get("xg", {}).get("sample_all")) / 8.0, 1.0)
    xg_away = min(safe_int(away_profile.get("xg", {}).get("sample_all")) / 8.0, 1.0)
    reliability = (
        (home_sample + away_sample) * 0.17 +
        (home_context + away_context) * 0.18 +
        league_sample * 0.15 +
        ((xg_home + xg_away) / 2.0) * 0.15
    )
    return round3(clamp(reliability, 0.0, 1.0) * 100.0)


def _catalog_rules() -> Dict[str, Any]:
    global _MARKET_RULES_CACHE
    if _MARKET_RULES_CACHE is not None:
        return _MARKET_RULES_CACHE
    catalog = read_json(BET_CATALOG_FILE, {"bets": []})
    rules = {
        MARKET_1X2: {1},
        MARKET_O25: {5},
        MARKET_O15HT: set(),
        MARKET_GGHT: set(),
        "catalog_available": False,
        "names": {},
    }
    for bet in (catalog.get("bets", []) if isinstance(catalog, dict) else []):
        bid = safe_int(bet.get("id"), 0)
        name = normalized_text(bet.get("name"))
        if not bid or not name:
            continue
        rules["catalog_available"] = True
        rules["names"][bid] = bet.get("name")
        first_half = _contains_first_half(name)
        if first_half and _is_btts_market(name):
            rules[MARKET_GGHT].add(bid)
        elif first_half and not _is_team_specific_goal_market(name) and any(token in name for token in ("total", "over under", "goals")):
            rules[MARKET_O15HT].add(bid)
    _MARKET_RULES_CACHE = rules
    return rules


def _new_market_audit() -> Dict[str, Any]:
    return {
        key: {
            "accepted": [], "rejected": [], "accepted_bet_ids": [],
            "accepted_bet_names": [], "bookmakers_found": 0,
            "status": "missing", "usable_for_signals": False,
        }
        for key in MARKET_SELECTIONS
    }


def extract_bookmaker_markets_with_audit(
    odds_payload: Mapping[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    rules = _catalog_rules()
    output: Dict[str, Dict[str, Any]] = {}
    audit = _new_market_audit()

    def accept(market_key: str, bid: int, name: str, book_name: str, source: str) -> None:
        audit[market_key]["accepted"].append({
            "bet_id": bid, "bet_name": name, "bookmaker": book_name, "source": source,
        })

    def reject(market_key: str, bid: int, name: str, book_name: str, reason: str) -> None:
        bucket = audit[market_key]["rejected"]
        if len(bucket) < 30:
            bucket.append({"bet_id": bid, "bet_name": name, "bookmaker": book_name, "reason": reason})

    for response_row in odds_payload.get("response", []) or []:
        for bookmaker in response_row.get("bookmakers", []) or []:
            bookmaker_id = str(bookmaker.get("id", ""))
            bookmaker_name = str(bookmaker.get("name", "N/D")).strip() or "N/D"
            book_key = bookmaker_id or bookmaker_name
            book_record = output.setdefault(book_key, {
                "bookmaker_id": bookmaker.get("id"), "bookmaker_name": bookmaker_name, "markets": {},
            })
            for bet in bookmaker.get("bets", []) or []:
                bid = safe_int(bet.get("id"), 0)
                raw_name = str(bet.get("name", ""))
                name = normalized_text(raw_name)
                values = _parse_market_values(bet)
                first_half = _contains_first_half(name)
                team_specific = _is_team_specific_goal_market(name)
                forbidden = any(token in name for token in ("corner", "card", "booking"))

                if bid == 1:
                    q1, qx, q2 = _find_odd(values, ("home", "1")), _find_odd(values, ("draw", "x")), _find_odd(values, ("away", "2"))
                    if q1 > 1 and qx > 1 and q2 > 1:
                        book_record["markets"][MARKET_1X2] = {"1": q1, "x": qx, "2": q2}
                        accept(MARKET_1X2, bid, raw_name, bookmaker_name, "strict_bet_id")
                    else:
                        reject(MARKET_1X2, bid, raw_name, bookmaker_name, "incomplete_1x2")
                    continue

                if bid == 5 and not first_half and not team_specific and not forbidden:
                    over, under = _find_odd(values, ("over 2.5", "over 2,5")), _find_odd(values, ("under 2.5", "under 2,5"))
                    if over > 1 and under > 1:
                        book_record["markets"][MARKET_O25] = {"over": over, "under": under}
                        accept(MARKET_O25, bid, raw_name, bookmaker_name, "strict_bet_id")
                    else:
                        reject(MARKET_O25, bid, raw_name, bookmaker_name, "incomplete_over_under_25")
                    continue

                if forbidden or team_specific:
                    continue

                o15_rule = bid in rules[MARKET_O15HT]
                o15_fallback = first_half and any(token in name for token in ("total", "over under", "goals")) and not _is_btts_market(name)
                if o15_rule or (not rules["catalog_available"] and o15_fallback):
                    over, under = _find_odd(values, ("over 1.5", "over 1,5")), _find_odd(values, ("under 1.5", "under 1,5"))
                    if over > 1 and under > 1:
                        book_record["markets"][MARKET_O15HT] = {"over": over, "under": under}
                        accept(MARKET_O15HT, bid, raw_name, bookmaker_name, "catalog_bet_id" if o15_rule else "name_fallback")
                    else:
                        reject(MARKET_O15HT, bid, raw_name, bookmaker_name, "incomplete_over_under_15_ht")
                    continue

                gg_rule = bid in rules[MARKET_GGHT]
                gg_fallback = first_half and _is_btts_market(name)
                if gg_rule or (not rules["catalog_available"] and gg_fallback):
                    yes, no = _find_odd(values, ("yes", "si", "sì")), _find_odd(values, ("no",))
                    if yes > 1:
                        market = {"yes": yes}
                        if no > 1:
                            market["no"] = no
                        book_record["markets"][MARKET_GGHT] = market
                        accept(MARKET_GGHT, bid, raw_name, bookmaker_name, "catalog_bet_id" if gg_rule else "name_fallback")
                    else:
                        reject(MARKET_GGHT, bid, raw_name, bookmaker_name, "missing_yes")

    for market_key in MARKET_SELECTIONS:
        books = complete_market_books(output, market_key)
        accepted = audit[market_key]["accepted"]
        ids = sorted({safe_int(row.get("bet_id")) for row in accepted if safe_int(row.get("bet_id"))})
        names = sorted({str(row.get("bet_name")) for row in accepted if row.get("bet_name")})
        sources = {row.get("source") for row in accepted}
        audit[market_key]["accepted_bet_ids"] = ids
        audit[market_key]["accepted_bet_names"] = names
        audit[market_key]["bookmakers_found"] = len(books)
        if not books:
            status = "missing"
        elif sources == {"strict_bet_id"} or sources == {"catalog_bet_id"}:
            status = "verified"
        else:
            status = "name_fallback"
        min_books = 1 if market_key == MARKET_GGHT else MARKET_MIN_BOOKS
        usable = status == "verified" and len(books) >= min_books
        audit[market_key]["status"] = status
        audit[market_key]["usable_for_signals"] = usable
        audit[market_key]["minimum_books_required"] = min_books
    audit["catalog"] = {
        "available": rules["catalog_available"],
        "over15_ht_ids": sorted(rules[MARKET_O15HT]),
        "gg_ht_ids": sorted(rules[MARKET_GGHT]),
    }
    return output, audit


def market_calibration_weight(
    market_analysis: Optional[Mapping[str, Any]],
    validation: Optional[Mapping[str, Any]],
) -> float:
    if not market_analysis or not validation or not validation.get("usable_for_signals"):
        return 0.0
    count = safe_int((market_analysis.get("current", {}) or {}).get("bookmakers_count"))
    if count < 2:
        return 0.0
    weight = 0.05 if count <= 3 else 0.08 if count <= 7 else 0.10
    aggio = str((market_analysis.get("aggio", {}) or {}).get("status", "unavailable"))
    if aggio == "high":
        weight -= 0.02
    book_strength = str((market_analysis.get("book", {}) or {}).get("strength", "none"))
    if book_strength == "medium":
        weight += 0.01
    elif book_strength == "strong":
        weight += 0.02
    return round4(clamp(weight, 0.0, 0.12))


def calibrate_market_probabilities(
    raw: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    validation: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, float], float]:
    weight = market_calibration_weight(market_analysis, validation)
    fair = ((market_analysis or {}).get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}) or {}
    if weight <= 0 or not fair:
        return ({key: round4(value) for key, value in raw.items()}, 0.0)
    calibrated = {
        key: safe_float(raw.get(key)) * (1.0 - weight) + safe_float(fair.get(key), safe_float(raw.get(key))) * weight
        for key in raw
    }
    total = sum(max(value, 0.0) for value in calibrated.values())
    if total > 0:
        calibrated = {key: value / total for key, value in calibrated.items()}
    return ({key: round4(value) for key, value in calibrated.items()}, weight)


def build_calibrated_probabilities(
    raw_probabilities: Mapping[str, Any],
    market_analyses: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    output: Dict[str, Any] = {}
    weights: Dict[str, float] = {}
    for market_key in MARKET_SELECTIONS:
        calibrated, weight = calibrate_market_probabilities(
            raw_probabilities.get(market_key, {}) or {},
            market_analyses.get(market_key),
            validation.get(market_key),
        )
        output[market_key] = calibrated
        weights[market_key] = weight
    output["over15_ft_extra"] = dict(raw_probabilities.get("over15_ft_extra", {}) or {})
    return output, weights


def model_strength(
    market_key: str,
    pick: str,
    probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    reliability_pct: float,
) -> Dict[str, Any]:
    """Forza del cerchio basata sul modello puro; il book resta informativo."""
    probability = safe_float(probabilities.get(pick))
    sorted_values = sorted((safe_float(value) for value in probabilities.values()), reverse=True)
    gap = sorted_values[0] - sorted_values[1] if len(sorted_values) > 1 else probability
    market_probability = safe_float(((market_analysis or {}).get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}).get(pick))
    edge_pp = (probability - market_probability) * 100.0 if market_probability > 0 else 0.0
    if market_key == MARKET_1X2:
        score = min(probability / 0.65, 1.0) * 55 + min(gap / 0.18, 1.0) * 25
    else:
        certainty = abs(probability - 0.5) * 2.0
        score = min(certainty, 1.0) * 58 + min(gap / 0.25, 1.0) * 12
    score += clamp(reliability_pct / 100.0, 0.0, 1.0) * 20
    strength, color = ("strong", "red") if score >= 72 else ("medium", "orange") if score >= 52 else ("low", "yellow")
    return {
        "probability": round4(probability), "probability_pct": round3(probability * 100.0),
        "market_fair_probability": round4(market_probability), "market_fair_probability_pct": round3(market_probability * 100.0),
        "edge_pp": round3(edge_pp), "score": round3(score), "strength": strength, "color": color,
        "marker_shape": "circle", "probability_gap": round4(gap), "basis": "arabsniper_raw",
    }


def build_market_markers(
    market_key: str,
    probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    reliability_pct: float,
) -> Dict[str, Any]:
    if market_key == MARKET_GGHT:
        yes_probability = safe_float(probabilities.get("yes"))
        model_pick = "yes" if yes_probability >= 0.18 and reliability_pct >= 35 else None
        if model_pick:
            model_pack = model_strength(market_key, model_pick, probabilities, market_analysis, reliability_pct)
        else:
            model_pack = {
                "probability": round4(yes_probability), "probability_pct": round3(yes_probability * 100.0),
                "market_fair_probability": 0.0, "market_fair_probability_pct": 0.0,
                "edge_pp": 0.0, "score": 0.0, "strength": "none", "color": "none",
                "marker_shape": "none", "probability_gap": 0.0, "basis": "arabsniper_raw",
            }
    else:
        model_pick = max(probabilities, key=lambda key: safe_float(probabilities[key])) if probabilities else None
        model_pack = model_strength(market_key, model_pick, probabilities, market_analysis, reliability_pct) if model_pick else {}

    book_pack = (market_analysis or {}).get("book", {}) or {}
    book_pick = book_pack.get("pick")
    selections: Dict[str, Any] = {}
    colors = {"yellow": 1, "orange": 2, "red": 3}
    reverse = {1: "yellow", 2: "orange", 3: "red"}
    for selection in MARKET_SELECTIONS[market_key]:
        model_here, book_here = selection == model_pick, selection == book_pick
        if model_here and book_here:
            shape, agreement = "circle_square", True
            combined_color = reverse[min(colors.get(model_pack.get("color"), 1), colors.get(book_pack.get("color"), 1))]
        elif model_here:
            shape, agreement, combined_color = "circle", False, model_pack.get("color", "yellow")
        elif book_here:
            shape, agreement, combined_color = "square", False, book_pack.get("color", "yellow")
        else:
            shape, agreement, combined_color = "none", False, "none"
        selections[selection] = {"shape": shape, "color": combined_color, "model": model_here, "book": book_here, "agreement": agreement}
    relation = "confirmed" if model_pick and book_pick == model_pick else "model_only" if model_pick and not book_pick else "contrast" if book_pick and model_pick != book_pick else "neutral"
    return {"model": {"pick": model_pick, **model_pack}, "book": book_pack, "relation": relation, "selections": selections}


def simplified_market_display_v110(
    market_key: str,
    market_analysis: Optional[Mapping[str, Any]],
    markers: Mapping[str, Any],
    raw_probabilities: Mapping[str, float],
    calibrated_probabilities: Mapping[str, float],
    calibration_weight: float,
    validation: Mapping[str, Any],
) -> Dict[str, Any]:
    current_odds = ((market_analysis or {}).get("current", {}) or {}).get("odds", {}) or {}
    open_odds = ((market_analysis or {}).get("open", {}) or {}).get("odds", {}) or {}
    selection_rows = ((market_analysis or {}).get("selections", {}) or {})
    fair = ((market_analysis or {}).get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}) or {}
    selections: Dict[str, Any] = {}
    for selection in MARKET_SELECTIONS[market_key]:
        movement = selection_rows.get(selection, {}) or {}
        selections[selection] = {
            "open_odd": round3(open_odds.get(selection)),
            "current_odd": round3(current_odds.get(selection)),
            "model_probability_pct": round3(safe_float(calibrated_probabilities.get(selection)) * 100.0),
            "model_probability_raw_pct": round3(safe_float(raw_probabilities.get(selection)) * 100.0),
            "book_fair_probability_pct": round3(safe_float(fair.get(selection)) * 100.0),
            "delta_odd": round3(movement.get("delta_odd")),
            "delta_fair_pp": round3(movement.get("delta_fair_pp")),
            "movement_color": (movement.get("movement", {}) or {}).get("color", "gray"),
            "movement_level": (movement.get("movement", {}) or {}).get("level", "stable"),
            "oci_as": round3((movement.get("oci_as", {}) or {}).get("value")),
            "marker": (markers.get("selections", {}) or {}).get(selection, {}),
        }
    return {
        "name": DISPLAY_MARKET_NAMES[market_key], "selection_order": list(MARKET_SELECTIONS[market_key]),
        "selections": selections, "model_pick": (markers.get("model", {}) or {}).get("pick"),
        "book_pick": (markers.get("book", {}) or {}).get("pick"), "relation": markers.get("relation"),
        "aggio_pct": round3(((market_analysis or {}).get("aggio", {}) or {}).get("current_pct")),
        "aggio_status": ((market_analysis or {}).get("aggio", {}) or {}).get("status"),
        "protection_pick": ((market_analysis or {}).get("book", {}) or {}).get("protection_pick"),
        "market_validation": dict(validation), "market_calibration_weight": round4(calibration_weight),
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

    bookmaker_markets, market_validation = extract_bookmaker_markets_with_audit(odds_payload)
    consensuses = {
        market_key: consensus
        for market_key in MARKET_SELECTIONS
        if (consensus := build_market_consensus(bookmaker_markets, market_key)) is not None
    }
    if MARKET_1X2 not in consensuses:
        return None

    timestamp = iso_now()
    fixture_state = state.setdefault("fixtures", {}).setdefault(fixture_id, {"meta": meta, "markets": {}})
    fixture_state["meta"] = meta
    market_analyses: Dict[str, Any] = {}
    for market_key, consensus in consensuses.items():
        market_state = fixture_state.setdefault("markets", {}).setdefault(market_key, {})
        analysis = update_market_state(market_state, consensus, timestamp)
        analysis["validation"] = market_validation.get(market_key, {})
        market_analyses[market_key] = analysis

    home_matches = enrich_team_matches_with_xg(client, get_team_recent_matches(client, safe_int(meta["home_id"])), safe_int(meta["home_id"]))
    away_matches = enrich_team_matches_with_xg(client, get_team_recent_matches(client, safe_int(meta["away_id"])), safe_int(meta["away_id"]))
    if len(home_matches) < 3 or len(away_matches) < 3:
        return None
    home_profile = summarize_team_matches(home_matches, "home")
    away_profile = summarize_team_matches(away_matches, "away")

    league_key = f"{meta['league_id']}::{meta['season']}"
    if league_key not in league_cache:
        league_cache[league_key] = get_league_baseline(client, safe_int(meta["league_id"]), safe_int(meta["season"]))
    league_profile = league_cache[league_key]
    h2h_profile = summarize_h2h(get_h2h_matches(client, safe_int(meta["home_id"]), safe_int(meta["away_id"])))

    lambdas = build_lambdas(home_profile, away_profile, league_profile, h2h_profile)
    raw_probabilities = probabilities_from_lambdas(lambdas)
    calibrated_probabilities, calibration_weights = build_calibrated_probabilities(raw_probabilities, market_analyses, market_validation)
    reliability = model_reliability(home_profile, away_profile, league_profile)

    markers: Dict[str, Any] = {}
    display_markets: Dict[str, Any] = {}
    for market_key in MARKET_SELECTIONS:
        raw = raw_probabilities.get(market_key, {}) or {}
        validated_analysis = market_analyses.get(market_key) if (market_validation.get(market_key, {}) or {}).get("usable_for_signals") else None
        markers[market_key] = build_market_markers(market_key, raw, validated_analysis, reliability)
        display_markets[market_key] = simplified_market_display_v110(
            market_key, market_analyses.get(market_key), markers[market_key], raw,
            calibrated_probabilities.get(market_key, {}) or {}, calibration_weights.get(market_key, 0.0),
            market_validation.get(market_key, {}) or {},
        )

    candidates: List[Dict[str, Any]] = []
    for market_key, marker_pack in markers.items():
        model = marker_pack.get("model", {}) or {}
        pick = model.get("pick")
        if not pick:
            continue
        current_odd = safe_float(((market_analyses.get(market_key, {}) or {}).get("current", {}) or {}).get("odds", {}).get(pick))
        probability = safe_float((calibrated_probabilities.get(market_key, {}) or {}).get(pick))
        raw_probability = safe_float((raw_probabilities.get(market_key, {}) or {}).get(pick))
        expected_value = probability * current_odd - 1.0 if current_odd > 1 else 0.0
        agreement_bonus = 15.0 if marker_pack.get("relation") == "confirmed" else 0.0
        ranking_score = safe_float(model.get("score")) + agreement_bonus + clamp(expected_value * 100.0, -10, 20)
        candidates.append({
            "market": market_key, "selection": pick, "odd": round3(current_odd),
            "probability_pct": round3(probability * 100.0), "probability_raw_pct": round3(raw_probability * 100.0),
            "expected_value_pct": round3(expected_value * 100.0), "relation": marker_pack.get("relation"),
            "ranking_score": round3(ranking_score), "marker": (marker_pack.get("selections", {}) or {}).get(pick, {}),
            "market_calibration_weight": calibration_weights.get(market_key, 0.0),
        })
    candidates.sort(key=lambda item: item["ranking_score"], reverse=True)

    return {
        "fixture": meta,
        "display": {
            "primary_choice": candidates[0] if candidates else None, "ranked_choices": candidates,
            "markets": display_markets,
            "legend": {
                "circle": "Scelta ArabSniper pura", "square": "Scelta del book",
                "circle_square": "ArabSniper puro e book concordano",
                "red": "Conferma forte", "orange": "Conferma media", "yellow": "Conferma bassa",
            },
        },
        "model": {
            "reliability_pct": reliability, "lambdas": lambdas,
            "probabilities_raw": raw_probabilities, "probabilities_calibrated": calibrated_probabilities,
            "probabilities": calibrated_probabilities, "market_calibration_weights": calibration_weights,
            "markers": markers,
            "calibration_note": "Il cerchio e la relazione usano il modello puro; la percentuale principale integra il book fino al 12%.",
        },
        "book": {"markets": market_analyses, "market_validation": market_validation},
        "statistics": {
            "home": home_profile, "away": away_profile, "league": league_profile, "h2h": h2h_profile,
            "xg_layer": {
                "enabled": XG_ENABLED, "home": home_profile.get("xg", {}), "away": away_profile.get("xg", {}),
                "api_calls_used": _XG_RUNTIME_CALLS, "api_call_budget": XG_MAX_CALLS_PER_RUN,
            },
            "notes": {
                "xg": "Usati soltanto xG reali restituiti da fixtures/statistics; fallback automatico ai goal quando assenti.",
                "streaks": "Le serie non generano eventi dovuti: continuita' o regressione contenuta.",
                "h2h": "Peso massimo 5%, mai usato come contatore di Over mancanti.",
            },
        },
    }


def run_market_audit(client: APIClient, target_date: str, excluded_countries: Sequence[str]) -> Dict[str, Any]:
    payload = client.get("fixtures", {"date": target_date, "timezone": "Europe/Rome"}, use_cache=False)
    excluded = set(excluded_countries)
    fixtures = [row for row in (payload or {}).get("response", []) or []
                if (row.get("fixture", {}) or {}).get("status", {}).get("short") == "NS"
                and competition_allowed((row.get("league", {}) or {}), excluded)]
    records: List[Dict[str, Any]] = []
    summary = {key: {"verified": 0, "fallback": 0, "missing": 0, "usable": 0} for key in MARKET_SELECTIONS}
    for index, fixture_row in enumerate(fixtures, start=1):
        meta = fixture_meta(fixture_row)
        log(f"  AUDIT [{index}/{len(fixtures)}] {meta['match']}")
        odds = client.get("odds", {"fixture": meta["fixture_id"]}, use_cache=False)
        books, audit = extract_bookmaker_markets_with_audit(odds or {})
        for key in MARKET_SELECTIONS:
            status = (audit.get(key, {}) or {}).get("status", "missing")
            summary[key]["verified" if status == "verified" else "fallback" if status == "name_fallback" else "missing"] += 1
            if (audit.get(key, {}) or {}).get("usable_for_signals"):
                summary[key]["usable"] += 1
        records.append({"fixture": meta, "market_validation": audit, "bookmakers_total": len(books)})
    result = {
        "engine_version": VERSION, "generated_at": iso_now(), "date": target_date,
        "fixtures_audited": len(records), "catalog_rules": _catalog_rules(),
        "summary": summary, "fixtures": records,
    }
    # I set del catalogo non sono serializzabili: convertiamoli.
    result["catalog_rules"] = {
        key: sorted(value) if isinstance(value, set) else value
        for key, value in result["catalog_rules"].items()
        if key != "names"
    }
    atomic_write_json(MARKET_AUDIT_FILE, result)
    return result


_scan_dates_v101 = scan_dates


def scan_dates(client: APIClient, target_dates: Sequence[str], excluded_countries: Sequence[str]) -> Dict[str, Any]:
    output = _scan_dates_v101(client, target_dates, excluded_countries)
    save_xg_cache()

    days_payload = list(output.get("days", []) or [])
    total_api_fixtures = sum(safe_int(day.get("api_fixtures_found"), 0) for day in days_payload)
    total_matches = sum(len(day.get("matches", []) or []) for day in days_payload)
    error_days = [
        day for day in days_payload
        if str(day.get("refresh_status", "")).startswith("api_error")
        or str(day.get("refresh_status", "")).startswith("preserved_api_error")
    ]

    # Un Night Scan globale con zero fixture su tutti i giorni è quasi sempre
    # quota API esaurita, chiave non valida o risposta API anomala.
    # Falliamo PRIMA del commit GitHub, evitando di pubblicare altri JSON vuoti.
    if len(target_dates) >= 2 and total_api_fixtures == 0 and total_matches == 0:
        detail = client.last_error or {"reason": "all_requested_days_empty"}
        log(f"❌ API GUARD: zero fixture su {len(target_dates)} giorni. Dettaglio: {detail}")
        raise RuntimeError(
            "API-Sports non ha restituito fixture per nessuno dei giorni richiesti. "
            "Controllare quota richieste, abbonamento e log API. I JSON online non vengono aggiornati."
        )

    if error_days and len(error_days) == len(days_payload) and total_matches == 0:
        detail = client.last_error or {"reason": "all_days_api_error"}
        log(f"❌ API GUARD: tutti i giorni in errore. Dettaglio: {detail}")
        raise RuntimeError(
            "Tutte le richieste fixture sono fallite. I JSON online non vengono aggiornati."
        )

    output["version"] = VERSION
    output["api_health"] = {
        "requests_this_run": getattr(client, "request_count", 0),
        "last_error": client.last_error,
        "total_api_fixtures": total_api_fixtures,
        "total_output_matches": total_matches,
        "guard": "passed",
    }
    output["method"] = {
        "book": "mediana multi-bookmaker + validazione bet_id/catalogo + probabilita fair + aggio + OCI-AS",
        "model": "Poisson con casa/trasferta, forma, campionato, H2H max 5% e xG reali dinamici",
        "calibration": "probabilita calibrata con book 0-12%; marker e concordanza basati sul modello puro",
        "markets": "1X2 bet_id 1; Over 2.5 FT bet_id 5; O1.5 PT e GG PT verificati da odds/bets",
        "markers": "cerchio ArabSniper puro, quadrato book, doppio bordo concordanza",
        "retention": "le partite analizzate restano visibili dopo il calcio d'inizio; errori API non svuotano i JSON",
        "legacy_labels_removed": ["GOLD", "MARKET", "BOOST", "PROBE", "OVER_TAG", "PT_TAG"],
    }
    atomic_write_json(OUTPUT_FILE, output)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ArabSniper Quote Engine V1.1.4")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--today", action="store_true", help="Scansiona il giorno corrente")
    mode.add_argument("--night", action="store_true", help="Scansione rolling da oggi")
    mode.add_argument("--date", type=str, help="Data iniziale YYYY-MM-DD")
    mode.add_argument("--catalog", action="store_true", help="Aggiorna solo il catalogo odds/bets")
    mode.add_argument("--audit-markets", action="store_true", help="Verifica i mercati quota sulle fixture del giorno")
    parser.add_argument("--audit-date", type=str, default="", help="Data audit YYYY-MM-DD, default oggi")
    parser.add_argument("--days", type=int, default=5, help="Numero giorni per --night/--date")
    parser.add_argument("--github", action="store_true", help="Pubblica output, stato e file day su GitHub")
    parser.add_argument("--include-country", action="append", default=[], help="Rimuove un paese dalla blacklist")
    return parser.parse_args()


def main() -> None:
    global XG_MAX_CALLS_PER_RUN
    args = parse_args()

    # I Fast Scan orari riusano la cache xG già costruita dal Night Scan,
    # evitando decine di chiamate statistiche ogni ora.
    if args.today and "ARAB_XG_MAX_CALLS_PER_RUN" not in os.environ:
        XG_MAX_CALLS_PER_RUN = 0

    api_key = str(os.getenv("API_SPORTS_KEY", "")).strip()
    if not api_key:
        log("❌ API_SPORTS_KEY mancante")
        raise SystemExit(2)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = APIClient(api_key=api_key)

    if args.catalog:
        global _MARKET_RULES_CACHE
        catalog = fetch_bet_catalog(client)
        _MARKET_RULES_CACHE = None
        log(f"✅ Catalogo mercati aggiornato: {len(catalog.get('bets', []))} voci")
        if args.github:
            log(f"GitHub catalog: {upload_json_to_github(REMOTE_BET_CATALOG_FILE, catalog, 'Update ArabSniper bet catalog')}")
        return

    excluded = sorted(DEFAULT_EXCLUDED_COUNTRIES - set(args.include_country))
    if args.audit_markets:
        audit_date = args.audit_date or now_rome().date().isoformat()
        try:
            date.fromisoformat(audit_date)
        except Exception:
            log("❌ Formato --audit-date non valido. Usa YYYY-MM-DD")
            raise SystemExit(2)
        audit = run_market_audit(client, audit_date, excluded)
        log(f"✅ Market audit scritto: {MARKET_AUDIT_FILE}")
        if args.github:
            log(f"GitHub audit: {upload_json_to_github(REMOTE_MARKET_AUDIT_FILE, audit, 'Update ArabSniper market audit')}")
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
    output = scan_dates(client, get_target_dates(start, days), excluded)
    log(f"✅ Output scritto: {OUTPUT_FILE}")
    log(f"✅ Stato quote scritto: {STATE_FILE}")
    log(f"✅ Cache xG scritta: {XG_CACHE_FILE}")

    if args.github:
        log(f"GitHub output: {upload_json_to_github(REMOTE_OUTPUT_FILE, output, 'Update ArabSniper quote dashboard')}")
        log(f"GitHub state: {upload_json_to_github(REMOTE_STATE_FILE, read_json(STATE_FILE, {}), 'Update ArabSniper quote state')}")
        if XG_CACHE_FILE.exists():
            log(f"GitHub xG cache: {upload_json_to_github(REMOTE_XG_CACHE_FILE, read_json(XG_CACHE_FILE, {}), 'Update ArabSniper xG cache')}")
        if PREDICTION_AUDIT_FILE.exists():
            log(f"GitHub prediction audit: {upload_json_to_github(REMOTE_PREDICTION_AUDIT_FILE, read_json(PREDICTION_AUDIT_FILE, {}), 'Update ArabSniper prediction audit')}")
        for day_payload in output.get("days", []) or []:
            day_num = safe_int(day_payload.get("day"), 0)
            if day_num in REMOTE_DAY_FILES:
                log(f"GitHub day{day_num}: {upload_json_to_github(REMOTE_DAY_FILES[day_num], day_payload, f'Update ArabSniper quote day {day_num}')}")


# =========================================================
# ENHANCEMENT V1.1.3 — QUALITA' COMPETIZIONI, GATE SEGNALI,
# STATISTICHE TIRI E AUDIT RISULTATI
# =========================================================
VERSION = "1.1.4"
PREDICTION_AUDIT_FILE = DATA_DIR / "arab_quote_prediction_audit.json"
REMOTE_PREDICTION_AUDIT_FILE = "data/arab_quote_prediction_audit.json"
STRICT_COMPETITIONS = _bool_env(os.getenv("ARAB_STRICT_COMPETITIONS", "1"), True)

CORE_COUNTRIES = {
    "england", "italy", "spain", "germany", "france", "netherlands",
    "portugal", "belgium", "scotland", "turkey", "brazil", "argentina",
    "usa", "mexico",
}
SECONDARY_COUNTRIES = {
    "austria", "switzerland", "denmark", "norway", "sweden", "poland",
    "czech republic", "croatia", "serbia", "romania", "ukraine", "greece",
    "hungary", "slovenia", "slovakia", "bulgaria", "finland", "iceland",
    "ireland", "northern ireland", "wales", "cyprus", "israel",
    "colombia", "chile", "uruguay", "ecuador", "paraguay", "peru",
}
ELITE_ASIA_OCEANIA = {
    "japan", "south korea", "korea republic", "australia", "saudi arabia", "qatar",
}
INTERNATIONAL_COUNTRIES = {"world", "europe", "international"}
EXTRA_TRUSTED_COUNTRIES = {
    normalized_text(item).replace("-", " ")
    for item in str(os.getenv("ARAB_EXTRA_COUNTRIES", "")).split(",")
    if item.strip()
}

HARD_MINOR_TOKENS = (
    "u17", "u18", "u19", "u20", "u21", "u22", "u23", "youth", "women",
    "friendly", "friendlies", "reserve", "reserves", "amateur", "academy",
    "regional", "state league", "estadual", "npl", "copa do nordeste", "copa verde", "carioca", "paulista", "mineiro",
    "gaucho", "paranaense", "catarinense", "baiano", "pernambucano", "potiguar",
    "cearense", "goiano", "paraense", "paraibano", "sergipano", "amazonense",
    "brasiliense", "capixaba", "alagoano", "maranhense", "piauiense", "rondoniense",
    "roraimense", "tocantinense", "copa paulista", "copa rio", "federal a",
    "torneo regional", "primera b metropolitana", "primera c", "primera d",
    "national 2", "national 3", "non league", "development league", "premier reserve",
)
THIRD_TIER_TOKENS = (
    "serie c", "serie d", "league one", "league two", "national league",
    "3. liga", "3 liga", "liga 3", "third division", "3rd division",
    "segunda federacion", "segunda división rfef", "primera federacion",
    "championnat national", "division 3", "fourth division", "4th division",
)
SECOND_TIER_TOKENS = (
    "serie b", "segunda division", "segunda división", "liga 2", "ligue 2",
    "2. bundesliga", "eerste divisie", "segunda liga", "division 2", "2nd division",
    "second division", "championship",
)
MAJOR_INTERNATIONAL_TOKENS = (
    "world cup", "uefa champions league", "uefa europa league", "conference league",
    "copa libertadores", "copa sudamericana", "nations league", "euro championship",
    "copa america", "afc champions league", "club world cup",
)


def _country_key(value: Any) -> str:
    return normalized_text(value).replace("-", " ")


def competition_profile(league: Mapping[str, Any]) -> Dict[str, Any]:
    """Classifica la competizione prima di spendere chiamate quote/statistiche."""
    name = normalized_text(league.get("name"))
    country = _country_key(league.get("country"))
    if not name:
        return {"allowed": False, "score": 0, "tier": "blocked", "reason": "league_name_missing"}
    if any(token in name for token in HARD_MINOR_TOKENS):
        return {"allowed": False, "score": 10, "tier": "blocked", "reason": "minor_or_regional"}
    if any(token in name for token in THIRD_TIER_TOKENS):
        return {"allowed": False, "score": 25, "tier": "blocked", "reason": "third_tier_or_lower"}

    is_qualifier = any(token in name for token in ("qualif", "preliminary", "preliminar"))
    is_major_international = any(token in name for token in MAJOR_INTERNATIONAL_TOKENS)
    if country in INTERNATIONAL_COUNTRIES or is_major_international:
        score = 70 if is_qualifier else 88
        return {
            "allowed": True, "score": score,
            "tier": "international_qualifier" if is_qualifier else "international_major",
            "reason": "major_international",
        }

    if country in CORE_COUNTRIES or country in EXTRA_TRUSTED_COUNTRIES:
        if any(token in name for token in SECOND_TIER_TOKENS):
            score = 72
            tier = "major_second_tier"
        elif any(token in name for token in ("cup", "copa", "coppa", "pokal", "coupe")):
            score = 76
            tier = "major_cup"
        else:
            score = 84
            tier = "major_top_tier"
        return {"allowed": True, "score": score, "tier": tier, "reason": "trusted_country"}

    if country in SECONDARY_COUNTRIES:
        if any(token in name for token in SECOND_TIER_TOKENS):
            return {"allowed": False, "score": 42, "tier": "blocked", "reason": "secondary_country_lower_tier"}
        return {"allowed": True, "score": 68, "tier": "secondary_top_tier", "reason": "secondary_country"}

    if country in ELITE_ASIA_OCEANIA:
        if any(token in name for token in SECOND_TIER_TOKENS):
            return {"allowed": False, "score": 38, "tier": "blocked", "reason": "asia_lower_tier"}
        return {"allowed": True, "score": 64, "tier": "elite_asia_top_tier", "reason": "selected_asia"}

    if STRICT_COMPETITIONS:
        return {"allowed": False, "score": 20, "tier": "blocked", "reason": "country_not_trusted"}
    return {"allowed": True, "score": 52, "tier": "unclassified", "reason": "strict_mode_off"}


def competition_allowed(league: Mapping[str, Any], excluded_countries: Iterable[str] = ()) -> bool:
    excluded = {_country_key(item) for item in excluded_countries}
    if _country_key(league.get("country")) in excluded:
        return False
    return bool(competition_profile(league).get("allowed"))


def is_blacklisted_league(name: Any) -> bool:
    text = normalized_text(name)
    return any(token in text for token in HARD_MINOR_TOKENS + THIRD_TIER_TOKENS)


# Conserva le implementazioni V1.1.2 e le estende senza duplicare le chiamate API.
_fixture_meta_v112 = fixture_meta
_parse_fixture_xg_statistics_v112 = parse_fixture_xg_statistics
_fetch_fixture_xg_v112 = fetch_fixture_xg
_enrich_team_matches_with_xg_v112 = enrich_team_matches_with_xg
_summarize_team_matches_v112 = summarize_team_matches
_build_lambdas_v112 = build_lambdas
_build_fixture_output_v112 = build_fixture_output
_preserve_existing_match_v112 = _preserve_existing_match
_scan_dates_v112 = scan_dates


def fixture_meta(fixture_row: Mapping[str, Any]) -> Dict[str, Any]:
    meta = _fixture_meta_v112(fixture_row)
    profile = competition_profile(fixture_row.get("league", {}) or {})
    meta["competition_quality_score"] = safe_int(profile.get("score"), 0)
    meta["competition_tier"] = profile.get("tier")
    meta["competition_reason"] = profile.get("reason")
    return meta


def _stat_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    parsed = safe_float(str(value).replace("%", ""), -1.0)
    return round3(parsed) if parsed >= 0 else None


def parse_fixture_xg_statistics(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Estrae xG, tiri nello specchio, tiri totali e grandi occasioni dallo stesso endpoint."""
    teams: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("response", []) or []:
        team = row.get("team", {}) or {}
        team_id = str(team.get("id", ""))
        if not team_id:
            continue
        record: Dict[str, Any] = {
            "team_id": team.get("id"), "team_name": team.get("name"),
            "xg": None, "shots_on_target": None, "total_shots": None,
            "big_chances": None, "matched_types": {},
        }
        for stat in row.get("statistics", []) or []:
            stat_type = normalized_text(stat.get("type")).replace("_", " ")
            value = _stat_number(stat.get("value"))
            if value is None:
                continue
            if _is_xg_stat_type(stat_type):
                record["xg"] = value
                record["matched_types"]["xg"] = stat.get("type")
            elif stat_type in {"shots on goal", "shots on target", "shots on goals"}:
                record["shots_on_target"] = value
                record["matched_types"]["shots_on_target"] = stat.get("type")
            elif stat_type in {"total shots", "shots total"}:
                record["total_shots"] = value
                record["matched_types"]["total_shots"] = stat.get("type")
            elif "big chance" in stat_type or "clear cut chance" in stat_type:
                record["big_chances"] = value
                record["matched_types"]["big_chances"] = stat.get("type")
        teams[team_id] = record
    available = sum(1 for row in teams.values() if row.get("xg") is not None) >= 2
    shots_available = sum(1 for row in teams.values() if row.get("shots_on_target") is not None) >= 2
    return {"available": available, "shots_available": shots_available, "teams": teams, "schema": 2}


def fetch_fixture_xg(client: APIClient, fixture_id: Any, home_id: Any, away_id: Any) -> Dict[str, Any]:
    global _XG_RUNTIME_CALLS, _XG_DIRTY
    key = str(fixture_id)
    cached = (_XG_STORE.get("fixtures", {}) or {}).get(key)
    cache_complete = isinstance(cached, dict) and safe_int(cached.get("stats_schema"), 0) >= 2
    if cache_complete:
        return cached
    if isinstance(cached, dict) and (not XG_ENABLED or _XG_RUNTIME_CALLS >= XG_MAX_CALLS_PER_RUN):
        return cached

    empty = {
        "fixture_id": key, "available": False, "home_xg": None, "away_xg": None,
        "home_stats": {}, "away_stats": {}, "source": "unavailable",
        "checked_at": iso_now(), "stats_schema": 2,
    }
    if not XG_ENABLED or _XG_RUNTIME_CALLS >= XG_MAX_CALLS_PER_RUN:
        empty["source"] = "disabled" if not XG_ENABLED else "run_budget_exhausted"
        return empty

    _XG_RUNTIME_CALLS += 1
    payload = client.get("fixtures/statistics", {"fixture": fixture_id}, use_cache=True)
    parsed = parse_fixture_xg_statistics(payload or {})
    teams = parsed.get("teams", {}) or {}
    home_stats = dict(teams.get(str(home_id), {}) or {})
    away_stats = dict(teams.get(str(away_id), {}) or {})
    home_xg, away_xg = home_stats.get("xg"), away_stats.get("xg")
    record = {
        "fixture_id": key,
        "available": home_xg is not None and away_xg is not None,
        "shots_available": home_stats.get("shots_on_target") is not None and away_stats.get("shots_on_target") is not None,
        "home_xg": home_xg, "away_xg": away_xg,
        "home_stats": home_stats, "away_stats": away_stats,
        "source": "api_fixture_statistics" if payload else "api_statistics_unavailable",
        "checked_at": iso_now(), "stats_schema": 2,
    }
    _XG_STORE.setdefault("fixtures", {})[key] = record
    _XG_DIRTY = True
    return record


def enrich_team_matches_with_xg(client: APIClient, rows: Sequence[Mapping[str, Any]], team_id: int) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = [dict(row) for row in rows]
    for index, row in enumerate(enriched):
        if index >= XG_MATCHES_PER_TEAM:
            break
        fixture_id = row.get("fixture_id")
        if not fixture_id:
            continue
        stats = fetch_fixture_xg(client, fixture_id, row.get("home_id"), row.get("away_id"))
        home_stats = stats.get("home_stats", {}) or {}
        away_stats = stats.get("away_stats", {}) or {}
        row["home_xg"], row["away_xg"] = stats.get("home_xg"), stats.get("away_xg")
        row["xg_available"] = bool(stats.get("available"))
        row["xg_source"] = stats.get("source")
        is_home = str(row.get("home_id")) == str(team_id)
        team_stats, opp_stats = (home_stats, away_stats) if is_home else (away_stats, home_stats)
        if stats.get("available"):
            row["team_xg_for"] = team_stats.get("xg")
            row["team_xg_against"] = opp_stats.get("xg")
        for key in ("shots_on_target", "total_shots", "big_chances"):
            if team_stats.get(key) is not None:
                row[f"team_{key}_for"] = team_stats.get(key)
            if opp_stats.get(key) is not None:
                row[f"team_{key}_against"] = opp_stats.get(key)
    return enriched


def _weighted_optional(rows: Sequence[Mapping[str, Any]], key: str) -> Tuple[float, int]:
    vals: List[float] = []
    weights: List[float] = []
    row_weights = recency_weights(rows)
    for row, weight in zip(rows, row_weights):
        if row.get(key) is None:
            continue
        value = safe_float(row.get(key), -1.0)
        if value < 0:
            continue
        vals.append(value)
        weights.append(weight)
    return (weighted_mean(vals, weights), len(vals)) if vals else (0.0, 0)


def summarize_team_matches(rows: Sequence[Mapping[str, Any]], expected_side: str) -> Dict[str, Any]:
    profile = _summarize_team_matches_v112(rows, expected_side)
    all_rows = list(rows)
    side_rows = [row for row in all_rows if row.get("side") == expected_side]
    context_weight = safe_float(profile.get("context_weight"), 0.0)
    metrics = profile.setdefault("metrics", {})
    shot_layer: Dict[str, Any] = {"available": False, "sample_all": 0, "sample_context": 0}
    for metric_key in ("shots_on_target", "total_shots", "big_chances"):
        for direction in ("for", "against"):
            row_key = f"team_{metric_key}_{direction}"
            all_value, all_sample = _weighted_optional(all_rows, row_key)
            side_value, side_sample = _weighted_optional(side_rows, row_key)
            blended = all_value * (1.0 - context_weight) + (side_value if side_sample else all_value) * context_weight
            metrics[f"{metric_key}_{direction}"] = round3(blended)
            shot_layer[f"{metric_key}_{direction}"] = round3(blended)
            shot_layer[f"{metric_key}_{direction}_sample"] = max(all_sample, side_sample)
            if metric_key == "shots_on_target" and direction == "for":
                shot_layer["sample_all"] = all_sample
                shot_layer["sample_context"] = side_sample
    shot_layer["available"] = safe_int(shot_layer.get("shots_on_target_for_sample"), 0) > 0
    sample_pair = min(
        safe_int(shot_layer.get("shots_on_target_for_sample"), 0),
        safe_int(shot_layer.get("shots_on_target_against_sample"), 0),
    )
    shot_layer["weight_candidate"] = round3(0.15 if sample_pair >= 8 else 0.10 if sample_pair >= 4 else 0.05 if sample_pair >= 2 else 0.0)
    shot_layer["source"] = "api_fixture_statistics" if shot_layer["available"] else "unavailable"
    profile["shot_quality"] = shot_layer
    return profile


def build_lambdas(home_profile: Mapping[str, Any], away_profile: Mapping[str, Any], league: Mapping[str, Any], h2h: Mapping[str, Any]) -> Dict[str, Any]:
    result = _build_lambdas_v112(home_profile, away_profile, league, h2h)
    home_shots = home_profile.get("shot_quality", {}) or {}
    away_shots = away_profile.get("shot_quality", {}) or {}
    hs_for = safe_float(home_shots.get("shots_on_target_for"), 0.0)
    hs_against = safe_float(home_shots.get("shots_on_target_against"), 0.0)
    as_for = safe_float(away_shots.get("shots_on_target_for"), 0.0)
    as_against = safe_float(away_shots.get("shots_on_target_against"), 0.0)
    pair_sample = min(
        safe_int(home_shots.get("shots_on_target_for_sample"), 0),
        safe_int(home_shots.get("shots_on_target_against_sample"), 0),
        safe_int(away_shots.get("shots_on_target_for_sample"), 0),
        safe_int(away_shots.get("shots_on_target_against_sample"), 0),
    )
    shot_weight = 0.0
    shot_home = safe_float(result.get("home_ft"), 0.0)
    shot_away = safe_float(result.get("away_ft"), 0.0)
    if min(hs_for, hs_against, as_for, as_against) > 0 and pair_sample >= 2:
        shot_home = clamp(mean((hs_for, as_against)) * 0.30, 0.20, 3.20)
        shot_away = clamp(mean((as_for, hs_against)) * 0.30, 0.15, 2.90)
        shot_weight = 0.15 if pair_sample >= 8 else 0.10 if pair_sample >= 4 else 0.05
        if bool((result.get("xg_only", {}) or {}).get("available")):
            shot_weight *= 0.55  # evita doppio conteggio quando xG è già presente
        base_home, base_away = safe_float(result.get("home_ft")), safe_float(result.get("away_ft"))
        final_home = base_home * (1.0 - shot_weight) + shot_home * shot_weight
        final_away = base_away * (1.0 - shot_weight) + shot_away * shot_weight
        old_total = max(base_home + base_away, 0.10)
        new_total = max(final_home + final_away, 0.10)
        ht_factor = clamp(math.sqrt(new_total / old_total), 0.95, 1.05)
        result["home_ft"], result["away_ft"] = round3(final_home), round3(final_away)
        result["total_ft"] = round3(final_home + final_away)
        result["home_ht"] = round3(safe_float(result.get("home_ht")) * ht_factor)
        result["away_ht"] = round3(safe_float(result.get("away_ht")) * ht_factor)
        result["total_ht"] = round3(safe_float(result.get("home_ht")) + safe_float(result.get("away_ht")))
    result["shots_layer"] = {
        "available": shot_weight > 0, "sample_pair": pair_sample, "weight": round3(shot_weight),
        "home_ft": round3(shot_home), "away_ft": round3(shot_away),
        "conversion_assumption_goal_per_sot": 0.30,
    }
    result.setdefault("components", {})["shots_weight"] = round3(shot_weight)
    return result


def _binary_empirical(home: Mapping[str, Any], away: Mapping[str, Any], league: Mapping[str, Any], h2h: Mapping[str, Any], key: str) -> float:
    hm, am = home.get("metrics", {}) or {}, away.get("metrics", {}) or {}
    values = [safe_float(hm.get(key)), safe_float(am.get(key)), safe_float(league.get(key))]
    weights = [0.30, 0.30, 0.35]
    if safe_int(h2h.get("sample"), 0) >= 2:
        values.append(safe_float(h2h.get(key)))
        weights.append(0.05)
    return clamp(weighted_mean(values, weights), 0.01, 0.99)


def blend_empirical_probabilities(poisson: Mapping[str, Any], home: Mapping[str, Any], away: Mapping[str, Any], league: Mapping[str, Any], h2h: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    output = json.loads(json.dumps(poisson))
    layer: Dict[str, Any] = {}
    mapping = {
        MARKET_O25: "over25_rate",
        MARKET_O15HT: "over15ht_rate",
        MARKET_GGHT: "gg_ht_rate",
    }
    context_sample = min(safe_int(home.get("sample_context")), safe_int(away.get("sample_context")))
    weight = 0.32 if context_sample >= 5 else 0.26 if context_sample >= 3 else 0.18
    for market_key, rate_key in mapping.items():
        empirical = _binary_empirical(home, away, league, h2h, rate_key)
        positive_key = "yes" if market_key == MARKET_GGHT else "over"
        negative_key = "no" if market_key == MARKET_GGHT else "under"
        poisson_positive = safe_float((poisson.get(market_key, {}) or {}).get(positive_key))
        blended = clamp(poisson_positive * (1.0 - weight) + empirical * weight, 0.01, 0.99)
        output[market_key] = {positive_key: round4(blended), negative_key: round4(1.0 - blended)}
        layer[market_key] = {
            "poisson_positive": round4(poisson_positive), "empirical_positive": round4(empirical),
            "blend_weight": round4(weight), "blended_positive": round4(blended),
        }
    return output, layer


def recommendation_gate(
    market_key: str,
    pick: Optional[str],
    raw_probabilities: Mapping[str, float],
    calibrated_probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    validation: Mapping[str, Any],
    reliability_pct: float,
    meta: Mapping[str, Any],
    relation: str,
    empirical_layer: Mapping[str, Any],
) -> Dict[str, Any]:
    reasons: List[str] = []
    if not pick:
        return {"eligible": False, "score": 0.0, "grade": "NO", "reasons": ["nessuna_selezione"]}
    odd = safe_float(((market_analysis or {}).get("current", {}) or {}).get("odds", {}).get(pick), 0.0)
    probability = safe_float(calibrated_probabilities.get(pick), 0.0)
    raw_probability = safe_float(raw_probabilities.get(pick), 0.0)
    fair = safe_float(((market_analysis or {}).get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}).get(pick), 0.0)
    expected_value = probability * odd - 1.0 if odd > 1 else -1.0
    disagreement_pp = abs(raw_probability - fair) * 100.0 if fair > 0 else 0.0
    quality = safe_int(meta.get("competition_quality_score"), 0)
    books = safe_int(validation.get("bookmakers_found"), 0)

    thresholds = {
        (MARKET_1X2, "1"): 0.46, (MARKET_1X2, "2"): 0.46, (MARKET_1X2, "x"): 0.32,
        (MARKET_O25, "over"): 0.57, (MARKET_O25, "under"): 0.61,
        (MARKET_O15HT, "over"): 0.50, (MARKET_O15HT, "under"): 0.70,
        (MARKET_GGHT, "yes"): 0.20, (MARKET_GGHT, "no"): 0.80,
    }
    threshold = thresholds.get((market_key, pick), 0.55)
    if relation == "confirmed":
        threshold = max(0.01, threshold - 0.015)

    if not validation.get("usable_for_signals"):
        reasons.append("mercato_non_validato")
    if odd <= 1.0:
        reasons.append("quota_assente")
    if reliability_pct < 52:
        reasons.append("affidabilita_bassa")
    if quality < 58:
        reasons.append("competizione_debole")
    if probability < threshold:
        reasons.append("probabilita_insufficiente")
    if expected_value < (-0.005 if relation == "confirmed" else 0.01):
        reasons.append("nessun_valore_sulla_quota")
    if disagreement_pp > 20:
        reasons.append("modello_book_troppo_distanti")

    empirical = empirical_layer.get(market_key, {}) or {}
    empirical_positive = safe_float(empirical.get("empirical_positive"), 0.0)
    if market_key == MARKET_O25:
        if pick == "over" and empirical_positive < 0.48:
            reasons.append("storico_over_non_conferma")
        if pick == "under" and empirical_positive > 0.54:
            reasons.append("storico_under_non_conferma")
    elif market_key == MARKET_O15HT:
        if pick == "over" and empirical_positive < 0.36:
            reasons.append("storico_pt_non_conferma")
        if pick == "under" and empirical_positive > 0.39:
            reasons.append("storico_under_pt_non_conferma")
    elif market_key == MARKET_GGHT and pick == "yes" and empirical_positive < 0.13:
        reasons.append("storico_gg_pt_non_conferma")

    probability_score = clamp((probability - threshold + 0.06) / 0.18, 0.0, 1.0) * 25.0
    reliability_score = clamp((reliability_pct - 45.0) / 35.0, 0.0, 1.0) * 20.0
    quality_score = clamp((quality - 50.0) / 35.0, 0.0, 1.0) * 15.0
    ev_score = clamp((expected_value + 0.01) / 0.12, 0.0, 1.0) * 20.0
    coverage_score = clamp(books / 8.0, 0.0, 1.0) * 10.0
    agreement_score = 10.0 if relation == "confirmed" else 4.0 if relation == "model_only" else 0.0
    disagreement_penalty = clamp((disagreement_pp - 10.0) / 12.0, 0.0, 1.0) * 15.0
    score = clamp(probability_score + reliability_score + quality_score + ev_score + coverage_score + agreement_score - disagreement_penalty, 0.0, 100.0)
    eligible = not reasons and score >= 62.0
    grade = "A" if eligible and score >= 78 else "B" if eligible and score >= 68 else "C" if eligible else "NO"
    return {
        "eligible": eligible, "score": round3(score), "grade": grade, "reasons": reasons,
        "threshold_probability_pct": round3(threshold * 100.0),
        "probability_pct": round3(probability * 100.0), "raw_probability_pct": round3(raw_probability * 100.0),
        "expected_value_pct": round3(expected_value * 100.0), "book_disagreement_pp": round3(disagreement_pp),
        "competition_quality_score": quality, "bookmakers": books,
    }


def suppress_model_marker(marker_pack: MutableMapping[str, Any]) -> None:
    model = marker_pack.setdefault("model", {})
    model["pick"] = None
    model["strength"] = "none"
    model["color"] = "none"
    model["marker_shape"] = "none"
    book = marker_pack.get("book", {}) or {}
    book_pick = book.get("pick")
    for selection, marker in (marker_pack.get("selections", {}) or {}).items():
        if not isinstance(marker, dict):
            continue
        marker["model"] = False
        marker["agreement"] = False
        if selection == book_pick:
            marker["shape"] = "square"
            marker["book"] = True
            marker["color"] = book.get("color", "yellow")
        else:
            marker["shape"] = "none"
            marker["color"] = "none"
    marker_pack["relation"] = "book_only" if book_pick else "neutral"


def build_fixture_output(client: APIClient, fixture_row: Mapping[str, Any], state: MutableMapping[str, Any], league_cache: MutableMapping[str, Any]) -> Optional[Dict[str, Any]]:
    output = _build_fixture_output_v112(client, fixture_row, state, league_cache)
    if not output:
        return None
    meta = output.get("fixture", {}) or {}
    stats = output.get("statistics", {}) or {}
    model = output.get("model", {}) or {}
    book = output.get("book", {}) or {}
    market_analyses = book.get("markets", {}) or {}
    validation = book.get("market_validation", {}) or {}
    poisson_probabilities = model.get("probabilities_raw", {}) or {}
    raw_probabilities, empirical_layer = blend_empirical_probabilities(
        poisson_probabilities,
        stats.get("home", {}) or {}, stats.get("away", {}) or {},
        stats.get("league", {}) or {}, stats.get("h2h", {}) or {},
    )
    calibrated_probabilities, calibration_weights = build_calibrated_probabilities(raw_probabilities, market_analyses, validation)
    reliability = safe_float(model.get("reliability_pct"), 0.0)

    markers: Dict[str, Any] = {}
    display_markets: Dict[str, Any] = {}
    candidates: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for market_key in MARKET_SELECTIONS:
        raw = raw_probabilities.get(market_key, {}) or {}
        calibrated = calibrated_probabilities.get(market_key, {}) or {}
        valid_analysis = market_analyses.get(market_key) if (validation.get(market_key, {}) or {}).get("usable_for_signals") else None
        marker_pack = build_market_markers(market_key, raw, valid_analysis, reliability)
        provisional_pick = (marker_pack.get("model", {}) or {}).get("pick")
        gate = recommendation_gate(
            market_key, provisional_pick, raw, calibrated, market_analyses.get(market_key),
            validation.get(market_key, {}) or {}, reliability, meta,
            marker_pack.get("relation", "neutral"), empirical_layer,
        )
        if not gate.get("eligible"):
            suppress_model_marker(marker_pack)
        markers[market_key] = marker_pack
        market_display = simplified_market_display_v110(
            market_key, market_analyses.get(market_key), marker_pack, raw, calibrated,
            calibration_weights.get(market_key, 0.0), validation.get(market_key, {}) or {},
        )
        market_display["recommendation_gate"] = gate
        market_display["empirical_layer"] = empirical_layer.get(market_key, {})
        display_markets[market_key] = market_display

        item = {
            "market": market_key, "selection": provisional_pick,
            "gate": gate, "relation": marker_pack.get("relation"),
        }
        if not gate.get("eligible"):
            rejected.append(item)
            continue
        odd = safe_float(((market_analyses.get(market_key, {}) or {}).get("current", {}) or {}).get("odds", {}).get(provisional_pick))
        probability = safe_float(calibrated.get(provisional_pick))
        raw_probability = safe_float(raw.get(provisional_pick))
        expected_value = probability * odd - 1.0 if odd > 1 else 0.0
        ranking_score = safe_float(gate.get("score")) + (6.0 if marker_pack.get("relation") == "confirmed" else 0.0)
        candidates.append({
            "market": market_key, "selection": provisional_pick, "odd": round3(odd),
            "probability_pct": round3(probability * 100.0), "probability_raw_pct": round3(raw_probability * 100.0),
            "expected_value_pct": round3(expected_value * 100.0), "relation": marker_pack.get("relation"),
            "ranking_score": round3(ranking_score), "recommendation_score": round3(gate.get("score")),
            "grade": gate.get("grade"), "gate": gate,
            "marker": (marker_pack.get("selections", {}) or {}).get(provisional_pick, {}),
            "market_calibration_weight": calibration_weights.get(market_key, 0.0),
        })
    candidates.sort(key=lambda item: item.get("ranking_score", 0.0), reverse=True)

    output.setdefault("display", {})["primary_choice"] = candidates[0] if candidates else None
    output["display"]["ranked_choices"] = candidates
    output["display"]["rejected_choices"] = rejected
    output["display"]["markets"] = display_markets
    model["probabilities_poisson"] = poisson_probabilities
    model["probabilities_raw"] = raw_probabilities
    model["probabilities_calibrated"] = calibrated_probabilities
    model["probabilities"] = calibrated_probabilities
    model["empirical_layer"] = empirical_layer
    model["market_calibration_weights"] = calibration_weights
    model["markers"] = markers
    model["selection_policy"] = "Solo segnali che superano validazione mercato, affidabilità, qualità competizione, probabilità, EV e coerenza storica."
    output["model"] = model
    stats["shots_layer"] = {
        "home": (stats.get("home", {}) or {}).get("shot_quality", {}),
        "away": (stats.get("away", {}) or {}).get("shot_quality", {}),
        "lambda": (model.get("lambdas", {}) or {}).get("shots_layer", {}),
    }
    output["statistics"] = stats
    return output


def _result_payload(status_row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = status_row.get("fixture", {}) or {}
    status = fixture.get("status", {}) or {}
    if status.get("short") not in {"FT", "AET", "PEN"}:
        return None
    score = status_row.get("score", {}) or {}
    fulltime = score.get("fulltime", {}) or {}
    halftime = score.get("halftime", {}) or {}
    goals = status_row.get("goals", {}) or {}
    home_ft = fulltime.get("home") if fulltime.get("home") is not None else goals.get("home")
    away_ft = fulltime.get("away") if fulltime.get("away") is not None else goals.get("away")
    if home_ft is None or away_ft is None:
        return None
    return {
        "status": status.get("short"), "settled_at": iso_now(),
        "home_ft": safe_int(home_ft), "away_ft": safe_int(away_ft),
        "home_ht": safe_int(halftime.get("home"), 0), "away_ht": safe_int(halftime.get("away"), 0),
    }


def _selection_won(market: str, selection: str, result: Mapping[str, Any]) -> Optional[bool]:
    hf, af = safe_int(result.get("home_ft")), safe_int(result.get("away_ft"))
    hh, ah = safe_int(result.get("home_ht")), safe_int(result.get("away_ht"))
    if market == MARKET_1X2:
        actual = "1" if hf > af else "x" if hf == af else "2"
        return selection == actual
    if market == MARKET_O25:
        return (hf + af >= 3) if selection == "over" else (hf + af <= 2) if selection == "under" else None
    if market == MARKET_O15HT:
        return (hh + ah >= 2) if selection == "over" else (hh + ah <= 1) if selection == "under" else None
    if market == MARKET_GGHT:
        yes = hh > 0 and ah > 0
        return yes if selection == "yes" else (not yes) if selection == "no" else None
    return None


def _audit_choice(choice: Mapping[str, Any], result: Mapping[str, Any]) -> Dict[str, Any]:
    audited = json.loads(json.dumps(choice))
    won = _selection_won(str(choice.get("market")), str(choice.get("selection")), result)
    audited["outcome"] = "won" if won is True else "lost" if won is False else "unsettled"
    return audited


def _preserve_existing_match(item: Mapping[str, Any], status_row: Optional[Mapping[str, Any]], reason: str) -> Dict[str, Any]:
    preserved = _preserve_existing_match_v112(item, status_row, reason)
    if not status_row:
        return preserved
    result = _result_payload(status_row)
    if not result:
        return preserved
    preserved.setdefault("fixture", {})["result"] = result
    display = preserved.get("display", {}) or {}
    primary = display.get("primary_choice")
    ranked = display.get("ranked_choices", []) or []
    preserved["prediction_audit"] = {
        "result": result,
        "primary": _audit_choice(primary, result) if isinstance(primary, dict) and primary.get("selection") else None,
        "ranked": [_audit_choice(choice, result) for choice in ranked if isinstance(choice, dict) and choice.get("selection")],
    }
    return preserved


def _empty_audit_store() -> Dict[str, Any]:
    return {"version": VERSION, "created_at": iso_now(), "updated_at": iso_now(), "fixtures": {}, "summary": {}}


def _audit_summary(records: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"settled_primary": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0, "by_market": {}, "by_grade": {}}
    for record in records:
        primary = record.get("primary") or {}
        outcome = primary.get("outcome")
        if outcome not in {"won", "lost"}:
            continue
        summary["settled_primary"] += 1
        summary["wins" if outcome == "won" else "losses"] += 1
        for bucket_name, key in (("by_market", str(primary.get("market", "unknown"))), ("by_grade", str(primary.get("grade", "N/D")))):
            bucket = summary[bucket_name].setdefault(key, {"settled": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0})
            bucket["settled"] += 1
            bucket["wins" if outcome == "won" else "losses"] += 1
    if summary["settled_primary"]:
        summary["win_rate_pct"] = round3(summary["wins"] / summary["settled_primary"] * 100.0)
    for group in (summary["by_market"], summary["by_grade"]):
        for bucket in group.values():
            bucket["win_rate_pct"] = round3(bucket["wins"] / bucket["settled"] * 100.0) if bucket["settled"] else 0.0
    return summary


def update_prediction_audit(output: Mapping[str, Any]) -> Dict[str, Any]:
    store = read_json(PREDICTION_AUDIT_FILE, _empty_audit_store())
    if not isinstance(store, dict):
        store = _empty_audit_store()
    fixtures = store.setdefault("fixtures", {})
    for day in output.get("days", []) or []:
        for match in day.get("matches", []) or []:
            audit = match.get("prediction_audit") or {}
            primary = audit.get("primary")
            fixture = match.get("fixture", {}) or {}
            fixture_id = str(fixture.get("fixture_id") or "")
            if not fixture_id or not primary or primary.get("outcome") not in {"won", "lost"}:
                continue
            fixtures[fixture_id] = {
                "fixture_id": fixture_id, "date": fixture.get("date"), "time": fixture.get("time"),
                "match": fixture.get("match"), "league": fixture.get("league"), "country": fixture.get("country"),
                "competition_quality_score": fixture.get("competition_quality_score"),
                "competition_tier": fixture.get("competition_tier"),
                "result": audit.get("result"), "primary": primary,
                "settled_at": (audit.get("result") or {}).get("settled_at", iso_now()),
            }
    store["version"] = VERSION
    store["updated_at"] = iso_now()
    store["summary"] = _audit_summary(fixtures.values())
    atomic_write_json(PREDICTION_AUDIT_FILE, store)
    return store


def scan_dates(client: APIClient, target_dates: Sequence[str], excluded_countries: Sequence[str]) -> Dict[str, Any]:
    output = _scan_dates_v112(client, target_dates, excluded_countries)
    audit_store = update_prediction_audit(output)
    output["version"] = VERSION
    output["prediction_audit"] = {
        "file": PREDICTION_AUDIT_FILE.name,
        "summary": audit_store.get("summary", {}),
        "updated_at": audit_store.get("updated_at"),
    }
    output.setdefault("method", {})["competition_filter"] = "whitelist qualitativa: campionati principali, seconde divisioni selezionate, tornei internazionali rilevanti; regionali e leghe minori escluse"
    output["method"]["recommendation_gate"] = "nessun cerchio o scelta primaria senza mercato validato, qualità competizione, affidabilità, soglia probabilità, EV e coerenza empirica"
    output["method"]["audit"] = "risultati FT/PT confrontati automaticamente con la scelta primaria e salvati in arab_quote_prediction_audit.json"
    atomic_write_json(OUTPUT_FILE, output)
    return output

# =========================================================
# ENHANCEMENT V1.1.4 — FIRST-HALF INTELLIGENCE
# Goal PT, Over 1.5 PT, GG PT, favorite trap e inversione quota
# =========================================================
VERSION = "1.1.4"
MARKET_O05HT = "over05_ht"
MARKET_SELECTIONS[MARKET_O05HT] = ("over", "under")
DISPLAY_MARKET_NAMES[MARKET_O05HT] = "Goal primo tempo (Over 0,5 PT)"

# Estende i filtri già presenti senza cambiare la whitelist qualitativa.
HARD_MINOR_TOKENS = HARD_MINOR_TOKENS + (
    "u15", "u16", "under 15", "under 16", "under 17", "under 18",
    "under 19", "under 20", "under 21", "under 23", "sub 17", "sub 19",
    "sub 20", "sub 21", "sub 23", "primavera", "juniores", "juniors",
    "ladies", "girls", "feminine", "femenina", "feminino", "femminile",
    "women's", "womens", "b team", "team b", "second team", "liga revelacao",
    "premier league 2", "professional development league", "reserve league",
    "liga profesional reservas", "torneo federal", "regional amateur",
)

EXTERNAL_SIGNALS_FILE = DATA_DIR / "arab_external_signals.json"


def _weighted_event_rate(rows: Sequence[Mapping[str, Any]], predicate) -> float:
    sample = list(rows)
    if not sample:
        return 0.0
    weights = recency_weights(sample)
    values = [1.0 if predicate(row) else 0.0 for row in sample]
    return clamp(weighted_mean(values, weights), 0.0, 1.0)


def _first_half_rates(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    sample = list(rows)
    if not sample:
        return {
            "sample": 0, "goal_ht_rate": 0.0, "zero_zero_ht_rate": 0.0,
            "over15_ht_rate": 0.0, "gg_ht_rate": 0.0,
            "scored_ht_rate": 0.0, "conceded_ht_rate": 0.0,
            "avg_total_ht": 0.0,
        }
    weights = recency_weights(sample)
    return {
        "sample": len(sample),
        "goal_ht_rate": round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("total_ht")) >= 1)),
        "zero_zero_ht_rate": round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("total_ht")) == 0)),
        "over15_ht_rate": round4(_weighted_event_rate(sample, lambda row: bool(row.get("over15_ht")))),
        "gg_ht_rate": round4(_weighted_event_rate(sample, lambda row: bool(row.get("gg_ht")))),
        "scored_ht_rate": round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("team_goals_ht")) >= 1)),
        "conceded_ht_rate": round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("team_conceded_ht")) >= 1)),
        "avg_total_ht": round3(weighted_mean([safe_float(row.get("total_ht")) for row in sample], weights)),
    }


_summarize_team_matches_v113 = summarize_team_matches


def summarize_team_matches(rows: Sequence[Mapping[str, Any]], expected_side: str) -> Dict[str, Any]:
    """Aggiunge un profilo primo tempo realmente recency-weighted."""
    profile = _summarize_team_matches_v113(rows, expected_side)
    all_rows = list(rows)
    side_rows = [row for row in all_rows if row.get("side") == expected_side]
    context_weight = safe_float(profile.get("context_weight"), 0.0)
    all_rates = _first_half_rates(all_rows)
    context_rates = _first_half_rates(side_rows)
    recent5 = _first_half_rates(all_rows[:5])
    previous = _first_half_rates(all_rows[5:10])

    blended: Dict[str, float] = {}
    for key in (
        "goal_ht_rate", "zero_zero_ht_rate", "over15_ht_rate", "gg_ht_rate",
        "scored_ht_rate", "conceded_ht_rate", "avg_total_ht",
    ):
        all_value = safe_float(all_rates.get(key))
        context_value = safe_float(context_rates.get(key), all_value) if context_rates.get("sample") else all_value
        blended[key] = round4(all_value * (1.0 - context_weight) + context_value * context_weight)

    metrics = profile.setdefault("metrics", {})
    metrics["goal_ht_rate"] = blended["goal_ht_rate"]
    metrics["zero_zero_ht_rate"] = blended["zero_zero_ht_rate"]
    metrics["over15ht_rate"] = blended["over15_ht_rate"]
    metrics["gg_ht_rate"] = blended["gg_ht_rate"]
    metrics["scored_ht_rate"] = blended["scored_ht_rate"]
    metrics["conceded_ht_rate"] = blended["conceded_ht_rate"]
    metrics["total_ht"] = round3(blended["avg_total_ht"])

    trend = {
        key: round3(safe_float(recent5.get(key)) - safe_float(previous.get(key)))
        for key in ("goal_ht_rate", "over15_ht_rate", "gg_ht_rate", "avg_total_ht")
    }
    profile["first_half"] = {
        "all": all_rates,
        "home_away_context": context_rates,
        "blended": blended,
        "recent5": recent5,
        "previous5": previous,
        "trend_recent5_vs_previous5": trend,
        "recency_method": "date_and_order_weighted",
    }
    return profile


_summarize_h2h_v113 = summarize_h2h


def _last_occurrence(rows: Sequence[Mapping[str, Any]], key: str) -> Dict[str, Any]:
    for index, row in enumerate(rows):
        if bool(row.get(key)):
            try:
                days = max((now_rome().date() - date.fromisoformat(str(row.get("date")))).days, 0)
            except Exception:
                days = None
            return {"matches_ago": index, "days_ago": days, "date": row.get("date")}
    return {"matches_ago": None, "days_ago": None, "date": None}


def summarize_h2h(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    base = _summarize_h2h_v113(rows)
    sample = list(rows)
    base["goal_ht_rate"] = round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("total_ht")) >= 1))
    base["zero_zero_ht_rate"] = round4(_weighted_event_rate(sample, lambda row: safe_int(row.get("total_ht")) == 0))
    base["last_occurrence"] = {
        "goal_ht": _last_occurrence(sample, "goal_ht") if sample and "goal_ht" in sample[0] else _last_occurrence(
            [{**row, "goal_ht": safe_int(row.get("total_ht")) >= 1} for row in sample], "goal_ht"
        ),
        "over15_ht": _last_occurrence(sample, "over15_ht"),
        "gg_ht": _last_occurrence(sample, "gg_ht"),
    }
    base["recurrence_support"] = {
        "sample_reliability": round4(clamp(len(sample) / 5.0, 0.0, 1.0)),
        "absence_is_not_boost": True,
        "method": "frequenza_recency_weighted; nessuna logica di evento dovuto",
    }
    return base


_get_league_baseline_v113 = get_league_baseline


def get_league_baseline(client: APIClient, league_id: int, season: int, last: int = MAX_LEAGUE_MATCHES) -> Dict[str, Any]:
    profile = _get_league_baseline_v113(client, league_id, season, last)
    total_ht = max(safe_float(profile.get("avg_total_ht"), 1.20), 0.05)
    profile.setdefault("goal_ht_rate", round4(1.0 - math.exp(-total_ht)))
    profile.setdefault("zero_zero_ht_rate", round4(math.exp(-total_ht)))
    profile.setdefault("home_scored_ht_rate", round4(1.0 - math.exp(-max(safe_float(profile.get("avg_home_ht"), 0.68), 0.01))))
    profile.setdefault("away_scored_ht_rate", round4(1.0 - math.exp(-max(safe_float(profile.get("avg_away_ht"), 0.52), 0.01))))
    return profile


_build_lambdas_v113 = build_lambdas


def build_lambdas(home_profile: Mapping[str, Any], away_profile: Mapping[str, Any], league: Mapping[str, Any], h2h: Mapping[str, Any]) -> Dict[str, Any]:
    """Rende il primo tempo un layer autonomo, non una semplice frazione del FT."""
    result = _build_lambdas_v113(home_profile, away_profile, league, h2h)
    hm = home_profile.get("metrics", {}) or {}
    am = away_profile.get("metrics", {}) or {}

    league_home_p = safe_float(league.get("home_scored_ht_rate"), 1.0 - math.exp(-max(safe_float(league.get("avg_home_ht"), 0.68), 0.01)))
    league_away_p = safe_float(league.get("away_scored_ht_rate"), 1.0 - math.exp(-max(safe_float(league.get("avg_away_ht"), 0.52), 0.01)))
    home_score_p = shrink_rate(
        weighted_mean([safe_float(hm.get("scored_ht_rate")), safe_float(am.get("conceded_ht_rate"))], [0.52, 0.48]),
        min(safe_int(home_profile.get("sample_context")), safe_int(away_profile.get("sample_context"))),
        league_home_p,
        full_sample=6,
    )
    away_score_p = shrink_rate(
        weighted_mean([safe_float(am.get("scored_ht_rate")), safe_float(hm.get("conceded_ht_rate"))], [0.52, 0.48]),
        min(safe_int(home_profile.get("sample_context")), safe_int(away_profile.get("sample_context"))),
        league_away_p,
        full_sample=6,
    )
    home_event_lambda = -math.log(max(1.0 - clamp(home_score_p, 0.03, 0.90), 0.01))
    away_event_lambda = -math.log(max(1.0 - clamp(away_score_p, 0.02, 0.85), 0.01))

    context_sample = min(safe_int(home_profile.get("sample_context")), safe_int(away_profile.get("sample_context")))
    event_weight = 0.34 if context_sample >= 5 else 0.27 if context_sample >= 3 else 0.20
    base_home_ht = safe_float(result.get("home_ht"), 0.60)
    base_away_ht = safe_float(result.get("away_ht"), 0.45)
    home_ht = base_home_ht * (1.0 - event_weight) + home_event_lambda * event_weight
    away_ht = base_away_ht * (1.0 - event_weight) + away_event_lambda * event_weight

    # Il trend recente può correggere il ritmo, ma con un tetto molto basso.
    home_trend = safe_float((home_profile.get("first_half", {}) or {}).get("trend_recent5_vs_previous5", {}).get("avg_total_ht"))
    away_trend = safe_float((away_profile.get("first_half", {}) or {}).get("trend_recent5_vs_previous5", {}).get("avg_total_ht"))
    trend_factor = clamp(1.0 + mean((home_trend, away_trend)) * 0.025, 0.96, 1.04)
    home_ht = clamp(home_ht * trend_factor, 0.03, 1.95)
    away_ht = clamp(away_ht * trend_factor, 0.02, 1.75)

    result["home_ht"] = round3(home_ht)
    result["away_ht"] = round3(away_ht)
    result["total_ht"] = round3(home_ht + away_ht)
    result["first_half_layer"] = {
        "home_score_probability_input": round4(home_score_p),
        "away_score_probability_input": round4(away_score_p),
        "home_event_lambda": round3(home_event_lambda),
        "away_event_lambda": round3(away_event_lambda),
        "base_home_ht": round3(base_home_ht),
        "base_away_ht": round3(base_away_ht),
        "event_weight": round3(event_weight),
        "trend_factor": round3(trend_factor),
        "context_sample_pair": context_sample,
        "method": "goal-rate HT + scored/conceded HT + recency + league shrinkage",
    }
    return result


_probabilities_from_lambdas_v113 = probabilities_from_lambdas


def probabilities_from_lambdas(lambdas: Mapping[str, Any]) -> Dict[str, Any]:
    output = _probabilities_from_lambdas_v113(lambdas)
    total_ht = max(safe_float(lambdas.get("total_ht")), 0.0)
    goal_ht = clamp(1.0 - math.exp(-total_ht), 0.0, 1.0)
    output[MARKET_O05HT] = {"over": round4(goal_ht), "under": round4(1.0 - goal_ht)}
    return output


_extract_bookmaker_markets_with_audit_v113 = extract_bookmaker_markets_with_audit


def extract_bookmaker_markets_with_audit(odds_payload: Mapping[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Estende l'audit al mercato Over/Under 0.5 primo tempo."""
    output, audit = _extract_bookmaker_markets_with_audit_v113(odds_payload)
    audit.setdefault(MARKET_O05HT, {
        "accepted": [], "rejected": [], "accepted_bet_ids": [],
        "accepted_bet_names": [], "bookmakers_found": 0,
        "status": "missing", "usable_for_signals": False,
    })
    rules = _catalog_rules()
    valid_first_half_ids = set(rules.get(MARKET_O15HT, set()))

    for response_row in odds_payload.get("response", []) or []:
        for bookmaker in response_row.get("bookmakers", []) or []:
            bookmaker_id = str(bookmaker.get("id", ""))
            bookmaker_name = str(bookmaker.get("name", "N/D")).strip() or "N/D"
            book_key = bookmaker_id or bookmaker_name
            book_record = output.setdefault(book_key, {
                "bookmaker_id": bookmaker.get("id"), "bookmaker_name": bookmaker_name, "markets": {},
            })
            for bet in bookmaker.get("bets", []) or []:
                bid = safe_int(bet.get("id"), 0)
                raw_name = str(bet.get("name", ""))
                name = normalized_text(raw_name)
                if not _contains_first_half(name) or _is_team_specific_goal_market(name):
                    continue
                if any(token in name for token in ("corner", "card", "booking")) or _is_btts_market(name):
                    continue
                values = _parse_market_values(bet)
                over = _find_odd(values, ("over 0.5", "over 0,5"))
                under = _find_odd(values, ("under 0.5", "under 0,5"))
                if over <= 1 or under <= 1:
                    continue
                source = "catalog_bet_id" if bid in valid_first_half_ids else "name_fallback"
                # Se il catalogo è disponibile, un id non verificato resta visibile ma non genera segnali.
                book_record["markets"][MARKET_O05HT] = {"over": over, "under": under}
                audit[MARKET_O05HT]["accepted"].append({
                    "bet_id": bid, "bet_name": raw_name, "bookmaker": bookmaker_name, "source": source,
                })

    books = complete_market_books(output, MARKET_O05HT)
    accepted = audit[MARKET_O05HT]["accepted"]
    sources = {row.get("source") for row in accepted}
    audit[MARKET_O05HT]["accepted_bet_ids"] = sorted({safe_int(row.get("bet_id")) for row in accepted if safe_int(row.get("bet_id"))})
    audit[MARKET_O05HT]["accepted_bet_names"] = sorted({str(row.get("bet_name")) for row in accepted if row.get("bet_name")})
    audit[MARKET_O05HT]["bookmakers_found"] = len(books)
    audit[MARKET_O05HT]["minimum_books_required"] = MARKET_MIN_BOOKS
    if not books:
        status = "missing"
    elif sources and sources <= {"catalog_bet_id"}:
        status = "verified"
    else:
        status = "name_fallback"
    audit[MARKET_O05HT]["status"] = status
    audit[MARKET_O05HT]["usable_for_signals"] = status == "verified" and len(books) >= MARKET_MIN_BOOKS
    return output, audit


def _team_interaction_probabilities(home: Mapping[str, Any], away: Mapping[str, Any], league: Mapping[str, Any]) -> Dict[str, float]:
    hm = home.get("metrics", {}) or {}
    am = away.get("metrics", {}) or {}
    home_p = shrink_rate(
        weighted_mean([safe_float(hm.get("scored_ht_rate")), safe_float(am.get("conceded_ht_rate"))], [0.52, 0.48]),
        min(safe_int(home.get("sample_context")), safe_int(away.get("sample_context"))),
        safe_float(league.get("home_scored_ht_rate"), 0.49),
        full_sample=6,
    )
    away_p = shrink_rate(
        weighted_mean([safe_float(am.get("scored_ht_rate")), safe_float(hm.get("conceded_ht_rate"))], [0.52, 0.48]),
        min(safe_int(home.get("sample_context")), safe_int(away.get("sample_context"))),
        safe_float(league.get("away_scored_ht_rate"), 0.41),
        full_sample=6,
    )
    return {
        "home_scores_ht": round4(clamp(home_p, 0.02, 0.92)),
        "away_scores_ht": round4(clamp(away_p, 0.01, 0.88)),
        "gg_ht": round4(clamp(home_p * away_p, 0.01, 0.85)),
        "goal_ht": round4(clamp(1.0 - (1.0 - home_p) * (1.0 - away_p), 0.01, 0.99)),
    }


_blend_empirical_probabilities_v113 = blend_empirical_probabilities


def blend_empirical_probabilities(poisson: Mapping[str, Any], home: Mapping[str, Any], away: Mapping[str, Any], league: Mapping[str, Any], h2h: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Mix dedicato PT: Poisson + frequenze recency-weighted + interazione squadre."""
    output, layer = _blend_empirical_probabilities_v113(poisson, home, away, league, h2h)
    interaction = _team_interaction_probabilities(home, away, league)
    context_sample = min(safe_int(home.get("sample_context")), safe_int(away.get("sample_context")))
    empirical_weight = 0.38 if context_sample >= 5 else 0.32 if context_sample >= 3 else 0.24
    interaction_weight = 0.12 if context_sample >= 3 else 0.08
    poisson_weight = 1.0 - empirical_weight - interaction_weight

    rate_mapping = {
        MARKET_O05HT: ("goal_ht_rate", "over", "under", "goal_ht"),
        MARKET_O15HT: ("over15ht_rate", "over", "under", None),
        MARKET_GGHT: ("gg_ht_rate", "yes", "no", "gg_ht"),
    }
    for market_key, (rate_key, positive, negative, interaction_key) in rate_mapping.items():
        poisson_positive = safe_float((poisson.get(market_key, {}) or {}).get(positive))
        empirical = _binary_empirical(home, away, league, h2h, rate_key)
        if interaction_key:
            interaction_positive = safe_float(interaction.get(interaction_key), poisson_positive)
        else:
            # Per O1.5 PT il lambda Poisson contiene già la distribuzione dei goal; l'interazione
            # viene usata soltanto come piccola conferma GG/ritmo.
            interaction_positive = clamp(
                poisson_positive * 0.75 + safe_float(interaction.get("gg_ht")) * 0.25,
                0.01, 0.99,
            )
        blended = clamp(
            poisson_positive * poisson_weight + empirical * empirical_weight + interaction_positive * interaction_weight,
            0.01, 0.99,
        )
        output[market_key] = {positive: round4(blended), negative: round4(1.0 - blended)}
        layer[market_key] = {
            "poisson_positive": round4(poisson_positive),
            "empirical_positive": round4(empirical),
            "team_interaction_positive": round4(interaction_positive),
            "poisson_weight": round4(poisson_weight),
            "empirical_weight": round4(empirical_weight),
            "interaction_weight": round4(interaction_weight),
            "blended_positive": round4(blended),
        }
    layer["team_scoring_ht"] = interaction
    return output, layer


def _marker_pack_for_pick(
    market_key: str,
    model_pick: Optional[str],
    probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    reliability_pct: float,
) -> Dict[str, Any]:
    if model_pick:
        model_pack = model_strength(market_key, model_pick, probabilities, market_analysis, reliability_pct)
    else:
        model_pack = {
            "probability": 0.0, "probability_pct": 0.0,
            "market_fair_probability": 0.0, "market_fair_probability_pct": 0.0,
            "edge_pp": 0.0, "score": 0.0, "strength": "none", "color": "none",
            "marker_shape": "none", "probability_gap": 0.0, "basis": "arabsniper_raw",
        }
    book_pack = (market_analysis or {}).get("book", {}) or {}
    book_pick = book_pack.get("pick")
    selections: Dict[str, Any] = {}
    colors = {"yellow": 1, "orange": 2, "red": 3}
    reverse = {1: "yellow", 2: "orange", 3: "red"}
    for selection in MARKET_SELECTIONS[market_key]:
        model_here = selection == model_pick
        book_here = selection == book_pick
        if model_here and book_here:
            shape, agreement = "circle_square", True
            combined_color = reverse[min(colors.get(model_pack.get("color"), 1), colors.get(book_pack.get("color"), 1))]
        elif model_here:
            shape, agreement, combined_color = "circle", False, model_pack.get("color", "yellow")
        elif book_here:
            shape, agreement, combined_color = "square", False, book_pack.get("color", "yellow")
        else:
            shape, agreement, combined_color = "none", False, "none"
        selections[selection] = {
            "shape": shape, "color": combined_color, "model": model_here,
            "book": book_here, "agreement": agreement,
        }
    relation = (
        "confirmed" if model_pick and book_pick == model_pick else
        "model_only" if model_pick and not book_pick else
        "contrast" if model_pick and book_pick and model_pick != book_pick else
        "book_only" if book_pick and not model_pick else "neutral"
    )
    return {"model": {"pick": model_pick, **model_pack}, "book": book_pack, "relation": relation, "selections": selections}


_build_market_markers_v113 = build_market_markers


def build_market_markers(market_key: str, probabilities: Mapping[str, float], market_analysis: Optional[Mapping[str, Any]], reliability_pct: float) -> Dict[str, Any]:
    # I mercati preferiti sono valutati sul lato positivo e sul valore della quota,
    # non scegliendo automaticamente il lato matematicamente più frequente.
    if market_key in {MARKET_O05HT, MARKET_O15HT}:
        positive_probability = safe_float(probabilities.get("over"))
        minimum = 0.60 if market_key == MARKET_O05HT else 0.25
        model_pick = "over" if positive_probability >= minimum and reliability_pct >= 40 else None
        return _marker_pack_for_pick(market_key, model_pick, probabilities, market_analysis, reliability_pct)
    if market_key == MARKET_GGHT:
        yes_probability = safe_float(probabilities.get("yes"))
        model_pick = "yes" if yes_probability >= 0.11 and reliability_pct >= 40 else None
        return _marker_pack_for_pick(market_key, model_pick, probabilities, market_analysis, reliability_pct)
    return _build_market_markers_v113(market_key, probabilities, market_analysis, reliability_pct)


def recommendation_gate(
    market_key: str,
    pick: Optional[str],
    raw_probabilities: Mapping[str, float],
    calibrated_probabilities: Mapping[str, float],
    market_analysis: Optional[Mapping[str, Any]],
    validation: Mapping[str, Any],
    reliability_pct: float,
    meta: Mapping[str, Any],
    relation: str,
    empirical_layer: Mapping[str, Any],
) -> Dict[str, Any]:
    """Gate price-aware: una giocata rara non deve superare il 50%, deve battere la quota."""
    reasons: List[str] = []
    if not pick:
        return {"eligible": False, "score": 0.0, "grade": "NO", "reasons": ["nessuna_selezione"]}
    odd = safe_float(((market_analysis or {}).get("current", {}) or {}).get("odds", {}).get(pick), 0.0)
    probability = safe_float(calibrated_probabilities.get(pick), 0.0)
    raw_probability = safe_float(raw_probabilities.get(pick), 0.0)
    fair = safe_float(((market_analysis or {}).get("current", {}) or {}).get("math", {}).get("fair_probabilities", {}).get(pick), 0.0)
    expected_value = probability * odd - 1.0 if odd > 1 else -1.0
    disagreement_pp = abs(raw_probability - fair) * 100.0 if fair > 0 else 0.0
    quality = safe_int(meta.get("competition_quality_score"), 0)
    books = safe_int(validation.get("bookmakers_found"), 0)

    absolute_floors = {
        (MARKET_1X2, "1"): 0.44, (MARKET_1X2, "2"): 0.44, (MARKET_1X2, "x"): 0.28,
        (MARKET_O25, "over"): 0.48, (MARKET_O25, "under"): 0.52,
        (MARKET_O05HT, "over"): 0.62, (MARKET_O05HT, "under"): 0.30,
        (MARKET_O15HT, "over"): 0.27, (MARKET_O15HT, "under"): 0.54,
        (MARKET_GGHT, "yes"): 0.12, (MARKET_GGHT, "no"): 0.70,
    }
    floor = absolute_floors.get((market_key, pick), 0.50)
    market_break_even = (1.0 / odd) if odd > 1 else 1.0
    edge_required = -0.004 if relation == "confirmed" else 0.008

    if not validation.get("usable_for_signals"):
        reasons.append("mercato_non_validato")
    if odd <= 1.0:
        reasons.append("quota_assente")
    if reliability_pct < 50:
        reasons.append("affidabilita_bassa")
    if quality < 58:
        reasons.append("competizione_debole")
    if probability < floor:
        reasons.append("probabilita_assoluta_insufficiente")
    if expected_value < edge_required:
        reasons.append("nessun_valore_sulla_quota")
    if disagreement_pp > 22:
        reasons.append("modello_book_troppo_distanti")

    empirical = empirical_layer.get(market_key, {}) or {}
    empirical_positive = safe_float(empirical.get("empirical_positive"), 0.0)
    if market_key == MARKET_O05HT and pick == "over" and empirical_positive < 0.58:
        reasons.append("storico_goal_pt_non_conferma")
    elif market_key == MARKET_O15HT and pick == "over" and empirical_positive < 0.28:
        reasons.append("storico_over15_pt_non_conferma")
    elif market_key == MARKET_GGHT and pick == "yes" and empirical_positive < 0.10:
        reasons.append("storico_gg_pt_non_conferma")

    edge_pp = (probability - market_break_even) * 100.0 if odd > 1 else -100.0
    probability_score = clamp((probability - floor + 0.05) / 0.16, 0.0, 1.0) * 20.0
    edge_score = clamp((edge_pp + 0.5) / 8.0, 0.0, 1.0) * 27.0
    reliability_score = clamp((reliability_pct - 45.0) / 35.0, 0.0, 1.0) * 18.0
    quality_score = clamp((quality - 50.0) / 35.0, 0.0, 1.0) * 13.0
    coverage_score = clamp(books / 8.0, 0.0, 1.0) * 10.0
    agreement_score = 12.0 if relation == "confirmed" else 4.0 if relation == "model_only" else 0.0
    disagreement_penalty = clamp((disagreement_pp - 12.0) / 12.0, 0.0, 1.0) * 14.0
    score = clamp(probability_score + edge_score + reliability_score + quality_score + coverage_score + agreement_score - disagreement_penalty, 0.0, 100.0)
    eligible = not reasons and score >= 62.0
    grade = "A" if eligible and score >= 79 else "B" if eligible and score >= 69 else "C" if eligible else "NO"
    return {
        "eligible": eligible, "score": round3(score), "grade": grade, "reasons": reasons,
        "absolute_probability_floor_pct": round3(floor * 100.0),
        "market_break_even_pct": round3(market_break_even * 100.0) if odd > 1 else 0.0,
        "probability_pct": round3(probability * 100.0),
        "raw_probability_pct": round3(raw_probability * 100.0),
        "expected_value_pct": round3(expected_value * 100.0),
        "edge_over_break_even_pp": round3(edge_pp),
        "book_disagreement_pp": round3(disagreement_pp),
        "competition_quality_score": quality, "bookmakers": books,
        "policy": "probability_floor_plus_price_edge",
    }


def _selection_fair(market: Mapping[str, Any], phase: str, selection: str) -> float:
    return safe_float(((market.get(phase, {}) or {}).get("math", {}) or {}).get("fair_probabilities", {}).get(selection))


def _selection_odd(market: Mapping[str, Any], phase: str, selection: str) -> float:
    return safe_float((market.get(phase, {}) or {}).get("odds", {}).get(selection))


def analyze_favorite_market_pattern(output: Mapping[str, Any]) -> Dict[str, Any]:
    """Individua inversione favorita e possibile vulnerabilità al gol PT dell'outsider."""
    book_markets = (output.get("book", {}) or {}).get("markets", {}) or {}
    one_x_two = book_markets.get(MARKET_1X2, {}) or {}
    stats = output.get("statistics", {}) or {}
    home = stats.get("home", {}) or {}
    away = stats.get("away", {}) or {}
    hm = home.get("metrics", {}) or {}
    am = away.get("metrics", {}) or {}

    open_probs = {side: _selection_fair(one_x_two, "open", side) for side in ("1", "2")}
    current_probs = {side: _selection_fair(one_x_two, "current", side) for side in ("1", "2")}
    if max(current_probs.values(), default=0.0) <= 0:
        return {"available": False, "score": 0.0, "classification": "unavailable"}

    open_favorite = max(open_probs, key=open_probs.get) if max(open_probs.values(), default=0.0) > 0 else None
    current_favorite = max(current_probs, key=current_probs.get)
    inversion = bool(open_favorite and current_favorite != open_favorite)
    favorite_side = current_favorite
    underdog_side = "2" if favorite_side == "1" else "1"
    favorite_profile = home if favorite_side == "1" else away
    underdog_profile = away if favorite_side == "1" else home
    favorite_metrics = favorite_profile.get("metrics", {}) or {}
    underdog_metrics = underdog_profile.get("metrics", {}) or {}

    favorite_open_odd = _selection_odd(one_x_two, "open", favorite_side)
    favorite_current_odd = _selection_odd(one_x_two, "current", favorite_side)
    underdog_open_odd = _selection_odd(one_x_two, "open", underdog_side)
    underdog_current_odd = _selection_odd(one_x_two, "current", underdog_side)
    favorite_delta_pp = (current_probs.get(favorite_side, 0.0) - open_probs.get(favorite_side, 0.0)) * 100.0
    underdog_delta_pp = (current_probs.get(underdog_side, 0.0) - open_probs.get(underdog_side, 0.0)) * 100.0
    favorite_drift = favorite_current_odd - favorite_open_odd if min(favorite_current_odd, favorite_open_odd) > 1 else 0.0
    underdog_steam = underdog_open_odd - underdog_current_odd if min(underdog_current_odd, underdog_open_odd) > 1 else 0.0

    favorite_concedes_ht = safe_float(favorite_metrics.get("conceded_ht_rate"))
    underdog_scores_ht = safe_float(underdog_metrics.get("scored_ht_rate"))
    favorite_scores_ht = safe_float(favorite_metrics.get("scored_ht_rate"))
    current_favorite_probability = current_probs.get(favorite_side, 0.0)

    favorite_selection = (one_x_two.get("selections", {}) or {}).get(favorite_side, {}) or {}
    underdog_selection = (one_x_two.get("selections", {}) or {}).get(underdog_side, {}) or {}
    favorite_oci = safe_float((favorite_selection.get("oci_as", {}) or {}).get("value"))
    underdog_oci = safe_float((underdog_selection.get("oci_as", {}) or {}).get("value"))
    underdog_consensus = safe_float((underdog_selection.get("bookmaker_consensus", {}) or {}).get("ratio"))

    score = 0.0
    score += clamp((current_favorite_probability - 0.47) / 0.25, 0.0, 1.0) * 14.0
    score += clamp((favorite_concedes_ht - 0.24) / 0.36, 0.0, 1.0) * 21.0
    score += clamp((underdog_scores_ht - 0.22) / 0.38, 0.0, 1.0) * 21.0
    score += clamp((favorite_scores_ht - 0.34) / 0.40, 0.0, 1.0) * 9.0
    score += 18.0 if inversion else clamp(max(favorite_drift, 0.0) / 0.22, 0.0, 1.0) * 9.0
    score += clamp(max(underdog_steam, 0.0) / 0.28, 0.0, 1.0) * 8.0
    score += clamp(max(underdog_oci, 0.0) / 65.0, 0.0, 1.0) * 6.0
    score += clamp(underdog_consensus, 0.0, 1.0) * 3.0
    score = clamp(score, 0.0, 100.0)

    if score >= 72:
        classification = "strong_favorite_trap"
    elif score >= 56:
        classification = "possible_favorite_trap"
    elif inversion:
        classification = "odds_inversion_only"
    else:
        classification = "neutral"
    statistical_confirmation = favorite_concedes_ht >= 0.30 and underdog_scores_ht >= 0.28
    return {
        "available": True,
        "score": round3(score),
        "classification": classification,
        "experimental": True,
        "opening_favorite": open_favorite,
        "current_favorite": current_favorite,
        "favorite_changed": inversion,
        "favorite_team": (output.get("fixture", {}) or {}).get("home") if favorite_side == "1" else (output.get("fixture", {}) or {}).get("away"),
        "underdog_team": (output.get("fixture", {}) or {}).get("away") if favorite_side == "1" else (output.get("fixture", {}) or {}).get("home"),
        "current_favorite_probability_pct": round3(current_favorite_probability * 100.0),
        "favorite_drift_odd": round3(favorite_drift),
        "underdog_steam_odd": round3(underdog_steam),
        "favorite_fair_delta_pp": round3(favorite_delta_pp),
        "underdog_fair_delta_pp": round3(underdog_delta_pp),
        "favorite_conceded_ht_rate_pct": round3(favorite_concedes_ht * 100.0),
        "underdog_scored_ht_rate_pct": round3(underdog_scores_ht * 100.0),
        "favorite_scored_ht_rate_pct": round3(favorite_scores_ht * 100.0),
        "favorite_oci": round3(favorite_oci),
        "underdog_oci": round3(underdog_oci),
        "underdog_book_consensus": round4(underdog_consensus),
        "statistical_confirmation": statistical_confirmation,
        "interpretation": "L'inversione o il drift non bastano: il correttivo PT si attiva solo con vulnerabilità statistica della favorita e capacità dell'outsider di segnare presto.",
    }


def _load_external_signal_consensus(fixture_id: str) -> Dict[str, Any]:
    payload = read_json(EXTERNAL_SIGNALS_FILE, {"signals": []})
    signals = payload.get("signals", []) if isinstance(payload, dict) else []
    accepted: List[Dict[str, Any]] = []
    for row in signals or []:
        if str(row.get("fixture_id")) != str(fixture_id):
            continue
        sample = safe_int(row.get("sample_size"), 0)
        brier = safe_float(row.get("brier_score"), 1.0)
        probability = safe_float(row.get("probability"), -1.0)
        if probability > 1.0:
            probability /= 100.0
        if sample < 100 or not (0.0 < probability < 1.0) or brier > 0.24:
            continue
        quality = clamp(sample / 500.0, 0.0, 1.0) * clamp((0.26 - brier) / 0.12, 0.0, 1.0)
        accepted.append({**dict(row), "probability": round4(probability), "quality_weight": round4(quality)})
    return {
        "available": bool(accepted),
        "accepted_sources": accepted,
        "maximum_model_weight": 0.05,
        "policy": "solo fonti con almeno 100 pronostici e Brier <= 0.24",
    }


def _apply_favorite_trap_adjustment(raw_probabilities: MutableMapping[str, Any], pattern: Mapping[str, Any]) -> Dict[str, float]:
    adjustments = {MARKET_O05HT: 0.0, MARKET_O15HT: 0.0, MARKET_GGHT: 0.0}
    if not pattern.get("available") or not pattern.get("statistical_confirmation"):
        return adjustments
    score = safe_float(pattern.get("score"))
    if score < 52:
        return adjustments
    intensity = clamp((score - 52.0) / 35.0, 0.0, 1.0)
    adjustments[MARKET_O05HT] = round4(0.012 * intensity)
    adjustments[MARKET_O15HT] = round4(0.030 * intensity)
    adjustments[MARKET_GGHT] = round4(0.036 * intensity)
    for market_key, delta in adjustments.items():
        positive = "yes" if market_key == MARKET_GGHT else "over"
        negative = "no" if market_key == MARKET_GGHT else "under"
        market = raw_probabilities.get(market_key, {}) or {}
        current = safe_float(market.get(positive))
        if current <= 0:
            continue
        updated = clamp(current + delta, 0.01, 0.99)
        raw_probabilities[market_key] = {positive: round4(updated), negative: round4(1.0 - updated)}
    return adjustments


def _first_half_intelligence(output: Mapping[str, Any], pattern: Mapping[str, Any], adjustments: Mapping[str, float]) -> Dict[str, Any]:
    model = output.get("model", {}) or {}
    raw = model.get("probabilities_raw", {}) or {}
    calibrated = model.get("probabilities_calibrated", {}) or {}
    empirical = model.get("empirical_layer", {}) or {}
    lambdas = model.get("lambdas", {}) or {}
    stats = output.get("statistics", {}) or {}
    h2h = stats.get("h2h", {}) or {}
    return {
        "goal_first_half": {
            "raw_probability_pct": round3(safe_float((raw.get(MARKET_O05HT, {}) or {}).get("over")) * 100.0),
            "calibrated_probability_pct": round3(safe_float((calibrated.get(MARKET_O05HT, {}) or {}).get("over")) * 100.0),
            "lambda_total_ht": round3(lambdas.get("total_ht")),
            "market": MARKET_O05HT,
        },
        "over15_first_half": {
            "raw_probability_pct": round3(safe_float((raw.get(MARKET_O15HT, {}) or {}).get("over")) * 100.0),
            "calibrated_probability_pct": round3(safe_float((calibrated.get(MARKET_O15HT, {}) or {}).get("over")) * 100.0),
            "empirical_probability_pct": round3(safe_float((empirical.get(MARKET_O15HT, {}) or {}).get("empirical_positive")) * 100.0),
            "market": MARKET_O15HT,
        },
        "gg_first_half": {
            "raw_probability_pct": round3(safe_float((raw.get(MARKET_GGHT, {}) or {}).get("yes")) * 100.0),
            "calibrated_probability_pct": round3(safe_float((calibrated.get(MARKET_GGHT, {}) or {}).get("yes")) * 100.0),
            "empirical_probability_pct": round3(safe_float((empirical.get(MARKET_GGHT, {}) or {}).get("empirical_positive")) * 100.0),
            "market": MARKET_GGHT,
        },
        "team_scoring_probabilities": empirical.get("team_scoring_ht", {}),
        "favorite_trap": dict(pattern),
        "favorite_trap_adjustments_pp": {key: round3(value * 100.0) for key, value in adjustments.items()},
        "h2h_recurrence": {
            "sample": h2h.get("sample"),
            "goal_ht_rate_pct": round3(safe_float(h2h.get("goal_ht_rate")) * 100.0),
            "over15_ht_rate_pct": round3(safe_float(h2h.get("over15ht_rate")) * 100.0),
            "gg_ht_rate_pct": round3(safe_float(h2h.get("gg_ht_rate")) * 100.0),
            "last_occurrence": h2h.get("last_occurrence", {}),
            "absence_is_not_boost": True,
        },
        "explanation": "Tre modelli separati per il PT: almeno un gol, almeno due gol e gol di entrambe. Il tempo dall'ultimo evento è mostrato ma non aumenta da solo la probabilità.",
    }


_build_fixture_output_v113_final = build_fixture_output


def build_fixture_output(client: APIClient, fixture_row: Mapping[str, Any], state: MutableMapping[str, Any], league_cache: MutableMapping[str, Any]) -> Optional[Dict[str, Any]]:
    output = _build_fixture_output_v113_final(client, fixture_row, state, league_cache)
    if not output:
        return None

    pattern = analyze_favorite_market_pattern(output)
    model = output.get("model", {}) or {}
    raw_probabilities = json.loads(json.dumps(model.get("probabilities_raw", {}) or {}))
    adjustments = _apply_favorite_trap_adjustment(raw_probabilities, pattern)
    book = output.get("book", {}) or {}
    market_analyses = book.get("markets", {}) or {}
    validation = book.get("market_validation", {}) or {}
    calibrated_probabilities, calibration_weights = build_calibrated_probabilities(raw_probabilities, market_analyses, validation)
    reliability = safe_float(model.get("reliability_pct"), 0.0)
    empirical_layer = model.get("empirical_layer", {}) or {}
    meta = output.get("fixture", {}) or {}

    markers: Dict[str, Any] = {}
    display_markets: Dict[str, Any] = {}
    candidates: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for market_key in MARKET_SELECTIONS:
        raw = raw_probabilities.get(market_key, {}) or {}
        calibrated = calibrated_probabilities.get(market_key, {}) or {}
        valid_analysis = market_analyses.get(market_key) if (validation.get(market_key, {}) or {}).get("usable_for_signals") else None
        marker_pack = build_market_markers(market_key, raw, valid_analysis, reliability)
        provisional_pick = (marker_pack.get("model", {}) or {}).get("pick")
        gate = recommendation_gate(
            market_key, provisional_pick, raw, calibrated, market_analyses.get(market_key),
            validation.get(market_key, {}) or {}, reliability, meta,
            marker_pack.get("relation", "neutral"), empirical_layer,
        )
        if not gate.get("eligible"):
            suppress_model_marker(marker_pack)
        markers[market_key] = marker_pack
        market_display = simplified_market_display_v110(
            market_key, market_analyses.get(market_key), marker_pack, raw, calibrated,
            calibration_weights.get(market_key, 0.0), validation.get(market_key, {}) or {},
        )
        market_display["recommendation_gate"] = gate
        market_display["empirical_layer"] = empirical_layer.get(market_key, {})
        if market_key in adjustments:
            market_display["favorite_trap_adjustment_pp"] = round3(adjustments.get(market_key, 0.0) * 100.0)
        display_markets[market_key] = market_display

        item = {"market": market_key, "selection": provisional_pick, "gate": gate, "relation": marker_pack.get("relation")}
        if not gate.get("eligible"):
            rejected.append(item)
            continue
        odd = safe_float(((market_analyses.get(market_key, {}) or {}).get("current", {}) or {}).get("odds", {}).get(provisional_pick))
        probability = safe_float(calibrated.get(provisional_pick))
        raw_probability = safe_float(raw.get(provisional_pick))
        expected_value = probability * odd - 1.0 if odd > 1 else 0.0
        preferred_bonus = 5.0 if market_key in {MARKET_O05HT, MARKET_O15HT, MARKET_GGHT} else 0.0
        ranking_score = safe_float(gate.get("score")) + (6.0 if marker_pack.get("relation") == "confirmed" else 0.0) + preferred_bonus
        candidates.append({
            "market": market_key, "selection": provisional_pick, "odd": round3(odd),
            "probability_pct": round3(probability * 100.0), "probability_raw_pct": round3(raw_probability * 100.0),
            "expected_value_pct": round3(expected_value * 100.0), "relation": marker_pack.get("relation"),
            "ranking_score": round3(ranking_score), "recommendation_score": round3(gate.get("score")),
            "grade": gate.get("grade"), "gate": gate,
            "marker": (marker_pack.get("selections", {}) or {}).get(provisional_pick, {}),
            "market_calibration_weight": calibration_weights.get(market_key, 0.0),
        })
    candidates.sort(key=lambda item: item.get("ranking_score", 0.0), reverse=True)

    model["probabilities_raw"] = raw_probabilities
    model["probabilities_calibrated"] = calibrated_probabilities
    model["probabilities"] = calibrated_probabilities
    model["market_calibration_weights"] = calibration_weights
    model["markers"] = markers
    model["favorite_trap_pattern"] = pattern
    model["first_half_method"] = "dedicated HT lambdas + recency empirical + team interaction + validated market movement"
    output["model"] = model
    output.setdefault("display", {})["primary_choice"] = candidates[0] if candidates else None
    output["display"]["ranked_choices"] = candidates
    output["display"]["rejected_choices"] = rejected
    output["display"]["markets"] = display_markets
    output["display"]["preferred_first_half_choices"] = [
        item for item in candidates if item.get("market") in {MARKET_O05HT, MARKET_O15HT, MARKET_GGHT}
    ]
    output["first_half_intelligence"] = _first_half_intelligence(output, pattern, adjustments)
    output["external_tipster_layer"] = _load_external_signal_consensus(str(meta.get("fixture_id")))
    return output


_selection_won_v113 = _selection_won


def _selection_won(market: str, selection: str, result: Mapping[str, Any]) -> Optional[bool]:
    if market == MARKET_O05HT:
        total_ht = safe_int(result.get("home_ht")) + safe_int(result.get("away_ht"))
        return (total_ht >= 1) if selection == "over" else (total_ht == 0) if selection == "under" else None
    return _selection_won_v113(market, selection, result)


_scan_dates_v113_final = scan_dates


def scan_dates(client: APIClient, target_dates: Sequence[str], excluded_countries: Sequence[str]) -> Dict[str, Any]:
    output = _scan_dates_v113_final(client, target_dates, excluded_countries)
    output["version"] = VERSION
    output.setdefault("method", {})["first_half"] = (
        "modello dedicato per Goal PT, Over 1.5 PT e GG PT: lambda HT, frequenze recency-weighted, "
        "interazione segnato/subito, H2H max 5%, mercato validato e pattern favorita vulnerabile"
    )
    output["method"]["favorite_trap"] = (
        "segnale sperimentale con correzione massima 3.6 punti percentuali; inversione quota da sola non basta"
    )
    output["method"]["price_gate"] = (
        "per mercati rari la soglia è price-aware: probabilità assoluta minima + vantaggio sul break-even della quota"
    )
    output["method"]["tipsters"] = (
        "hook opzionale arab_external_signals.json; peso massimo 5% solo con almeno 100 pronostici e Brier <= 0.24"
    )
    atomic_write_json(OUTPUT_FILE, output)
    return output

if __name__ == "__main__":
    main()
