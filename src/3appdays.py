import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
import os
import time
import sys
import math
from pathlib import Path
from github import Github

# ==========================================
# ARAB SNIPER V25 - MULTI-DAY MARKET ENGINE
# BLOCCO 1
# - import / config
# - helper base
# - github sync
# - session state
# - api core
# - snapshot rolling
# ==========================================

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

DB_FILE = str(DATA_DIR / "arab_sniper_database.json")
SNAP_FILE = str(DATA_DIR / "arab_snapshot_database.json")
CONFIG_FILE = str(DATA_DIR / "nazioni_config.json")
DETAILS_FILE = str(DATA_DIR / "match_details.json")

DEFAULT_EXCLUDED = [
    "Thailand", "Indonesia", "India", "Kenya", "Morocco",
    "Rwanda", "Nigeria", "Oman", "Algeria", "UAE"
]

LEAGUE_BLACKLIST = [
    "u19", "u20", "youth", "women", "friendly",
    "carioca", "paulista", "mineiro"
]

ROLLING_SNAPSHOT_HORIZONS = [1, 2, 3, 4, 5]

REMOTE_MAIN_FILE = "data/data.json"
REMOTE_SNAPSHOT_FILE = "data/arab_snapshot_database.json"

REMOTE_DAY_FILES = {
    1: "data/data_day1.json",
    2: "data/data_day2.json",
    3: "data/data_day3.json",
    4: "data/data_day4.json",
    5: "data/data_day5.json",
}

REMOTE_DETAILS_FILES = {
    1: "data/details_day1.json",
    2: "data/details_day2.json",
    3: "data/details_day3.json",
    4: "data/details_day4.json",
    5: "data/details_day5.json",
}

REMOTE_SNAPSHOT_DAY_FILES = {
    1: "data/snapshot_day1.json",
    2: "data/snapshot_day2.json",
    3: "data/snapshot_day3.json",
    4: "data/snapshot_day4.json",
    5: "data/snapshot_day5.json",
}

try:
    from zoneinfo import ZoneInfo
    ROME_TZ = ZoneInfo("Europe/Rome")
except Exception:
    ROME_TZ = None

st.set_page_config(page_title="ARAB SNIPER V25 MULTI-DAY WEB", layout="wide")


#===========================
# HELPER BASE
#====================================
def now_rome():
    return datetime.now(ROME_TZ) if ROME_TZ else datetime.now()


def round3(x):
    try:
        return round(float(x), 3)
    except Exception:
        return 0.0

def calculate_margin_and_fair_odds(q1, qx, q2):
    """
    Calcola overround (aggio) e fair odds normalizzate.
    """
    q1 = safe_float(q1, 0.0)
    qx = safe_float(qx, 0.0)
    q2 = safe_float(q2, 0.0)

    if q1 <= 1 or qx <= 1 or q2 <= 1:
        return 0.0, (q1, qx, q2)

    p1 = 1.0 / q1
    px = 1.0 / qx
    p2 = 1.0 / q2

    total_implied = p1 + px + p2
    margin = round3(total_implied - 1.0)

    fair_q1 = round3(q1 * total_implied)
    fair_qx = round3(qx * total_implied)
    fair_q2 = round3(q2 * total_implied)

    return margin, (fair_q1, fair_qx, fair_q2)


def fair_implied_probability(odd, total_implied):
    """
    Probabilità implicita netta, depurata dall'aggio.
    """
    odd = safe_float(odd, 0.0)
    total_implied = safe_float(total_implied, 0.0)

    if odd <= 1 or total_implied <= 0:
        return 0.0

    raw_prob = 1.0 / odd
    return round3(raw_prob / total_implied)

def clamp(x, low, high):
    try:
        return max(low, min(high, float(x)))
    except Exception:
        return low


def safe_logit(p):
    p = clamp(p, 1e-6, 1 - 1e-6)
    return math.log(p / (1.0 - p))


def poisson_pmf(k, lam):
    lam = max(safe_float(lam, 0.0), 0.0)
    if k < 0:
        return 0.0
    try:
        return math.exp(-lam) * (lam ** k) / math.factorial(k)
    except Exception:
        return 0.0


def poisson_over25_prob(lam_home, lam_away, max_goals=8):
    """
    Probabilità Over 2.5 via doppia Poisson indipendente.
    Prima versione semplice e stabile.
    """
    lam_home = clamp(lam_home, 0.05, 4.50)
    lam_away = clamp(lam_away, 0.05, 4.50)

    prob = 0.0
    for gh in range(max_goals + 1):
        p_h = poisson_pmf(gh, lam_home)
        for ga in range(max_goals + 1):
            if gh + ga >= 3:
                prob += p_h * poisson_pmf(ga, lam_away)

    return round3(clamp(prob, 0.0, 1.0))


def poisson_over05ht_prob(lam_home_ht, lam_away_ht, max_goals=6):
    """
    Probabilità di almeno 1 goal nel primo tempo.
    """
    lam_home_ht = clamp(lam_home_ht, 0.01, 3.00)
    lam_away_ht = clamp(lam_away_ht, 0.01, 3.00)

    p00 = poisson_pmf(0, lam_home_ht) * poisson_pmf(0, lam_away_ht)
    return round3(clamp(1.0 - p00, 0.0, 1.0))


def poisson_over15ht_prob(lam_home_ht, lam_away_ht, max_goals=6):
    """
    Probabilità di almeno 2 goal nel primo tempo.
    """
    lam_home_ht = clamp(lam_home_ht, 0.01, 3.00)
    lam_away_ht = clamp(lam_away_ht, 0.01, 3.00)

    prob = 0.0
    for gh in range(max_goals + 1):
        p_h = poisson_pmf(gh, lam_home_ht)
        for ga in range(max_goals + 1):
            if gh + ga >= 2:
                prob += p_h * poisson_pmf(ga, lam_away_ht)

    return round3(clamp(prob, 0.0, 1.0))


def fair_prob_from_single_odd(odd):
    """
    Prima versione minimale:
    converte una quota singola in probabilità implicita grezza.
    La depurazione completa dall'aggio la faremo nel prossimo step.
    """
    odd = safe_float(odd, 0.0)
    if odd <= 1.0:
        return 0.0
    return round3(clamp(1.0 / odd, 0.0, 1.0))

def fair_prob_from_two_way_market(over_odd, under_odd, pick="over"):
    """
    Probabilità fair normalizzata su mercato binario.
    Se manca un lato del mercato, fallback sulla quota singola.
    """
    over_odd = safe_float(over_odd, 0.0)
    under_odd = safe_float(under_odd, 0.0)

    if over_odd > 1.0 and under_odd > 1.0:
        p_over_raw = 1.0 / over_odd
        p_under_raw = 1.0 / under_odd
        total = p_over_raw + p_under_raw

        if total > 0:
            p_over_fair = p_over_raw / total
            p_under_fair = p_under_raw / total

            if str(pick).lower() == "under":
                return round3(clamp(p_under_fair, 0.0, 1.0))

            return round3(clamp(p_over_fair, 0.0, 1.0))

    # fallback prudenziale
    if str(pick).lower() == "under":
        return fair_prob_from_single_odd(under_odd)

    return fair_prob_from_single_odd(over_odd)

def classify_edge_level(edge_value):
    edge_value = safe_float(edge_value, 0.0)
    if edge_value >= 0.10:
        return "ELITE"
    if edge_value >= 0.07:
        return "STRONG"
    if edge_value >= 0.04:
        return "GOOD"
    if edge_value >= 0.02:
        return "LIGHT"
    return "NONE"

def safe_edge_logit(p_model, p_market):
    p_model = safe_float(p_model, 0.0)
    p_market = safe_float(p_market, 0.0)

    if p_model > 0 and p_market > 0:
        return round3(safe_logit(p_model) - safe_logit(p_market))
    return 0.0


def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", ".")
        if s in ("", "-", "None", "null"):
            return default
        return float(s)
    except Exception:
        return default


def fixture_dt_rome(fixture_obj):
    """
    Converte la data fixture in Europe/Rome in modo robusto.
    Usa timestamp se disponibile, altrimenti prova il campo date ISO.
    """
    try:
        ts = fixture_obj.get("timestamp")
        if ts:
            dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            return dt_utc.astimezone(ROME_TZ) if ROME_TZ else dt_utc
    except Exception:
        pass

    try:
        raw = str(fixture_obj.get("date", "")).strip()
        if raw:
            raw = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(ROME_TZ) if ROME_TZ else dt
    except Exception:
        pass

    return None


def get_target_dates():
    return [
        (now_rome().date() + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(5)
    ]


def is_blacklisted_league(league_name):
    name = str(league_name or "").lower()
    return any(k in name for k in LEAGUE_BLACKLIST)


def _contains_ht(text):
    t = str(text or "").lower()
    return any(k in t for k in [
        "1st half", "first half", "1h", "ht",
        "half time", "halftime", "1° tempo"
    ])


#====================================
# GITHUB WRITE CORE
#====================================
def get_github_token():
    token = os.getenv("GITHUB_TOKEN")
    if token:
        return token

    try:
        return st.secrets["GITHUB_TOKEN"]
    except Exception:
        return None


def github_write_json(filename, payload, commit_message):
    try:
        token = get_github_token()
        if not token:
            print("❌ GITHUB_TOKEN mancante", flush=True)
            return "MISSING_TOKEN"

        repo_name = "arabsnipertech-bet/arabsniper2"
        g = Github(token)
        repo = g.get_repo(repo_name)

        print(f"📦 Repo target: {repo_name}", flush=True)
        print(f"📄 File target: {filename}", flush=True)

        content_str = json.dumps(payload, indent=4, ensure_ascii=False)

        try:
            contents = repo.get_contents(filename)
            repo.update_file(contents.path, commit_message, content_str, contents.sha)
            print(f"✅ GitHub update OK: {filename}", flush=True)
            return "SUCCESS"
        except Exception as e_update:
            print(f"⚠️ Update fallito su {filename}: {e_update}", flush=True)

        try:
            repo.create_file(filename, commit_message, content_str)
            print(f"✅ GitHub create OK: {filename}", flush=True)
            return "SUCCESS"
        except Exception as e_create:
            print(f"❌ Create fallito su {filename}: {e_create}", flush=True)
            return f"CREATE_FAILED: {e_create}"

    except Exception as e:
        print(f"❌ GitHub write error su {filename}: {e}", flush=True)
        return f"GITHUB_ERROR: {e}"


def upload_to_github_main(results):
    return github_write_json(
        REMOTE_MAIN_FILE,
        results,
        "Update Arab Sniper Main Data"
    )


def upload_day_to_github(day_num, results):
    return github_write_json(
        REMOTE_DAY_FILES[day_num],
        results,
        f"Update Arab Sniper Day {day_num} Data"
    )


def upload_details_to_github(day_num, payload):
    return github_write_json(
        REMOTE_DETAILS_FILES[day_num],
        payload,
        f"Update Arab Sniper Day {day_num} Details"
    )


def upload_snapshot_to_github(payload):
    try:
        github_write_json(
            REMOTE_SNAPSHOT_FILE,
            payload,
            "Update snapshot database"
        )
    except Exception as e:
        print(f"Snapshot upload error: {e}", flush=True)


def upload_snapshot_day_to_github(day_num, payload):
    try:
        github_write_json(
            REMOTE_SNAPSHOT_DAY_FILES[day_num],
            payload,
            f"Update snapshot_day{day_num}"
        )
    except Exception as e:
        print(f"Snapshot day{day_num} upload error: {e}", flush=True)


#====================================
# SESSION STATE
#====================================
if "config" not in st.session_state:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                st.session_state.config = json.load(f)
        except Exception:
            st.session_state.config = {"excluded": DEFAULT_EXCLUDED}
    else:
        st.session_state.config = {"excluded": DEFAULT_EXCLUDED}

if "team_stats_cache" not in st.session_state:
    st.session_state.team_stats_cache = {}

if "team_last_matches_cache" not in st.session_state:
    st.session_state.team_last_matches_cache = {}

if "available_countries" not in st.session_state:
    st.session_state.available_countries = []

if "scan_results" not in st.session_state:
    st.session_state.scan_results = []

if "odds_memory" not in st.session_state:
    st.session_state.odds_memory = {}

if "match_details" not in st.session_state:
    st.session_state.match_details = {}

if "selected_fixture_for_modal" not in st.session_state:
    st.session_state.selected_fixture_for_modal = None


def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(st.session_state.config, f, indent=4, ensure_ascii=False)


#====================================
# RUNTIME API CACHE + THROTTLE
#====================================
RUNTIME_ODDS_CACHE = {}
LAST_API_CALL_TS = 0.0
API_MIN_INTERVAL = 0.14  # ~428 req/min teorici max


def reset_runtime_api_cache():
    global RUNTIME_ODDS_CACHE, LAST_API_CALL_TS
    RUNTIME_ODDS_CACHE = {}
    LAST_API_CALL_TS = 0.0


def api_throttle():
    global LAST_API_CALL_TS
    now_ts = time.time()
    elapsed = now_ts - LAST_API_CALL_TS

    if elapsed < API_MIN_INTERVAL:
        time.sleep(API_MIN_INTERVAL - elapsed)

    LAST_API_CALL_TS = time.time()


#====================================
# SNAPSHOT / DB LOAD-SAVE
#====================================
def save_snapshot_file(payload):
    with open(SNAP_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, ensure_ascii=False)


def _normalize_snapshot_record(fid, rec):
    """
    Normalizza un record snapshot vecchio o nuovo.
    Mantiene compatibilità col vecchio formato che salvava solo q1/q2.
    """
    if not isinstance(rec, dict):
        return None

    norm = dict(rec)
    norm["fixture_id"] = str(fid)

    legacy_q1 = safe_float(norm.get("q1"), 0.0)
    legacy_q2 = safe_float(norm.get("q2"), 0.0)

    norm["q1_open"] = safe_float(norm.get("q1_open", legacy_q1), 0.0)
    norm["qx_open"] = safe_float(norm.get("qx_open", 0.0), 0.0)
    norm["q2_open"] = safe_float(norm.get("q2_open", legacy_q2), 0.0)
    norm["o25_open"] = safe_float(norm.get("o25_open", 0.0), 0.0)
    norm["o05ht_open"] = safe_float(norm.get("o05ht_open", 0.0), 0.0)
    norm["o15ht_open"] = safe_float(norm.get("o15ht_open", 0.0), 0.0)

    # compatibilità legacy
    norm["q1"] = norm["q1_open"]
    norm["q2"] = norm["q2_open"]

    norm.setdefault("first_seen_date", None)
    norm.setdefault("first_seen_horizon", None)
    norm.setdefault("first_seen_ts", None)
    norm.setdefault("last_seen_date", norm.get("first_seen_date"))
    norm.setdefault("last_seen_horizon", norm.get("first_seen_horizon"))
    norm.setdefault("last_seen_ts", norm.get("first_seen_ts"))

    return norm


def load_existing_snapshot_payload():
    """
    Carica snapshot esistente e lo migra al nuovo formato se necessario.
    """
    if os.path.exists(SNAP_FILE):
        try:
            with open(SNAP_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)

            if isinstance(payload, dict):
                raw_odds = payload.get("odds", {}) or {}
                normalized_odds = {}

                for fid, rec in raw_odds.items():
                    norm = _normalize_snapshot_record(fid, rec)
                    if norm:
                        normalized_odds[str(fid)] = norm

                payload["odds"] = normalized_odds
                payload.setdefault("timestamp", None)
                payload.setdefault("updated_at", None)
                payload.setdefault("coverage", "rolling_day1_day5")
                return payload
        except Exception:
            pass

    return {
        "odds": {},
        "timestamp": None,
        "updated_at": None,
        "coverage": "rolling_day1_day5"
    }


def load_snapshot_from_github():
    """
    Fallback: carica lo snapshot da GitHub se il file locale
    non esiste o non contiene odds valide.
    """
    try:
        token = get_github_token()
        if not token:
            print("⚠️ GITHUB_TOKEN mancante: impossibile caricare snapshot da GitHub", flush=True)
            return None

        g = Github(token)
        repo = g.get_repo("arabsnipertech-bet/arabsniper2")
        contents = repo.get_contents(REMOTE_SNAPSHOT_FILE)
        raw = contents.decoded_content.decode("utf-8")
        payload = json.loads(raw)

        if not isinstance(payload, dict):
            return None

        odds = payload.get("odds", {}) or {}
        if not isinstance(odds, dict):
            return None

        print(f"✅ Snapshot caricato da GitHub: {len(odds)} fixture", flush=True)
        return payload

    except Exception as e:
        print(f"⚠️ Errore load_snapshot_from_github: {e}", flush=True)
        return None


def load_db():
    ts = "N/D"
    today = now_rome().strftime("%Y-%m-%d")

    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f).get("results", [])
                st.session_state.scan_results = [
                    r for r in data if r.get("Data", "") >= today
                ]
        except Exception:
            pass

    snap_data = None

    if os.path.exists(SNAP_FILE):
        try:
            with open(SNAP_FILE, "r", encoding="utf-8") as f:
                snap_data = json.load(f)
        except Exception:
            snap_data = None

    local_odds = {}
    if isinstance(snap_data, dict):
        local_odds = snap_data.get("odds", {}) or {}

    if not local_odds:
        snap_data = load_snapshot_from_github()
        if isinstance(snap_data, dict) and snap_data.get("odds"):
            try:
                save_snapshot_file(snap_data)
            except Exception as e:
                print(f"⚠️ Impossibile salvare snapshot locale dal fallback GitHub: {e}", flush=True)

    if isinstance(snap_data, dict):
        try:
            st.session_state.odds_memory = snap_data.get("odds", {}) or {}
            ts = snap_data.get("timestamp", "N/D")
        except Exception:
            pass

    if os.path.exists(DETAILS_FILE):
        try:
            with open(DETAILS_FILE, "r", encoding="utf-8") as f:
                details_data = json.load(f)
                st.session_state.match_details = details_data.get("details", {})
        except Exception:
            pass

    return ts


last_snap_ts = load_db()


#====================================
# API CORE
#====================================
def get_api_key():
    """
    Recupera la API key a runtime.
    Prima prova env, poi Streamlit secrets.
    """
    key = os.getenv("API_SPORTS_KEY")
    if key:
        return str(key).strip()

    try:
        key = st.secrets["API_SPORTS_KEY"]
        if key:
            return str(key).strip()
    except Exception:
        pass

    return None


def api_get(session, path, params):
    api_key = get_api_key()

    if not api_key:
        print("❌ API_KEY assente dentro api_get", flush=True)
        return None

    headers = {"x-apisports-key": api_key}
    safe_key = f"{api_key[:5]}***" if len(api_key) >= 5 else "***"

    print(f"🔑 API key rilevata: {safe_key}", flush=True)
    print(f"🌐 API GET path={path} params={params}", flush=True)

    backoff_plan = [0, 2, 5]

    for attempt in range(len(backoff_plan)):
        try:
            if backoff_plan[attempt] > 0:
                time.sleep(backoff_plan[attempt])

            api_throttle()

            r = session.get(
                f"https://v3.football.api-sports.io/{path}",
                headers=headers,
                params=params,
                timeout=20
            )

            print(f"📡 Tentativo {attempt + 1} -> status_code={r.status_code}", flush=True)

            if r.status_code != 200:
                print(f"❌ HTTP status non 200: {r.status_code}", flush=True)
                print(f"🧾 Response text preview: {r.text[:300]}", flush=True)
                continue

            try:
                data = r.json()
            except Exception as json_err:
                print(f"❌ JSON decode error: {json_err}", flush=True)
                print(f"🧾 Response text preview: {r.text[:300]}", flush=True)
                continue

            if not isinstance(data, dict):
                print(f"❌ Risposta non dict: {type(data)}", flush=True)
                print(f"🧾 Response preview: {str(data)[:300]}", flush=True)
                continue

            api_errors = data.get("errors") or {}

            if isinstance(api_errors, dict) and api_errors.get("rateLimit"):
                print(f"⏳ RATE LIMIT API: {api_errors}", flush=True)
                continue

            if api_errors:
                print(f"⚠️ API errors: {api_errors}", flush=True)

            if "response" not in data:
                print("❌ Chiave 'response' assente nel payload", flush=True)
                print(f"🧾 Payload preview: {str(data)[:500]}", flush=True)
                continue

            try:
                print(f"✅ Response entries: {len(data.get('response', []))}", flush=True)
            except Exception:
                print("✅ Response presente", flush=True)

            return data

        except Exception as e:
            print(f"❌ Exception api_get attempt {attempt + 1}: {e}", flush=True)

    return None


#====================================
# ESTRAZIONE MERCATI
#====================================
def extract_elite_markets(session, fid):
    global RUNTIME_ODDS_CACHE

    cache_key = str(fid)
    if cache_key in RUNTIME_ODDS_CACHE:
        cached = RUNTIME_ODDS_CACHE[cache_key]
        if isinstance(cached, dict):
            return dict(cached)
        return cached

    res = api_get(session, "odds", {"fixture": fid})
    if not res or not res.get("response"):
        RUNTIME_ODDS_CACHE[cache_key] = None
        return None

    mk = {
        "q1": 0.0,
        "qx": 0.0,
        "q2": 0.0,

        "o25": 0.0,
        "u25": 0.0,

        "o05ht": 0.0,
        "u05ht": 0.0,

        "o15ht": 0.0,
        "u15ht": 0.0,
    }

    for bm in res["response"][0].get("bookmakers", []):
        for b in bm.get("bets", []):
            name = (b.get("name") or "").lower()
            bid = b.get("id")

            if bid == 1 and mk["q1"] == 0:
                for v in b.get("values", []):
                    vl = str(v.get("value", "")).lower()
                    odd = safe_float(v.get("odd"), 0.0)
                    if "home" in vl:
                        mk["q1"] = odd
                    elif "draw" in vl:
                        mk["qx"] = odd
                    elif "away" in vl:
                        mk["q2"] = odd

            if bid == 5:
                if any(j in name for j in ["corner", "card", "booking"]):
                    continue

                for v in b.get("values", []):
                    val_txt = str(v.get("value", "")).lower().replace(",", ".")

                    if "over 2.5" in val_txt and mk["o25"] == 0:
                        mk["o25"] = safe_float(v.get("odd"), 0.0)

                    elif "under 2.5" in val_txt and mk["u25"] == 0:
                        mk["u25"] = safe_float(v.get("odd"), 0.0)

            if _contains_ht(name) and any(k in name for k in ["total", "over/under", "ou", "goals"]):
                if "team" in name:
                    continue

                for v in b.get("values", []):
                    val_txt = str(v.get("value", "")).lower().replace(",", ".")

                    if "over 0.5" in val_txt and mk["o05ht"] == 0:
                        mk["o05ht"] = safe_float(v.get("odd"), 0.0)
                    elif "under 0.5" in val_txt and mk["u05ht"] == 0:
                        mk["u05ht"] = safe_float(v.get("odd"), 0.0)

                    elif "over 1.5" in val_txt and mk["o15ht"] == 0:
                        mk["o15ht"] = safe_float(v.get("odd"), 0.0)
                    elif "under 1.5" in val_txt and mk["u15ht"] == 0:
                        mk["u15ht"] = safe_float(v.get("odd"), 0.0)

        has_1x2 = mk["q1"] > 0 and mk["qx"] > 0 and mk["q2"] > 0
        has_ft_pair = mk["o25"] > 0 and mk["u25"] > 0
        has_ht05_pair = mk["o05ht"] > 0 and mk["u05ht"] > 0
        has_ht15_pair = mk["o15ht"] > 0 and mk["u15ht"] > 0

        if has_1x2 and has_ft_pair and has_ht05_pair and has_ht15_pair:
            break

    # filtro quote troppo estreme / poco utili
    if (
        (1.01 <= mk["q1"] <= 1.10)
        or (1.01 <= mk["q2"] <= 1.10)
        or (1.01 <= mk["o25"] <= 1.30)
    ):
        RUNTIME_ODDS_CACHE[cache_key] = "SKIP"
        return "SKIP"

    RUNTIME_ODDS_CACHE[cache_key] = dict(mk)
    return mk


#====================================
# SNAPSHOT OPEN/CURRENT
#====================================
def get_open_quote_pack(fid):
    odds_memory = st.session_state.get("odds_memory", {}) or {}
    rec = odds_memory.get(str(fid), {}) or {}

    return {
        "q1": safe_float(rec.get("q1_open", rec.get("q1", 0.0)), 0.0),
        "qx": safe_float(rec.get("qx_open", 0.0), 0.0),
        "q2": safe_float(rec.get("q2_open", rec.get("q2", 0.0)), 0.0),
        "o25": safe_float(rec.get("o25_open", 0.0), 0.0),
        "o05ht": safe_float(rec.get("o05ht_open", 0.0), 0.0),
        "o15ht": safe_float(rec.get("o15ht_open", 0.0), 0.0),
    }


def get_current_quote_pack(mk):
    mk = mk or {}
    return {
        "q1": safe_float(mk.get("q1"), 0.0),
        "qx": safe_float(mk.get("qx"), 0.0),
        "q2": safe_float(mk.get("q2"), 0.0),
        "o25": safe_float(mk.get("o25"), 0.0),
        "o05ht": safe_float(mk.get("o05ht"), 0.0),
        "o15ht": safe_float(mk.get("o15ht"), 0.0),
    }


def build_rolling_multiday_snapshot(session):
    """
    Snapshot rolling Day1-Day5 basato su fixture_id.

    Regola fondamentale:
    - se il fixture NON esiste -> salva la baseline open completa
    - se il fixture ESISTE -> NON sovrascrive mai le open
    - aggiorna solo i campi last_seen_*
    """
    target_dates = get_target_dates()
    existing_payload = load_existing_snapshot_payload()
    existing_odds = existing_payload.get("odds", {}) or {}

    new_odds = {}
    active_fixture_ids = set()
    current_ts = now_rome().strftime("%Y-%m-%d %H:%M:%S")
    current_hhmm = now_rome().strftime("%H:%M")

    for fid, rec in existing_odds.items():
        norm = _normalize_snapshot_record(fid, rec)
        if norm:
            new_odds[str(fid)] = norm

    for horizon in ROLLING_SNAPSHOT_HORIZONS:
        target_date = target_dates[horizon - 1]

        res = api_get(session, "fixtures", {
            "date": target_date,
            "timezone": "Europe/Rome"
        })
        if not res:
            continue

        fx_list = [
            f for f in res.get("response", [])
            if f["fixture"]["status"]["short"] == "NS"
            and not is_blacklisted_league(f.get("league", {}).get("name", ""))
        ]

        for f in fx_list:
            fid = str(f["fixture"]["id"])
            active_fixture_ids.add(fid)

            mk = extract_elite_markets(session, f["fixture"]["id"])
            if not mk or mk == "SKIP":
                continue

            existing_rec = new_odds.get(fid)

            if not existing_rec:
                new_odds[fid] = {
                    "fixture_id": fid,
                    "q1_open": safe_float(mk.get("q1"), 0.0),
                    "qx_open": safe_float(mk.get("qx"), 0.0),
                    "q2_open": safe_float(mk.get("q2"), 0.0),
                    "o25_open": safe_float(mk.get("o25"), 0.0),
                    "o05ht_open": safe_float(mk.get("o05ht"), 0.0),
                    "o15ht_open": safe_float(mk.get("o15ht"), 0.0),
                    "q1": safe_float(mk.get("q1"), 0.0),
                    "q2": safe_float(mk.get("q2"), 0.0),
                    "first_seen_date": target_date,
                    "first_seen_horizon": horizon,
                    "first_seen_ts": current_ts,
                    "last_seen_date": target_date,
                    "last_seen_horizon": horizon,
                    "last_seen_ts": current_ts,
                }
            else:
                existing_rec["fixture_id"] = fid
                existing_rec["q1_open"] = safe_float(existing_rec.get("q1_open", existing_rec.get("q1", 0.0)), 0.0)
                existing_rec["qx_open"] = safe_float(existing_rec.get("qx_open", 0.0), 0.0)
                existing_rec["q2_open"] = safe_float(existing_rec.get("q2_open", existing_rec.get("q2", 0.0)), 0.0)
                existing_rec["o25_open"] = safe_float(existing_rec.get("o25_open", 0.0), 0.0)
                existing_rec["o05ht_open"] = safe_float(existing_rec.get("o05ht_open", 0.0), 0.0)
                existing_rec["o15ht_open"] = safe_float(existing_rec.get("o15ht_open", 0.0), 0.0)
                existing_rec["q1"] = existing_rec["q1_open"]
                existing_rec["q2"] = existing_rec["q2_open"]
                existing_rec["last_seen_date"] = target_date
                existing_rec["last_seen_horizon"] = horizon
                existing_rec["last_seen_ts"] = current_ts
                new_odds[fid] = existing_rec

        time.sleep(0.15)

    cleaned_odds = {}
    for fid, data in new_odds.items():
        if fid in active_fixture_ids:
            cleaned_odds[fid] = data

    payload = {
        "odds": cleaned_odds,
        "timestamp": current_hhmm,
        "updated_at": current_ts,
        "coverage": "rolling_day1_day5"
    }

    st.session_state.odds_memory = cleaned_odds
    save_snapshot_file(payload)
    upload_snapshot_to_github(payload)

    return payload


def build_daily_snapshots_from_rolling():
    """
    Crea snapshot_day1...snapshot_day5 filtrando il rolling snapshot centrale.
    """
    payload = load_existing_snapshot_payload()
    odds_map = payload.get("odds", {}) or {}
    target_dates = get_target_dates()
    current_ts = now_rome().strftime("%Y-%m-%d %H:%M:%S")

    for day_num in range(1, 6):
        day_date = target_dates[day_num - 1]
        day_odds = {}

        for fid, rec in odds_map.items():
            if not isinstance(rec, dict):
                continue
            if str(rec.get("last_seen_date", "")).strip() == day_date:
                day_odds[str(fid)] = rec

        day_payload = {
            "day": day_num,
            "date": day_date,
            "updated_at": current_ts,
            "odds": day_odds,
        }

        out_file = DATA_DIR / f"snapshot_day{day_num}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(day_payload, f, indent=4, ensure_ascii=False)

        upload_snapshot_day_to_github(day_num, day_payload)
        print(f"📦 snapshot_day{day_num} aggiornato: {len(day_odds)} match", flush=True)
        
#====================================
# BLOCCO 2
# TEAM MATCH HISTORY + CONTESTUAL STATS
# - ultime partite arricchite
# - split casa/trasferta
# - recency weighted
# - profilo generale + profilo contestuale
#====================================

def weighted_mean(values, weights=None):
    vals = [safe_float(v, 0.0) for v in values]
    if not vals:
        return 0.0

    if not weights:
        return sum(vals) / len(vals)

    usable = []
    for i, v in enumerate(vals):
        w = weights[i] if i < len(weights) else 1.0
        usable.append((v, safe_float(w, 1.0)))

    den = sum(w for _, w in usable)
    if den <= 0:
        return sum(v for v, _ in usable) / len(usable)

    return sum(v * w for v, w in usable) / den

def calculate_consistency(values):
    """
    Deviazione standard semplice.
    Più è bassa, più la serie è regolare.
    """
    vals = [safe_float(v, 0.0) for v in values]
    if len(vals) < 2:
        return 9.9

    mean = sum(vals) / len(vals)
    variance = sum((x - mean) ** 2 for x in vals) / len(vals)
    return round3(math.sqrt(variance))

def trimmed_mean(values):
    vals = sorted([safe_float(v, 0.0) for v in values])
    if not vals:
        return 0.0
    if len(vals) >= 5:
        core = vals[1:-1]
    else:
        core = vals
    if not core:
        return 0.0
    return sum(core) / len(core)


def weighted_trimmed_mean(values, weights=None):
    vals = [safe_float(v, 0.0) for v in values]
    if not vals:
        return 0.0

    if len(vals) != len(weights or []):
        return trimmed_mean(vals)

    pairs = list(zip(vals, weights))
    pairs = sorted(pairs, key=lambda x: x[0])

    if len(pairs) >= 5:
        pairs = pairs[1:-1]

    if not pairs:
        return 0.0

    den = sum(safe_float(w, 1.0) for _, w in pairs)
    if den <= 0:
        return sum(v for v, _ in pairs) / len(pairs)

    return sum(v * safe_float(w, 1.0) for v, w in pairs) / den


def rate_at_least(values, threshold):
    vals = [safe_float(v, 0.0) for v in values]
    if not vals:
        return 0.0
    return sum(1 for x in vals if x >= threshold) / len(vals)


def rate_at_most(values, threshold):
    vals = [safe_float(v, 0.0) for v in values]
    if not vals:
        return 0.0
    return sum(1 for x in vals if x <= threshold) / len(vals)


def rate_equal(values, target):
    vals = [safe_float(v, 0.0) for v in values]
    if not vals:
        return 0.0
    return sum(1 for x in vals if x == target) / len(vals)

def days_since_match(match_date_str):
    """
    Giorni trascorsi tra la data del match storico e oggi (Rome).
    Se parsing fallisce, ritorna 0.
    """
    try:
        if not match_date_str:
            return 0
        match_dt = datetime.strptime(str(match_date_str), "%Y-%m-%d").date()
        today_dt = now_rome().date()
        return max((today_dt - match_dt).days, 0)
    except Exception:
        return 0
    
def build_recent_weights(matches):
    """
    Pesi recency:
    - peso base per ordine lista
    - decay reale per età del match
    """
    matches = matches or []
    if not matches:
        return []

    weights = []

    for i, m in enumerate(matches):
        if i <= 1:
            base_w = 1.35
        elif i <= 3:
            base_w = 1.15
        else:
            base_w = 1.00

        age_days = days_since_match(m.get("date"))

        if age_days > 21:
            age_factor = 0.40
        elif age_days > 14:
            age_factor = 0.50
        elif age_days > 10:
            age_factor = 0.70
        elif age_days > 7:
            age_factor = 0.85
        else:
            age_factor = 1.00

        weights.append(round3(base_w * age_factor))

    return weights


def pick_last_match(matches):
    return matches[0] if matches else {}


def get_team_last_matches(session, tid):
    """
    Restituisce le ultime 8 FT della squadra, con campi squadra-specifici
    e contesto casa/trasferta già esplicito.
    """
    cache_key = f"team_last_matches::{tid}"
    if cache_key in st.session_state.team_last_matches_cache:
        return st.session_state.team_last_matches_cache[cache_key]

    res = api_get(session, "fixtures", {"team": tid, "last": 8, "status": "FT"})
    fx = res.get("response", []) if res else []

    last_matches = []

    for idx, f in enumerate(fx):
        home = f.get("teams", {}).get("home", {})
        away = f.get("teams", {}).get("away", {})

        home_id = home.get("id")
        away_id = away.get("id")

        home_name = home.get("name", "N/D")
        away_name = away.get("name", "N/D")

        gh = safe_float(f.get("goals", {}).get("home", 0), 0.0)
        ga = safe_float(f.get("goals", {}).get("away", 0), 0.0)

        hth = safe_float(f.get("score", {}).get("halftime", {}).get("home", 0), 0.0)
        hta = safe_float(f.get("score", {}).get("halftime", {}).get("away", 0), 0.0)

        is_home_team = str(home_id) == str(tid)
        is_away_team = str(away_id) == str(tid)

        team_side = "home" if is_home_team else "away" if is_away_team else "unknown"

        team_ht_scored = 0.0
        team_ht_conceded = 0.0
        team_ft_scored = 0.0
        team_ft_conceded = 0.0
        opp_name = "N/D"

        if is_home_team:
            team_ht_scored = hth
            team_ht_conceded = hta
            team_ft_scored = gh
            team_ft_conceded = ga
            opp_name = away_name
        elif is_away_team:
            team_ht_scored = hta
            team_ht_conceded = hth
            team_ft_scored = ga
            team_ft_conceded = gh
            opp_name = home_name

        total_ht_goals = hth + hta
        total_ft_goals = gh + ga

        second_half_scored = max(team_ft_scored - team_ht_scored, 0.0)
        second_half_conceded = max(team_ft_conceded - team_ht_conceded, 0.0)

        fixture_local_dt = fixture_dt_rome(f.get("fixture", {}))
        local_date = fixture_local_dt.strftime("%Y-%m-%d") if fixture_local_dt else str(f.get("fixture", {}).get("date", ""))[:10]

        row = {
            "seq": idx + 1,
            "date": local_date,
            "league": f.get("league", {}).get("name", "N/D"),
            "match": f"{home_name} - {away_name}",
            "opponent": opp_name,
            "team_side": team_side,
            "is_home": team_side == "home",
            "is_away": team_side == "away",

            "ht": f"{int(hth)}-{int(hta)}",
            "ft": f"{int(gh)}-{int(ga)}",

            "total_ht_goals": total_ht_goals,
            "total_ft_goals": total_ft_goals,

            "team_ht_scored": team_ht_scored,
            "team_ht_conceded": team_ht_conceded,
            "team_ft_scored": team_ft_scored,
            "team_ft_conceded": team_ft_conceded,
            "team_2h_scored": second_half_scored,
            "team_2h_conceded": second_half_conceded,

            "team_ht_scored_1plus": 1 if team_ht_scored >= 1 else 0,
            "team_ht_scored_2plus": 1 if team_ht_scored >= 2 else 0,
            "team_ht_conceded_1plus": 1 if team_ht_conceded >= 1 else 0,
            "match_ht_1plus": 1 if total_ht_goals >= 1 else 0,
            "match_ft_2plus": 1 if total_ft_goals >= 2 else 0,
            "match_ft_3plus": 1 if total_ft_goals >= 3 else 0,
            "match_ft_low": 1 if total_ft_goals <= 1 else 0,

            "team_scored_by_ht": 1 if team_ht_scored >= 1 else 0,
            "team_conceded_by_ht": 1 if team_ht_conceded >= 1 else 0,
            "match_ht_00": 1 if total_ht_goals == 0 else 0,
            "match_ht_2plus": 1 if total_ht_goals >= 2 else 0,
        }

        last_matches.append(row)

    try:
        last_matches = sorted(
            last_matches,
            key=lambda x: str(x.get("date", "")),
            reverse=True
        )
        for idx, row in enumerate(last_matches):
            row["seq"] = idx + 1
    except Exception:
        pass
    
    st.session_state.team_last_matches_cache[cache_key] = last_matches
    return last_matches


def filter_matches_by_side(last_matches, side=None):
    if side not in ("home", "away"):
        return list(last_matches or [])
    return [m for m in (last_matches or []) if m.get("team_side") == side]


def extract_metric_list(matches, key):
    return [safe_float(m.get(key), 0.0) for m in (matches or [])]


def summarize_match_set(matches, label="all"):
    """
    Riassume un set di match in metriche utili.
    label solo descrittivo.
    """
    matches = matches or []
    if not matches:
        return {
            "label": label,
            "count": 0,

            "avg_total": 0.0,
            "avg_ht": 0.0,
            "avg_total_clean": 0.0,
            "avg_ht_clean": 0.0,
            "avg_total_weighted": 0.0,
            "avg_ht_weighted": 0.0,
            "avg_total_wclean": 0.0,
            "avg_ht_wclean": 0.0,

            "avg_ht_scored": 0.0,
            "avg_ht_conceded": 0.0,
            "avg_ft_scored": 0.0,
            "avg_ft_conceded": 0.0,

            "avg_ht_scored_clean": 0.0,
            "avg_ht_conceded_clean": 0.0,
            "avg_ft_scored_clean": 0.0,
            "avg_ft_conceded_clean": 0.0,

            "avg_ht_scored_weighted": 0.0,
            "avg_ht_conceded_weighted": 0.0,
            "avg_ft_scored_weighted": 0.0,
            "avg_ft_conceded_weighted": 0.0,

            "avg_ht_scored_wclean": 0.0,
            "avg_ht_conceded_wclean": 0.0,
            "avg_ft_scored_wclean": 0.0,
            "avg_ft_conceded_wclean": 0.0,

            "ht_1plus_rate": 0.0,
            "ht_zero_rate": 0.0,
            "ft_2plus_rate": 0.0,
            "ft_3plus_rate": 0.0,
            "ft_low_rate": 0.0,

            "ht_scored_1plus_rate": 0.0,
            "ht_scored_2plus_rate": 0.0,
            "ht_conceded_1plus_rate": 0.0,

            "ft_peak_count": 0,

            "last_2h_zero": False,
            "last_2h_conceded_zero": False,

            "ft_stdev": 9.9,
            "ht_stdev": 9.9,
            "ft_scored_stdev": 9.9,
            "scoring_regularity": 0.0,
        }

    weights = build_recent_weights(matches)

    ft_list = extract_metric_list(matches, "total_ft_goals")
    ht_list = extract_metric_list(matches, "total_ht_goals")

    ht_scored_list = extract_metric_list(matches, "team_ht_scored")
    ht_conceded_list = extract_metric_list(matches, "team_ht_conceded")
    ft_scored_list = extract_metric_list(matches, "team_ft_scored")
    ft_conceded_list = extract_metric_list(matches, "team_ft_conceded")

    last_match = pick_last_match(matches)
    last_2h_scored = safe_float(last_match.get("team_2h_scored"), 0.0)
    last_2h_conceded = safe_float(last_match.get("team_2h_conceded"), 0.0)

    scored_by_ht_list = extract_metric_list(matches, "team_scored_by_ht")
    conceded_by_ht_list = extract_metric_list(matches, "team_conceded_by_ht")
    ht_00_list = extract_metric_list(matches, "match_ht_00")
    ht_2plus_list = extract_metric_list(matches, "match_ht_2plus")

    summary = {
        "label": label,
        "count": len(matches),

        "avg_total": round3(weighted_mean(ft_list)),
        "avg_ht": round3(weighted_mean(ht_list)),
        "avg_total_clean": round3(trimmed_mean(ft_list)),
        "avg_ht_clean": round3(trimmed_mean(ht_list)),
        "avg_total_weighted": round3(weighted_mean(ft_list, weights)),
        "avg_ht_weighted": round3(weighted_mean(ht_list, weights)),
        "avg_total_wclean": round3(weighted_trimmed_mean(ft_list, weights)),
        "avg_ht_wclean": round3(weighted_trimmed_mean(ht_list, weights)),

        "avg_ht_scored": round3(weighted_mean(ht_scored_list)),
        "avg_ht_conceded": round3(weighted_mean(ht_conceded_list)),
        "avg_ft_scored": round3(weighted_mean(ft_scored_list)),
        "avg_ft_conceded": round3(weighted_mean(ft_conceded_list)),

        "avg_ht_scored_clean": round3(trimmed_mean(ht_scored_list)),
        "avg_ht_conceded_clean": round3(trimmed_mean(ht_conceded_list)),
        "avg_ft_scored_clean": round3(trimmed_mean(ft_scored_list)),
        "avg_ft_conceded_clean": round3(trimmed_mean(ft_conceded_list)),

        "avg_ht_scored_weighted": round3(weighted_mean(ht_scored_list, weights)),
        "avg_ht_conceded_weighted": round3(weighted_mean(ht_conceded_list, weights)),
        "avg_ft_scored_weighted": round3(weighted_mean(ft_scored_list, weights)),
        "avg_ft_conceded_weighted": round3(weighted_mean(ft_conceded_list, weights)),

        "avg_ht_scored_wclean": round3(weighted_trimmed_mean(ht_scored_list, weights)),
        "avg_ht_conceded_wclean": round3(weighted_trimmed_mean(ht_conceded_list, weights)),
        "avg_ft_scored_wclean": round3(weighted_trimmed_mean(ft_scored_list, weights)),
        "avg_ft_conceded_wclean": round3(weighted_trimmed_mean(ft_conceded_list, weights)),

        "ht_1plus_rate": round3(rate_at_least(ht_list, 1)),
        "ht_zero_rate": round3(rate_equal(ht_list, 0)),
        "ft_2plus_rate": round3(rate_at_least(ft_list, 2)),
        "ft_3plus_rate": round3(rate_at_least(ft_list, 3)),
        "ft_low_rate": round3(rate_at_most(ft_list, 1)),

        "ht_scored_1plus_rate": round3(rate_at_least(ht_scored_list, 1)),
        "ht_scored_2plus_rate": round3(rate_at_least(ht_scored_list, 2)),
        "ht_conceded_1plus_rate": round3(rate_at_least(ht_conceded_list, 1)),

        "ft_peak_count": int(sum(1 for x in ft_list if x >= 5)),

        "last_2h_zero": (last_2h_scored == 0),
        "last_2h_conceded_zero": (last_2h_conceded == 0),

        "scored_by_ht_rate": round3(rate_at_least(scored_by_ht_list, 1)),
        "conceded_by_ht_rate": round3(rate_at_least(conceded_by_ht_list, 1)),
        "ht_00_rate": round3(rate_at_least(ht_00_list, 1)),
        "early_2goal_rate": round3(rate_at_least(ht_2plus_list, 1)),

        "ft_stdev": round3(calculate_consistency(ft_list)),
        "ht_stdev": round3(calculate_consistency(ht_list)),
        "ft_scored_stdev": round3(calculate_consistency(ft_scored_list)),
        "scoring_regularity": round3(
            sum(1 for x in ft_scored_list if x > 0) / len(matches)
        ) if matches else 0.0,
    }

    return summary


def merge_stat_layers(base_stats, context_stats, prefix_context="ctx_"):
    """
    Unisce il layer generale con il layer contestuale.
    I campi contestuali vengono aggiunti con prefisso.
    """
    merged = dict(base_stats or {})
    for k, v in (context_stats or {}).items():
        merged[f"{prefix_context}{k}"] = v
    return merged


def get_team_performance(session, tid, expected_side=None):
    """
    Profilo squadra V25.

    Ritorna:
    - statistiche generali ultime 8
    - statistiche contestuali casa/trasferta
    - layer blended da usare nel motore
    """
    side_key = expected_side if expected_side in ("home", "away") else "all"
    cache_key = f"team_stats::{tid}::{side_key}"

    if cache_key in st.session_state.team_stats_cache:
        return st.session_state.team_stats_cache[cache_key]

    last_matches = get_team_last_matches(session, tid)
    if not last_matches:
        return None

    all_stats = summarize_match_set(last_matches, label="all")
    home_stats = summarize_match_set(filter_matches_by_side(last_matches, "home"), label="home")
    away_stats = summarize_match_set(filter_matches_by_side(last_matches, "away"), label="away")

    if expected_side == "home":
        context_stats = home_stats
    elif expected_side == "away":
        context_stats = away_stats
    else:
        context_stats = all_stats

    # --------------------------------------
    # BLENDED LOGIC
    # Il generale resta la base.
    # Il contestuale pesa molto ma non domina se campione corto.
    # --------------------------------------
    context_count = int(context_stats.get("count", 0))
    all_count = int(all_stats.get("count", 0))

    if context_count >= 5:
        ctx_weight = 0.62
    elif context_count == 4:
        ctx_weight = 0.55
    elif context_count == 3:
        ctx_weight = 0.48
    elif context_count == 2:
        ctx_weight = 0.35
    elif context_count == 1:
        ctx_weight = 0.22
    else:
        ctx_weight = 0.0

    base_weight = 1.0 - ctx_weight

    def blend_metric(key_all, key_ctx=None):
        key_ctx = key_ctx or key_all
        a = safe_float(all_stats.get(key_all), 0.0)
        c = safe_float(context_stats.get(key_ctx), a)
        return round3((a * base_weight) + (c * ctx_weight))

    stats = {
        # base blended operativa
        "avg_ht": blend_metric("avg_ht_weighted"),
        "avg_total": blend_metric("avg_total_weighted"),
        "avg_ht_clean": blend_metric("avg_ht_wclean"),
        "avg_total_clean": blend_metric("avg_total_wclean"),

        "avg_ht_scored": blend_metric("avg_ht_scored_weighted"),
        "avg_ht_conceded": blend_metric("avg_ht_conceded_weighted"),
        "avg_ft_scored": blend_metric("avg_ft_scored_weighted"),
        "avg_ft_conceded": blend_metric("avg_ft_conceded_weighted"),

        "avg_ht_scored_clean": blend_metric("avg_ht_scored_wclean"),
        "avg_ht_conceded_clean": blend_metric("avg_ht_conceded_wclean"),
        "avg_ft_scored_clean": blend_metric("avg_ft_scored_wclean"),
        "avg_ft_conceded_clean": blend_metric("avg_ft_conceded_wclean"),

        "ht_1plus_rate": blend_metric("ht_1plus_rate"),
        "ht_zero_rate": blend_metric("ht_zero_rate"),
        "ft_2plus_rate": blend_metric("ft_2plus_rate"),
        "ft_3plus_rate": blend_metric("ft_3plus_rate"),
        "ft_low_rate": blend_metric("ft_low_rate"),

        "ht_scored_1plus_rate": blend_metric("ht_scored_1plus_rate"),
        "ht_scored_2plus_rate": blend_metric("ht_scored_2plus_rate"),
        "ht_conceded_1plus_rate": blend_metric("ht_conceded_1plus_rate"),

        "scored_by_ht_rate": blend_metric("scored_by_ht_rate"),
        "conceded_by_ht_rate": blend_metric("conceded_by_ht_rate"),
        "ht_00_rate": blend_metric("ht_00_rate"),
        "early_2goal_rate": blend_metric("early_2goal_rate"),

        "ft_peak_count": int(round((safe_float(all_stats.get("ft_peak_count"), 0) * base_weight) + (safe_float(context_stats.get("ft_peak_count"), 0) * ctx_weight))),
        "last_2h_zero": bool(context_stats.get("last_2h_zero")) if context_count > 0 else bool(all_stats.get("last_2h_zero")),
        "last_2h_conceded_zero": bool(context_stats.get("last_2h_conceded_zero")) if context_count > 0 else bool(all_stats.get("last_2h_conceded_zero")),

        "ft_stdev": blend_metric("ft_stdev"),
        "ht_stdev": blend_metric("ht_stdev"),
        "ft_scored_stdev": blend_metric("ft_scored_stdev"),
        "scoring_regularity": blend_metric("scoring_regularity"),

        # meta
        "expected_side": expected_side if expected_side in ("home", "away") else "all",
        "all_sample_count": all_count,
        "context_sample_count": context_count,
        "context_weight": round3(ctx_weight),
        "base_weight": round3(base_weight),

        # dump layer generale
        "all_avg_ht": all_stats.get("avg_ht", 0.0),
        "all_avg_total": all_stats.get("avg_total", 0.0),
        "all_avg_ht_clean": all_stats.get("avg_ht_clean", 0.0),
        "all_avg_total_clean": all_stats.get("avg_total_clean", 0.0),
        "all_avg_ht_scored_clean": all_stats.get("avg_ht_scored_clean", 0.0),
        "all_avg_ht_conceded_clean": all_stats.get("avg_ht_conceded_clean", 0.0),
        "all_avg_ft_scored_clean": all_stats.get("avg_ft_scored_clean", 0.0),
        "all_avg_ft_conceded_clean": all_stats.get("avg_ft_conceded_clean", 0.0),
        "all_ft_stdev": all_stats.get("ft_stdev", 9.9),
        "all_ht_stdev": all_stats.get("ht_stdev", 9.9),
        "all_ft_scored_stdev": all_stats.get("ft_scored_stdev", 9.9),
        "all_scoring_regularity": all_stats.get("scoring_regularity", 0.0),

        # dump contestuale
        "ctx_avg_ht": context_stats.get("avg_ht", 0.0),
        "ctx_avg_total": context_stats.get("avg_total", 0.0),
        "ctx_avg_ht_clean": context_stats.get("avg_ht_clean", 0.0),
        "ctx_avg_total_clean": context_stats.get("avg_total_clean", 0.0),
        "ctx_avg_ht_scored_clean": context_stats.get("avg_ht_scored_clean", 0.0),
        "ctx_avg_ht_conceded_clean": context_stats.get("avg_ht_conceded_clean", 0.0),
        "ctx_avg_ft_scored_clean": context_stats.get("avg_ft_scored_clean", 0.0),
        "ctx_avg_ft_conceded_clean": context_stats.get("avg_ft_conceded_clean", 0.0),
        "ctx_ht_1plus_rate": context_stats.get("ht_1plus_rate", 0.0),
        "ctx_ht_zero_rate": context_stats.get("ht_zero_rate", 0.0),
        "ctx_ft_2plus_rate": context_stats.get("ft_2plus_rate", 0.0),
        "ctx_ft_3plus_rate": context_stats.get("ft_3plus_rate", 0.0),
        "ctx_ft_low_rate": context_stats.get("ft_low_rate", 0.0),
        "ctx_ht_scored_1plus_rate": context_stats.get("ht_scored_1plus_rate", 0.0),
        "ctx_ht_scored_2plus_rate": context_stats.get("ht_scored_2plus_rate", 0.0),
        "ctx_ht_conceded_1plus_rate": context_stats.get("ht_conceded_1plus_rate", 0.0),
        "ctx_scored_by_ht_rate": context_stats.get("scored_by_ht_rate", 0.0),
        "ctx_conceded_by_ht_rate": context_stats.get("conceded_by_ht_rate", 0.0),
        "ctx_ht_00_rate": context_stats.get("ht_00_rate", 0.0),
        "ctx_early_2goal_rate": context_stats.get("early_2goal_rate", 0.0),
        "ctx_ft_stdev": context_stats.get("ft_stdev", 9.9),
        "ctx_ht_stdev": context_stats.get("ht_stdev", 9.9),
        "ctx_ft_scored_stdev": context_stats.get("ft_scored_stdev", 9.9),
        "ctx_scoring_regularity": context_stats.get("scoring_regularity", 0.0),

        # raw blocks completi, utili per debug/details
        "all_block": all_stats,
        "home_block": home_stats,
        "away_block": away_stats,
        "context_block": context_stats,
        "last_matches": last_matches,
    }

    st.session_state.team_stats_cache[cache_key] = stats
    return stats


def build_team_debug_summary(stats):
    """
    Piccolo helper per details/debug.
    """
    if not stats:
        return {}

    return {
        "expected_side": stats.get("expected_side", "all"),
        "all_sample_count": stats.get("all_sample_count", 0),
        "context_sample_count": stats.get("context_sample_count", 0),
        "context_weight": stats.get("context_weight", 0.0),

        "avg_ht_clean": stats.get("avg_ht_clean", 0.0),
        "avg_total_clean": stats.get("avg_total_clean", 0.0),
        "avg_ht_scored_clean": stats.get("avg_ht_scored_clean", 0.0),
        "avg_ht_conceded_clean": stats.get("avg_ht_conceded_clean", 0.0),
        "avg_ft_scored_clean": stats.get("avg_ft_scored_clean", 0.0),
        "avg_ft_conceded_clean": stats.get("avg_ft_conceded_clean", 0.0),

        "ht_1plus_rate": stats.get("ht_1plus_rate", 0.0),
        "ht_zero_rate": stats.get("ht_zero_rate", 0.0),
        "ft_2plus_rate": stats.get("ft_2plus_rate", 0.0),
        "ft_3plus_rate": stats.get("ft_3plus_rate", 0.0),
        "ft_low_rate": stats.get("ft_low_rate", 0.0),

        "ctx_avg_ht_clean": stats.get("ctx_avg_ht_clean", 0.0),
        "ctx_avg_total_clean": stats.get("ctx_avg_total_clean", 0.0),
        "ctx_avg_ft_scored_clean": stats.get("ctx_avg_ft_scored_clean", 0.0),
        "ctx_avg_ft_conceded_clean": stats.get("ctx_avg_ft_conceded_clean", 0.0),
        "ctx_ht_1plus_rate": stats.get("ctx_ht_1plus_rate", 0.0),
        "ctx_ft_2plus_rate": stats.get("ctx_ft_2plus_rate", 0.0),

        "ft_stdev": stats.get("ft_stdev", 9.9),
        "ht_stdev": stats.get("ht_stdev", 9.9),
        "ft_scored_stdev": stats.get("ft_scored_stdev", 9.9),
        "scoring_regularity": stats.get("scoring_regularity", 0.0),

        "ctx_ft_stdev": stats.get("ctx_ft_stdev", 9.9),
        "ctx_ht_stdev": stats.get("ctx_ht_stdev", 9.9),
        "ctx_ft_scored_stdev": stats.get("ctx_ft_scored_stdev", 9.9),
        "ctx_scoring_regularity": stats.get("ctx_scoring_regularity", 0.0),
    }

def estimate_match_lambdas(s_h, s_a):
    """
    Stima lambda FT e HT per casa e trasferta usando i campi reali del motore V25.

    Output:
    - lam_home_ft
    - lam_away_ft
    - lam_home_ht
    - lam_away_ht
    - debug factors
    """
    # -------------------------
    # BASE CLEAN CROSS
    # -------------------------
    home_att_ft = safe_float(s_h.get("avg_ft_scored_clean", 0.0), 0.0)
    away_def_ft = safe_float(s_a.get("avg_ft_conceded_clean", 0.0), 0.0)

    away_att_ft = safe_float(s_a.get("avg_ft_scored_clean", 0.0), 0.0)
    home_def_ft = safe_float(s_h.get("avg_ft_conceded_clean", 0.0), 0.0)

    home_att_ht = safe_float(s_h.get("avg_ht_scored_clean", 0.0), 0.0)
    away_def_ht = safe_float(s_a.get("avg_ht_conceded_clean", 0.0), 0.0)

    away_att_ht = safe_float(s_a.get("avg_ht_scored_clean", 0.0), 0.0)
    home_def_ht = safe_float(s_h.get("avg_ht_conceded_clean", 0.0), 0.0)

    # cross base
    lam_home_ft = (home_att_ft + away_def_ft) / 2.0
    lam_away_ft = (away_att_ft + home_def_ft) / 2.0

    lam_home_ht = (home_att_ht + away_def_ht) / 2.0
    lam_away_ht = (away_att_ht + home_def_ht) / 2.0

    # -------------------------
    # BONUS CONTESTUALE
    # più il profilo contestuale è affidabile,
    # più lasciamo respirare la lambda
    # -------------------------
    ctx_h = safe_float(s_h.get("context_weight", 0.0), 0.0)
    ctx_a = safe_float(s_a.get("context_weight", 0.0), 0.0)
    ctx_avg = (ctx_h + ctx_a) / 2.0

    ctx_multiplier_ft = 1.0 + (ctx_avg * 0.08)
    ctx_multiplier_ht = 1.0 + (ctx_avg * 0.06)

    lam_home_ft *= ctx_multiplier_ft
    lam_away_ft *= ctx_multiplier_ft
    lam_home_ht *= ctx_multiplier_ht
    lam_away_ht *= ctx_multiplier_ht

    # -------------------------
    # REGOLARITÀ
    # squadre più regolari = lambda più credibile
    # -------------------------
    reg_h = safe_float(s_h.get("scoring_regularity", 0.0), 0.0)
    reg_a = safe_float(s_a.get("scoring_regularity", 0.0), 0.0)
    reg_avg = (reg_h + reg_a) / 2.0

    if reg_avg >= 0.78:
        reg_mult = 1.06
    elif reg_avg >= 0.66:
        reg_mult = 1.03
    elif reg_avg <= 0.45:
        reg_mult = 0.93
    else:
        reg_mult = 1.00

    lam_home_ft *= reg_mult
    lam_away_ft *= reg_mult

    # HT più delicato: bonus più piccolo
    if reg_avg >= 0.78:
        reg_mult_ht = 1.04
    elif reg_avg >= 0.66:
        reg_mult_ht = 1.02
    elif reg_avg <= 0.45:
        reg_mult_ht = 0.95
    else:
        reg_mult_ht = 1.00

    lam_home_ht *= reg_mult_ht
    lam_away_ht *= reg_mult_ht

    # -------------------------
    # DEVIAZIONE STANDARD
    # serie troppo sporche = riduciamo un po' la fiducia
    # -------------------------
    ft_sd_h = safe_float(s_h.get("ft_stdev", 9.9), 9.9)
    ft_sd_a = safe_float(s_a.get("ft_stdev", 9.9), 9.9)
    ht_sd_h = safe_float(s_h.get("ht_stdev", 9.9), 9.9)
    ht_sd_a = safe_float(s_a.get("ht_stdev", 9.9), 9.9)

    ft_sd_avg = (ft_sd_h + ft_sd_a) / 2.0
    ht_sd_avg = (ht_sd_h + ht_sd_a) / 2.0

    if ft_sd_avg >= 1.70:
        ft_sd_mult = 0.92
    elif ft_sd_avg >= 1.45:
        ft_sd_mult = 0.96
    elif ft_sd_avg <= 1.10:
        ft_sd_mult = 1.04
    else:
        ft_sd_mult = 1.00

    lam_home_ft *= ft_sd_mult
    lam_away_ft *= ft_sd_mult

    if ht_sd_avg >= 0.95:
        ht_sd_mult = 0.94
    elif ht_sd_avg >= 0.80:
        ht_sd_mult = 0.97
    elif ht_sd_avg <= 0.55:
        ht_sd_mult = 1.03
    else:
        ht_sd_mult = 1.00

    lam_home_ht *= ht_sd_mult
    lam_away_ht *= ht_sd_mult

    # -------------------------
    # DEFENSIVE SUPPRESSION
    # Difese che spengono davvero il match:
    # alta frequenza di low-event / HT bloccati / seconda metà sterile
    # -------------------------
    home_ft_low = safe_float(s_h.get("ft_low_rate", 0.0), 0.0)
    away_ft_low = safe_float(s_a.get("ft_low_rate", 0.0), 0.0)

    home_ht_zero = safe_float(s_h.get("ht_zero_rate", 0.0), 0.0)
    away_ht_zero = safe_float(s_a.get("ht_zero_rate", 0.0), 0.0)

    home_last_2h_zero = bool(s_h.get("last_2h_zero", False))
    away_last_2h_zero = bool(s_a.get("last_2h_zero", False))

    # Se la squadra di casa concede struttura “bloccata”, penalizza l’attacco away
    if home_ft_low >= 0.38 and home_ht_zero >= 0.38:
        lam_away_ft *= 0.94
        lam_away_ht *= 0.95
    elif home_ft_low >= 0.30 and home_ht_zero >= 0.30:
        lam_away_ft *= 0.97
        lam_away_ht *= 0.98

    # Se la squadra ospite concede struttura “bloccata”, penalizza l’attacco home
    if away_ft_low >= 0.38 and away_ht_zero >= 0.38:
        lam_home_ft *= 0.94
        lam_home_ht *= 0.95
    elif away_ft_low >= 0.30 and away_ht_zero >= 0.30:
        lam_home_ft *= 0.97
        lam_home_ht *= 0.98

    # -----------------------------------------
    # LAST_2H_ZERO DINAMICO
    # - malus se conferma profilo che si spegne
    # - bonus se è in controtendenza rispetto a media sana
    # -----------------------------------------
    home_ft_scored_clean = safe_float(s_h.get("avg_ft_scored_clean", 0.0), 0.0)
    away_ft_scored_clean = safe_float(s_a.get("avg_ft_scored_clean", 0.0), 0.0)

    home_ft_2plus = safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0)
    away_ft_2plus = safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0)

    home_regularity = safe_float(s_h.get("scoring_regularity", 0.0), 0.0)
    away_regularity = safe_float(s_a.get("scoring_regularity", 0.0), 0.0)

    # Se HOME ha fatto 0 nel 2T ultimo match:
    # - se HOME normalmente è offensiva/regolare -> piccolo bonus per AWAY FT
    # - se HOME è già squadra che spegne/blocca -> malus per AWAY FT
    if home_last_2h_zero:
        if (
            home_ft_scored_clean >= 1.15
            and home_ft_2plus >= 0.62
            and home_regularity >= 0.62
            and home_ft_low < 0.30
            and home_ht_zero < 0.30
        ):
            lam_away_ft *= 1.020
        elif (
            home_ft_low >= 0.30
            or home_ht_zero >= 0.30
            or home_regularity < 0.55
        ):
            lam_away_ft *= 0.970
        else:
            lam_away_ft *= 0.985

    # Se AWAY ha fatto 0 nel 2T ultimo match:
    # - se AWAY normalmente è offensiva/regolare -> piccolo bonus per HOME FT
    # - se AWAY è già squadra che spegne/blocca -> malus per HOME FT
    if away_last_2h_zero:
        if (
            away_ft_scored_clean >= 1.15
            and away_ft_2plus >= 0.62
            and away_regularity >= 0.62
            and away_ft_low < 0.30
            and away_ht_zero < 0.30
        ):
            lam_home_ft *= 1.020
        elif (
            away_ft_low >= 0.30
            or away_ht_zero >= 0.30
            or away_regularity < 0.55
        ):
            lam_home_ft *= 0.970
        else:
            lam_home_ft *= 0.985

    # -------------------------
    # CLAMP FINALE
    # -------------------------
    lam_home_ft = round3(clamp(lam_home_ft, 0.15, 3.20))
    lam_away_ft = round3(clamp(lam_away_ft, 0.15, 3.20))
    lam_home_ht = round3(clamp(lam_home_ht, 0.05, 1.80))
    lam_away_ht = round3(clamp(lam_away_ht, 0.05, 1.80))

    return {
        "lam_home_ft": lam_home_ft,
        "lam_away_ft": lam_away_ft,
        "lam_home_ht": lam_home_ht,
        "lam_away_ht": lam_away_ht,
        "ctx_avg": round3(ctx_avg),
        "reg_avg": round3(reg_avg),
        "ft_sd_avg": round3(ft_sd_avg),
        "ht_sd_avg": round3(ht_sd_avg),
    }

def classify_match_tempo(s_h, s_a):
    """
    Tempo di attivazione del match:
    - FAST  = partita pronta presto
    - BUILD = partita da sviluppo
    - SLOW  = partita più lenta / bloccabile

    Non sostituisce PT.
    Lo completa con una lettura di ritmo iniziale.
    """
    home_ht_scored = safe_float(s_h.get("avg_ht_scored_clean", 0.0), 0.0)
    away_ht_scored = safe_float(s_a.get("avg_ht_scored_clean", 0.0), 0.0)

    home_ht_conceded = safe_float(s_h.get("avg_ht_conceded_clean", 0.0), 0.0)
    away_ht_conceded = safe_float(s_a.get("avg_ht_conceded_clean", 0.0), 0.0)

    home_ht1_rate = safe_float(s_h.get("ht_1plus_rate", 0.0), 0.0)
    away_ht1_rate = safe_float(s_a.get("ht_1plus_rate", 0.0), 0.0)

    home_reg = safe_float(s_h.get("scoring_regularity", 0.0), 0.0)
    away_reg = safe_float(s_a.get("scoring_regularity", 0.0), 0.0)

    # pressione offensiva reciproca nel 1T
    early_home = home_ht_scored + away_ht_conceded
    early_away = away_ht_scored + home_ht_conceded
    early_total = early_home + early_away

    # se una sola squadra porta quasi tutto il peso, rischio match meno pronto
    early_balance = abs(early_home - early_away)

    # se entrambe concedono nel 1T, il match tende a essere più vivo presto
    pressure_factor = home_ht_conceded + away_ht_conceded

    # conferma minima di frequenza / continuità
    activation_factor = (
        (home_ht1_rate + away_ht1_rate) * 0.35 +
        (home_reg + away_reg) * 0.15
    )

    early_index = (
        early_total * 0.60 +
        pressure_factor * 0.30 +
        activation_factor -
        early_balance * 0.30
    )

    tempo_tag = "SLOW"
    if early_index >= 2.60:
        tempo_tag = "FAST"
    elif early_index >= 2.20:
        tempo_tag = "BUILD"

    # guard rail: evita FAST troppo falsi su carico monolato
    one_side_guard = abs(home_ht_scored - away_ht_scored)
    if tempo_tag == "FAST" and one_side_guard >= 0.65 and pressure_factor < 0.95:
        tempo_tag = "BUILD"

    return {
        "early_home": round3(early_home),
        "early_away": round3(early_away),
        "early_total": round3(early_total),
        "early_balance": round3(early_balance),
        "pressure_factor": round3(pressure_factor),
        "activation_factor": round3(activation_factor),
        "early_index": round3(early_index),
        "tempo_tag": tempo_tag,
    }

#====================================
# BLOCCO 3
# QUOTE MOVEMENT + MARKET COHERENCE ENGINE
# - open/current movement
# - inversione 1X2
# - qualità drop
# - coerenza book
# - disallineamento utile / trappola
#====================================

def classify_single_quote_move(open_q, current_q):
    """
    Classifica il movimento di una singola quota.

    range:
    - 0.00 -> none
    - 0.01 - 0.05 -> soft
    - 0.06 - 0.14 -> medium
    - >= 0.15 -> hard

    dir:
    - current < open -> down
    - current > open -> up
    """
    open_q = safe_float(open_q, 0.0)
    current_q = safe_float(current_q, 0.0)

    if open_q <= 0 or current_q <= 0:
        return {
            "open": open_q,
            "current": current_q,
            "diff": 0.0,
            "abs_diff": 0.0,
            "dir": "flat",
            "tier": "none",
            "arrow": "",
            "label": ""
        }

    diff = round(current_q - open_q, 3)
    abs_diff = round(abs(diff), 3)

    if abs_diff == 0:
        tier = "none"
    elif abs_diff <= 0.05:
        tier = "soft"
    elif abs_diff <= 0.14:
        tier = "medium"
    else:
        tier = "hard"

    if diff < 0:
        direction = "down"
        arrow = "↓"
    elif diff > 0:
        direction = "up"
        arrow = "↑"
    else:
        direction = "flat"
        arrow = ""

    label = f"{arrow}{abs_diff:.2f}" if arrow else ""

    return {
        "open": open_q,
        "current": current_q,
        "diff": diff,
        "abs_diff": abs_diff,
        "dir": direction,
        "tier": tier,
        "arrow": arrow,
        "label": label
    }


def get_favorite_side_from_1x2(pack, min_gap=0.03):
    """
    Restituisce:
    - '1'
    - '2'
    - '' se gap troppo piccolo o dati invalidi
    """
    q1 = safe_float(pack.get("q1"), 0.0)
    q2 = safe_float(pack.get("q2"), 0.0)

    if q1 <= 0 or q2 <= 0:
        return ""

    if abs(q1 - q2) < min_gap:
        return ""

    return "1" if q1 < q2 else "2"


def detect_1x2_inversion(open_pack, current_pack, min_gap=0.03):
    fav_open = get_favorite_side_from_1x2(open_pack, min_gap=min_gap)
    fav_current = get_favorite_side_from_1x2(current_pack, min_gap=min_gap)

    inversion = bool(fav_open and fav_current and fav_open != fav_current)

    return {
        "INVERSION": inversion,
        "INV_FROM": fav_open if inversion else "",
        "INV_TO": fav_current if inversion else "",
        "FAV_OPEN": fav_open,
        "FAV_CURRENT": fav_current,
    }


def build_quote_movement_package(fid, mk):
    """
    Costruisce:
    - open
    - current
    - movimenti
    - inversione
    """
    open_pack = get_open_quote_pack(fid)
    current_pack = get_current_quote_pack(mk)

    q1_move = classify_single_quote_move(open_pack["q1"], current_pack["q1"])
    qx_move = classify_single_quote_move(open_pack["qx"], current_pack["qx"])
    q2_move = classify_single_quote_move(open_pack["q2"], current_pack["q2"])
    o25_move = classify_single_quote_move(open_pack["o25"], current_pack["o25"])
    o05ht_move = classify_single_quote_move(open_pack["o05ht"], current_pack["o05ht"])
    o15ht_move = classify_single_quote_move(open_pack["o15ht"], current_pack["o15ht"])

    inversion_pack = detect_1x2_inversion(open_pack, current_pack, min_gap=0.03)

    return {
        "Q1_OPEN": open_pack["q1"],
        "QX_OPEN": open_pack["qx"],
        "Q2_OPEN": open_pack["q2"],
        "O25_OPEN": open_pack["o25"],
        "O05HT_OPEN": open_pack["o05ht"],
        "O15HT_OPEN": open_pack["o15ht"],

        "Q1_CURR": current_pack["q1"],
        "QX_CURR": current_pack["qx"],
        "Q2_CURR": current_pack["q2"],
        "O25_CURR": current_pack["o25"],
        "O05HT_CURR": current_pack["o05ht"],
        "O15HT_CURR": current_pack["o15ht"],

        "Q1_MOVE_DATA": q1_move,
        "QX_MOVE_DATA": qx_move,
        "Q2_MOVE_DATA": q2_move,
        "O25_MOVE_DATA": o25_move,
        "O05HT_MOVE_DATA": o05ht_move,
        "O15HT_MOVE_DATA": o15ht_move,

        "Q1_MOVE": q1_move["label"],
        "QX_MOVE": qx_move["label"],
        "Q2_MOVE": q2_move["label"],
        "O25_MOVE": o25_move["label"],
        "O05HT_MOVE": o05ht_move["label"],
        "O15HT_MOVE": o15ht_move["label"],

        "INVERSION": inversion_pack["INVERSION"],
        "INV_FROM": inversion_pack["INV_FROM"],
        "INV_TO": inversion_pack["INV_TO"],
        "FAV_OPEN": inversion_pack["FAV_OPEN"],
        "FAV_CURRENT": inversion_pack["FAV_CURRENT"],
    }


def build_movement_summary(row):
    """
    Riassunto leggibile da UI.
    Più intuitivo: priorità a inversione e 1X2, poi goal market.
    """
    parts = []

    inv = bool(row.get("INVERSION", False))
    inv_from = str(row.get("INV_FROM", "")).strip()
    inv_to = str(row.get("INV_TO", "")).strip()

    if inv and inv_from and inv_to:
        parts.append(f"INV {inv_from}→{inv_to}")

    def add_move(label, move_data):
        move_data = move_data or {}
        direction = str(move_data.get("dir", "")).strip()
        abs_diff = safe_float(move_data.get("abs_diff", 0.0), 0.0)

        if abs_diff < 0.06:
            return

        if direction == "down":
            parts.append(f"{label} ↓ {abs_diff:.2f}")
        elif direction == "up":
            parts.append(f"{label} ↑ {abs_diff:.2f}")

    add_move("1", row.get("Q1_MOVE_DATA"))
    add_move("X", row.get("QX_MOVE_DATA"))
    add_move("2", row.get("Q2_MOVE_DATA"))
    add_move("O2.5", row.get("O25_MOVE_DATA"))
    add_move("O0.5 HT", row.get("O05HT_MOVE_DATA"))

    return " • ".join(parts)


def compute_drop_diff(fid, mk):
    """
    Drop della favorita 1/2 rispetto all'open snapshot.
    Compatibile con struttura snapshot esistente.
    """
    odds_memory = st.session_state.get("odds_memory", {}) or {}
    if str(fid) not in odds_memory:
        return 0.0

    old_data = odds_memory.get(str(fid), {})
    if not isinstance(old_data, dict):
        return 0.0

    q1_old = safe_float(old_data.get("q1_open", old_data.get("q1", 0.0)), 0.0)
    q2_old = safe_float(old_data.get("q2_open", old_data.get("q2", 0.0)), 0.0)

    q1_now = safe_float(mk.get("q1"), 0.0)
    q2_now = safe_float(mk.get("q2"), 0.0)

    if q1_now <= 0 or q2_now <= 0:
        return 0.0

    fav_is_home = q1_now <= q2_now
    old_q = q1_old if fav_is_home else q2_old
    fav_now = min(q1_now, q2_now)

    if old_q > 0 and fav_now > 0 and old_q > fav_now:
        return round3(old_q - fav_now)

    return 0.0


def score_drop(drop_diff):
    if drop_diff >= 0.15:
        return 1.2
    if drop_diff >= 0.10:
        return 0.9
    if drop_diff >= 0.05:
        return 0.5
    return 0.0


def detect_primary_market_pressure(quote_pack):
    """
    Individua quale mercato sembra guidare il movimento:
    - 1X2
    - O25
    - HT
    - mixed
    - none
    """
    q1 = quote_pack.get("Q1_MOVE_DATA", {}) or {}
    q2 = quote_pack.get("Q2_MOVE_DATA", {}) or {}
    o25 = quote_pack.get("O25_MOVE_DATA", {}) or {}
    o05 = quote_pack.get("O05HT_MOVE_DATA", {}) or {}
    o15 = quote_pack.get("O15HT_MOVE_DATA", {}) or {}

    one_x_two_pressure = 0.0
    goals_ft_pressure = 0.0
    goals_ht_pressure = 0.0

    if q1.get("dir") == "down":
        one_x_two_pressure += safe_float(q1.get("abs_diff", 0.0), 0.0)
    if q2.get("dir") == "down":
        one_x_two_pressure += safe_float(q2.get("abs_diff", 0.0), 0.0)

    if o25.get("dir") == "down":
        goals_ft_pressure += safe_float(o25.get("abs_diff", 0.0), 0.0)

    if o05.get("dir") == "down":
        goals_ht_pressure += safe_float(o05.get("abs_diff", 0.0), 0.0)
    if o15.get("dir") == "down":
        goals_ht_pressure += safe_float(o15.get("abs_diff", 0.0), 0.0) * 0.85

    pressures = {
        "1x2": round3(one_x_two_pressure),
        "o25": round3(goals_ft_pressure),
        "ht": round3(goals_ht_pressure),
    }

    ranked = sorted(pressures.items(), key=lambda x: x[1], reverse=True)
    top_name, top_val = ranked[0]
    second_val = ranked[1][1]

    if top_val <= 0:
        return {
            "leader": "none",
            "pressures": pressures,
            "strength": "none"
        }

    if second_val > 0 and abs(top_val - second_val) <= 0.03:
        leader = "mixed"
    else:
        leader = top_name

    if top_val >= 0.18:
        strength = "hard"
    elif top_val >= 0.08:
        strength = "medium"
    else:
        strength = "soft"

    return {
        "leader": leader,
        "pressures": pressures,
        "strength": strength
    }


def classify_drop_quality(quote_pack):
    """
    Classifica il drop in chiave qualitativa:
    - structural
    - sterile
    - divergent
    - exhausted
    - none
    """
    q1 = quote_pack.get("Q1_MOVE_DATA", {}) or {}
    q2 = quote_pack.get("Q2_MOVE_DATA", {}) or {}
    o25 = quote_pack.get("O25_MOVE_DATA", {}) or {}
    o05 = quote_pack.get("O05HT_MOVE_DATA", {}) or {}
    o15 = quote_pack.get("O15HT_MOVE_DATA", {}) or {}

    drop_1x2 = 0.0
    if q1.get("dir") == "down":
        drop_1x2 = max(drop_1x2, safe_float(q1.get("abs_diff", 0.0), 0.0))
    if q2.get("dir") == "down":
        drop_1x2 = max(drop_1x2, safe_float(q2.get("abs_diff", 0.0), 0.0))

    drop_o25 = safe_float(o25.get("abs_diff", 0.0), 0.0) if o25.get("dir") == "down" else 0.0
    drop_o05 = safe_float(o05.get("abs_diff", 0.0), 0.0) if o05.get("dir") == "down" else 0.0
    drop_o15 = safe_float(o15.get("abs_diff", 0.0), 0.0) if o15.get("dir") == "down" else 0.0

    ht_confirm = max(drop_o05, drop_o15)
    goal_confirm = max(drop_o25, ht_confirm)

    curr_fav = min(
        safe_float(quote_pack.get("Q1_CURR", 0.0), 0.0) or 99.0,
        safe_float(quote_pack.get("Q2_CURR", 0.0), 0.0) or 99.0
    )

    if drop_1x2 <= 0 and goal_confirm <= 0:
        return {
            "drop_type": "none",
            "drop_strength": "none",
            "confirmed": False,
            "value_left": "unknown"
        }

    top_drop = max(drop_1x2, drop_o25, drop_o05, drop_o15)

    if top_drop >= 0.18:
        strength = "hard"
    elif top_drop >= 0.08:
        strength = "medium"
    else:
        strength = "soft"

    # 1) structural = 1X2 scende e goal markets seguono
    if drop_1x2 >= 0.06 and goal_confirm >= 0.05:
        drop_type = "structural"
        confirmed = True
    # 2) sterile = 1X2 scende ma goal markets non seguono
    elif drop_1x2 >= 0.06 and goal_confirm < 0.04:
        drop_type = "sterile"
        confirmed = False
    # 3) divergent = goal scendono ma 1X2 non conferma davvero
    elif drop_1x2 < 0.04 and goal_confirm >= 0.06:
        drop_type = "divergent"
        confirmed = False
    else:
        drop_type = "mixed"
        confirmed = False

    # valore residuo approssimato
    if curr_fav <= 1.35 and strength in ("hard", "medium"):
        value_left = "low"
    elif curr_fav <= 1.48 and strength == "hard":
        value_left = "low"
    elif curr_fav <= 1.65:
        value_left = "medium"
    else:
        value_left = "high"

    # se i markets goal sono già stati molto compressi, valore residuo si riduce
    o25_curr = safe_float(quote_pack.get("O25_CURR", 0.0), 0.0)
    o05_curr = safe_float(quote_pack.get("O05HT_CURR", 0.0), 0.0)

    if o25_curr > 0 and o25_curr <= 1.48:
        value_left = "low"
    if o05_curr > 0 and o05_curr <= 1.18 and value_left == "high":
        value_left = "medium"

    return {
        "drop_type": drop_type,
        "drop_strength": strength,
        "confirmed": confirmed,
        "value_left": value_left
    }

def classify_signal_stability(structure_pack, market_pack, signal_pack):
    structure_score = safe_float(structure_pack.get("structure_score", 0.0), 0.0)
    coherence_score = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)

    drop_type = str(market_pack.get("drop_type", "none")).strip().lower()
    drop_confirmed = bool(market_pack.get("drop_confirmed", False))
    warning_flags = market_pack.get("warning_flags", []) or []

    over_level = int(signal_pack.get("over_level", 0) or 0)

    heavy_warning = any(w in warning_flags for w in [
        "market_value_trap",
        "suspicious_limit",
        "favorite_ultra_but_ft_structure_weak",
        "ft_market_ahead_of_structure"
    ])

    if (
        structure_score >= 1.20
        and coherence_score >= 1.35
        and one_sided_risk <= 1.05
        and not heavy_warning
        and (drop_confirmed or drop_type == "structural" or over_level >= 2)
    ):
        return "Alta"

    if (
        structure_score >= 0.90
        and coherence_score >= 1.00
        and one_sided_risk <= 1.30
        and not heavy_warning
    ):
        return "Media"

    return "Speculativa"

def build_signal_summary(structure_pack, market_pack, signal_pack):
    structure_score = safe_float(structure_pack.get("structure_score", 0.0), 0.0)
    coherence_score = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)

    drop_type = str(market_pack.get("drop_type", "none")).strip().lower()
    drop_confirmed = bool(market_pack.get("drop_confirmed", False))

    over_level = int(signal_pack.get("over_level", 0) or 0)

    if drop_confirmed and drop_type == "structural":
        return "DROP strutturale"

    if over_level >= 3 and coherence_score >= 1.20:
        return "Over molto solido"

    if structure_score >= 1.20 and coherence_score >= 1.30:
        return "Struttura + mercato allineati"

    if one_sided_risk >= 1.20:
        return "Rischio partita bloccata"

    if coherence_score < 1.00:
        return "Mercato poco convinto"

    return "Segnale da monitorare"

def classify_favorite_zone(fav):
    fav = safe_float(fav, 0.0)
    if fav <= 0:
        return "unknown"
    if fav < 1.30:
        return "ultra"
    if fav < 1.45:
        return "strong"
    if fav <= 1.90:
        return "gold"
    if fav <= 2.25:
        return "balanced"
    return "wide"


def build_market_baseline(mk):
    """
    Legge il book in modo assoluto, senza ancora usare statistiche.
    Serve per interpretare la 'storia' del mercato.
    """
    q1 = safe_float(mk.get("q1"), 0.0)
    q2 = safe_float(mk.get("q2"), 0.0)
    o25 = safe_float(mk.get("o25"), 0.0)
    o05 = safe_float(mk.get("o05ht"), 0.0)
    o15 = safe_float(mk.get("o15ht"), 0.0)

    fav = min(q1, q2) if q1 > 0 and q2 > 0 else 0.0
    fav_zone = classify_favorite_zone(fav)

    # FT openness
    if 0 < o25 <= 1.60:
        ft_market = "very_open"
    elif 1.60 < o25 <= 1.85:
        ft_market = "open"
    elif 1.85 < o25 <= 2.15:
        ft_market = "moderate"
    elif o25 > 2.15:
        ft_market = "tight"
    else:
        ft_market = "unknown"

    # HT openness
    if 0 < o05 <= 1.22:
        ht_market = "very_open"
    elif 1.22 < o05 <= 1.32:
        ht_market = "open"
    elif 1.32 < o05 <= 1.42:
        ht_market = "moderate"
    elif o05 > 1.42:
        ht_market = "tight"
    else:
        ht_market = "unknown"

    # HT 2-goal pressure
    if 0 < o15 <= 2.20:
        ht15_market = "strong"
    elif 2.20 < o15 <= 3.00:
        ht15_market = "medium"
    elif o15 > 3.00:
        ht15_market = "light"
    else:
        ht15_market = "unknown"

    return {
        "fav_quote": round3(fav),
        "fav_zone": fav_zone,
        "ft_market": ft_market,
        "ht_market": ht_market,
        "ht15_market": ht15_market,
        "is_gold_zone": bool(1.40 <= fav <= 1.90),
    }


def build_stat_structure_snapshot(s_h, s_a):
    """
    Fotografia strutturale del match derivata dai dati squadra.
    Qui NON decidiamo ancora i tag finali.
    """
    home_attack = safe_float(s_h.get("avg_ft_scored_clean"), 0.0)
    away_attack = safe_float(s_a.get("avg_ft_scored_clean"), 0.0)
    home_concede = safe_float(s_h.get("avg_ft_conceded_clean"), 0.0)
    away_concede = safe_float(s_a.get("avg_ft_conceded_clean"), 0.0)

    home_ht_scored = safe_float(s_h.get("avg_ht_scored_clean"), 0.0)
    away_ht_scored = safe_float(s_a.get("avg_ht_scored_clean"), 0.0)

    combined_ft_clean = round3((safe_float(s_h.get("avg_total_clean"), 0.0) + safe_float(s_a.get("avg_total_clean"), 0.0)) / 2)
    combined_ht_clean = round3((safe_float(s_h.get("avg_ht_clean"), 0.0) + safe_float(s_a.get("avg_ht_clean"), 0.0)) / 2)
    combined_ht_scored_clean = round3((home_ht_scored + away_ht_scored) / 2)

    cross_home_clean = round3(home_attack + away_concede)
    cross_away_clean = round3(away_attack + home_concede)

    attack_gap = round3(abs(home_attack - away_attack))
    ht_attack_gap = round3(abs(home_ht_scored - away_ht_scored))

    bilateral_ft = bool(
        home_attack >= 1.00 and away_attack >= 1.00
    )

    bilateral_ht = bool(
        home_ht_scored >= 0.60 and away_ht_scored >= 0.60
    )

    return {
        "combined_ft_clean": combined_ft_clean,
        "combined_ht_clean": combined_ht_clean,
        "combined_ht_scored_clean": combined_ht_scored_clean,
        "cross_home_clean": cross_home_clean,
        "cross_away_clean": cross_away_clean,
        "attack_gap": attack_gap,
        "ht_attack_gap": ht_attack_gap,
        "bilateral_ft": bilateral_ft,
        "bilateral_ht": bilateral_ht,
    }


def analyze_market_coherence(mk, s_h, s_a, quote_pack):
    """
    Motore V25:
    legge se il mercato è coerente con la struttura statistica
    e se c'è qualche disallineamento utile.

    output chiave:
    - coherence_score
    - dislocation_score
    - market_profile
    - leading_market
    - lagging_market
    - warning_flags
    """
    baseline = build_market_baseline(mk)
    struct = build_stat_structure_snapshot(s_h, s_a)
    pressure = detect_primary_market_pressure(quote_pack)
    drop_info = classify_drop_quality(quote_pack)

    o25 = safe_float(mk.get("o25"), 0.0)
    o05 = safe_float(mk.get("o05ht"), 0.0)
    o15 = safe_float(mk.get("o15ht"), 0.0)
    fav = safe_float(baseline.get("fav_quote", 0.0), 0.0)

    q1_curr = safe_float(mk.get("q1"), 0.0)
    qx_curr = safe_float(mk.get("qx"), 0.0)
    q2_curr = safe_float(mk.get("q2"), 0.0)

    q1_open = safe_float(quote_pack.get("Q1_OPEN", 0.0), 0.0)
    qx_open = safe_float(quote_pack.get("QX_OPEN", 0.0), 0.0)
    q2_open = safe_float(quote_pack.get("Q2_OPEN", 0.0), 0.0)

    margin_curr, fair_odds_curr = calculate_margin_and_fair_odds(q1_curr, qx_curr, q2_curr)
    margin_open, fair_odds_open = calculate_margin_and_fair_odds(q1_open, qx_open, q2_open)

    total_implied_curr = (
        (1 / q1_curr) + (1 / qx_curr) + (1 / q2_curr)
        if q1_curr > 1 and qx_curr > 1 and q2_curr > 1 else 0.0
    )
    total_implied_open = (
        (1 / q1_open) + (1 / qx_open) + (1 / q2_open)
        if q1_open > 1 and qx_open > 1 and q2_open > 1 else 0.0
    )

    fav_side_curr = get_favorite_side_from_1x2({"q1": q1_curr, "q2": q2_curr}, min_gap=0.03)
    if not fav_side_curr:
        fav_side_curr = "1" if q1_curr <= q2_curr else "2"

    fav_odd_curr = q1_curr if fav_side_curr == "1" else q2_curr
    fav_odd_open = q1_open if fav_side_curr == "1" else q2_open

    fav_fair_prob_curr = fair_implied_probability(fav_odd_curr, total_implied_curr)
    fav_fair_prob_open = fair_implied_probability(fav_odd_open, total_implied_open)
    fav_fair_prob_delta = round3(fav_fair_prob_curr - fav_fair_prob_open)

    fav_fair_curr = fair_odds_curr[0] if fav_side_curr == "1" else fair_odds_curr[2]

    combined_ft_clean = safe_float(struct.get("combined_ft_clean", 0.0), 0.0)
    combined_ht_clean = safe_float(struct.get("combined_ht_clean", 0.0), 0.0)
    combined_ht_scored_clean = safe_float(struct.get("combined_ht_scored_clean", 0.0), 0.0)
    cross_home_clean = safe_float(struct.get("cross_home_clean", 0.0), 0.0)
    cross_away_clean = safe_float(struct.get("cross_away_clean", 0.0), 0.0)

    bilateral_ft = bool(struct.get("bilateral_ft", False))
    bilateral_ht = bool(struct.get("bilateral_ht", False))

    coherence_score = 0.0
    dislocation_score = 0.0
    warning_flags = []
    positive_flags = []

    # -------------------------
    # COERENZA FT
    # -------------------------
    if combined_ft_clean >= 1.68 and 1.52 <= o25 <= 2.18:
        coherence_score += 1.20
        positive_flags.append("ft_struct_market_aligned")
    elif combined_ft_clean >= 1.58 and 1.48 <= o25 <= 2.28:
        coherence_score += 0.70

    if cross_home_clean >= 2.10 and cross_away_clean >= 2.10 and o25 <= 2.15:
        coherence_score += 0.95
        positive_flags.append("double_cross_supported")
    elif (cross_home_clean >= 2.18 and cross_away_clean >= 1.95) or (cross_away_clean >= 2.18 and cross_home_clean >= 1.95):
        coherence_score += 0.40

    # -------------------------
    # COERENZA HT
    # -------------------------
    if combined_ht_clean >= 0.96 and 1.20 <= o05 <= 1.38:
        coherence_score += 0.95
        positive_flags.append("ht_struct_market_aligned")
    elif combined_ht_clean >= 0.88 and 1.18 <= o05 <= 1.42:
        coherence_score += 0.45

    if combined_ht_scored_clean >= 0.78 and 2.00 <= o15 <= 3.25:
        coherence_score += 0.55
    elif combined_ht_scored_clean >= 0.68 and 1.90 <= o15 <= 3.60:
        coherence_score += 0.25

    # -------------------------
    # COERENZA FAVORITA / MATCH OPENNESS
    # -------------------------
    if 1.40 <= fav <= 1.90 and bilateral_ft and o25 <= 2.18:
        coherence_score += 0.55
        positive_flags.append("fav_open_match_coherent")

    if fav < 1.30 and o25 <= 1.62 and not bilateral_ft:
        warning_flags.append("favorite_too_strong_for_open_ft")
    if fav < 1.30 and combined_ft_clean < 1.62:
        warning_flags.append("favorite_ultra_but_ft_structure_weak")

    # -------------------------
    # DISLOCATION FT vs HT
    # -------------------------
    # HT forte ma O15 ancora larga = potenziale lag HT
    if combined_ht_scored_clean >= 0.82 and o05 <= 1.34 and o15 >= 2.95:
        dislocation_score += 1.10
        positive_flags.append("lagged_ht15")
    elif combined_ht_scored_clean >= 0.74 and o05 <= 1.36 and o15 >= 3.10:
        dislocation_score += 0.65

    # FT aperto ma HT ancora non del tutto assorbito
    if combined_ft_clean >= 1.72 and o25 <= 1.88 and o05 >= 1.31:
        dislocation_score += 0.60
        positive_flags.append("lagged_ht_opening")

    # HT molto pronto ma FT non del tutto compresso
    if combined_ht_clean >= 1.00 and o05 <= 1.26 and o25 >= 1.82:
        dislocation_score += 0.45
        positive_flags.append("lagged_ft_after_ht")

    # -------------------------
    # MOVIMENTO / PRESSIONE
    # -------------------------
    leader = pressure.get("leader", "none")
    drop_type = drop_info.get("drop_type", "none")
    value_left = drop_info.get("value_left", "unknown")

    if leader == "mixed":
        coherence_score += 0.40
        positive_flags.append("mixed_pressure_confirmation")

    if drop_type == "structural":
        coherence_score += 0.55
        positive_flags.append("structural_drop")
    elif drop_type == "sterile":
        warning_flags.append("sterile_1x2_drop")
        dislocation_score -= 0.20
    elif drop_type == "divergent":
        warning_flags.append("goals_move_without_1x2_support")

    if value_left == "low":
        warning_flags.append("low_remaining_value")

    # -------------------------
    # FAIR VALUE / BOOKIE MARGIN
    # -------------------------
    fav_drop_now = (
        fav_odd_open > 0
        and fav_odd_curr > 0
        and fav_odd_curr < fav_odd_open
    )

    if margin_curr >= 0.08:
        warning_flags.append("high_bookie_protection")
        coherence_score -= 0.25

        if fav_drop_now:
            warning_flags.append("suspicious_limit")
            coherence_score -= 0.18

    if margin_open > 0 and margin_curr > margin_open + 0.015:
        warning_flags.append("bookie_margin_rising")
        coherence_score -= 0.20

    if fav_fair_prob_delta >= 0.045:
        positive_flags.append("true_fair_support")
        coherence_score += 0.35
    elif fav_fair_prob_delta >= 0.025:
        coherence_score += 0.15
    elif fav_fair_prob_delta <= 0.000:
        warning_flags.append("flat_fair_move")
        coherence_score -= 0.10

    if fav_fair_curr > 0 and fav_odd_curr < (fav_fair_curr * 0.87):
        warning_flags.append("market_value_trap")
        coherence_score -= 0.28
        
    # -------------------------
    # WARNING STRUTTURALI
    # -------------------------
    if not bilateral_ft and o25 <= 1.68:
        warning_flags.append("o25_too_low_for_one_sided_ft")
    if not bilateral_ht and o05 <= 1.24:
        warning_flags.append("o05ht_too_low_for_one_sided_ht")

    if cross_home_clean < 2.00 and cross_away_clean < 2.00 and o25 <= 1.80:
        warning_flags.append("ft_market_ahead_of_structure")

    if combined_ht_clean < 0.84 and o05 <= 1.28:
        warning_flags.append("ht_market_ahead_of_structure")

    # -------------------------
    # MARKET PROFILE
    # -------------------------
    market_profile = "neutral"

    if baseline["ft_market"] in ("very_open", "open") and baseline["ht_market"] in ("very_open", "open"):
        market_profile = "full_open"
    elif baseline["ft_market"] in ("very_open", "open") and baseline["ht_market"] in ("moderate", "tight"):
        market_profile = "ft_open_ht_lag"
    elif baseline["ft_market"] in ("moderate", "tight") and baseline["ht_market"] in ("very_open", "open"):
        market_profile = "ht_open_ft_lag"
    elif baseline["fav_zone"] in ("ultra", "strong") and baseline["ft_market"] in ("open", "very_open"):
        market_profile = "favorite_pressure_open"
    elif baseline["fav_zone"] in ("ultra", "strong") and baseline["ft_market"] in ("tight", "moderate"):
        market_profile = "favorite_control"
    elif baseline["fav_zone"] in ("gold", "balanced") and baseline["ft_market"] in ("open", "very_open"):
        market_profile = "balanced_open"

    # -------------------------
    # LAGGING MARKET
    # -------------------------
    lagging_market = "none"

    if "lagged_ht15" in positive_flags:
        lagging_market = "o15ht"
    elif "lagged_ht_opening" in positive_flags:
        lagging_market = "o05ht"
    elif "lagged_ft_after_ht" in positive_flags:
        lagging_market = "o25"

    coherence_score = round3(max(coherence_score, 0.0))
    dislocation_score = round3(dislocation_score)

    if dislocation_score < 0:
        dislocation_score = 0.0

    # rating sintetico
    if coherence_score >= 2.80:
        coherence_level = "high"
    elif coherence_score >= 1.65:
        coherence_level = "medium"
    else:
        coherence_level = "low"

    if dislocation_score >= 1.00:
        dislocation_level = "high"
    elif dislocation_score >= 0.45:
        dislocation_level = "medium"
    else:
        dislocation_level = "low"

    return {
        "market_profile": market_profile,
        "coherence_score": coherence_score,
        "coherence_level": coherence_level,
        "dislocation_score": dislocation_score,
        "dislocation_level": dislocation_level,
        "leading_market": leader,
        "lagging_market": lagging_market,
        "pressure_strength": pressure.get("strength", "none"),
        "drop_type": drop_type,
        "drop_strength": drop_info.get("drop_strength", "none"),
        "drop_confirmed": bool(drop_info.get("confirmed", False)),
        "value_left": value_left,
        "positive_flags": positive_flags,
        "warning_flags": warning_flags,
        "baseline": baseline,
        "struct_snapshot": struct,
        "pressure_snapshot": pressure,

        "margin_open": round3(margin_open),
        "margin_curr": round3(margin_curr),
        "margin_delta": round3(margin_curr - margin_open) if (margin_open or margin_curr) else 0.0,
        "fav_fair_prob_open": fav_fair_prob_open,
        "fav_fair_prob_curr": fav_fair_prob_curr,
        "fav_fair_prob_delta": fav_fair_prob_delta,
        "fav_fair_curr": round3(fav_fair_curr),

        "home_ft_stdev": safe_float(s_h.get("ft_stdev", 9.9), 9.9),
        "away_ft_stdev": safe_float(s_a.get("ft_stdev", 9.9), 9.9),
        "home_scoring_regularity": safe_float(s_h.get("scoring_regularity", 0.0), 0.0),
        "away_scoring_regularity": safe_float(s_a.get("scoring_regularity", 0.0), 0.0),
    }


def build_market_debug_summary(market_pack):
    if not market_pack:
        return {}

    return {
        "market_profile": market_pack.get("market_profile", "neutral"),
        "coherence_score": market_pack.get("coherence_score", 0.0),
        "coherence_level": market_pack.get("coherence_level", "low"),
        "dislocation_score": market_pack.get("dislocation_score", 0.0),
        "dislocation_level": market_pack.get("dislocation_level", "low"),
        "leading_market": market_pack.get("leading_market", "none"),
        "lagging_market": market_pack.get("lagging_market", "none"),
        "drop_type": market_pack.get("drop_type", "none"),
        "drop_strength": market_pack.get("drop_strength", "none"),
        "drop_confirmed": market_pack.get("drop_confirmed", False),
        "value_left": market_pack.get("value_left", "unknown"),
        "positive_flags": market_pack.get("positive_flags", []),
        "warning_flags": market_pack.get("warning_flags", []),
        "margin_open": market_pack.get("margin_open", 0.0),
        "margin_curr": market_pack.get("margin_curr", 0.0),
        "margin_delta": market_pack.get("margin_delta", 0.0),
        "fav_fair_prob_open": market_pack.get("fav_fair_prob_open", 0.0),
        "fav_fair_prob_curr": market_pack.get("fav_fair_prob_curr", 0.0),
        "fav_fair_prob_delta": market_pack.get("fav_fair_prob_delta", 0.0),
        "fav_fair_curr": market_pack.get("fav_fair_curr", 0.0),
        "home_ft_stdev": market_pack.get("home_ft_stdev", 9.9),
        "away_ft_stdev": market_pack.get("away_ft_stdev", 9.9),
        "home_scoring_regularity": market_pack.get("home_scoring_regularity", 0.0),
        "away_scoring_regularity": market_pack.get("away_scoring_regularity", 0.0),
    }
    
#====================================
# BLOCCO 4
# MATCH STRUCTURE + ARCHETYPES + SCORING V25
# - struttura partita
# - rischio one-sided
# - archetipi
# - score PTGG / PT1.5 / OVER / BOOST / GOLD
#====================================

def band_score(value, core_low, core_high, soft_low=None, soft_high=None, core_pts=1.0, soft_pts=0.45):
    v = safe_float(value, 0.0)
    if core_low <= v <= core_high:
        return core_pts
    if soft_low is not None and soft_high is not None and soft_low <= v <= soft_high:
        return soft_pts
    return 0.0


def symmetry_bonus(a, b, tight=0.22, medium=0.45):
    diff = abs(safe_float(a, 0.0) - safe_float(b, 0.0))
    if diff <= tight:
        return 0.8
    if diff <= medium:
        return 0.4
    return 0.0

def get_goldilocks_multiplier(fav_quote):
    """
    Bonus leggero per la fascia quota favorita più fertile
    per match dinamici ma non troppo squilibrati.

    Non crea segnali: amplifica leggermente score già validi.
    """
    fav = safe_float(fav_quote, 0.0)

    if 1.55 <= fav <= 1.83:
        return 1.10
    if 1.50 <= fav <= 1.90:
        return 1.04
    return 1.0


def calc_one_sided_risk(s_h, s_a):
    """
    Rischio partita troppo unilaterale.
    Più alto = più probabile match da 1-0 / 2-0 / 3-0 sporco,
    quindi meno adatto soprattutto a GOLD e OVER bilaterali.
    """
    home_attack = safe_float(s_h.get("avg_ft_scored_clean"), 0.0)
    away_attack = safe_float(s_a.get("avg_ft_scored_clean"), 0.0)

    home_concede = safe_float(s_h.get("avg_ft_conceded_clean"), 0.0)
    away_concede = safe_float(s_a.get("avg_ft_conceded_clean"), 0.0)

    home_ht_scored = safe_float(s_h.get("avg_ht_scored"), 0.0)
    away_ht_scored = safe_float(s_a.get("avg_ht_scored"), 0.0)

    attack_gap = abs(home_attack - away_attack)
    ht_gap = abs(home_ht_scored - away_ht_scored)

    weaker_attack = min(home_attack, away_attack)
    weaker_ht_attack = min(home_ht_scored, away_ht_scored)

    risk = 0.0

    if attack_gap >= 1.00:
        risk += 1.40
    elif attack_gap >= 0.75:
        risk += 0.90
    elif attack_gap >= 0.50:
        risk += 0.40

    if ht_gap >= 0.65:
        risk += 0.75
    elif ht_gap >= 0.45:
        risk += 0.40

    if weaker_attack < 0.90:
        risk += 1.00
    elif weaker_attack < 1.05:
        risk += 0.50

    if weaker_ht_attack < 0.40:
        risk += 0.60
    elif weaker_ht_attack < 0.55:
        risk += 0.28

    if home_concede < 0.82:
        risk += 0.25
    if away_concede < 0.82:
        risk += 0.25

    return round3(risk)


def build_match_structure_profile(mk, s_h, s_a, market_pack=None, quote_pack=None):
    """
    Costruisce il profilo strutturale del match.
    Non assegna ancora i tag finali.
    """
    market_pack = market_pack or {}
    quote_pack = quote_pack or {}

    home_attack = safe_float(s_h.get("avg_ft_scored_clean"), 0.0)
    away_attack = safe_float(s_a.get("avg_ft_scored_clean"), 0.0)
    home_concede = safe_float(s_h.get("avg_ft_conceded_clean"), 0.0)
    away_concede = safe_float(s_a.get("avg_ft_conceded_clean"), 0.0)

    home_ht_attack = safe_float(s_h.get("avg_ht_scored_clean"), 0.0)
    away_ht_attack = safe_float(s_a.get("avg_ht_scored_clean"), 0.0)

    combined_ht_clean = round3((safe_float(s_h.get("avg_ht_clean"), 0.0) + safe_float(s_a.get("avg_ht_clean"), 0.0)) / 2)
    combined_ft_clean = round3((safe_float(s_h.get("avg_total_clean"), 0.0) + safe_float(s_a.get("avg_total_clean"), 0.0)) / 2)
    combined_ht_scored_clean = round3((home_ht_attack + away_ht_attack) / 2)

    cross_home_clean = round3(home_attack + away_concede)
    cross_away_clean = round3(away_attack + home_concede)

    combined_ht_dirty = round3((safe_float(s_h.get("avg_ht", 0.0), 0.0) + safe_float(s_a.get("avg_ht", 0.0), 0.0)) / 2)
    combined_ft_dirty = round3((safe_float(s_h.get("avg_total", 0.0), 0.0) + safe_float(s_a.get("avg_total", 0.0), 0.0)) / 2)

    cross_home_dirty = round3(
        safe_float(s_h.get("avg_ft_scored", 0.0), 0.0) +
        safe_float(s_a.get("avg_ft_conceded", 0.0), 0.0)
    )
    cross_away_dirty = round3(
        safe_float(s_a.get("avg_ft_scored", 0.0), 0.0) +
        safe_float(s_h.get("avg_ft_conceded", 0.0), 0.0)
    )

    bilateral_ft = bool(home_attack >= 1.00 and away_attack >= 1.00)
    bilateral_ht = bool(home_ht_attack >= 0.60 and away_ht_attack >= 0.60)

    one_sided_risk = calc_one_sided_risk(s_h, s_a)

    fav = min(safe_float(mk.get("q1"), 0.0), safe_float(mk.get("q2"), 0.0))
    fav_zone = classify_favorite_zone(fav)

    match_profile = "neutral"

    if bilateral_ht and combined_ht_clean >= 1.00 and combined_ht_scored_clean >= 0.80:
        match_profile = "early_pressure"

    if bilateral_ft and combined_ft_clean >= 1.72 and cross_home_clean >= 2.10 and cross_away_clean >= 2.10:
        match_profile = "open_match"

    if fav_zone in ("ultra", "strong") and combined_ft_clean >= 1.60 and one_sided_risk <= 1.05:
        match_profile = "favorite_pressure"

    if one_sided_risk >= 1.35 and (home_attack >= 1.35 or away_attack >= 1.35):
        match_profile = "asymmetric"

    if combined_ft_clean < 1.48 and combined_ht_clean < 0.84:
        match_profile = "low_event"

    if (
        market_pack.get("market_profile") == "favorite_control"
        and one_sided_risk >= 1.10
        and not bilateral_ft
    ):
        match_profile = "control_risk"

    structure_grade = "low"
    structure_score = 0.0

    # 1) ingresso base: sporco
    if combined_ft_dirty >= 1.72:
        structure_score += 0.70
    elif combined_ft_dirty >= 1.58:
        structure_score += 0.35

    if combined_ht_dirty >= 0.96:
        structure_score += 0.40
    elif combined_ht_dirty >= 0.84:
        structure_score += 0.18

    if cross_home_dirty >= 2.15:
        structure_score += 0.40
    elif cross_home_dirty >= 2.00:
        structure_score += 0.18

    if cross_away_dirty >= 2.15:
        structure_score += 0.40
    elif cross_away_dirty >= 2.00:
        structure_score += 0.18

    # 2) conferma qualità: pulito
    if combined_ft_clean >= 1.66:
        structure_score += 0.25
    elif combined_ft_clean < 1.48:
        structure_score -= 0.18

    if combined_ht_clean >= 0.92:
        structure_score += 0.18
    elif combined_ht_clean < 0.76:
        structure_score -= 0.12

    if cross_home_clean >= 2.08:
        structure_score += 0.18
    elif cross_home_clean < 1.92:
        structure_score -= 0.10

    if cross_away_clean >= 2.08:
        structure_score += 0.18
    elif cross_away_clean < 1.92:
        structure_score -= 0.10

    if bilateral_ft:
        structure_score += 0.40
    if bilateral_ht:
        structure_score += 0.20

    if one_sided_risk <= 0.90:
        structure_score += 0.35
    elif one_sided_risk <= 1.20:
        structure_score += 0.10
    else:
        structure_score -= 0.40

    structure_score = round3(max(structure_score, 0.0))

    if structure_score >= 2.10:
        structure_grade = "high"
    elif structure_score >= 1.20:
        structure_grade = "medium"

    return {
        "match_profile": match_profile,
        "structure_score": structure_score,
        "structure_grade": structure_grade,
        "combined_ht_clean": combined_ht_clean,
        "combined_ft_clean": combined_ft_clean,
        "combined_ht_scored_clean": combined_ht_scored_clean,
        "cross_home_clean": cross_home_clean,
        "cross_away_clean": cross_away_clean,
        "bilateral_ft": bilateral_ft,
        "bilateral_ht": bilateral_ht,
        "one_sided_risk": round3(one_sided_risk),
        "fav_quote": round3(fav),
        "fav_zone": fav_zone,
        "combined_ht_dirty": combined_ht_dirty,
        "combined_ft_dirty": combined_ft_dirty,
        "cross_home_dirty": cross_home_dirty,
        "cross_away_dirty": cross_away_dirty,
    }

def build_structure_debug_summary(structure_pack):
    if not structure_pack:
        return {}

    return {
        "match_profile": structure_pack.get("match_profile", "neutral"),
        "structure_score": structure_pack.get("structure_score", 0.0),
        "structure_grade": structure_pack.get("structure_grade", "low"),
        "combined_ht_clean": structure_pack.get("combined_ht_clean", 0.0),
        "combined_ft_clean": structure_pack.get("combined_ft_clean", 0.0),
        "combined_ht_scored_clean": structure_pack.get("combined_ht_scored_clean", 0.0),
        "cross_home_clean": structure_pack.get("cross_home_clean", 0.0),
        "cross_away_clean": structure_pack.get("cross_away_clean", 0.0),
        "bilateral_ft": structure_pack.get("bilateral_ft", False),
        "bilateral_ht": structure_pack.get("bilateral_ht", False),
        "one_sided_risk": structure_pack.get("one_sided_risk", 0.0),
        "fav_quote": structure_pack.get("fav_quote", 0.0),
        "fav_zone": structure_pack.get("fav_zone", "unknown"),
        "combined_ht_dirty": structure_pack.get("combined_ht_dirty", 0.0),
        "combined_ft_dirty": structure_pack.get("combined_ft_dirty", 0.0),
        "cross_home_dirty": structure_pack.get("cross_home_dirty", 0.0),
        "cross_away_dirty": structure_pack.get("cross_away_dirty", 0.0),
    }

def score_ptgg_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack):
    """
    PTGG = candidata da almeno 1 goal nel primo tempo.
    Versione V26:
    - meno dipendente dalle medie nude HT
    - più dipendente da frequenze reali HT
    - il mercato HT deve almeno confermare
    """
    score = 0.0

    fav = safe_float(structure_pack.get("fav_quote", 0.0), 0.0)
    drop_type = market_pack.get("drop_type", "none")
    coherence = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    dislocation = safe_float(market_pack.get("dislocation_score", 0.0), 0.0)
    lagging_market = market_pack.get("lagging_market", "none")

    home_ht_scored = safe_float(s_h.get("avg_ht_scored"), 0.0)
    away_ht_scored = safe_float(s_a.get("avg_ht_scored"), 0.0)

    home_ht_scored_1plus = safe_float(s_h.get("ht_scored_1plus_rate", 0.0), 0.0)
    away_ht_scored_1plus = safe_float(s_a.get("ht_scored_1plus_rate", 0.0), 0.0)
    home_ht_conceded_1plus = safe_float(s_h.get("ht_conceded_1plus_rate", 0.0), 0.0)
    away_ht_conceded_1plus = safe_float(s_a.get("ht_conceded_1plus_rate", 0.0), 0.0)

    home_scored_by_ht_rate = safe_float(s_h.get("scored_by_ht_rate", 0.0), 0.0)
    away_scored_by_ht_rate = safe_float(s_a.get("scored_by_ht_rate", 0.0), 0.0)
    home_conceded_by_ht_rate = safe_float(s_h.get("conceded_by_ht_rate", 0.0), 0.0)
    away_conceded_by_ht_rate = safe_float(s_a.get("conceded_by_ht_rate", 0.0), 0.0)

    home_ht_00_rate = safe_float(s_h.get("ht_00_rate", 0.0), 0.0)
    away_ht_00_rate = safe_float(s_a.get("ht_00_rate", 0.0), 0.0)

    home_early_2goal_rate = safe_float(s_h.get("early_2goal_rate", 0.0), 0.0)
    away_early_2goal_rate = safe_float(s_a.get("early_2goal_rate", 0.0), 0.0)

    combined_ht_clean = safe_float(structure_pack.get("combined_ht_clean", 0.0), 0.0)
    combined_ht_scored_clean = safe_float(structure_pack.get("combined_ht_scored_clean", 0.0), 0.0)
    bilateral_ht = bool(structure_pack.get("bilateral_ht", False))
    match_profile = structure_pack.get("match_profile", "neutral")

    o05 = safe_float(mk.get("o05ht"), 0.0)
    o15 = safe_float(mk.get("o15ht"), 0.0)

    # -------------------------
    # BASE REALI DI FREQUENZA
    # -------------------------
    if home_ht_scored_1plus >= 0.62:
        score += 0.95
    elif home_ht_scored_1plus >= 0.50:
        score += 0.45

    if away_ht_scored_1plus >= 0.62:
        score += 0.95
    elif away_ht_scored_1plus >= 0.50:
        score += 0.45

    if home_scored_by_ht_rate >= 0.62:
        score += 0.55
    elif home_scored_by_ht_rate >= 0.50:
        score += 0.25

    if away_scored_by_ht_rate >= 0.62:
        score += 0.55
    elif away_scored_by_ht_rate >= 0.50:
        score += 0.25

    if home_ht_conceded_1plus >= 0.50:
        score += 0.35
    elif home_ht_conceded_1plus >= 0.38:
        score += 0.15

    if away_ht_conceded_1plus >= 0.50:
        score += 0.35
    elif away_ht_conceded_1plus >= 0.38:
        score += 0.15

    if home_conceded_by_ht_rate >= 0.50:
        score += 0.18
    if away_conceded_by_ht_rate >= 0.50:
        score += 0.18

    if home_early_2goal_rate >= 0.35:
        score += 0.18
    if away_early_2goal_rate >= 0.35:
        score += 0.18

    # -------------------------
    # MEDIE HT SOLO COME SUPPORTO
    # -------------------------
    if home_ht_scored >= 0.78:
        score += 0.30
    elif home_ht_scored < 0.55:
        score -= 0.70

    if away_ht_scored >= 0.78:
        score += 0.30
    elif away_ht_scored < 0.55:
        score -= 0.70

    if combined_ht_clean >= 0.96:
        score += 0.45
    elif combined_ht_clean >= 0.88:
        score += 0.18

    if combined_ht_scored_clean >= 0.78:
        score += 0.35
    elif combined_ht_scored_clean < 0.64:
        score -= 0.35

    if bilateral_ht:
        score += 0.55

    # -------------------------
    # MERCATO HT: ORA È VERO GATE
    # -------------------------
    if 1.20 <= o05 <= 1.38:
        score += 1.10
    elif 1.38 < o05 <= 1.44:
        score += 0.20
    elif o05 == 0 or o05 > 1.44:
        score -= 1.10

    if 2.00 <= o15 <= 3.30:
        score += 0.25
    elif o15 > 4.00 and o15 != 0:
        score -= 0.25

    if lagging_market in ("o05ht", "o15ht") and dislocation >= 0.55:
        score += 0.28

    if coherence >= 2.00:
        score += 0.22

    if drop_type == "structural":
        score += 0.12

    # -------------------------
    # PROFILO PARTITA
    # -------------------------
    if match_profile == "early_pressure":
        score += 0.45
    elif match_profile in ("open_match", "favorite_pressure"):
        score += 0.15

    # -------------------------
    # PENALITÀ
    # -------------------------
    if home_ht_00_rate >= 0.50:
        score -= 0.55
    elif home_ht_00_rate >= 0.38:
        score -= 0.22

    if away_ht_00_rate >= 0.50:
        score -= 0.55
    elif away_ht_00_rate >= 0.38:
        score -= 0.22

    if home_ht_scored_1plus < 0.40:
        score -= 0.65
    if away_ht_scored_1plus < 0.40:
        score -= 0.65

    if fav < 1.28:
        score -= 0.22

    # Goldilocks più leggero sul PT base
    multiplier = get_goldilocks_multiplier(fav)
    score *= (1.0 + ((multiplier - 1.0) * 0.60))

    return round3(max(score, 0.0))


def score_pto15_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack):
    """
    PT1.5 = candidata da 2+ goal nel primo tempo.
    Versione V26:
    - molto più severa
    - deve avere frequenza HT alta e mercato HT coerente
    """
    score = 0.0

    fav = safe_float(structure_pack.get("fav_quote", 0.0), 0.0)
    combined_ht_scored = safe_float(structure_pack.get("combined_ht_scored_clean", 0.0), 0.0)
    combined_ht_clean = safe_float(structure_pack.get("combined_ht_clean", 0.0), 0.0)
    bilateral_ht = bool(structure_pack.get("bilateral_ht", False))
    match_profile = structure_pack.get("match_profile", "neutral")

    coherence = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    dislocation = safe_float(market_pack.get("dislocation_score", 0.0), 0.0)
    lagging_market = market_pack.get("lagging_market", "none")

    home_ht_scored = safe_float(s_h.get("avg_ht_scored"), 0.0)
    away_ht_scored = safe_float(s_a.get("avg_ht_scored"), 0.0)

    home_ht_scored_1plus = safe_float(s_h.get("ht_scored_1plus_rate", 0.0), 0.0)
    away_ht_scored_1plus = safe_float(s_a.get("ht_scored_1plus_rate", 0.0), 0.0)
    home_ht_scored_2plus = safe_float(s_h.get("ht_scored_2plus_rate", 0.0), 0.0)
    away_ht_scored_2plus = safe_float(s_a.get("ht_scored_2plus_rate", 0.0), 0.0)

    home_ht_conceded_1plus = safe_float(s_h.get("ht_conceded_1plus_rate", 0.0), 0.0)
    away_ht_conceded_1plus = safe_float(s_a.get("ht_conceded_1plus_rate", 0.0), 0.0)

    home_scored_by_ht_rate = safe_float(s_h.get("scored_by_ht_rate", 0.0), 0.0)
    away_scored_by_ht_rate = safe_float(s_a.get("scored_by_ht_rate", 0.0), 0.0)
    home_early_2goal_rate = safe_float(s_h.get("early_2goal_rate", 0.0), 0.0)
    away_early_2goal_rate = safe_float(s_a.get("early_2goal_rate", 0.0), 0.0)
    home_ht_00_rate = safe_float(s_h.get("ht_00_rate", 0.0), 0.0)
    away_ht_00_rate = safe_float(s_a.get("ht_00_rate", 0.0), 0.0)

    o05 = safe_float(mk.get("o05ht"), 0.0)
    o15 = safe_float(mk.get("o15ht"), 0.0)

    # frequenze vere
    if home_ht_scored_1plus >= 0.62:
        score += 0.55
    elif home_ht_scored_1plus >= 0.50:
        score += 0.18

    if away_ht_scored_1plus >= 0.62:
        score += 0.55
    elif away_ht_scored_1plus >= 0.50:
        score += 0.18

    if home_ht_scored_2plus >= 0.25:
        score += 0.55
    elif home_ht_scored_2plus >= 0.18:
        score += 0.20

    if away_ht_scored_2plus >= 0.25:
        score += 0.55
    elif away_ht_scored_2plus >= 0.18:
        score += 0.20

    if home_ht_conceded_1plus >= 0.50:
        score += 0.22
    if away_ht_conceded_1plus >= 0.50:
        score += 0.22

    if home_scored_by_ht_rate >= 0.62:
        score += 0.28
    if away_scored_by_ht_rate >= 0.62:
        score += 0.28

    if home_early_2goal_rate >= 0.35:
        score += 0.30
    if away_early_2goal_rate >= 0.35:
        score += 0.30

    # medie come conferma
    if combined_ht_scored >= 0.92:
        score += 0.70
    elif combined_ht_scored >= 0.82:
        score += 0.25
    elif combined_ht_scored < 0.72:
        score -= 0.50

    if combined_ht_clean >= 1.00:
        score += 0.42
    elif combined_ht_clean < 0.88:
        score -= 0.25

    if home_ht_scored >= 0.90 and away_ht_scored >= 0.90:
        score += 0.55
    elif home_ht_scored < 0.55 or away_ht_scored < 0.55:
        score -= 0.75

    if bilateral_ht:
        score += 0.22

    # mercato: obbligatorio
    if 1.20 <= o05 <= 1.36:
        score += 0.45
    elif o05 > 1.42 and o05 != 0:
        score -= 0.45

    if 2.00 <= o15 <= 3.20:
        score += 1.00
    elif 3.20 < o15 <= 3.80:
        score += 0.22
    elif o15 == 0 or o15 > 3.80:
        score -= 1.00

    if lagging_market == "o15ht" and dislocation >= 0.70:
        score += 0.35

    if coherence >= 2.05:
        score += 0.15

    if match_profile == "early_pressure":
        score += 0.25

    # penalità
    if home_ht_00_rate >= 0.50:
        score -= 0.50
    if away_ht_00_rate >= 0.50:
        score -= 0.50

    if home_ht_scored_1plus < 0.40:
        score -= 0.55
    if away_ht_scored_1plus < 0.40:
        score -= 0.55

    if fav < 1.28:
        score -= 0.18

    # Goldilocks multiplier: il PT1.5 è molto sensibile a questa fascia
    multiplier = get_goldilocks_multiplier(fav)
    score *= multiplier

    return round3(max(score, 0.0))


def score_over_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack):
    """
    OVER = segnale FT puro.
    Qui contano:
    - bilateralità
    - cross attacco/difesa
    - coerenza mercato
    - evitare false aperture da una sola squadra
    """
    score = 0.0

    home_scored = safe_float(s_h.get("avg_ft_scored", 0.0), 0.0)
    away_scored = safe_float(s_a.get("avg_ft_scored", 0.0), 0.0)
    home_conceded = safe_float(s_h.get("avg_ft_conceded", 0.0), 0.0)
    away_conceded = safe_float(s_a.get("avg_ft_conceded", 0.0), 0.0)

    home_scored_clean = safe_float(s_h.get("avg_ft_scored_clean", 0.0), 0.0)
    away_scored_clean = safe_float(s_a.get("avg_ft_scored_clean", 0.0), 0.0)
    home_conceded_clean = safe_float(s_h.get("avg_ft_conceded_clean", 0.0), 0.0)
    away_conceded_clean = safe_float(s_a.get("avg_ft_conceded_clean", 0.0), 0.0)

    cross_home_dirty = home_scored + away_conceded
    cross_away_dirty = away_scored + home_conceded
    cross_home_clean = safe_float(structure_pack.get("cross_home_clean", 0.0), 0.0)
    cross_away_clean = safe_float(structure_pack.get("cross_away_clean", 0.0), 0.0)

    combined_ft_clean = safe_float(structure_pack.get("combined_ft_clean", 0.0), 0.0)
    bilateral_ft = bool(structure_pack.get("bilateral_ft", False))
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)
    fav = safe_float(structure_pack.get("fav_quote", 0.0), 0.0)

    coherence = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    dislocation = safe_float(market_pack.get("dislocation_score", 0.0), 0.0)
    market_profile = market_pack.get("market_profile", "neutral")
    drop_type = market_pack.get("drop_type", "none")

    home_ft_stdev = safe_float(s_h.get("ft_stdev", 9.9), 9.9)
    away_ft_stdev = safe_float(s_a.get("ft_stdev", 9.9), 9.9)
    home_scoring_regularity = safe_float(s_h.get("scoring_regularity", 0.0), 0.0)
    away_scoring_regularity = safe_float(s_a.get("scoring_regularity", 0.0), 0.0)

    if cross_home_clean >= 2.20:
        score += 0.65
    elif cross_home_clean >= 2.05:
        score += 0.25

    if cross_away_clean >= 2.20:
        score += 0.65
    elif cross_away_clean >= 2.05:
        score += 0.25

    if cross_home_clean >= 2.15 and cross_away_clean >= 2.15:
        score += 1.20
    elif (cross_home_clean >= 2.20 and cross_away_clean >= 1.98) or (cross_away_clean >= 2.20 and cross_home_clean >= 1.98):
        score += 0.58

    if cross_home_dirty >= 2.35:
        score += 0.95
    elif cross_home_dirty >= 2.20:
        score += 0.38

    if cross_away_dirty >= 2.35:
        score += 0.95
    elif cross_away_dirty >= 2.20:
        score += 0.38

    if home_scored_clean >= 1.10:
        score += 0.35
    elif home_scored_clean >= 0.82:
        score += 0.00
    else:
        score -= 0.12

    if away_scored_clean >= 1.10:
        score += 0.35
    elif away_scored_clean >= 0.82:
        score += 0.00
    else:
        score -= 0.12

    if bilateral_ft:
        score += 0.48

    if away_conceded_clean >= 1.05:
        score += 0.28
    if home_conceded_clean >= 1.05:
        score += 0.28

    if safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0) >= 0.75:
        score += 0.40
    elif safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0) >= 0.62:
        score += 0.18

    if safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0) >= 0.75:
        score += 0.40
    elif safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0) >= 0.62:
        score += 0.18

    score += band_score(
        safe_float(mk.get("o25"), 0.0),
        1.52, 2.18,
        1.42, 2.40,
        core_pts=1.28,
        soft_pts=0.52
    )

    if market_profile in ("full_open", "balanced_open", "favorite_pressure_open"):
        score += 0.28

    if coherence >= 2.00:
        score += 0.35
    elif coherence >= 1.50:
        score += 0.15

    if drop_type == "structural":
        score += 0.18
    elif drop_type == "sterile":
        score -= 0.12

    if dislocation >= 0.55 and market_pack.get("lagging_market") == "o25":
        score += 0.22

    if 1.35 <= fav <= 2.20:
        score += 0.18

    if one_sided_risk <= 0.90:
        score += 0.20
    elif one_sided_risk >= 1.35:
        score -= 0.42
    elif one_sided_risk >= 1.20:
        score -= 0.24
    elif one_sided_risk >= 1.10:
        score -= 0.12

    # -------------------------
    # REGOLARITÀ / STDEV
    # -------------------------
    if home_ft_stdev <= 1.15:
        score += 0.28
    elif home_ft_stdev <= 1.35:
        score += 0.12
    elif home_ft_stdev >= 1.90:
        score -= 0.42
    elif home_ft_stdev >= 1.70:
        score -= 0.34

    if away_ft_stdev <= 1.15:
        score += 0.28
    elif away_ft_stdev <= 1.35:
        score += 0.12
    elif away_ft_stdev >= 1.90:
        score -= 0.42
    elif away_ft_stdev >= 1.70:
        score -= 0.34

    if home_scoring_regularity >= 0.75:
        score += 0.28
    elif home_scoring_regularity >= 0.62:
        score += 0.12
    elif home_scoring_regularity <= 0.45:
        score -= 0.35

    if away_scoring_regularity >= 0.75:
        score += 0.28
    elif away_scoring_regularity >= 0.62:
        score += 0.12
    elif away_scoring_regularity <= 0.45:
        score -= 0.35

    if combined_ft_clean < 1.48:
        score -= 0.28
    elif combined_ft_clean < 1.58:
        score -= 0.12

    if home_scored_clean < 0.92:
        score -= 0.28
    if away_scored_clean < 0.92:
        score -= 0.28

    if away_conceded_clean < 0.88:
        score -= 0.35
    if home_conceded_clean < 0.88:
        score -= 0.35

    if safe_float(s_h.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.60
    if safe_float(s_a.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.60

    if cross_home_clean < 2.00:
        score -= 0.55
    if cross_away_clean < 2.00:
        score -= 0.55

    # Goldilocks multiplier: piccolo boost ai match nel range quota più fertile
    multiplier = get_goldilocks_multiplier(fav)
    score *= multiplier

    return round3(max(score, 0.0))


def score_boost_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack, pt_score, over_score):
    """
    BOOST = convergenza forte tra PT e OVER.
    Non deve essere solo score alto: deve sembrare una partita che accelera davvero.
    """
    score = 0.0

    combined_ht_clean = safe_float(structure_pack.get("combined_ht_clean", 0.0), 0.0)
    combined_ft_clean = safe_float(structure_pack.get("combined_ft_clean", 0.0), 0.0)
    match_profile = structure_pack.get("match_profile", "neutral")
    coherence = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    dislocation = safe_float(market_pack.get("dislocation_score", 0.0), 0.0)
    leading_market = market_pack.get("leading_market", "none")
    lagging_market = market_pack.get("lagging_market", "none")
    drop_type = market_pack.get("drop_type", "none")
    home_ft_stdev = safe_float(s_h.get("ft_stdev", 9.9), 9.9)
    away_ft_stdev = safe_float(s_a.get("ft_stdev", 9.9), 9.9)
    home_scoring_regularity = safe_float(s_h.get("scoring_regularity", 0.0), 0.0)
    away_scoring_regularity = safe_float(s_a.get("scoring_regularity", 0.0), 0.0)

    score += pt_score * 0.16
    score += over_score * 0.58

    if over_score < 4.40:
        score -= 0.90
    elif over_score < 4.80:
        score -= 0.35

    if combined_ht_clean >= 1.00:
        score += 0.35
    elif combined_ht_clean >= 0.92:
        score += 0.12

    if combined_ft_clean >= 1.72:
        score += 0.70
    elif combined_ft_clean >= 1.64:
        score += 0.25

    if safe_float(s_h.get("ht_1plus_rate", 0.0), 0.0) >= 0.62 and safe_float(s_a.get("ht_1plus_rate", 0.0), 0.0) >= 0.62:
        score += 0.42

    if safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0) >= 0.62 and safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0) >= 0.62:
        score += 0.42

    if 1.55 <= safe_float(mk.get("o25"), 0.0) <= 2.10 and 1.25 <= safe_float(mk.get("o05ht"), 0.0) <= 1.40:
        score += 0.58
    elif 1.50 <= safe_float(mk.get("o25"), 0.0) <= 2.20 and 1.22 <= safe_float(mk.get("o05ht"), 0.0) <= 1.42:
        score += 0.25

    if match_profile in ("early_pressure", "open_match", "favorite_pressure"):
        score += 0.28

    if coherence >= 2.20:
        score += 0.38

    if dislocation >= 0.60 and lagging_market in ("o15ht", "o05ht", "o25"):
        score += 0.25

    if leading_market == "mixed":
        score += 0.18

    if drop_type == "structural":
        score += 0.20

    # -------------------------
    # REGOLARITÀ / STDEV
    # BOOST deve essere forte ma non sporco
    # -------------------------
    if home_ft_stdev <= 1.15 and away_ft_stdev <= 1.15:
        score += 0.38
    elif home_ft_stdev <= 1.35 and away_ft_stdev <= 1.35:
        score += 0.18
    elif home_ft_stdev >= 1.75 or away_ft_stdev >= 1.75:
        score -= 0.42

    if home_scoring_regularity >= 0.70 and away_scoring_regularity >= 0.70:
        score += 0.34
    elif home_scoring_regularity <= 0.45 or away_scoring_regularity <= 0.45:
        score -= 0.42

    if safe_float(s_h.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.70
    if safe_float(s_a.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.70

    if safe_float(s_h.get("ht_zero_rate", 0.0), 0.0) >= 0.38:
        score -= 0.60
    if safe_float(s_a.get("ht_zero_rate", 0.0), 0.0) >= 0.38:
        score -= 0.60

    if combined_ht_clean < 0.88:
        score -= 0.50
    if combined_ft_clean < 1.48:
        score -= 0.55

    return round3(max(score, 0.0))


def score_gold_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack, pt_score, over_score):
    """
    GOLD = partita forte ma soprattutto leggibile.
    Deve essere:
    - strutturalmente sana
    - con contributo bilaterale abbastanza reale
    - coerente col book
    - senza rischio unilaterale eccessivo
    """
    score = 0.0

    combined_ht_clean = safe_float(structure_pack.get("combined_ht_clean", 0.0), 0.0)
    combined_ft_clean = safe_float(structure_pack.get("combined_ft_clean", 0.0), 0.0)
    combined_ht_scored_clean = safe_float(structure_pack.get("combined_ht_scored_clean", 0.0), 0.0)
    cross_home_clean = safe_float(structure_pack.get("cross_home_clean", 0.0), 0.0)
    cross_away_clean = safe_float(structure_pack.get("cross_away_clean", 0.0), 0.0)
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)
    fav = safe_float(structure_pack.get("fav_quote", 0.0), 0.0)
    match_profile = structure_pack.get("match_profile", "neutral")

    coherence = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    dislocation = safe_float(market_pack.get("dislocation_score", 0.0), 0.0)
    market_profile = market_pack.get("market_profile", "neutral")
    drop_type = market_pack.get("drop_type", "none")
    value_left = market_pack.get("value_left", "unknown")

    home_scored_clean = safe_float(s_h.get("avg_ft_scored_clean", 0.0), 0.0)
    away_scored_clean = safe_float(s_a.get("avg_ft_scored_clean", 0.0), 0.0)

    score += over_score * 0.56
    score += pt_score * 0.06

    if over_score < 4.70:
        score -= 1.10
    elif over_score < 5.05:
        score -= 0.45

    if 1.40 <= fav <= 1.90:
        score += 0.72
    elif 1.36 <= fav <= 2.00:
        score += 0.25

    if cross_home_clean >= 2.18:
        score += 0.72
    elif cross_home_clean >= 2.06:
        score += 0.28

    if cross_away_clean >= 2.18:
        score += 0.72
    elif cross_away_clean >= 2.06:
        score += 0.28

    if cross_home_clean >= 2.15 and cross_away_clean >= 2.15:
        score += 0.82
    elif (cross_home_clean >= 2.22 and cross_away_clean >= 2.00) or (cross_away_clean >= 2.22 and cross_home_clean >= 2.00):
        score += 0.36

    if home_scored_clean >= 1.15:
        score += 0.30
    if away_scored_clean >= 1.15:
        score += 0.30
    if home_scored_clean >= 1.15 and away_scored_clean >= 1.15:
        score += 0.38

    if combined_ht_clean >= 0.98:
        score += 0.10
    if combined_ht_scored_clean >= 0.76:
        score += 0.10
    if safe_float(s_h.get("ht_1plus_rate", 0.0), 0.0) >= 0.62 and safe_float(s_a.get("ht_1plus_rate", 0.0), 0.0) >= 0.62:
        score += 0.22

    if 1.56 <= safe_float(mk.get("o25"), 0.0) <= 2.12 and 1.20 <= safe_float(mk.get("o05ht"), 0.0) <= 1.39:
        score += 0.60
    elif 1.52 <= safe_float(mk.get("o25"), 0.0) <= 2.22 and 1.18 <= safe_float(mk.get("o05ht"), 0.0) <= 1.42:
        score += 0.22

    if coherence >= 2.35:
        score += 0.68
    elif coherence >= 1.90:
        score += 0.30

    if market_profile in ("full_open", "balanced_open", "favorite_pressure_open"):
        score += 0.18

    if match_profile in ("open_match", "favorite_pressure"):
        score += 0.16

    if drop_type == "structural":
        score += 0.25
    elif drop_type == "sterile":
        score -= 0.18

    if dislocation >= 0.55:
        score += 0.12  # conferma, non traino

    if one_sided_risk <= 0.55:
        score += 0.42
    elif one_sided_risk <= 0.90:
        score += 0.18

    if value_left == "low":
        score -= 0.18

    score -= one_sided_risk * 0.92

    if one_sided_risk >= 1.35:
        score -= 0.42
    elif one_sided_risk >= 1.20:
        score -= 0.24
    elif one_sided_risk >= 1.10:
        score -= 0.12

    if combined_ft_clean < 1.62:
        score -= 0.75
    elif combined_ft_clean < 1.70:
        score -= 0.30

    if home_scored_clean < 1.00:
        score -= 0.50
    if away_scored_clean < 1.00:
        score -= 0.50

    if safe_float(s_h.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.52
    if safe_float(s_a.get("ft_low_rate", 0.0), 0.0) >= 0.38:
        score -= 0.52

    if safe_float(s_h.get("ht_zero_rate", 0.0), 0.0) >= 0.38:
        score -= 0.35
    if safe_float(s_a.get("ht_zero_rate", 0.0), 0.0) >= 0.38:
        score -= 0.35

    if safe_float(mk.get("o25"), 0.0) > 2.28 and safe_float(mk.get("o25"), 0.0) != 0:
        score -= 0.22

    return round3(max(score, 0.0))


def build_scoring_snapshot(mk, s_h, s_a, structure_pack, market_pack, quote_pack):
    """
    Punto unico dove costruiamo tutti gli score grezzi.
    """
    ptgg_score = score_ptgg_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack)
    pto15_score = score_pto15_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack)

    over_score = score_over_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack)

    # PT composito V26:
    # il PT esiste ancora, ma se OVER è debole perde forza
    base_pt_score = round3(max(ptgg_score, pto15_score) + (min(ptgg_score, pto15_score) * 0.16))

    if over_score >= 4.60:
        pt_context_factor = 1.08
    elif over_score >= 4.10:
        pt_context_factor = 1.00
    elif over_score >= 3.70:
        pt_context_factor = 0.88
    else:
        pt_context_factor = 0.72

    pt_score = round3(base_pt_score * pt_context_factor)

    boost_score = score_boost_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack, pt_score, over_score)
    gold_score = score_gold_signal(mk, s_h, s_a, structure_pack, market_pack, quote_pack, pt_score, over_score)

    return {
        "ptgg": ptgg_score,
        "pto15": pto15_score,
        "pt": pt_score,
        "over": over_score,
        "boost": boost_score,
        "gold": gold_score,
        "max": round3(max(ptgg_score, pto15_score, pt_score, over_score, boost_score, gold_score))
    }
    
#====================================
# BLOCCO 5
# SIGNAL PACKAGE + TAG LOGIC + KEEP FILTER
# - build_signal_package
# - gerarchia tag
# - gates GOLD / BOOST / PROBE
# - should_keep_match
#====================================

def has_warning(market_pack, flag_name):
    warns = market_pack.get("warning_flags", []) or []
    return flag_name in warns


def has_positive(market_pack, flag_name):
    pos = market_pack.get("positive_flags", []) or []
    return flag_name in pos

def should_keep_match(signal_pack):
    """
    Filtro finale V27.
    Tiene solo:
    - GOLD
    - OVER
    - MARKET
    - PROBE
    """
    tags = signal_pack.get("tags", []) or []
    if not tags:
        return False

    market_pack = signal_pack.get("market_pack", {}) or {}
    structure_pack = signal_pack.get("structure_pack", {}) or {}
    scores = signal_pack.get("scores", {}) or {}

    edge_o25 = safe_float(signal_pack.get("edge_o25", scores.get("edge_o25", 0.0)), 0.0)
    combined_ft_clean = safe_float(structure_pack.get("combined_ft_clean", 0.0), 0.0)
    structure_score = safe_float(structure_pack.get("structure_score", 0.0), 0.0)
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)

    coherence_score = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    value_left = market_pack.get("value_left", "unknown")
    warning_flags = market_pack.get("warning_flags", []) or []

    if any(w in {"favorite_ultra_but_ft_structure_weak", "market_value_trap"} for w in warning_flags):
        return False

    if value_left == "low":
        return False

    label = tags[0]

    if label == "GOLD":
        return bool(
            combined_ft_clean >= 1.40
            and structure_score >= 0.85
            and one_sided_risk <= 1.75
            and coherence_score >= 0.95
        )

    if label == "OVER":
        return bool(
            combined_ft_clean >= 1.52
            and structure_score >= 0.95
            and one_sided_risk <= 1.60
            and edge_o25 >= 0.00
        )

    if label == "MARKET":
        return bool(
            combined_ft_clean >= 1.45
            and structure_score >= 0.85
            and one_sided_risk <= 1.75
            and coherence_score >= 1.00
        )

    if label == "PROBE":
        return bool(
            combined_ft_clean >= 1.40
            and structure_score >= 0.85
            and one_sided_risk <= 1.75
        )

    return False

def should_keep_match(signal_pack):
    """
    Filtro finale V26 SNELLO.
    Se i fatti non tengono, il mercato non inventa nulla.
    """
    tags = signal_pack.get("tags", []) or []
    if not tags:
        return False

    scores = signal_pack.get("scores", {}) or {}
    market_pack = signal_pack.get("market_pack", {}) or {}
    structure_pack = signal_pack.get("structure_pack", {}) or {}

    edge_o25 = safe_float(signal_pack.get("edge_o25", scores.get("edge_o25", 0.0)), 0.0)
    edge_o05ht = safe_float(signal_pack.get("edge_o05ht", scores.get("edge_o05ht", 0.0)), 0.0)
    edge_o15ht = safe_float(signal_pack.get("edge_o15ht", scores.get("edge_o15ht", 0.0)), 0.0)

    combined_ht_clean = safe_float(structure_pack.get("combined_ht_clean", 0.0), 0.0)
    combined_ft_clean = safe_float(structure_pack.get("combined_ft_clean", 0.0), 0.0)
    structure_score = safe_float(structure_pack.get("structure_score", 0.0), 0.0)
    one_sided_risk = safe_float(structure_pack.get("one_sided_risk", 0.0), 0.0)
    fav_quote = safe_float(structure_pack.get("fav_quote", 0.0), 0.0)

    coherence_score = safe_float(market_pack.get("coherence_score", 0.0), 0.0)
    value_left = market_pack.get("value_left", "unknown")
    warning_flags = market_pack.get("warning_flags", []) or []
    drop_confirmed = bool(market_pack.get("drop_confirmed", False))

    if any(w in {"favorite_ultra_but_ft_structure_weak", "market_value_trap"} for w in warning_flags):
        return False

    if value_left == "low" and not drop_confirmed:
        return False

    label = tags[0]

    if label == "GOLD SNIPER":
        return bool(
            1.60 <= fav_quote <= 1.90
            and combined_ht_clean >= 1.20
            and combined_ft_clean >= 1.52
            and structure_score >= 1.00
            and one_sided_risk <= 1.55
            and edge_o25 >= 0.00
            and (edge_o05ht >= -0.02 or edge_o15ht >= 0.00)
            and coherence_score >= 1.20
        )

    if label == "OVER TARGET":
        return bool(
            combined_ft_clean >= 1.52
            and structure_score >= 0.95
            and one_sided_risk <= 1.60
            and edge_o25 >= 0.00
            and coherence_score >= 1.00
        )

    if label == "MARKET ATTACK":
        return bool(
            1.60 <= fav_quote <= 1.90
            and combined_ft_clean >= 1.45
            and combined_ht_clean >= 1.05
            and structure_score >= 0.95
            and one_sided_risk <= 1.60
            and edge_o25 >= -0.01
            and coherence_score >= 1.05
        )

    return False

def build_signal_debug_summary(signal_pack):
    if not signal_pack:
        return {}

    return {
        "tags": signal_pack.get("tags", []),
        "strong_tag_count": signal_pack.get("strong_tag_count", 0),
        "fav_quote": signal_pack.get("fav_quote", 0.0),
        "is_gold_zone": signal_pack.get("is_gold_zone", False),
        "drop_diff": signal_pack.get("drop_diff", 0.0),
        "internal_labels": signal_pack.get("internal_labels", []),
        "over_level": signal_pack.get("over_level", 0),
        "drop_visual_level": signal_pack.get("drop_visual_level", "none"),
        "signal_stability": signal_pack.get("signal_stability", ""),
        "signal_summary": signal_pack.get("signal_summary", ""),
        "tempo_tag": signal_pack.get("tempo_tag", ""),
        "early_index": signal_pack.get("early_index", 0.0),
        "early_home": safe_float((signal_pack.get("tempo_pack", {}) or {}).get("early_home", 0.0), 0.0),
        "early_away": safe_float((signal_pack.get("tempo_pack", {}) or {}).get("early_away", 0.0), 0.0),
        "early_balance": safe_float((signal_pack.get("tempo_pack", {}) or {}).get("early_balance", 0.0), 0.0),
        "pressure_factor": safe_float((signal_pack.get("tempo_pack", {}) or {}).get("pressure_factor", 0.0), 0.0),
        "activation_factor": safe_float((signal_pack.get("tempo_pack", {}) or {}).get("activation_factor", 0.0), 0.0),
    }
    
#====================================
# BLOCCO 6
# DETAILS / MERGE / SCAN CORE / NIGHT BUILD
# - save details
# - day payload helpers
# - merge rows
# - run_full_scan V25
# - nightly multiday build
#====================================

def save_match_details_file():
    payload = {
        "updated_at": now_rome().strftime("%Y-%m-%d %H:%M:%S"),
        "details": st.session_state.match_details
    }
    with open(DETAILS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, ensure_ascii=False)
    return payload


def build_day_results(day_num):
    target_date = get_target_dates()[day_num - 1]
    results = [r for r in st.session_state.scan_results if r.get("Data") == target_date]
    results.sort(key=lambda x: x.get("Ora", "99:99"))
    return results


def build_day_details_payload(day_num):
    target_date = get_target_dates()[day_num - 1]
    details = {
        k: v for k, v in st.session_state.match_details.items()
        if v.get("date") == target_date
    }
    return {
        "updated_at": now_rome().strftime("%Y-%m-%d %H:%M:%S"),
        "day": day_num,
        "date": target_date,
        "details": details
    }


def sync_day_outputs_to_github(day_num, update_main=False):
    day_results = build_day_results(day_num)
    details_payload = build_day_details_payload(day_num)

    status_day = upload_day_to_github(day_num, day_results)
    status_details = upload_details_to_github(day_num, details_payload)

    if update_main:
        status_main = upload_to_github_main(day_results)
    else:
        status_main = None

    return status_main, status_day, status_details


#====================================
# MERGE HELPERS
#====================================
def build_curr_pack_from_row(row: dict) -> dict:
    return {
        "Q1_CURR": safe_float(row.get("Q1", row.get("Q1_CURR", 0))),
        "QX_CURR": safe_float(row.get("QX", row.get("QX_CURR", 0))),
        "Q2_CURR": safe_float(row.get("Q2", row.get("Q2_CURR", 0))),
        "O25_CURR": safe_float(row.get("O2.5", row.get("O25_CURR", 0))),
        "O05HT_CURR": safe_float(row.get("O0.5H", row.get("O05HT_CURR", 0))),
        "O15HT_CURR": safe_float(row.get("O1.5H", row.get("O15HT_CURR", 0))),
    }


def build_open_pack_from_row(row: dict) -> dict:
    return {
        "Q1_OPEN": safe_float(row.get("Q1_OPEN", row.get("Q1", 0))),
        "QX_OPEN": safe_float(row.get("QX_OPEN", row.get("QX", 0))),
        "Q2_OPEN": safe_float(row.get("Q2_OPEN", row.get("Q2", 0))),
        "O25_OPEN": safe_float(row.get("O25_OPEN", row.get("O2.5", 0))),
        "O05HT_OPEN": safe_float(row.get("O05HT_OPEN", row.get("O0.5H", 0))),
        "O15HT_OPEN": safe_float(row.get("O15HT_OPEN", row.get("O1.5H", 0))),
    }


def build_merge_base_row(row: dict) -> dict:
    curr_pack = build_curr_pack_from_row(row)
    open_pack = build_open_pack_from_row(row)

    merged = dict(row)
    merged.update(open_pack)
    merged.update(curr_pack)

    if "status" not in merged:
        merged["status"] = "active"

    if "missing_count" not in merged:
        merged["missing_count"] = 0

    if "Fixture_ID" in merged and "fixture_id" not in merged:
        merged["fixture_id"] = merged["Fixture_ID"]

    return merged


def merge_existing_and_new_row(old_row: dict, new_row: dict) -> dict:
    old_row = dict(old_row or {})
    new_row = build_merge_base_row(new_row or {})

    merged = dict(old_row)
    merged.update(new_row)

    # OPEN mai sovrascritte se presenti
    for key in ["Q1_OPEN", "QX_OPEN", "Q2_OPEN", "O25_OPEN", "O05HT_OPEN", "O15HT_OPEN"]:
        old_val = safe_float(old_row.get(key), 0.0)
        new_val = safe_float(new_row.get(key), 0.0)
        merged[key] = old_val if old_val > 0 else new_val

    # CURRENT aggiornate sempre
    for key in ["Q1_CURR", "QX_CURR", "Q2_CURR", "O25_CURR", "O05HT_CURR", "O15HT_CURR"]:
        merged[key] = safe_float(new_row.get(key), 0.0)

    merged["status"] = "active"
    merged["missing_count"] = 0

    if "Fixture_ID" in new_row:
        merged["Fixture_ID"] = new_row["Fixture_ID"]

    if "fixture_id" not in merged and "Fixture_ID" in merged:
        merged["fixture_id"] = merged["Fixture_ID"]

    return merged


def mark_row_as_stale(row: dict) -> dict:
    stale = dict(row or {})
    stale["status"] = "stale"
    stale["missing_count"] = int(stale.get("missing_count", 0)) + 1

    if "fixture_id" not in stale and "Fixture_ID" in stale:
        stale["fixture_id"] = stale["Fixture_ID"]

    return stale


def merge_day_rows(old_rows: list, new_rows: list) -> list:
    old_rows = old_rows or []
    new_rows = new_rows or []

    old_map = {}
    for row in old_rows:
        fid = row.get("Fixture_ID", row.get("fixture_id"))
        if fid is not None:
            old_map[str(fid)] = dict(row)

    new_map = {}
    for row in new_rows:
        fid = row.get("Fixture_ID", row.get("fixture_id"))
        if fid is not None:
            new_map[str(fid)] = build_merge_base_row(row)

    merged_map = {}

    for fid, new_row in new_map.items():
        if fid in old_map:
            merged_map[fid] = merge_existing_and_new_row(old_map[fid], new_row)
        else:
            merged_map[fid] = build_merge_base_row(new_row)

    for fid, old_row in old_map.items():
        if fid not in merged_map:
            merged_map[fid] = mark_row_as_stale(old_row)

    merged_rows = list(merged_map.values())
    merged_rows.sort(
        key=lambda r: (
            str(r.get("Data", "")),
            str(r.get("Ora", "99:99")),
            str(r.get("Match", "")),
        )
    )
    return merged_rows


#====================================
# SCAN CORE V25
#====================================
def run_full_scan(horizon=None, snap=False, update_main_site=False, show_success=True):
    use_horizon = horizon if horizon is not None else HORIZON
    target_dates = get_target_dates()

    with st.spinner(f"🚀 Analisi mercati {target_dates[use_horizon - 1]}..."):
        with requests.Session() as s:
            target_date = target_dates[use_horizon - 1]

            # --------------------------------------
            # 1) FIXTURE API
            # --------------------------------------
            res = api_get(s, "fixtures", {"date": target_date, "timezone": "Europe/Rome"})
            if not res or not isinstance(res, dict):
                print(f"❌ API non valida per day {use_horizon} ({target_date}) -> skip totale", flush=True)
                if show_success:
                    st.error(f"❌ API non valida per {target_date}. Nessun file aggiornato.")
                return

            api_response = res.get("response", [])
            print(f"📊 FIXTURE API TROVATE per {target_date}: {len(api_response) if isinstance(api_response, list) else 0}", flush=True)

            if not api_response or not isinstance(api_response, list):
                print(f"❌ API vuota per day {use_horizon} ({target_date}) -> skip totale", flush=True)
                if show_success:
                    st.error(f"❌ API vuota per {target_date}. Nessun file aggiornato.")
                return

            day_fx = [
                f for f in api_response
                if f.get("fixture", {}).get("status", {}).get("short") == "NS"
                and not is_blacklisted_league(f.get("league", {}).get("name", ""))
            ]

            print(f"📊 FIXTURE NS FILTRATE per {target_date}: {len(day_fx)}", flush=True)

            if not day_fx:
                print(f"❌ Nessun fixture NS valido per day {use_horizon} ({target_date}) -> skip totale", flush=True)
                if show_success:
                    st.error(f"❌ Nessun match pre-match valido trovato per {target_date}. Nessun file aggiornato.")
                return

            st.session_state.available_countries = sorted(
                list(set(st.session_state.available_countries) | {
                    fx.get("league", {}).get("country", "N/D") for fx in day_fx
                })
            )

            # --------------------------------------
            # 2) SNAPSHOT rolling solo day1
            # --------------------------------------
            if snap and use_horizon == 1:
                try:
                    snap_bar = st.progress(0, text="📌 SNAPSHOT ROLLING DAY1+DAY2+DAY3+DAY4+DAY5...")
                    build_rolling_multiday_snapshot(s)
                    snap_bar.progress(1.0)
                    time.sleep(0.3)
                    snap_bar.empty()
                except Exception as e:
                    print(f"❌ Errore snapshot rolling: {e}", flush=True)
                    if show_success:
                        st.error(f"❌ Errore snapshot: {e}")
                    return

            final_list = []
            details_map = dict(st.session_state.match_details)
            pb = st.progress(0, text="🚀 ANALISI V25: STRUTTURA + MERCATO + COERENZA...")

            # --------------------------------------
            # 3) ANALISI MATCH
            # --------------------------------------
            for i, f in enumerate(day_fx):
                pb.progress((i + 1) / len(day_fx) if day_fx else 1.0)

                try:
                    cnt = f.get("league", {}).get("country", "N/D")
                    if cnt in st.session_state.config["excluded"]:
                        continue

                    fid = str(f.get("fixture", {}).get("id"))
                    if not fid or fid == "None":
                        continue

                    mk = extract_elite_markets(s, fid)
                    if not mk or mk == "SKIP" or safe_float(mk.get("q1"), 0.0) == 0:
                        continue

                    home_team = f.get("teams", {}).get("home", {})
                    away_team = f.get("teams", {}).get("away", {})

                    if not home_team.get("id") or not away_team.get("id"):
                        continue

                    fixture_local_dt = fixture_dt_rome(f.get("fixture", {}))
                    ora_local = fixture_local_dt.strftime("%H:%M") if fixture_local_dt else str(
                        f.get("fixture", {}).get("date", "")
                    )[11:16]

                    # ----------------------------------
                    # PROFILI CONTESTUALI V25
                    # ----------------------------------
                    s_h = get_team_performance(s, home_team["id"], expected_side="home")
                    s_a = get_team_performance(s, away_team["id"], expected_side="away")
                    if not s_h or not s_a:
                        continue

                    signal_pack = build_signal_package(fid, mk, s_h, s_a)
                    
                    combined_ht_clean = round3((safe_float(s_h.get("avg_ht_clean", 0.0), 0.0) + safe_float(s_a.get("avg_ht_clean", 0.0), 0.0)) / 2)
                    combined_ft_clean = round3((safe_float(s_h.get("avg_total_clean", 0.0), 0.0) + safe_float(s_a.get("avg_total_clean", 0.0), 0.0)) / 2)

                    # taglio rumore minimo, ma meno grezzo del vecchio
                    if (
                        combined_ht_clean < 0.60
                        and combined_ft_clean < 1.02
                        and safe_float(s_h.get("ht_1plus_rate", 0.0), 0.0) < 0.30
                        and safe_float(s_a.get("ht_1plus_rate", 0.0), 0.0) < 0.30
                        and safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0) < 0.30
                        and safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0) < 0.30
                    ):
                        continue

                    if not should_keep_match(signal_pack):
                        continue

                    tags = signal_pack.get("tags", [])
                    scores = signal_pack.get("scores", {})
                    quote_pack = signal_pack.get("quote_pack", {})
                    market_pack = signal_pack.get("market_pack", {})
                    structure_pack = signal_pack.get("structure_pack", {})
                    tempo_pack = signal_pack.get("tempo_pack", {})
                    lambda_pack = signal_pack.get("lambda_pack", {})

                    drop_diff = safe_float(signal_pack.get("drop_diff", 0.0), 0.0)
                    drop_visual_level = signal_pack.get("drop_visual_level", "none")
                    signal_stability = signal_pack.get("signal_stability", "")
                    signal_summary = signal_pack.get("signal_summary", "")
                    over_level = int(signal_pack.get("over_level", 0) or 0)
                    strong_tag_count = int(signal_pack.get("strong_tag_count", 0) or 0)

                    lam_home_ft = safe_float(lambda_pack.get("lam_home_ft", 0.0), 0.0)
                    lam_away_ft = safe_float(lambda_pack.get("lam_away_ft", 0.0), 0.0)
                    lam_home_ht = safe_float(lambda_pack.get("lam_home_ht", 0.0), 0.0)
                    lam_away_ht = safe_float(lambda_pack.get("lam_away_ht", 0.0), 0.0)

                    tempo_tag = signal_pack.get("tempo_tag", "")
                    early_index = safe_float(signal_pack.get("early_index", 0.0), 0.0)

                    p_model_over25 = safe_float(signal_pack.get("p_model_o25", 0.0), 0.0)
                    p_market_over25 = safe_float(signal_pack.get("p_market_o25", 0.0), 0.0)
                    edge_over25 = safe_float(signal_pack.get("edge_o25", 0.0), 0.0)
                    edge_level_over25 = signal_pack.get("edge_level_o25", "NONE")

                    p_model_o05ht = safe_float(signal_pack.get("p_model_o05ht", 0.0), 0.0)
                    p_market_o05ht = safe_float(signal_pack.get("p_market_o05ht", 0.0), 0.0)
                    edge_o05ht = safe_float(signal_pack.get("edge_o05ht", 0.0), 0.0)
                    edge_level_o05ht = signal_pack.get("edge_level_o05ht", "NONE")

                    p_model_o15ht = safe_float(signal_pack.get("p_model_o15ht", 0.0), 0.0)
                    p_market_o15ht = safe_float(signal_pack.get("p_market_o15ht", 0.0), 0.0)
                    edge_o15ht = safe_float(signal_pack.get("edge_o15ht", 0.0), 0.0)
                    edge_level_o15ht = signal_pack.get("edge_level_o15ht", "NONE")

                    edge_logit_over25 = safe_edge_logit(p_model_over25, p_market_over25)
                    edge_logit_o05ht = safe_edge_logit(p_model_o05ht, p_market_o05ht)
                    edge_logit_o15ht = safe_edge_logit(p_model_o15ht, p_market_o15ht)

                    row = {
                        "Ora": ora_local,
                        "Lega": f"{f.get('league', {}).get('name', 'N/D')} ({cnt})",
                        "Match": f"{home_team.get('name', 'N/D')} - {away_team.get('name', 'N/D')}",
                        "FAV": "✅" if signal_pack.get("is_gold_zone") else "❌",
                        "FAV_VAL": f"{signal_pack.get('fav_quote', 0.0):.2f}",
                        "1X2": f"{safe_float(mk.get('q1'), 0):.2f}|{safe_float(mk.get('qx'), 0):.2f}|{safe_float(mk.get('q2'), 0):.2f}",
                        "O2.5": f"{safe_float(mk.get('o25'), 0):.2f}",
                        "O0.5H": f"{safe_float(mk.get('o05ht'), 0):.2f}",
                        "O1.5H": f"{safe_float(mk.get('o15ht'), 0):.2f}",
                        "AVG FT": f"{safe_float(s_h.get('avg_total', 0), 0.0):.2f}|{safe_float(s_a.get('avg_total', 0), 0.0):.2f}",
                        "AVG HT": f"{safe_float(s_h.get('avg_ht', 0), 0.0):.2f}|{safe_float(s_a.get('avg_ht', 0), 0.0):.2f}",
                        "Info": " ".join(tags),
                        "OVER_LEVEL": signal_pack.get("over_level", 0),
                        "DROP_DIFF": signal_pack.get("drop_diff", 0.0),
                        "SIGNAL_STABILITY": signal_pack.get("signal_stability", ""),
                        "SIGNAL_SUMMARY": signal_pack.get("signal_summary", ""),
                        "DROP_VISUAL_LEVEL": signal_pack.get("drop_visual_level", "none"),
                        "HAS_INVERSION": quote_pack["INVERSION"],
                        "Data": target_date,
                        "Fixture_ID": f.get("fixture", {}).get("id"),

                        "Q1_OPEN": quote_pack["Q1_OPEN"],
                        "QX_OPEN": quote_pack["QX_OPEN"],
                        "Q2_OPEN": quote_pack["Q2_OPEN"],
                        "O25_OPEN": quote_pack["O25_OPEN"],
                        "O05HT_OPEN": quote_pack["O05HT_OPEN"],
                        "O15HT_OPEN": quote_pack["O15HT_OPEN"],

                        "Q1_CURR": quote_pack["Q1_CURR"],
                        "QX_CURR": quote_pack["QX_CURR"],
                        "Q2_CURR": quote_pack["Q2_CURR"],
                        "O25_CURR": quote_pack["O25_CURR"],
                        "O05HT_CURR": quote_pack["O05HT_CURR"],
                        "O15HT_CURR": quote_pack["O15HT_CURR"],

                        "Q1_MOVE": quote_pack["Q1_MOVE"],
                        "QX_MOVE": quote_pack["QX_MOVE"],
                        "Q2_MOVE": quote_pack["Q2_MOVE"],
                        "O25_MOVE": quote_pack["O25_MOVE"],
                        "O05HT_MOVE": quote_pack["O05HT_MOVE"],
                        "O15HT_MOVE": quote_pack["O15HT_MOVE"],

                        "Q1_MOVE_DATA": quote_pack["Q1_MOVE_DATA"],
                        "QX_MOVE_DATA": quote_pack["QX_MOVE_DATA"],
                        "Q2_MOVE_DATA": quote_pack["Q2_MOVE_DATA"],
                        "O25_MOVE_DATA": quote_pack["O25_MOVE_DATA"],
                        "O05HT_MOVE_DATA": quote_pack["O05HT_MOVE_DATA"],
                        "O15HT_MOVE_DATA": quote_pack["O15HT_MOVE_DATA"],

                        "INVERSION": quote_pack["INVERSION"],
                        "INV_FROM": quote_pack["INV_FROM"],
                        "INV_TO": quote_pack["INV_TO"],
                        "FAV_OPEN": quote_pack["FAV_OPEN"],
                        "FAV_CURRENT": quote_pack["FAV_CURRENT"],

                        "MARKET_PROFILE": market_pack.get("market_profile", "neutral"),
                        "COHERENCE_SCORE": market_pack.get("coherence_score", 0.0),
                        "DISLOCATION_SCORE": market_pack.get("dislocation_score", 0.0),
                        "DROP_TYPE": market_pack.get("drop_type", "none"),
                        "VALUE_LEFT": market_pack.get("value_left", "unknown"),
                        "MATCH_PROFILE": structure_pack.get("match_profile", "neutral"),
                        "STRUCTURE_SCORE": structure_pack.get("structure_score", 0.0),
                        "LAM_HOME_FT": lam_home_ft,
                        "LAM_AWAY_FT": lam_away_ft,
                        "LAM_HOME_HT": lam_home_ht,
                        "LAM_AWAY_HT": lam_away_ht,

                        "TEMPO_TAG": signal_pack.get("tempo_tag", ""),
                        "EARLY_INDEX": signal_pack.get("early_index", 0.0),
                        "DROP_VISUAL_LEVEL": signal_pack.get("drop_visual_level", "none"),

                        "P_MODEL_O25": signal_pack.get("p_model_o25", 0.0),
                        "P_MARKET_O25": signal_pack.get("p_market_o25", 0.0),
                        "EDGE_O25": signal_pack.get("edge_o25", 0.0),
                        "EDGE_LOGIT_O25": safe_edge_logit(signal_pack.get("p_model_o25", 0.0), signal_pack.get("p_market_o25", 0.0)),
                        "EDGE_LEVEL_O25": signal_pack.get("edge_level_o25", "NONE"),

                        "P_MODEL_O05HT": signal_pack.get("p_model_o05ht", 0.0),
                        "P_MARKET_O05HT": signal_pack.get("p_market_o05ht", 0.0),
                        "EDGE_O05HT": signal_pack.get("edge_o05ht", 0.0),
                        "EDGE_LOGIT_O05HT": safe_edge_logit(signal_pack.get("p_model_o05ht", 0.0), signal_pack.get("p_market_o05ht", 0.0)),
                        "EDGE_LEVEL_O05HT": signal_pack.get("edge_level_o05ht", "NONE"),

                        "P_MODEL_O15HT": signal_pack.get("p_model_o15ht", 0.0),
                        "P_MARKET_O15HT": signal_pack.get("p_market_o15ht", 0.0),
                        "EDGE_O15HT": signal_pack.get("edge_o15ht", 0.0),
                        "EDGE_LOGIT_O15HT": safe_edge_logit(signal_pack.get("p_model_o15ht", 0.0), signal_pack.get("p_market_o15ht", 0.0)),
                        "EDGE_LEVEL_O15HT": signal_pack.get("edge_level_o15ht", "NONE"),
                    }

                    row["MOVE_SUMMARY"] = build_movement_summary(row)
                    final_list.append(row)

                    # ----------------------------------
                    # DETAILS RICCHI V25
                    # ----------------------------------
                    details_map[fid] = {
                        "fixture_id": f.get("fixture", {}).get("id"),
                        "date": target_date,
                        "time": ora_local,
                        "league": f.get("league", {}).get("name", "N/D"),
                        "country": cnt,
                        "match": f"{home_team.get('name', 'N/D')} - {away_team.get('name', 'N/D')}",
                        "home_team": home_team.get("name", "N/D"),
                        "away_team": away_team.get("name", "N/D"),

                        "markets": {
                            "q1": safe_float(mk.get("q1"), 0.0),
                            "qx": safe_float(mk.get("qx"), 0.0),
                            "q2": safe_float(mk.get("q2"), 0.0),
                            "o25": safe_float(mk.get("o25"), 0.0),
                            "o05ht": safe_float(mk.get("o05ht"), 0.0),
                            "o15ht": safe_float(mk.get("o15ht"), 0.0),
                            "u25": safe_float(mk.get("u25"), 0.0),
                            "u05ht": safe_float(mk.get("u05ht"), 0.0),
                            "u15ht": safe_float(mk.get("u15ht"), 0.0),
                        },

                        "averages": {
                            "home_avg_ft": round3(safe_float(s_h.get("avg_total", 0.0), 0.0)),
                            "away_avg_ft": round3(safe_float(s_a.get("avg_total", 0.0), 0.0)),
                            "home_avg_ht": round3(safe_float(s_h.get("avg_ht", 0.0), 0.0)),
                            "away_avg_ht": round3(safe_float(s_a.get("avg_ht", 0.0), 0.0)),

                            "home_avg_ft_clean": round3(safe_float(s_h.get("avg_total_clean", 0.0), 0.0)),
                            "away_avg_ft_clean": round3(safe_float(s_a.get("avg_total_clean", 0.0), 0.0)),
                            "home_avg_ht_clean": round3(safe_float(s_h.get("avg_ht_clean", 0.0), 0.0)),
                            "away_avg_ht_clean": round3(safe_float(s_a.get("avg_ht_clean", 0.0), 0.0)),

                            "home_avg_ht_scored_clean": round3(safe_float(s_h.get("avg_ht_scored_clean", 0.0), 0.0)),
                            "away_avg_ht_scored_clean": round3(safe_float(s_a.get("avg_ht_scored_clean", 0.0), 0.0)),
                            "home_avg_ht_conceded_clean": round3(safe_float(s_h.get("avg_ht_conceded_clean", 0.0), 0.0)),
                            "away_avg_ht_conceded_clean": round3(safe_float(s_a.get("avg_ht_conceded_clean", 0.0), 0.0)),

                            "home_avg_ft_scored_clean": round3(safe_float(s_h.get("avg_ft_scored_clean", 0.0), 0.0)),
                            "away_avg_ft_scored_clean": round3(safe_float(s_a.get("avg_ft_scored_clean", 0.0), 0.0)),
                            "home_avg_ft_conceded_clean": round3(safe_float(s_h.get("avg_ft_conceded_clean", 0.0), 0.0)),
                            "away_avg_ft_conceded_clean": round3(safe_float(s_a.get("avg_ft_conceded_clean", 0.0), 0.0)),

                            "home_ft_2plus_rate": round3(safe_float(s_h.get("ft_2plus_rate", 0.0), 0.0)),
                            "away_ft_2plus_rate": round3(safe_float(s_a.get("ft_2plus_rate", 0.0), 0.0)),
                            "home_ft_3plus_rate": round3(safe_float(s_h.get("ft_3plus_rate", 0.0), 0.0)),
                            "away_ft_3plus_rate": round3(safe_float(s_a.get("ft_3plus_rate", 0.0), 0.0)),
                            "home_ft_low_rate": round3(safe_float(s_h.get("ft_low_rate", 0.0), 0.0)),
                            "away_ft_low_rate": round3(safe_float(s_a.get("ft_low_rate", 0.0), 0.0)),

                            "home_ht_1plus_rate": round3(safe_float(s_h.get("ht_1plus_rate", 0.0), 0.0)),
                            "away_ht_1plus_rate": round3(safe_float(s_a.get("ht_1plus_rate", 0.0), 0.0)),
                            "home_ht_zero_rate": round3(safe_float(s_h.get("ht_zero_rate", 0.0), 0.0)),
                            "away_ht_zero_rate": round3(safe_float(s_a.get("ht_zero_rate", 0.0), 0.0)),

                            "home_ht_scored_1plus_rate": round3(safe_float(s_h.get("ht_scored_1plus_rate", 0.0), 0.0)),
                            "away_ht_scored_1plus_rate": round3(safe_float(s_a.get("ht_scored_1plus_rate", 0.0), 0.0)),
                            "home_ht_scored_2plus_rate": round3(safe_float(s_h.get("ht_scored_2plus_rate", 0.0), 0.0)),
                            "away_ht_scored_2plus_rate": round3(safe_float(s_a.get("ht_scored_2plus_rate", 0.0), 0.0)),
                            "home_ht_conceded_1plus_rate": round3(safe_float(s_h.get("ht_conceded_1plus_rate", 0.0), 0.0)),
                            "away_ht_conceded_1plus_rate": round3(safe_float(s_a.get("ht_conceded_1plus_rate", 0.0), 0.0)),
                        },

                        "flags": {
                            "fav_quote": round3(safe_float(signal_pack.get("fav_quote", 0.0), 0.0)),
                            "is_gold_zone": bool(signal_pack.get("is_gold_zone", False)),
                            "drop_diff": round3(safe_float(signal_pack.get("drop_diff", 0.0), 0.0)),
                            "home_last_2h_zero": bool(s_h.get("last_2h_zero", False)),
                            "away_last_2h_zero": bool(s_a.get("last_2h_zero", False)),
                        },

                        "fair_value": {
                            "margin_open": round3(safe_float(market_pack.get("margin_open", 0.0), 0.0)),
                            "margin_curr": round3(safe_float(market_pack.get("margin_curr", 0.0), 0.0)),
                            "margin_delta": round3(safe_float(market_pack.get("margin_delta", 0.0), 0.0)),
                            "fav_fair_prob_open": round3(safe_float(market_pack.get("fav_fair_prob_open", 0.0), 0.0)),
                            "fav_fair_prob_curr": round3(safe_float(market_pack.get("fav_fair_prob_curr", 0.0), 0.0)),
                            "fav_fair_prob_delta": round3(safe_float(market_pack.get("fav_fair_prob_delta", 0.0), 0.0)),
                            "fav_fair_curr": round3(safe_float(market_pack.get("fav_fair_curr", 0.0), 0.0)),
                        },

                       "model_edge": {
                            "lam_home_ft": safe_float(signal_pack.get("lambda_pack", {}).get("lam_home_ft", 0.0), 0.0),
                            "lam_away_ft": safe_float(signal_pack.get("lambda_pack", {}).get("lam_away_ft", 0.0), 0.0),
                            "lam_home_ht": safe_float(signal_pack.get("lambda_pack", {}).get("lam_home_ht", 0.0), 0.0),
                            "lam_away_ht": safe_float(signal_pack.get("lambda_pack", {}).get("lam_away_ht", 0.0), 0.0),

                            "p_model_o25": safe_float(signal_pack.get("p_model_o25", 0.0), 0.0),
                            "p_market_o25": safe_float(signal_pack.get("p_market_o25", 0.0), 0.0),
                            "edge_o25": safe_float(signal_pack.get("edge_o25", 0.0), 0.0),
                            "edge_logit_o25": safe_edge_logit(signal_pack.get("p_model_o25", 0.0), signal_pack.get("p_market_o25", 0.0)),
                            "edge_level_o25": signal_pack.get("edge_level_o25", "NONE"),

                            "p_model_o05ht": safe_float(signal_pack.get("p_model_o05ht", 0.0), 0.0),
                            "p_market_o05ht": safe_float(signal_pack.get("p_market_o05ht", 0.0), 0.0),
                            "edge_o05ht": safe_float(signal_pack.get("edge_o05ht", 0.0), 0.0),
                            "edge_logit_o05ht": safe_edge_logit(signal_pack.get("p_model_o05ht", 0.0), signal_pack.get("p_market_o05ht", 0.0)),
                            "edge_level_o05ht": signal_pack.get("edge_level_o05ht", "NONE"),
    
                            "p_model_o15ht": safe_float(signal_pack.get("p_model_o15ht", 0.0), 0.0),
                            "p_market_o15ht": safe_float(signal_pack.get("p_market_o15ht", 0.0), 0.0),
                            "edge_o15ht": safe_float(signal_pack.get("edge_o15ht", 0.0), 0.0),
                            "edge_logit_o15ht": safe_edge_logit(signal_pack.get("p_model_o15ht", 0.0), signal_pack.get("p_market_o15ht", 0.0)),
                            "edge_level_o15ht": signal_pack.get("edge_level_o15ht", "NONE"),

                            "ctx_avg": safe_float(signal_pack.get("lambda_pack", {}).get("ctx_avg", 0.0), 0.0),
                            "reg_avg": safe_float(signal_pack.get("lambda_pack", {}).get("reg_avg", 0.0), 0.0),
                             "ft_sd_avg": safe_float(signal_pack.get("lambda_pack", {}).get("ft_sd_avg", 0.0), 0.0),
                            "ht_sd_avg": safe_float(signal_pack.get("lambda_pack", {}).get("ht_sd_avg", 0.0), 0.0),
                        },

                        "scores": scores,
                        "tags": tags,
                        "internal_labels": signal_pack.get("internal_labels", []),

                        "structure": build_structure_debug_summary(structure_pack),
                        "market_reading": build_market_debug_summary(market_pack),
                        "signal_debug": build_signal_debug_summary(signal_pack),

                        "home_profile": build_team_debug_summary(s_h),
                        "away_profile": build_team_debug_summary(s_a),

                        "home_last_8": s_h.get("last_matches", []),
                        "away_last_8": s_a.get("last_matches", []),
                    }

                    time.sleep(0.18)

                except Exception as e:
                    print(f"⚠️ Errore su fixture {f.get('fixture', {}).get('id', 'N/D')}: {e}", flush=True)
                    continue

            pb.empty()

            # --------------------------------------
            # 4) PROTEZIONI SALVATAGGIO
            # --------------------------------------
            existing_day_results = [
                r for r in st.session_state.scan_results
                if r.get("Data") == target_date
            ]

            if not existing_day_results:
                try:
                    with open(DB_FILE, "r", encoding="utf-8") as f:
                        db_payload = json.load(f)
                        db_rows = db_payload.get("results", []) if isinstance(db_payload, dict) else []
                        existing_day_results = [r for r in db_rows if r.get("Data") == target_date]
                        print(f"📦 Fallback DB file per {target_date}: {len(existing_day_results)} match esistenti", flush=True)
                except Exception as e:
                    print(f"⚠️ Fallback DB file fallito per {target_date}: {e}", flush=True)

            if not final_list:
                print(
                    f"⚠️ Nessun match valido trovato per day {use_horizon} ({target_date}) "
                    f"-> mantengo i file esistenti, nessuna sovrascrittura.",
                    flush=True
                )
                if show_success:
                    st.warning(f"⚠️ Nessun match valido per {target_date}. File esistenti mantenuti.")
                return

            if existing_day_results and len(final_list) < 5:
                print(
                    f"⚠️ Troppi pochi match trovati ({len(final_list)}) per day {use_horizon} ({target_date}) "
                    f"con dati già esistenti -> skip salvataggio prudenziale.",
                    flush=True
                )
                if show_success:
                    st.warning(
                        f"⚠️ Trovati solo {len(final_list)} match validi per {target_date}. "
                        f"Per sicurezza non aggiorno i file esistenti."
                    )
                return

            if existing_day_results and len(final_list) < max(5, int(len(existing_day_results) * 0.50)):
                print(
                    f"⚠️ Nuovo scan troppo ridotto: {len(final_list)} vs vecchio {len(existing_day_results)} "
                    f"per day {use_horizon} ({target_date}) -> skip salvataggio prudenziale.",
                    flush=True
                )
                if show_success:
                    st.warning(
                        f"⚠️ Nuovo scan anomalo per {target_date}: {len(final_list)} match contro "
                        f"{len(existing_day_results)} esistenti. Nessun aggiornamento eseguito."
                    )
                return

            # --------------------------------------
            # 5) SALVATAGGIO LOCALE
            # --------------------------------------
            old_day_results = [
                r for r in st.session_state.scan_results
                if r.get("Data") == target_date
            ]

            merged_day_results = merge_day_rows(old_day_results, final_list)

            other_days_results = [
                r for r in st.session_state.scan_results
                if r.get("Data") != target_date
            ]

            new_scan_results = other_days_results + merged_day_results
            new_scan_results.sort(key=lambda x: (x.get("Data", ""), x.get("Ora", "99:99")))

            try:
                with open(DB_FILE, "w", encoding="utf-8") as f:
                    json.dump({"results": new_scan_results}, f, indent=4, ensure_ascii=False)
            except Exception as e:
                print(f"❌ Errore salvataggio DB locale: {e}", flush=True)
                if show_success:
                    st.error(f"❌ Errore salvataggio DB locale: {e}")
                return

            st.session_state.scan_results = new_scan_results
            st.session_state.match_details = details_map

            try:
                save_match_details_file()
            except Exception as e:
                print(f"❌ Errore salvataggio details locale: {e}", flush=True)
                if show_success:
                    st.error(f"❌ Errore salvataggio details locale: {e}")
                return

            # --------------------------------------
            # 6) SYNC GITHUB
            # --------------------------------------
            try:
                status_main, status_day, status_details = sync_day_outputs_to_github(
                    day_num=use_horizon,
                    update_main=update_main_site
                )
            except Exception as e:
                print(f"❌ Errore sync GitHub: {e}", flush=True)
                if show_success:
                    st.error(f"❌ Errore sync GitHub: {e}")
                return

            # --------------------------------------
            # 7) FEEDBACK UI
            # --------------------------------------
            if show_success:
                if update_main_site:
                    if status_main == "SUCCESS":
                        st.success("✅ data aggiornato!")
                    else:
                        st.error(f"❌ Errore data: {status_main}")

                if status_day == "SUCCESS":
                    st.success(f"✅ {REMOTE_DAY_FILES[use_horizon]} aggiornato!")
                else:
                    st.error(f"❌ Errore {REMOTE_DAY_FILES[use_horizon]}: {status_day}")

                if status_details == "SUCCESS":
                    st.success(f"✅ {REMOTE_DETAILS_FILES[use_horizon]} aggiornato!")
                else:
                    st.error(f"❌ Errore {REMOTE_DETAILS_FILES[use_horizon]}: {status_details}")

            if "--auto" not in sys.argv and "--fast" not in sys.argv and "--day2-refresh" not in sys.argv:
                time.sleep(2)
                st.rerun()


#====================================
# NIGHTLY MULTI-DAY BUILD
#====================================
def run_nightly_multiday_build():
    print("🚀 Avvio scan notturno multi-day...", flush=True)
    reset_runtime_api_cache()

    print("🔄 Rotazione file day1-day5...", flush=True)
    try:
        import subprocess
        subprocess.run([sys.executable, str(BASE_DIR / "3appdays_runner.py"), "--rotate-live"], check=True)
        print("✅ Rotazione file completata.", flush=True)

        print("🔄 Reload DB dopo rotazione...", flush=True)
        try:
            load_db()
            print("✅ DB ricaricato dopo rotazione", flush=True)
        except Exception as e:
            print(f"❌ Errore reload DB dopo rotazione: {e}", flush=True)
            raise

    except Exception as e:
        print(f"❌ Errore rotazione file day: {e}", flush=True)
        raise

    try:
        print("📌 DAY 1: SNAPSHOT rolling + refresh quote + update data/data_day1/details_day1", flush=True)
        run_full_scan(horizon=1, snap=True, update_main_site=True, show_success=False)

        print("📆 DAY 2: scan statico + update data_day2/details_day2", flush=True)
        run_full_scan(horizon=2, snap=False, update_main_site=False, show_success=False)

        print("📆 DAY 3: scan statico + update data_day3/details_day3", flush=True)
        run_full_scan(horizon=3, snap=False, update_main_site=False, show_success=False)

        print("📆 DAY 4: scan statico + update data_day4/details_day4", flush=True)
        run_full_scan(horizon=4, snap=False, update_main_site=False, show_success=False)

        print("📌 DAY 5: scan statico + update data_day5/details_day5", flush=True)
        run_full_scan(horizon=5, snap=False, update_main_site=False, show_success=False)

        build_daily_snapshots_from_rolling()
        print("✅ Build multi-day completata.", flush=True)

    except Exception as e:
        print(f"❌ Errore build multi-day: {e}", flush=True)
        raise
        
# ==========================================
# BLOCCO 7
# UI SIDEBAR + UI MAIN + TABLE + MAIN EXEC
# ==========================================

# ------------------------------------------
# UI SIDEBAR
# ------------------------------------------
st.sidebar.header("👑 Arab Sniper V25 Multi-Day WEB")
HORIZON = st.sidebar.selectbox("Orizzonte Temporale:", options=[1, 2, 3, 4, 5], index=0)
target_dates = get_target_dates()

all_discovered = sorted(list(set(st.session_state.get("available_countries", []))))
if st.session_state.scan_results:
    historical_cnt = {
        r["Lega"].split("(")[-1].replace(")", "")
        for r in st.session_state.scan_results
        if "Lega" in r and "(" in str(r["Lega"])
    }
    all_discovered = sorted(list(set(all_discovered) | historical_cnt))

if all_discovered:
    new_ex = st.sidebar.multiselect(
        "Escludi Nazioni:",
        options=all_discovered,
        default=[c for c in st.session_state.config.get("excluded", []) if c in all_discovered]
    )
    if st.sidebar.button("💾 SALVA CONFIG"):
        st.session_state.config["excluded"] = new_ex
        save_config()
        st.rerun()

if last_snap_ts:
    st.sidebar.success(f"✅ SNAPSHOT: {last_snap_ts}")
else:
    st.sidebar.warning("⚠️ SNAPSHOT ASSENTE")

st.sidebar.markdown("---")
st.sidebar.caption(f"DB: {Path(DB_FILE).name}")
st.sidebar.caption(f"SNAP: {Path(SNAP_FILE).name}")
st.sidebar.caption(f"DETAILS: {Path(DETAILS_FILE).name}")
st.sidebar.caption("GitHub: data.json + data_day1/2/3/4/5 + details_day1/2/3/4/5")


# ------------------------------------------
# UI MAIN ACTIONS
# ------------------------------------------
c1, c2 = st.columns(2)
if c1.button("📌 SNAP + SCAN"):
    run_full_scan(horizon=HORIZON, snap=(HORIZON == 1), update_main_site=(HORIZON == 1))

if c2.button("🚀 SCAN VELOCE"):
    run_full_scan(horizon=HORIZON, snap=False, update_main_site=(HORIZON == 1))

# ------------------------------------------
# MODAL DETTAGLI MATCH
# ------------------------------------------
@st.dialog("🔎 Dettagli partita", width="large")
def show_match_modal(fixture_id: str):
    detail = st.session_state.match_details.get(str(fixture_id))

    if not detail:
        st.warning("Dettagli non disponibili per questa partita.")
        return

    avg = detail.get("averages", {})
    flags = detail.get("flags", {})
    fair_value = detail.get("fair_value", {})
    scores = detail.get("scores", {})
    structure = detail.get("structure", {})
    market_reading = detail.get("market_reading", {})
    signal_debug = detail.get("signal_debug", {})
    home_profile = detail.get("home_profile", {})
    away_profile = detail.get("away_profile", {})
    model_edge = detail.get("model_edge", {})

    st.markdown(f"## {detail['match']}")
    st.write(f"**Data:** {detail['date']}  |  **Ora:** {detail['time']}")
    st.write(f"**Lega:** {detail['league']} ({detail['country']})")
    st.write(f"**Tag:** {' '.join(detail.get('tags', []))}")

    m1, m2, m3 = st.columns(3)
    m1.metric("1", f"{detail['markets'].get('q1', 0):.2f}")
    m2.metric("X", f"{detail['markets'].get('qx', 0):.2f}")
    m3.metric("2", f"{detail['markets'].get('q2', 0):.2f}")

    m4, m5, m6 = st.columns(3)
    m4.metric("O2.5", f"{detail['markets'].get('o25', 0):.2f}")
    m5.metric("O0.5 HT", f"{detail['markets'].get('o05ht', 0):.2f}")
    m6.metric("O1.5 HT", f"{detail['markets'].get('o15ht', 0):.2f}")

    st.markdown("---")
    st.subheader("📊 Struttura partita")

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Match Profile", structure.get("match_profile", "neutral"))
    a2.metric("Structure Score", f"{safe_float(structure.get('structure_score', 0), 0.0):.2f}")
    a3.metric("One-sided Risk", f"{safe_float(structure.get('one_sided_risk', 0), 0.0):.2f}")
    a4.metric("Fav Quote", f"{safe_float(structure.get('fav_quote', 0), 0.0):.2f}")

    st.write(
        f"**Combined HT Clean:** {safe_float(structure.get('combined_ht_clean', 0), 0.0):.2f} | "
        f"**Combined FT Clean:** {safe_float(structure.get('combined_ft_clean', 0), 0.0):.2f} | "
        f"**Combined HT Scored Clean:** {safe_float(structure.get('combined_ht_scored_clean', 0), 0.0):.2f}"
    )
    st.write(
        f"**Cross Home Clean:** {safe_float(structure.get('cross_home_clean', 0), 0.0):.2f} | "
        f"**Cross Away Clean:** {safe_float(structure.get('cross_away_clean', 0), 0.0):.2f}"
    )
    st.write(
        f"**Bilateral FT:** {'✅' if structure.get('bilateral_ft') else '❌'} | "
        f"**Bilateral HT:** {'✅' if structure.get('bilateral_ht') else '❌'} | "
        f"**Fav Zone:** {structure.get('fav_zone', 'unknown')}"
    )

    st.markdown("---")
    st.subheader("📈 Lettura Mercato")

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Market Profile", market_reading.get("market_profile", "neutral"))
    b2.metric("Coherence", f"{safe_float(market_reading.get('coherence_score', 0), 0.0):.2f}")
    b3.metric("Dislocation", f"{safe_float(market_reading.get('dislocation_score', 0), 0.0):.2f}")
    b4.metric("Value Left", market_reading.get("value_left", "unknown"))

    st.write(
        f"**Leading Market:** {market_reading.get('leading_market', 'none')} | "
        f"**Lagging Market:** {market_reading.get('lagging_market', 'none')} | "
        f"**Drop Type:** {market_reading.get('drop_type', 'none')} | "
        f"**Drop Strength:** {market_reading.get('drop_strength', 'none')} | "
        f"**Drop Confirmed:** {'✅' if market_reading.get('drop_confirmed') else '❌'}"
    )

    pos_flags = market_reading.get("positive_flags", []) or []
    warn_flags = market_reading.get("warning_flags", []) or []

    st.write(f"**Positive Flags:** {', '.join(pos_flags) if pos_flags else 'Nessuna'}")
    st.write(f"**Warning Flags:** {', '.join(warn_flags) if warn_flags else 'Nessuna'}")

    st.write(
        f"**Margin Open:** {safe_float(fair_value.get('margin_open', 0), 0.0):.3f} | "
        f"**Margin Curr:** {safe_float(fair_value.get('margin_curr', 0), 0.0):.3f} | "
        f"**Margin Delta:** {safe_float(fair_value.get('margin_delta', 0), 0.0):.3f}"
    )

    st.write(
        f"**Fav Fair Prob Open:** {safe_float(fair_value.get('fav_fair_prob_open', 0), 0.0):.3f} | "
        f"**Fav Fair Prob Curr:** {safe_float(fair_value.get('fav_fair_prob_curr', 0), 0.0):.3f} | "
        f"**Fav Fair Prob Delta:** {safe_float(fair_value.get('fav_fair_prob_delta', 0), 0.0):.3f}"
    )

    st.write(
        f"**Fav Fair Odd Curr:** {safe_float(fair_value.get('fav_fair_curr', 0), 0.0):.3f}"
    )

    st.markdown("---")
    st.subheader("🧬 Evolution Edge Model")

    e1, e2, e3, e4 = st.columns(4)
    e1.metric("λ Home FT", f"{safe_float(model_edge.get('lam_home_ft', 0), 0.0):.2f}")
    e2.metric("λ Away FT", f"{safe_float(model_edge.get('lam_away_ft', 0), 0.0):.2f}")
    e3.metric("λ Home HT", f"{safe_float(model_edge.get('lam_home_ht', 0), 0.0):.2f}")
    e4.metric("λ Away HT", f"{safe_float(model_edge.get('lam_away_ht', 0), 0.0):.2f}")

    st.write(
        f"**Context Avg:** {safe_float(model_edge.get('ctx_avg', 0), 0.0):.2f} | "
        f"**Regularity Avg:** {safe_float(model_edge.get('reg_avg', 0), 0.0):.2f} | "
        f"**FT StDev Avg:** {safe_float(model_edge.get('ft_sd_avg', 0), 0.0):.2f} | "
        f"**HT StDev Avg:** {safe_float(model_edge.get('ht_sd_avg', 0), 0.0):.2f}"
    )

    st.markdown("#### FT Over 2.5")
    eo1, eo2, eo3, eo4 = st.columns(4)
    eo1.metric("P Modello O2.5", f"{safe_float(model_edge.get('p_model_o25', 0), 0.0):.3f}")
    eo2.metric("P Mercato O2.5", f"{safe_float(model_edge.get('p_market_o25', 0), 0.0):.3f}")
    eo3.metric("Edge O2.5", f"{safe_float(model_edge.get('edge_o25', 0), 0.0):.3f}")
    eo4.metric("Edge Level", f"{model_edge.get('edge_level_o25', 'NONE')}")

    st.write(
        f"**Edge Logit O2.5:** {safe_float(model_edge.get('edge_logit_o25', 0), 0.0):.3f}"
    )

    st.markdown("#### HT Over 0.5 / Over 1.5")
    eh1, eh2, eh3 = st.columns(3)
    eh1.metric(
        "P Modello O0.5HT",
        f"{safe_float(model_edge.get('p_model_o05ht', 0), 0.0):.3f}"
    )
    eh2.metric(
        "P Mercato O0.5HT",
        f"{safe_float(model_edge.get('p_market_o05ht', 0), 0.0):.3f}"
    )
    eh3.metric(
        "Edge O0.5HT",
        f"{safe_float(model_edge.get('edge_o05ht', 0), 0.0):.3f}"
    )

    eh4, eh5, eh6 = st.columns(3)
    eh4.metric(
        "P Modello O1.5HT",
        f"{safe_float(model_edge.get('p_model_o15ht', 0), 0.0):.3f}"
    )
    eh5.metric(
        "P Mercato O1.5HT",
        f"{safe_float(model_edge.get('p_market_o15ht', 0), 0.0):.3f}"
    )
    eh6.metric(
        "Edge O1.5HT",
        f"{safe_float(model_edge.get('edge_o15ht', 0), 0.0):.3f}"
    )

    st.write(
        f"**Edge Level O0.5HT:** {model_edge.get('edge_level_o05ht', 'NONE')} | "
        f"**Edge Logit O0.5HT:** {safe_float(model_edge.get('edge_logit_o05ht', 0), 0.0):.3f}"
    )
    st.write(
        f"**Edge Level O1.5HT:** {model_edge.get('edge_level_o15ht', 'NONE')} | "
        f"**Edge Logit O1.5HT:** {safe_float(model_edge.get('edge_logit_o15ht', 0), 0.0):.3f}"
    )
    s1, s2, s3, s4, s5, s6 = st.columns(6)
    s1.metric("PTGG", f"{safe_float(scores.get('ptgg', 0), 0.0):.2f}")
    s2.metric("PT1.5", f"{safe_float(scores.get('pto15', 0), 0.0):.2f}")
    s3.metric("PT", f"{safe_float(scores.get('pt', 0), 0.0):.2f}")
    s4.metric("OVER", f"{safe_float(scores.get('over', 0), 0.0):.2f}")
    s5.metric("BOOST", f"{safe_float(scores.get('boost', 0), 0.0):.2f}")
    s6.metric("GOLD", f"{safe_float(scores.get('gold', 0), 0.0):.2f}")

    st.write(
        f"**Internal Labels:** {', '.join(signal_debug.get('internal_labels', [])) if signal_debug.get('internal_labels') else 'Nessuna'} | "
        f"**Strong Tag Count:** {signal_debug.get('strong_tag_count', 0)} | "
        f"**Drop Diff:** {safe_float(signal_debug.get('drop_diff', 0), 0.0):.2f}"
    )

    st.markdown("---")
    st.subheader("🏠 / ✈️ Profili squadra")

    p1, p2 = st.columns(2)

    with p1:
        st.markdown(f"### 🏠 {detail['home_team']}")
        st.write(
            f"**Expected Side:** {home_profile.get('expected_side', 'all')} | "
            f"**Context Weight:** {safe_float(home_profile.get('context_weight', 0), 0.0):.2f}"
        )
        st.write(
            f"**AVG FT Clean:** {safe_float(home_profile.get('avg_total_clean', 0), 0.0):.2f} | "
            f"**AVG HT Clean:** {safe_float(home_profile.get('avg_ht_clean', 0), 0.0):.2f}"
        )
        st.write(
            f"**FT Scored Clean:** {safe_float(home_profile.get('avg_ft_scored_clean', 0), 0.0):.2f} | "
            f"**FT Conceded Clean:** {safe_float(home_profile.get('avg_ft_conceded_clean', 0), 0.0):.2f}"
        )
        st.write(
            f"**HT 1+ Rate:** {safe_float(home_profile.get('ht_1plus_rate', 0), 0.0):.2f} | "
            f"**FT 2+ Rate:** {safe_float(home_profile.get('ft_2plus_rate', 0), 0.0):.2f}"
        )
        st.write(
            f"**FT StDev:** {safe_float(home_profile.get('ft_stdev', 9.9), 9.9):.2f} | "
            f"**Scoring Regularity:** {safe_float(home_profile.get('scoring_regularity', 0), 0.0):.2f}"
        )
        st.write(
            f"**CTX AVG FT Clean:** {safe_float(home_profile.get('ctx_avg_total_clean', 0), 0.0):.2f} | "
            f"**CTX AVG HT Clean:** {safe_float(home_profile.get('ctx_avg_ht_clean', 0), 0.0):.2f}"
        )

    with p2:
        st.markdown(f"### ✈️ {detail['away_team']}")
        st.write(
            f"**Expected Side:** {away_profile.get('expected_side', 'all')} | "
            f"**Context Weight:** {safe_float(away_profile.get('context_weight', 0), 0.0):.2f}"
        )
        st.write(
            f"**AVG FT Clean:** {safe_float(away_profile.get('avg_total_clean', 0), 0.0):.2f} | "
            f"**AVG HT Clean:** {safe_float(away_profile.get('avg_ht_clean', 0), 0.0):.2f}"
        )
        st.write(
            f"**FT Scored Clean:** {safe_float(away_profile.get('avg_ft_scored_clean', 0), 0.0):.2f} | "
            f"**FT Conceded Clean:** {safe_float(away_profile.get('avg_ft_conceded_clean', 0), 0.0):.2f}"
        )
        st.write(
            f"**HT 1+ Rate:** {safe_float(away_profile.get('ht_1plus_rate', 0), 0.0):.2f} | "
            f"**FT 2+ Rate:** {safe_float(away_profile.get('ft_2plus_rate', 0), 0.0):.2f}"
        )
        st.write(
            f"**FT StDev:** {safe_float(away_profile.get('ft_stdev', 9.9), 9.9):.2f} | "
            f"**Scoring Regularity:** {safe_float(away_profile.get('scoring_regularity', 0), 0.0):.2f}"
        )
        st.write(
            f"**CTX AVG FT Clean:** {safe_float(away_profile.get('ctx_avg_total_clean', 0), 0.0):.2f} | "
            f"**CTX AVG HT Clean:** {safe_float(away_profile.get('ctx_avg_ht_clean', 0), 0.0):.2f}"
        )

    st.markdown("---")
    c_home, c_away = st.columns(2)

    with c_home:
        st.markdown(f"### 🏠 Ultime 8 {detail['home_team']}")
        df_home = pd.DataFrame(detail.get("home_last_8", []))
        if not df_home.empty:
            st.dataframe(df_home, use_container_width=True, hide_index=True)
        else:
            st.info("Nessun dato home disponibile.")

    with c_away:
        st.markdown(f"### ✈️ Ultime 8 {detail['away_team']}")
        df_away = pd.DataFrame(detail.get("away_last_8", []))
        if not df_away.empty:
            st.dataframe(df_away, use_container_width=True, hide_index=True)
        else:
            st.info("Nessun dato away disponibile.")

if st.session_state.selected_fixture_for_modal:
    show_match_modal(st.session_state.selected_fixture_for_modal)

# ------------------------------------------
# VISUAL HELPERS TABELLA
# ------------------------------------------
def outcome_block(label, q_open, q_curr, data=None):
    try:
        q_open = safe_float(q_open, 0.0)
        q_curr = safe_float(q_curr, 0.0)

        if q_open > 0 and q_curr > 0:
            diff = q_curr - q_open

            if diff <= -0.15:
                color = "#ff4d4d"
            elif diff < 0:
                color = "#ffa500"
            elif diff > 0:
                color = "#00cc66"
            else:
                color = "#999999"
        else:
            color = "#999999"

        return f"<span style='color:{color}'><b>{label}</b> {q_curr:.2f}</span>"
    except Exception:
        return f"{label} {q_curr}"


def build_1x2_visual(row):
    q1_open = safe_float(row.get("Q1_OPEN"), 0.0)
    qx_open = safe_float(row.get("QX_OPEN"), 0.0)
    q2_open = safe_float(row.get("Q2_OPEN"), 0.0)

    q1_curr = safe_float(row.get("Q1_CURR"), 0.0)
    qx_curr = safe_float(row.get("QX_CURR"), 0.0)
    q2_curr = safe_float(row.get("Q2_CURR"), 0.0)

    return f"""
    <div style="
        display:flex;
        align-items:flex-start;
        justify-content:center;
        gap:10px;
        white-space:nowrap;
    ">
        {outcome_block("1", q1_open, q1_curr)}
        {outcome_block("X", qx_open, qx_curr)}
        {outcome_block("2", q2_open, q2_curr)}
    </div>
    """


def build_o25_visual(row):
    move = str(row.get("O25_MOVE", "")).strip()
    current = str(row.get("O2.5", "")).strip()

    if move:
        return f"""
        <div style="line-height:1.15; white-space:pre-line;">
            {move}
        </div>
        """
    return current


# ------------------------------------------
# TABLE RENDER
# ------------------------------------------
if st.session_state.scan_results:
    df = pd.DataFrame(st.session_state.scan_results)
    full_view = df[df["Data"] == target_dates[HORIZON - 1]]

    if not full_view.empty:
        full_view = full_view.sort_values(by=["Ora", "Match"])
        view = full_view.copy()

        if "MOVE_SUMMARY" not in view.columns:
            view["MOVE_SUMMARY"] = ""

        if "Info" in view.columns and "MOVE_SUMMARY" in view.columns:
            view["Info"] = view.apply(
                lambda r: (
                    f"{r['Info']} | {r['MOVE_SUMMARY']}"
                    if str(r.get("MOVE_SUMMARY", "")).strip()
                    else str(r.get("Info", ""))
                ),
                axis=1
            )

        view["1X2_VIS"] = view.apply(build_1x2_visual, axis=1)
        view["O25_VIS"] = view.apply(build_o25_visual, axis=1)

        cols_to_drop = [
            "Data", "Fixture_ID",

            "Q1_OPEN", "QX_OPEN", "Q2_OPEN",
            "O25_OPEN", "O05HT_OPEN", "O15HT_OPEN",

            "Q1_CURR", "QX_CURR", "Q2_CURR",
            "O25_CURR", "O05HT_CURR", "O15HT_CURR",

            "Q1_MOVE_DATA", "QX_MOVE_DATA", "Q2_MOVE_DATA",
            "O25_MOVE_DATA", "O05HT_MOVE_DATA", "O15HT_MOVE_DATA",

            "Q1_MOVE", "QX_MOVE", "Q2_MOVE",
            "O25_MOVE", "O05HT_MOVE", "O15HT_MOVE",

            "INVERSION", "INV_FROM", "INV_TO",
            "FAV_OPEN", "FAV_CURRENT",

            "MOVE_SUMMARY",

            "MARKET_PROFILE", "COHERENCE_SCORE", "DISLOCATION_SCORE",
            "DROP_TYPE", "VALUE_LEFT", "MATCH_PROFILE", "STRUCTURE_SCORE",

            "LAM_HOME_FT", "LAM_AWAY_FT", "LAM_HOME_HT", "LAM_AWAY_HT",

            "P_MODEL_O25", "P_MARKET_O25", "EDGE_O25", "EDGE_LOGIT_O25", "EDGE_LEVEL_O25",
            "P_MODEL_O05HT", "P_MARKET_O05HT", "EDGE_O05HT", "EDGE_LOGIT_O05HT", "EDGE_LEVEL_O05HT",
            "P_MODEL_O15HT", "P_MARKET_O15HT", "EDGE_O15HT", "EDGE_LOGIT_O15HT", "EDGE_LEVEL_O15HT",
        ]
        view = view.drop(columns=[c for c in cols_to_drop if c in view.columns], errors="ignore")

        if "1X2" in view.columns:
            view["1X2"] = view["1X2_VIS"]

        if "O2.5" in view.columns:
            view["O2.5"] = view["O25_VIS"]

        view = view.drop(columns=["1X2_VIS", "O25_VIS"], errors="ignore")

        st.markdown("""
            <style>
                .main-container { width: 100%; max-height: 800px; overflow: auto; border: 1px solid #444; border-radius: 8px; background-color: #0e1117; }
                .mobile-table { width: 100%; min-width: 1000px; border-collapse: separate; border-spacing: 0; font-family: sans-serif; font-size: 11px; }
                .mobile-table th { position: sticky; top: 0; background: #1a1c23; color: #00e5ff; z-index: 10; padding: 12px 5px; border-bottom: 2px solid #333; border-right: 1px solid #333; }
                .mobile-table td { padding: 8px 5px; border-bottom: 1px solid #333; border-right: 1px solid #333; text-align: center; white-space: nowrap; vertical-align: middle; }
                .mobile-table td div { white-space: pre-line; }

                .row-gold {
                    background: linear-gradient(90deg, #FFD700, #FFC300) !important;
                    color: #000000 !important;
                    font-weight: 700;
                    border-left: 5px solid #ff9900;
                }
                .row-gold td {
                    box-shadow: inset 0 0 6px rgba(255, 200, 0, 0.6);
                }

                .row-boost {
                    background: linear-gradient(90deg, #0f5132, #198754) !important;
                    color: #ffffff !important;
                    font-weight: 600;
                    border-left: 5px solid #00ff88;
                }

                .row-over {
                    background-color: #d1f7e3 !important;
                    color: #003d2e !important;
                    font-weight: 500;
                }

                .row-pt {
                    background-color: #d6e4ff !important;
                    color: #002b5c !important;
                    font-weight: 500;
                }

                .row-probe {
                    background-color: #f3e8ff !important;
                    color: #4b0082 !important;
                    font-style: italic;
                    opacity: 0.92;
                }

                .row-std {
                    background-color: #ffffff !important;
                    color: #000000 !important;
                }
            </style>
        """, unsafe_allow_html=True)

        def get_row_class(info):
            if "GOLD" in info:
                return "row-gold"
            if "BOOST" in info:
                return "row-boost"
            if "OVER" in info:
                return "row-over"
            if "PT" in info:
                return "row-pt"
            if "🐟" in info:
                return "row-probe"
            return "row-std"

        html = '<div class="main-container"><table class="mobile-table"><thead><tr>'
        html += ''.join(f'<th>{c}</th>' for c in view.columns)
        html += '</tr></thead><tbody>'

        for _, row in view.iterrows():
            cls = get_row_class(str(row["Info"]))
            html += f'<tr class="{cls}">' + ''.join(f'<td>{v}</td>' for v in row) + '</tr>'

        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("🔎 Dettagli partite")

        for _, row in full_view.iterrows():
            fid = str(row["Fixture_ID"])
            c_btn, c_ora, c_match, c_lega = st.columns([1, 1.3, 4, 3])

            with c_btn:
                if st.button("🔎", key=f"open_modal_{fid}", help="Apri dettagli match"):
                    st.session_state.selected_fixture_for_modal = fid
                    st.rerun()

            with c_ora:
                st.write(row["Ora"])

            with c_match:
                st.write(row["Match"])

            with c_lega:
                st.write(row["Lega"])

        st.markdown("---")
        d1, d2, d3 = st.columns(3)
        d1.download_button(
            "💾 CSV",
            full_view.to_csv(index=False).encode("utf-8"),
            f"arab_v25_{target_dates[HORIZON - 1]}.csv"
        )
        d2.download_button(
            "🌐 HTML",
            html.encode("utf-8"),
            f"arab_v25_{target_dates[HORIZON - 1]}.html"
        )
        d3.download_button(
            "🧠 DETAILS JSON",
            json.dumps(
                {
                    k: v for k, v in st.session_state.match_details.items()
                    if v.get("date") == target_dates[HORIZON - 1]
                },
                indent=4,
                ensure_ascii=False
            ).encode("utf-8"),
            f"details_v25_{target_dates[HORIZON - 1]}.json"
        )
else:
    st.info("Esegui uno scan.")


# ------------------------------------------
# MAIN EXEC
# ------------------------------------------
if __name__ == "__main__":
    if "--auto" in sys.argv:
        reset_runtime_api_cache()
        print("🚀 Avvio Scan Automatico Notturno Multi-Day V25...", flush=True)
        HORIZON = 1
        run_nightly_multiday_build()
        print("✅ Scan completo terminato: data.json + data_day1/2/3/4/5 + details_day1/2/3/4/5 aggiornati.", flush=True)

    elif "--fast" in sys.argv:
        reset_runtime_api_cache()
        HORIZON = 1
        print("⚡ Avvio Scan Veloce Automatico V25 (solo Day 1)...", flush=True)
        run_full_scan(horizon=1, snap=False, update_main_site=True, show_success=False)
        print("✅ Scan veloce terminato: data.json + data_day1 + details_day1 aggiornati.", flush=True)

    elif "--day2-refresh" in sys.argv:
        reset_runtime_api_cache()
        HORIZON = 2
        print("🌙 Avvio Refresh Serale Day 2 V25...", flush=True)
        run_full_scan(horizon=2, snap=False, update_main_site=False, show_success=False)
        print("✅ Refresh Day 2 terminato: data_day2 + details_day2 aggiornati.", flush=True)
