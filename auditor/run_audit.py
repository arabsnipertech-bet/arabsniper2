import os
import json
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FREEZE_DIR = os.path.join(BASE_DIR, "auditordata")
OUTPUT_DIR = os.path.join(BASE_DIR, "auditarchive")

API_KEY = os.environ.get("API_SPORTS_KEY", "").strip()

HEADERS = {
    "x-apisports-key": API_KEY
}

ITALY_TZ = ZoneInfo("Europe/Rome")


# =========================
# HELPERS
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_target_date(cli_date: str | None) -> str:
    """
    Se viene passato --date usa quello.
    Altrimenti prende ieri.
    """
    if cli_date:
        try:
            datetime.strptime(cli_date, "%Y-%m-%d")
            return cli_date
        except ValueError:
            raise ValueError("Formato data non valido. Usa YYYY-MM-DD")

    now_italy = datetime.now(ITALY_TZ)
    target = now_italy.date() - timedelta(days=1)
    return target.isoformat()


def load_freeze_by_date(target_date: str) -> dict:
    filename = f"freeze_for_audit_{target_date}.json"
    path = os.path.join(FREEZE_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Freeze non trovato: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_fixture_result(fixture_id: int | str):
    url = f"https://v3.football.api-sports.io/fixtures?id={fixture_id}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        data = response.json()

        if not data.get("response"):
            return None

        fixture = data["response"][0]

        status = fixture.get("fixture", {}).get("status", {}).get("short", "")

        goals = fixture.get("goals", {}) or {}
        score = fixture.get("score", {}) or {}
        halftime = score.get("halftime", {}) or {}

        return {
            "status": status,
            "ht_home": halftime.get("home"),
            "ht_away": halftime.get("away"),
            "ft_home": goals.get("home"),
            "ft_away": goals.get("away"),
        }

    except Exception as e:
        print(f"[ERROR] Fixture {fixture_id}: {e}")
        return None


def is_finished_status(status: str) -> bool:
    if not status:
        return False

    status = str(status).upper().strip()

    valid_exact = {"FT", "AET", "PEN"}
    if status in valid_exact:
        return True

    if "FT" in status:
        return True

    return False


def safe_int(value):
    return 0 if value is None else int(value)


def evaluate_tags(tags, ht_goals, ft_goals, ft_home, ft_away, ht_home, ht_away):
    """
    Settlement auditor V25:
    - PTGG = entrambe segnano nel primo tempo
    - PTO15 = almeno 2 gol nel primo tempo
    - OVER = almeno 3 gol FT
    - BOOST = PTO15 + OVER
    - GOLD = BOOST (proxy attuale, coerente con il vecchio auditor)
    - FISH_GG = entrambe segnano FT
    - FISH_OVER = almeno 3 gol FT
    """
    tag_set = set(tags or [])

    ptgg = ht_home >= 1 and ht_away >= 1
    pto15 = ht_goals >= 2
    over = ft_goals >= 3
    boost = pto15 and over
    gold = boost
    fish_gg = ft_home >= 1 and ft_away >= 1
    fish_over = ft_goals >= 3

    return {
        "PTGG": ptgg if "PTGG" in tag_set else None,
        "PTO15": pto15 if "PTO15" in tag_set else None,
        "OVER": over if "OVER" in tag_set else None,
        "BOOST": boost if "BOOST" in tag_set else None,
        "GOLD": gold if "GOLD" in tag_set else None,
        "FISH_GG": fish_gg if "FISH_GG" in tag_set else None,
        "FISH_OVER": fish_over if "FISH_OVER" in tag_set else None,
    }


def build_audit_index():
    dates = []

    if not os.path.isdir(OUTPUT_DIR):
        return

    for name in os.listdir(OUTPUT_DIR):
        if not name.startswith("audit_") or not name.endswith("_summary.json"):
            continue
        if name == "audit_last_summary.json":
            continue

        parts = name.split("_")
        if len(parts) >= 3:
            date_part = parts[1]
            try:
                datetime.strptime(date_part, "%Y-%m-%d")
                dates.append(date_part)
            except ValueError:
                continue

    dates = sorted(set(dates), reverse=True)

    index_data = {
        "dates": dates
    }

    index_path = os.path.join(OUTPUT_DIR, "audit_index.json")
    save_json(index_path, index_data)


# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(description="Run auditor on frozen day file")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data target in formato YYYY-MM-DD. Se assente usa ieri.",
    )
    args = parser.parse_args()

    if not API_KEY:
        raise RuntimeError("API_SPORTS_KEY non trovata nelle variabili ambiente")

    ensure_dir(OUTPUT_DIR)

    target_date = get_target_date(args.date)
    freeze = load_freeze_by_date(target_date)

    audit_date = freeze.get("audit_date", target_date)
    matches = freeze.get("matches", []) or []

    detail_rows = []

    stats = {
        "PTGG": {"total": 0, "hit": 0},
        "PTO15": {"total": 0, "hit": 0},
        "OVER": {"total": 0, "hit": 0},
        "BOOST": {"total": 0, "hit": 0},
        "GOLD": {"total": 0, "hit": 0},
        "FISH_GG": {"total": 0, "hit": 0},
        "FISH_OVER": {"total": 0, "hit": 0},
    }

    counts = {
        "freeze_matches": len(matches),
        "api_found": 0,
        "finished_matches": 0,
        "analyzed_rows": 0,
        "skipped_not_finished": 0,
        "skipped_no_result": 0,
    }

    for m in matches:
        fixture_id = m.get("fixture_id")
        tags = m.get("tags", []) or []

        result = fetch_fixture_result(fixture_id)

        if not result:
            counts["skipped_no_result"] += 1
            continue

        counts["api_found"] += 1

        status = result.get("status", "")
        if not is_finished_status(status):
            counts["skipped_not_finished"] += 1
            continue

        counts["finished_matches"] += 1

        ht_home = safe_int(result.get("ht_home"))
        ht_away = safe_int(result.get("ht_away"))
        ft_home = safe_int(result.get("ft_home"))
        ft_away = safe_int(result.get("ft_away"))

        ht_goals = ht_home + ht_away
        ft_goals = ft_home + ft_away

        tag_results = evaluate_tags(
            tags, ht_goals, ft_goals, ft_home, ft_away, ht_home, ht_away
        )

        for tag, value in tag_results.items():
            if value is None:
                continue
            stats[tag]["total"] += 1
            if value:
                stats[tag]["hit"] += 1

        detail_rows.append({
            "fixture_id": fixture_id,
            "date": m.get("date", audit_date),
            "league": m.get("league", ""),
            "match": m.get("match", ""),
            "status": status,
            "primary_signal": m.get("primary_signal", ""),
            "info_raw": m.get("info_raw", ""),
            "ht_score": f"{ht_home}-{ht_away}",
            "ft_score": f"{ft_home}-{ft_away}",
            "tags": tags,
            "PTGG": tag_results["PTGG"],
            "PTO15": tag_results["PTO15"],
            "OVER": tag_results["OVER"],
            "BOOST": tag_results["BOOST"],
            "GOLD": tag_results["GOLD"],
            "FISH_GG": tag_results["FISH_GG"],
            "FISH_OVER": tag_results["FISH_OVER"],
        })

    counts["analyzed_rows"] = len(detail_rows)

    summary_stats = {}
    for tag, data in stats.items():
        total = data["total"]
        hit = data["hit"]
        rate = round((hit / total) * 100, 2) if total > 0 else 0.0

        summary_stats[tag] = {
            "total": total,
            "hit": hit,
            "rate": rate
        }

    generated_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    details_output = {
        "audit_date": audit_date,
        "generated_at": generated_at,
        "rows_count": len(detail_rows),
        "rows": detail_rows
    }

    summary_output = {
        "audit_date": audit_date,
        "generated_at": generated_at,
        "counts": counts,
        "stats": summary_stats,
        "settlement_rules": {
            "PTGG": "HT entrambe segnano",
            "PTO15": "HT almeno 2 gol",
            "OVER": "FT almeno 3 gol",
            "BOOST": "PTO15 + OVER",
            "GOLD": "BOOST proxy",
            "FISH_GG": "FT entrambe segnano",
            "FISH_OVER": "FT almeno 3 gol"
        }
    }

    details_path = os.path.join(OUTPUT_DIR, f"audit_{audit_date}_details.json")
    summary_path = os.path.join(OUTPUT_DIR, f"audit_{audit_date}_summary.json")

    last_details_path = os.path.join(OUTPUT_DIR, "audit_last_details.json")
    last_summary_path = os.path.join(OUTPUT_DIR, "audit_last_summary.json")

    save_json(details_path, details_output)
    save_json(summary_path, summary_output)
    save_json(last_details_path, details_output)
    save_json(last_summary_path, summary_output)
    build_audit_index()

    print(f"[OK] Audit completato per {audit_date}")
    print(f"[OK] Freeze matches: {counts['freeze_matches']}")
    print(f"[OK] Match trovati via API: {counts['api_found']}")
    print(f"[OK] Match finiti: {counts['finished_matches']}")
    print(f"[OK] Match analizzati: {counts['analyzed_rows']}")
    print(f"[OK] Match non finiti saltati: {counts['skipped_not_finished']}")
    print(f"[OK] Match senza risultato API: {counts['skipped_no_result']}")


if __name__ == "__main__":
    main()
