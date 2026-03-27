import os
import json
import requests
from datetime import datetime, timezone, timedelta

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FREEZE_DIR = os.path.join(BASE_DIR, "auditordata")
OUTPUT_DIR = os.path.join(BASE_DIR, "auditarchive")

API_KEY = os.environ.get("API_SPORTS_KEY")

HEADERS = {
    "x-apisports-key": API_KEY
}

ITALY_TZ = timezone(timedelta(hours=1))


# =========================
# HELPERS
# =========================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def load_latest_freeze():
    files = [f for f in os.listdir(FREEZE_DIR) if f.startswith("freeze_for_audit_")]
    if not files:
        raise ValueError("Nessun file freeze trovato")

    latest = sorted(files)[-1]
    path = os.path.join(FREEZE_DIR, latest)

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_fixture_result(fixture_id):
    url = f"https://v3.football.api-sports.io/fixtures?id={fixture_id}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        data = response.json()

        if not data.get("response"):
            return None

        fixture = data["response"][0]

        goals = fixture["goals"]
        score = fixture["score"]

        return {
            "status": fixture["fixture"]["status"]["short"],
            "ht_home": score["halftime"]["home"],
            "ht_away": score["halftime"]["away"],
            "ft_home": goals["home"],
            "ft_away": goals["away"]
        }

    except Exception as e:
        print(f"[ERROR] Fixture {fixture_id}: {e}")
        return None


def evaluate_tags(tags, ht_goals, ft_goals, home_ft, away_ft):
    results = {}

    pt = ht_goals >= 1
    over = ft_goals >= 3
    boost = pt and over
    gold = boost

    results["PT"] = pt if "PT" in tags else None
    results["OVER"] = over if "OVER" in tags else None
    results["BOOST"] = boost if "BOOST" in tags else None
    results["GOLD"] = gold if "GOLD" in tags else None

    return results


# =========================
# MAIN
# =========================
def main():
    ensure_dir(OUTPUT_DIR)

    freeze = load_latest_freeze()
    audit_date = freeze["audit_date"]
    matches = freeze["matches"]

    detail_rows = []

    stats = {
        "PT": {"total": 0, "hit": 0},
        "OVER": {"total": 0, "hit": 0},
        "BOOST": {"total": 0, "hit": 0},
        "GOLD": {"total": 0, "hit": 0},
    }

    for m in matches:
        fixture_id = m["fixture_id"]
        tags = m["tags"]

        result = fetch_fixture_result(fixture_id)

        if not result:
            continue

        valid_status = result["status"]

        if not valid_status or "FT" not in valid_status:
            continue

        ht_home = result["ht_home"] or 0
        ht_away = result["ht_away"] or 0
        ft_home = result["ft_home"] or 0
        ft_away = result["ft_away"] or 0

        ht_goals = ht_home + ht_away
        ft_goals = ft_home + ft_away

        tag_results = evaluate_tags(tags, ht_goals, ft_goals, ft_home, ft_away)

        # aggiorna stats
        for tag, value in tag_results.items():
            if value is None:
                continue
            stats[tag]["total"] += 1
            if value:
                stats[tag]["hit"] += 1

        detail_rows.append({
            "fixture_id": fixture_id,
            "league": m["league"],
            "match": m["match"],
            "ht_score": f"{ht_home}-{ht_away}",
            "ft_score": f"{ft_home}-{ft_away}",
            "PT": tag_results["PT"],
            "OVER": tag_results["OVER"],
            "BOOST": tag_results["BOOST"],
            "GOLD": tag_results["GOLD"]
        })

    # calcolo percentuali
    summary = {}
    for tag, data in stats.items():
        total = data["total"]
        hit = data["hit"]
        rate = round((hit / total) * 100, 2) if total > 0 else 0

        summary[tag] = {
            "total": total,
            "hit": hit,
            "rate": rate
        }

    details_output = {
        "audit_date": audit_date,
        "rows": detail_rows
    }

    summary_output = {
        "audit_date": audit_date,
        "stats": summary
    }

    details_path = os.path.join(OUTPUT_DIR, f"audit_{audit_date}_details.json")
    summary_path = os.path.join(OUTPUT_DIR, f"audit_{audit_date}_summary.json")

    save_json(details_path, details_output)
    save_json(summary_path, summary_output)

    print(f"[OK] Audit completato per {audit_date}")
    print(f"[OK] Match analizzati: {len(detail_rows)}")


if __name__ == "__main__":
    main()
