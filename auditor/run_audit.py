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
    return "FT" in status


def safe_int(value):
    return 0 if value is None else int(value)


def parse_num(value, default=None):
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sì"}


def as_list(value):
    if isinstance(value, list):
        return [str(x) for x in value if x is not None and str(x).strip()]
    if isinstance(value, str) and value.strip():
        s = value.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except Exception:
                pass
        return [x.strip() for x in s.split("|") if x.strip()]
    return []


def get_any(row: dict, keys: list[str], default=None):
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def empty_stat():
    return {"total": 0, "hit": 0}


def bump(stats: dict, key: str, hit: bool) -> None:
    if key not in stats:
        stats[key] = empty_stat()
    stats[key]["total"] += 1
    if hit:
        stats[key]["hit"] += 1


def finalize_stats(stats: dict) -> dict:
    out = {}
    for key, item in stats.items():
        total = int(item.get("total", 0) or 0)
        hit = int(item.get("hit", 0) or 0)
        rate = round((hit / total) * 100, 2) if total > 0 else 0.0
        out[key] = {"total": total, "hit": hit, "rate": rate}
    return out


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

    index_data = {"dates": dates}
    index_path = os.path.join(OUTPUT_DIR, "audit_index.json")
    save_json(index_path, index_data)


def copy_refined_to_detail(m: dict) -> dict:
    keys = [
        "ELITE_SIGNAL", "elite_signal",
        "SIGNAL_TIER", "signal_tier",
        "HTML_PRIORITY", "html_priority",
        "REFINED_TAGS", "refined_tags",
        "REFINED_BADGES", "refined_badges",
        "REFINED_INFO", "refined_info",
        "HTML_BADGE_TEXT", "html_badge_text",
        "FT_OVER_SCORE", "ft_over_score",
        "HT_PRESSURE_SCORE", "ht_pressure_score",
        "FT_TAG", "ft_tag",
        "HT_TAG", "ht_tag",
        "DROP_WARNING", "drop_warning",
        "MARKET_VALIDATOR", "market_validator",
        "OVER_LEVEL_REFINED", "over_level_refined",
        "DROP_CLASS", "drop_class",
        "DROP_LABEL", "drop_label",
        "DROP_VALUE", "drop_value",
        "FAV_BAND", "fav_band",
        "GG_HT_SNIPER", "gg_ht_sniper",
        "TEMPO_TAG", "tempo_tag",
        "EDGE_LEVEL_O25", "edge_level_o25",
        "EDGE_O25", "edge_o25",
        "SIGNAL_STABILITY", "signal_stability",
        "SIGNAL_SUMMARY", "signal_summary",
    ]
    out = {}
    for key in keys:
        if key in m and m[key] not in (None, ""):
            out[key] = m[key]
    return out


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

    canonical_stats = {
        "gold": empty_stat(),
        "over_l1": empty_stat(),
        "over_l2": empty_stat(),
        "over_l3": empty_stat(),
        "pt": empty_stat(),
        "market": empty_stat(),
        "drop": empty_stat(),
        "inv": empty_stat(),
        "probe": empty_stat(),
    }

    combo_stats = {
        "gold_over": empty_stat(),
        "gold_market": empty_stat(),
        "gold_drop": empty_stat(),
        "gold_inv": empty_stat(),
        "over_market": empty_stat(),
        "pt_market": empty_stat(),
        "market_drop": empty_stat(),
        "market_inv": empty_stat(),
        "drop_inv": empty_stat(),
        "probe_market": empty_stat(),
    }

    legacy_stats = {
        "PTGG": empty_stat(),
        "PTO15": empty_stat(),
        "OVER": empty_stat(),
        "STRONG_OVER": empty_stat(),
        "BOOST": empty_stat(),
        "GOLD": empty_stat(),
        "MARKET": empty_stat(),
        "DROP": empty_stat(),
        "INV": empty_stat(),
        "PROBE": empty_stat(),
    }

    refined_stats = {
        "elite_signal": empty_stat(),
        "market_validator": empty_stat(),
        "ft_ball2": empty_stat(),
        "ft_over_plus": empty_stat(),
        "ft_monitor": empty_stat(),
        "ht_watch": empty_stat(),
        "ht_pt": empty_stat(),
        "drop_trap": empty_stat(),
        "gold_monitor": empty_stat(),
        "strong_market_over": empty_stat(),
        "elite_market_over": empty_stat(),
        "elite_gold_confirmed": empty_stat(),
        "over_base": empty_stat(),
        "probe_ht_only": empty_stat(),
        "drop_soft": empty_stat(),
        "drop_mid": empty_stat(),
        "drop_big": empty_stat(),
        "drop_hard": empty_stat(),
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
        canonical = m.get("canonical", {}) or {}
        legacy_tags = set(m.get("legacy_tags", []) or [])

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

        ht1_hit = ht_goals >= 1
        ptgg_hit = ht_home >= 1 and ht_away >= 1
        pto15_hit = ht_goals >= 2
        over_hit = ft_goals >= 3
        btts_hit = ft_home >= 1 and ft_away >= 1

        if "PTGG" in legacy_tags and "PTO15" in legacy_tags:
            pt_hit = ptgg_hit or pto15_hit
        elif "PTGG" in legacy_tags:
            pt_hit = ptgg_hit
        elif "PTO15" in legacy_tags:
            pt_hit = pto15_hit
        else:
            pt_hit = pto15_hit

        gold_flag = bool(canonical.get("gold"))
        over_level = int(canonical.get("over_level") or 0)
        pt_flag = bool(canonical.get("pt"))
        market_flag = bool(canonical.get("market"))
        drop_flag = bool(canonical.get("drop"))
        inv_flag = bool(canonical.get("inv"))
        probe_flag = bool(canonical.get("probe"))

        if gold_flag:
            bump(canonical_stats, "gold", over_hit)

        if over_level == 1:
            bump(canonical_stats, "over_l1", over_hit)
        elif over_level == 2:
            bump(canonical_stats, "over_l2", over_hit)
        elif over_level == 3:
            bump(canonical_stats, "over_l3", over_hit)

        if pt_flag:
            bump(canonical_stats, "pt", pt_hit)
        if market_flag:
            bump(canonical_stats, "market", over_hit)
        if drop_flag:
            bump(canonical_stats, "drop", over_hit)
        if inv_flag:
            bump(canonical_stats, "inv", over_hit)
        if probe_flag:
            bump(canonical_stats, "probe", over_hit)

        if gold_flag and over_level >= 1:
            bump(combo_stats, "gold_over", over_hit)
        if gold_flag and market_flag:
            bump(combo_stats, "gold_market", over_hit)
        if gold_flag and drop_flag:
            bump(combo_stats, "gold_drop", over_hit)
        if gold_flag and inv_flag:
            bump(combo_stats, "gold_inv", over_hit)
        if over_level >= 1 and market_flag:
            bump(combo_stats, "over_market", over_hit)
        if pt_flag and market_flag:
            bump(combo_stats, "pt_market", pt_hit)
        if market_flag and drop_flag:
            bump(combo_stats, "market_drop", over_hit)
        if market_flag and inv_flag:
            bump(combo_stats, "market_inv", over_hit)
        if drop_flag and inv_flag:
            bump(combo_stats, "drop_inv", over_hit)
        if probe_flag and market_flag:
            bump(combo_stats, "probe_market", over_hit)

        if "PTGG" in legacy_tags:
            bump(legacy_stats, "PTGG", ptgg_hit)
        if "PTO15" in legacy_tags:
            bump(legacy_stats, "PTO15", pto15_hit)
        if "OVER" in legacy_tags:
            bump(legacy_stats, "OVER", over_hit)
        if "STRONG_OVER" in legacy_tags:
            bump(legacy_stats, "STRONG_OVER", over_hit)
        if "BOOST" in legacy_tags:
            bump(legacy_stats, "BOOST", over_hit)
        if "GOLD" in legacy_tags:
            bump(legacy_stats, "GOLD", over_hit)
        if "MARKET" in legacy_tags:
            bump(legacy_stats, "MARKET", over_hit)
        if "DROP" in legacy_tags:
            bump(legacy_stats, "DROP", over_hit)
        if "INV" in legacy_tags:
            bump(legacy_stats, "INV", over_hit)
        if "PROBE" in legacy_tags:
            bump(legacy_stats, "PROBE", over_hit)

        # Refined layer settlement.
        tier = str(get_any(m, ["SIGNAL_TIER", "signal_tier"], ""))
        ft_tag = str(get_any(m, ["FT_TAG", "ft_tag"], ""))
        ht_tag = str(get_any(m, ["HT_TAG", "ht_tag"], ""))
        ft_score = parse_num(get_any(m, ["FT_OVER_SCORE", "ft_over_score"], None), None)
        ht_score = parse_num(get_any(m, ["HT_PRESSURE_SCORE", "ht_pressure_score"], None), None)
        drop_class = str(get_any(m, ["DROP_CLASS", "drop_class"], "")).lower()

        elite_signal = boolish(get_any(m, ["ELITE_SIGNAL", "elite_signal"], False))
        market_validator = boolish(get_any(m, ["MARKET_VALIDATOR", "market_validator"], False))
        drop_warning = boolish(get_any(m, ["DROP_WARNING", "drop_warning"], False))

        if elite_signal:
            bump(refined_stats, "elite_signal", over_hit)
        if market_validator:
            bump(refined_stats, "market_validator", over_hit)
        if "⚽⚽" in ft_tag or (ft_score is not None and ft_score >= 2.50):
            bump(refined_stats, "ft_ball2", over_hit)
        if "FT OVER" in ft_tag.upper() or (ft_score is not None and ft_score >= 1.80):
            bump(refined_stats, "ft_over_plus", over_hit)
        if "FT MONITOR" in ft_tag.upper() or (ft_score is not None and 1.20 <= ft_score < 1.80):
            bump(refined_stats, "ft_monitor", over_hit)
        if ht_tag or (ht_score is not None and ht_score >= 0.95):
            bump(refined_stats, "ht_watch", pto15_hit)
        if "⚽PT" in ht_tag or (ht_score is not None and ht_score >= 1.35):
            bump(refined_stats, "ht_pt", ht1_hit)
        if drop_warning:
            bump(refined_stats, "drop_trap", over_hit)
        if tier == "GOLD_MONITOR":
            bump(refined_stats, "gold_monitor", over_hit)
        if tier == "STRONG_MARKET_OVER":
            bump(refined_stats, "strong_market_over", over_hit)
        if tier == "ELITE_MARKET_OVER":
            bump(refined_stats, "elite_market_over", over_hit)
        if tier == "ELITE_GOLD_CONFIRMED":
            bump(refined_stats, "elite_gold_confirmed", over_hit)
        if tier == "OVER_BASE":
            bump(refined_stats, "over_base", over_hit)
        if tier == "PROBE_HT_ONLY":
            bump(refined_stats, "probe_ht_only", pto15_hit)
        if drop_class in {"soft", "mid", "big", "hard"}:
            bump(refined_stats, f"drop_{drop_class}", over_hit)

        row_out = {
            "fixture_id": fixture_id,
            "date": m.get("date", audit_date),
            "league": m.get("league", ""),
            "match": m.get("match", ""),
            "status": status,
            "primary_signal": m.get("primary_signal", ""),
            "info_raw": m.get("info_raw", ""),
            "ht_score": f"{ht_home}-{ht_away}",
            "ft_score": f"{ft_home}-{ft_away}",
            "goals": {
                "ht": ht_goals,
                "ft": ft_goals,
                "btts_ft": btts_hit,
            },
            "canonical": {
                "gold": gold_flag,
                "over_level": over_level,
                "pt": pt_flag,
                "market": market_flag,
                "drop": drop_flag,
                "inv": inv_flag,
                "probe": probe_flag,
            },
            "hits": {
                "gold": over_hit if gold_flag else None,
                "over": over_hit if over_level >= 1 else None,
                "pt": pt_hit if pt_flag else None,
                "market": over_hit if market_flag else None,
                "drop": over_hit if drop_flag else None,
                "inv": over_hit if inv_flag else None,
                "probe": over_hit if probe_flag else None,
                "ptgg": ptgg_hit if "PTGG" in legacy_tags else None,
                "pto15": pto15_hit if "PTO15" in legacy_tags else None,
                "ht1": ht1_hit,
                "ht2": pto15_hit,
                "ft_over": over_hit,
            },
            "legacy_tags": sorted(legacy_tags),
            "drop_diff": m.get("drop_diff"),
            "has_inversion": bool(m.get("has_inversion")),
            "inv_from": m.get("inv_from", ""),
            "inv_to": m.get("inv_to", ""),
        }
        row_out.update(copy_refined_to_detail(m))
        detail_rows.append(row_out)

    counts["analyzed_rows"] = len(detail_rows)

    summary_output = {
        "audit_date": audit_date,
        "generated_at": datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
        "counts": counts,
        "canonical_stats": finalize_stats(canonical_stats),
        "combo_stats": finalize_stats(combo_stats),
        "legacy_stats": finalize_stats(legacy_stats),
        "refined_stats": finalize_stats(refined_stats),
        "settlement_rules": {
            "gold": "Hit se FT >= 3 gol",
            "over_l1": "Hit se FT >= 3 gol",
            "over_l2": "Hit se FT >= 3 gol",
            "over_l3": "Hit se FT >= 3 gol",
            "pt": "Hit se il sotto-segnale PT presente va a segno (PTGG oppure HT>=2; generic PT => HT>=2)",
            "market": "Hit se FT >= 3 gol",
            "drop": "Hit se FT >= 3 gol",
            "inv": "Hit se FT >= 3 gol",
            "probe": "Hit se FT >= 3 gol",
            "elite_signal": "Hit se FT >= 3 gol",
            "market_validator": "Hit se FT >= 3 gol",
            "ft_ball2": "Hit se FT >= 3 gol",
            "ft_over_plus": "Hit se FT >= 3 gol",
            "ht_watch": "Hit se HT >= 2 gol",
            "ht_pt": "Hit se HT >= 1 gol",
            "drop_trap": "Controllo warning: valore basso è positivo perché segnala partite da evitare",
        },
    }

    details_output = {
        "audit_date": audit_date,
        "generated_at": datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
        "rows_count": len(detail_rows),
        "rows": detail_rows,
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
