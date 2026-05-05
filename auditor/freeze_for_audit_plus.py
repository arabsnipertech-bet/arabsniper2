import os
import json
import re
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "auditordata")

DAY1_FILE = os.path.join(DATA_DIR, "data_day1.json")
ITALY_TZ = ZoneInfo("Europe/Rome")


# =========================
# HELPERS
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_json_file(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File non trovato: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_field_any(row: dict, keys: list[str], fallback=""):
    if not isinstance(row, dict):
        return fallback
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return fallback


def normalize_payload(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "rows", "matches"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def parse_num(value):
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sì"}


def normalize_upper_text(value) -> str:
    raw = str(value or "")
    upper_raw = raw.upper()
    upper_raw = (
        upper_raw
        .replace("⭐", " ")
        .replace("🚀", " ")
        .replace("⚽", " ")
        .replace("🔥", " ")
        .replace("🎯", " ")
        .replace("📉", " DROP ")
        .replace("📈", " RISE ")
        .replace("↗", " RISE ")
        .replace("↘", " DROP ")
        .replace("↑", " RISE ")
        .replace("↓", " DROP ")
        .replace("🐟", " PROBE ")
    )
    upper_raw = re.sub(r"[_|,/;:+]+", " ", upper_raw)
    upper_raw = re.sub(r"\s+", " ", upper_raw).strip()
    return upper_raw


def get_fixture_id(row: dict):
    return get_field_any(row, ["Fixture_ID", "fixture_id", "fixtureId", "id"], None)


def get_match_date(row: dict):
    return get_field_any(row, ["Data", "data", "date", "Date", "match_date", "fixture_date"], "")


def get_league(row: dict):
    return get_field_any(row, ["Lega", "lega", "league", "League", "competition", "campionato"], "")


def get_match_name(row: dict):
    return get_field_any(row, ["Match", "match", "fixture", "Fixture", "match_name", "teams"], "")


def get_info_raw(row: dict):
    info = get_field_any(
        row,
        ["Info", "info", "INFO", "signals", "Signals", "SIGNALS", "tags_display", "tag_display", "Signali"],
        "",
    )
    if isinstance(info, list):
        return " ".join(str(x) for x in info if x is not None)
    return str(info or "")


def detect_main_match_date(rows: list[dict]) -> str:
    dates = [get_match_date(r) for r in rows if isinstance(r, dict) and get_match_date(r)]
    if not dates:
        raise ValueError("Nessuna data trovata dentro data_day1.json")
    counts = Counter(dates)
    main_date, _ = counts.most_common(1)[0]
    return main_date


def copy_new_layer_fields(row: dict) -> dict:
    """
    Conserva nel freeze i campi prodotti dal nuovo 3appdays/refiner.
    Senza questi campi auditplus non può misurare ELITE, FT/HT score, DROP TRAP, MARKET validator.
    """
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
        "GOLD_REFINED", "gold_refined",
        "MARKET_REFINED", "market_refined",
        "DROP_REFINED", "drop_refined",
        "INV_REFINED", "inv_refined",
        "PROBE_REFINED", "probe_refined",
        "PT_REFINED", "pt_refined",
        "BALL2_ORIGINAL", "ball2_original",
        "DROP_CLASS", "drop_class",
        "DROP_LABEL", "drop_label",
        "DROP_VALUE", "drop_value",
        "drop_weight",
        "FAV_BAND", "fav_band",
        "GG_HT_SNIPER", "gg_ht_sniper",
        "TEMPO_TAG", "tempo_tag",
        "EDGE_LEVEL_O25", "edge_level_o25",
        "EDGE_O25", "edge_o25",
        "SIGNAL_STABILITY", "signal_stability",
        "SIGNAL_SUMMARY", "signal_summary",
        "QUOTE_DYNAMICS", "quote_dynamics",
        "MARKET_ACCELERATION_LABEL", "market_acceleration_label",
        "FAV_VELOCITY", "fav_velocity",
        "FAV_ACCELERATION", "fav_acceleration",
        "O25_VELOCITY", "o25_velocity",
        "O25_ACCELERATION", "o25_acceleration",
        "O05HT_VELOCITY", "o05ht_velocity",
        "O05HT_ACCELERATION", "o05ht_acceleration",
        "O15HT_VELOCITY", "o15ht_velocity",
        "O15HT_ACCELERATION", "o15ht_acceleration",
        "Q1_OPEN", "QX_OPEN", "Q2_OPEN", "Q1_CURR", "QX_CURR", "Q2_CURR",
        "O25_OPEN", "O25_CURR", "O05HT_OPEN", "O05HT_CURR", "O15HT_OPEN", "O15HT_CURR",
        "q1_open", "qx_open", "q2_open", "q1_curr", "qx_curr", "q2_curr",
        "o25_open", "o25_curr", "o05ht_open", "o05ht_curr", "o15ht_open", "o15ht_curr",
    ]
    out = {}
    for key in keys:
        if key in row and row[key] not in (None, ""):
            out[key] = row[key]
    return out


# =========================
# SIGNAL EXTRACTION
# =========================
def extract_signals(row: dict) -> dict:
    info_raw = get_info_raw(row)
    clean = normalize_upper_text(info_raw)

    drop_diff = parse_num(get_field_any(row, ["DROP_DIFF", "drop_diff", "DROP_VALUE", "drop_value"], None))
    has_inversion_field = boolish(get_field_any(row, ["INVERSION", "HAS_INVERSION", "has_inversion"], False))
    inv_from = str(get_field_any(row, ["INV_FROM", "inv_from"], "") or "").strip()
    inv_to = str(get_field_any(row, ["INV_TO", "inv_to"], "") or "").strip()

    raw_over_level = parse_num(
        get_field_any(row, ["OVER_LEVEL_REFINED", "over_level_refined", "OVER_LEVEL", "over_level", "Over_Level"], None)
    )
    raw_over_level = int(raw_over_level) if raw_over_level is not None else 0

    refined_tags_text = normalize_upper_text(" ".join(map(str, row.get("REFINED_TAGS", row.get("refined_tags", [])) or [])))
    refined_badges_text = normalize_upper_text(" ".join(map(str, row.get("REFINED_BADGES", row.get("refined_badges", [])) or [])))
    refined_all = f"{clean} {refined_tags_text} {refined_badges_text} {normalize_upper_text(row.get('SIGNAL_TIER', row.get('signal_tier', '')))}"

    has_gold = bool(re.search(r"(^|\s)GOLD(?=\s|$)", refined_all)) or boolish(row.get("GOLD_REFINED", row.get("gold_refined", False)))
    has_boost = bool(re.search(r"(^|\s)BOOST(?=\s|$)", refined_all))
    has_strong_over = bool(re.search(r"STRONG\s+OVER", refined_all))
    has_over = bool(re.search(r"(^|\s)OVER(?=\s|$)", refined_all)) or has_boost or has_strong_over or raw_over_level > 0

    has_ptgg = "PTGG" in refined_all or bool(re.search(r"(GG\s*PT|PT\s*GG)", refined_all))
    has_pt15 = (
        "PT1.5" in refined_all
        or "PTO1.5" in refined_all
        or "PTO15" in refined_all
        or bool(re.search(r"(PT\s*1\.5|O15\s*PT|OVER1\.5\s*PT)", refined_all))
    )
    has_pt_generic = bool(re.search(r"(^|\s)PT(?=\s|$)", refined_all))
    has_pt = has_ptgg or has_pt15 or has_pt_generic or boolish(row.get("PT_REFINED", row.get("pt_refined", False))) or bool(row.get("HT_TAG", row.get("ht_tag", "")))

    has_market = (
        bool(re.search(r"(^|\s)MARKET(?=\s|$)", refined_all))
        or boolish(row.get("MARKET_VALIDATOR", row.get("market_validator", False)))
        or boolish(row.get("MARKET_REFINED", row.get("market_refined", False)))
    )
    has_drop = (
        bool(re.search(r"(^|\s)DROP(?=\s|$)", refined_all))
        or boolish(row.get("DROP_REFINED", row.get("drop_refined", False)))
        or boolish(row.get("DROP_WARNING", row.get("drop_warning", False)))
        or (drop_diff is not None and drop_diff >= 0.05)
        or bool(row.get("DROP_CLASS", row.get("drop_class", "")))
    )
    has_inv = (
        has_inversion_field
        or boolish(row.get("INV_REFINED", row.get("inv_refined", False)))
        or bool(re.search(r"(^|\s)INV(?=\s|$)", refined_all))
        or bool(inv_from and inv_to)
    )
    has_probe = (
        bool(re.search(r"(^|\s)PROBE(?=\s|$)", refined_all))
        or bool(re.search(r"(^|\s)(PESCE|PESCIOLINO|FISH)(?=\s|$)", refined_all))
        or "🐟" in info_raw
        or boolish(row.get("PROBE_REFINED", row.get("probe_refined", False)))
    )

    if raw_over_level in (1, 2, 3):
        over_level = raw_over_level
    else:
        if has_boost:
            over_level = 3
        elif has_strong_over:
            over_level = 2
        elif has_over:
            over_level = 1
        else:
            over_level = 0

    legacy_tags = []
    if has_gold:
        legacy_tags.append("GOLD")
    if over_level >= 3:
        legacy_tags.append("BOOST")
    if over_level >= 2:
        legacy_tags.append("STRONG_OVER")
    if over_level >= 1:
        legacy_tags.append("OVER")
    if has_ptgg:
        legacy_tags.append("PTGG")
    if has_pt15:
        legacy_tags.append("PTO15")
    if has_pt and not (has_ptgg or has_pt15):
        legacy_tags.append("PT")
    if has_market:
        legacy_tags.append("MARKET")
    if has_drop:
        legacy_tags.append("DROP")
    if has_inv:
        legacy_tags.append("INV")
    if has_probe:
        legacy_tags.append("PROBE")
    legacy_tags = list(dict.fromkeys(legacy_tags))

    canonical = {
        "gold": has_gold,
        "over_level": over_level,
        "pt": has_pt,
        "market": has_market,
        "drop": has_drop,
        "inv": has_inv,
        "probe": has_probe,
    }

    return {
        "info_raw": info_raw,
        "clean_text": clean,
        "canonical": canonical,
        "legacy_tags": legacy_tags,
        "drop_diff": drop_diff,
        "inv_from": inv_from,
        "inv_to": inv_to,
        "has_inversion": has_inv,
    }


def get_primary_signal(canonical: dict) -> str:
    if canonical.get("gold"):
        return "GOLD"
    over_level = int(canonical.get("over_level") or 0)
    if over_level == 3:
        return "OVER_L3"
    if over_level == 2:
        return "OVER_L2"
    if over_level == 1:
        return "OVER_L1"
    if canonical.get("pt"):
        return "PT"
    if canonical.get("market"):
        return "MARKET"
    if canonical.get("drop"):
        return "DROP"
    if canonical.get("inv"):
        return "INV"
    if canonical.get("probe"):
        return "PROBE"
    return "STD"


def normalize_match_row(row: dict, main_date: str) -> dict | None:
    fixture_id = get_fixture_id(row)
    match_date = get_match_date(row)
    league = get_league(row)
    match_name = get_match_name(row)

    if not fixture_id or not match_date or not match_name:
        return None
    if match_date != main_date:
        return None

    signals = extract_signals(row)
    canonical = signals["canonical"]

    if not any([
        canonical.get("gold"),
        canonical.get("over_level", 0) > 0,
        canonical.get("pt"),
        canonical.get("market"),
        canonical.get("drop"),
        canonical.get("inv"),
        canonical.get("probe"),
        bool(row.get("SIGNAL_TIER", row.get("signal_tier", ""))),
    ]):
        return None

    base = {
        "fixture_id": fixture_id,
        "date": match_date,
        "league": league,
        "match": match_name,
        "info_raw": signals["info_raw"],
        "canonical": canonical,
        "legacy_tags": signals["legacy_tags"],
        "primary_signal": get_primary_signal(canonical),
        "drop_diff": signals["drop_diff"],
        "has_inversion": signals["has_inversion"],
        "inv_from": signals["inv_from"],
        "inv_to": signals["inv_to"],
    }
    base.update(copy_new_layer_fields(row))
    return base


# =========================
# MAIN
# =========================
def main():
    ensure_dir(OUTPUT_DIR)

    payload = load_json_file(DAY1_FILE)
    rows = normalize_payload(payload)

    if not isinstance(rows, list) or not rows:
        raise ValueError("Formato inatteso: data_day1.json deve contenere una lista valida di partite")

    main_date = detect_main_match_date(rows)

    matches = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized = normalize_match_row(row, main_date)
        if normalized:
            matches.append(normalized)

    deduped = {}
    for item in matches:
        deduped[str(item["fixture_id"])] = item

    final_matches = sorted(
        deduped.values(),
        key=lambda x: (x.get("league", ""), x.get("match", ""))
    )

    primary_counts = Counter()
    canonical_counts = Counter()
    legacy_counts = Counter()
    refined_counts = Counter()

    for item in final_matches:
        primary_counts[item["primary_signal"]] += 1

        canonical = item.get("canonical", {})
        if canonical.get("gold"):
            canonical_counts["gold"] += 1
        over_level = int(canonical.get("over_level") or 0)
        if over_level >= 1:
            canonical_counts["over_l1"] += 1
        if over_level >= 2:
            canonical_counts["over_l2"] += 1
        if over_level >= 3:
            canonical_counts["over_l3"] += 1
        if canonical.get("pt"):
            canonical_counts["pt"] += 1
        if canonical.get("market"):
            canonical_counts["market"] += 1
        if canonical.get("drop"):
            canonical_counts["drop"] += 1
        if canonical.get("inv"):
            canonical_counts["inv"] += 1
        if canonical.get("probe"):
            canonical_counts["probe"] += 1

        for tag in item.get("legacy_tags", []):
            legacy_counts[tag] += 1

        if boolish(item.get("ELITE_SIGNAL", item.get("elite_signal", False))):
            refined_counts["elite_signal"] += 1
        if boolish(item.get("MARKET_VALIDATOR", item.get("market_validator", False))):
            refined_counts["market_validator"] += 1
        if boolish(item.get("DROP_WARNING", item.get("drop_warning", False))):
            refined_counts["drop_trap"] += 1
        tier = str(item.get("SIGNAL_TIER", item.get("signal_tier", "")))
        if tier:
            refined_counts[f"tier:{tier}"] += 1
        ft_tag = str(item.get("FT_TAG", item.get("ft_tag", "")))
        ht_tag = str(item.get("HT_TAG", item.get("ht_tag", "")))
        if ft_tag:
            refined_counts[f"ft_tag:{ft_tag}"] += 1
        if ht_tag:
            refined_counts[f"ht_tag:{ht_tag}"] += 1
        drop_class = str(item.get("DROP_CLASS", item.get("drop_class", "")))
        if drop_class:
            refined_counts[f"drop_class:{drop_class}"] += 1

    created_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    output_data = {
        "audit_date": main_date,
        "created_at": created_at,
        "source_file": "data_day1.json",
        "matches_count": len(final_matches),
        "primary_counts": dict(primary_counts),
        "canonical_counts": dict(canonical_counts),
        "legacy_counts": dict(legacy_counts),
        "refined_counts": dict(refined_counts),
        "matches": final_matches,
    }

    output_filename = f"freeze_for_audit_{main_date}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    save_json_file(output_path, output_data)

    print(f"[OK] Freeze creato: {output_path}")
    print(f"[OK] Data freeze: {main_date}")
    print(f"[OK] Match salvati: {len(final_matches)}")
    print(f"[OK] Primary counts: {dict(primary_counts)}")
    print(f"[OK] Canonical counts: {dict(canonical_counts)}")
    print(f"[OK] Legacy counts: {dict(legacy_counts)}")
    print(f"[OK] Refined counts: {dict(refined_counts)}")


if __name__ == "__main__":
    main()
