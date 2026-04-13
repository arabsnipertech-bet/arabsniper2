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


# =========================
# SIGNAL EXTRACTION
# =========================
def extract_signals(row: dict) -> dict:
    info_raw = get_info_raw(row)
    clean = normalize_upper_text(info_raw)

    drop_diff = parse_num(get_field_any(row, ["DROP_DIFF", "drop_diff"], None))
    has_inversion_field = bool(get_field_any(row, ["INVERSION", "HAS_INVERSION", "has_inversion"], False))
    inv_from = str(get_field_any(row, ["INV_FROM", "inv_from"], "") or "").strip()
    inv_to = str(get_field_any(row, ["INV_TO", "inv_to"], "") or "").strip()

    has_gold = bool(re.search(r"(^|\s)GOLD(?=\s|$)", clean))
    has_boost = bool(re.search(r"(^|\s)BOOST(?=\s|$)", clean))
    has_strong_over = bool(re.search(r"STRONG\s+OVER", clean))
    has_over = bool(re.search(r"(^|\s)OVER(?=\s|$)", clean)) or has_boost or has_strong_over

    has_ptgg = "PTGG" in clean or bool(re.search(r"(GG\s*PT|PT\s*GG)", clean))
    has_pt15 = (
        "PT1.5" in clean
        or "PTO1.5" in clean
        or "PTO15" in clean
        or bool(re.search(r"(PT\s*1\.5|O15\s*PT|OVER1\.5\s*PT)", clean))
    )
    has_pt_generic = bool(re.search(r"(^|\s)PT(?=\s|$)", clean))
    has_pt = has_ptgg or has_pt15 or has_pt_generic

    has_market = bool(re.search(r"(^|\s)MARKET(?=\s|$)", clean))
    has_drop = bool(re.search(r"(^|\s)DROP(?=\s|$)", clean)) or (drop_diff is not None and drop_diff >= 0.05)
    has_inv = has_inversion_field or bool(re.search(r"(^|\s)INV(?=\s|$)", clean)) or bool(inv_from and inv_to)

    has_probe = (
        bool(re.search(r"(^|\s)PROBE(?=\s|$)", clean))
        or bool(re.search(r"(^|\s)(PESCE|PESCIOLINO|FISH)(?=\s|$)", clean))
        or "🐟" in info_raw
    )

    row_over_level = int(parse_num(get_field_any(row, ["OVER_LEVEL", "over_level"], 0)) or 0)

    over_level = row_over_level
    if over_level <= 0:
        if has_boost:
            over_level = 3
        elif has_strong_over:
            over_level = 2
        elif has_over:
            over_level = 1

    legacy_tags = []
    if has_gold:
        legacy_tags.append("GOLD")
    if has_boost:
        legacy_tags.append("BOOST")
    if has_strong_over:
        legacy_tags.append("STRONG_OVER")
    if has_over:
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
    ]):
        return None

    return {
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

    created_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    output_data = {
        "audit_date": main_date,
        "created_at": created_at,
        "source_file": "data_day1.json",
        "matches_count": len(final_matches),
        "primary_counts": dict(primary_counts),
        "canonical_counts": dict(canonical_counts),
        "legacy_counts": dict(legacy_counts),
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


if __name__ == "__main__":
    main()
