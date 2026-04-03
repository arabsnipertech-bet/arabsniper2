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


def normalize_text(value) -> str:
    return str(value or "").upper().strip()


def extract_tags_from_info(info_raw):
    """
    Parser V25 coerente con la UI:
    - GOLD
    - BOOST (nascosto se c'è GOLD)
    - OVER
    - PTGG
    - PTO15
    - PT fallback
    - FISH_GG
    - FISH_OVER
    """
    if not info_raw:
        return []

    raw = str(info_raw)
    upper_raw = raw.upper()

    normalized = (
        upper_raw
        .replace("⭐", " ")
        .replace("🚀", " ")
        .replace("⚽", " ")
        .replace("🔥", " ")
        .replace("🎯", " ")
        .replace("📉", " ")
        .replace("📈", " ")
        .replace("↗", " ")
        .replace("↘", " ")
        .replace("↑", " ")
        .replace("↓", " ")
    )

    normalized = re.sub(r"[_|,/;]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    clean = re.sub(r"\bDROP\b", " ", normalized)
    clean = re.sub(r"\s+", " ", clean).strip()

    has_gold = bool(re.search(r"(^|\s)GOLD(?=\s|$)", clean))
    has_boost = bool(re.search(r"(^|\s)BOOST(?=\s|$)", clean))
    has_strong_over = bool(re.search(r"STRONG\s+OVER", clean))
    has_over = bool(re.search(r"(^|\s)OVER(?=\s|$)", clean))

    has_ptgg = "PTGG" in clean or bool(re.search(r"(GG\s*PT|PT\s*GG)", clean))
    has_pt15 = (
        "PT1.5" in clean
        or "PTO1.5" in clean
        or "PTO15" in clean
        or bool(re.search(r"(PT\s*1\.5|O15\s*PT|OVER1\.5\s*PT)", clean))
    )
    has_pt_generic = bool(re.search(r"(^|\s)PT(?=\s|$)", clean))

    has_fish_word = bool(re.search(r"(^|\s)(PESCE|PESCIOLINO|FISH)(?=\s|$)", clean))
    has_standalone_gg = bool(re.search(r"(^|\s)(GG|BTTS)(?=\s|$)", clean))
    has_standalone_o = bool(re.search(r"(^|\s)O(?=\s|$)", clean))
    has_fish_gg = bool(re.search(r"(PESCE|PESCIOLINO|FISH)\s*(GG|BTTS)|(GG|BTTS)\s*(PESCE|PESCIOLINO|FISH)", clean))
    has_fish_o = bool(re.search(r"(PESCE|PESCIOLINO|FISH)\s*(O|OVER)|(O|OVER)\s*(PESCE|PESCIOLINO|FISH)", clean))

    show_fish_gg = has_fish_gg or (
        not has_gold and not has_boost and not has_over and not (has_ptgg or has_pt15 or has_pt_generic) and has_standalone_gg
    )
    show_fish_o = has_fish_o or (
        not has_gold and not has_boost and not has_over and not (has_ptgg or has_pt15 or has_pt_generic) and has_standalone_o
    )

    tags = []

    if has_gold:
        tags.append("GOLD")
    if has_boost and not has_gold:
        tags.append("BOOST")
    if has_over:
        tags.append("OVER")
    if has_strong_over:
        tags.append("STRONG_OVER")
    if has_ptgg:
        tags.append("PTGG")
    if has_pt15:
        tags.append("PTO15")
    if (has_ptgg or has_pt15) is False and has_pt_generic:
        tags.append("PT")
    if show_fish_gg:
        tags.append("FISH_GG")
    if show_fish_o:
        tags.append("FISH_OVER")

    dedup = []
    seen = set()
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            dedup.append(tag)

    return dedup


def get_fixture_id(row: dict):
    return get_field_any(row, ["Fixture_ID", "fixture_id", "fixtureId", "id"], None)


def get_match_date(row: dict):
    return get_field_any(row, ["Data", "data", "date", "Date", "match_date", "fixture_date"], "")


def get_league(row: dict):
    return get_field_any(row, ["Lega", "lega", "league", "League", "competition", "campionato"], "")


def get_match_name(row: dict):
    return get_field_any(row, ["Match", "match", "fixture", "Fixture", "match_name", "teams"], "")


def get_info_raw(row: dict):
    info = get_field_any(row, ["Info", "info", "INFO", "signals", "Signals", "SIGNALS", "tags_display", "tag_display", "Signali"], "")
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


def get_primary_signal(tags: list[str]) -> str:
    if "GOLD" in tags:
        return "GOLD"
    if "BOOST" in tags:
        return "BOOST"
    if "STRONG_OVER" in tags:
        return "STRONG_OVER"
    if "OVER" in tags:
        return "OVER"
    if "PTGG" in tags or "PTO15" in tags or "PT" in tags:
        return "PT"
    if "FISH_GG" in tags or "FISH_OVER" in tags:
        return "FISH"
    return "STD"


def normalize_match_row(row: dict, main_date: str) -> dict | None:
    fixture_id = get_fixture_id(row)
    match_date = get_match_date(row)
    league = get_league(row)
    match_name = get_match_name(row)
    info_raw = get_info_raw(row)

    if not fixture_id or not match_date or not match_name:
        return None

    if match_date != main_date:
        return None

    tags = extract_tags_from_info(info_raw)
    if not tags:
        return None

    return {
        "fixture_id": fixture_id,
        "date": match_date,
        "league": league,
        "match": match_name,
        "info_raw": info_raw,
        "tags": tags,
        "primary_signal": get_primary_signal(tags),
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

    tag_counts = Counter()
    primary_counts = Counter()

    for item in final_matches:
        primary_counts[item["primary_signal"]] += 1
        for tag in item["tags"]:
            tag_counts[tag] += 1

    created_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    output_data = {
        "audit_date": main_date,
        "created_at": created_at,
        "source_file": "data_day1.json",
        "matches_count": len(final_matches),
        "primary_counts": dict(primary_counts),
        "tag_counts": dict(tag_counts),
        "matches": final_matches,
    }

    output_filename = f"freeze_for_audit_{main_date}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    save_json_file(output_path, output_data)

    print(f"[OK] Freeze creato: {output_path}")
    print(f"[OK] Data freeze: {main_date}")
    print(f"[OK] Match salvati: {len(final_matches)}")
    print(f"[OK] Primary counts: {dict(primary_counts)}")
    print(f"[OK] Tag counts: {dict(tag_counts)}")


if __name__ == "__main__":
    main()
