import os
import json
import re
from collections import Counter
from datetime import datetime, timezone, timedelta

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "auditordata")

DAY1_FILE = os.path.join(DATA_DIR, "data_day1.json")

ITALY_TZ = timezone(timedelta(hours=1))


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


def extract_tags_from_info(info_raw):
    if not info_raw:
        return []

    raw = str(info_raw)
    text = raw.upper()

    # pulizia emoji e simboli
    text = text.replace("⭐", " ")
    text = text.replace("🚀", " ")
    text = text.replace("⚽", " ")
    text = text.replace("🔥", " ")
    text = text.replace("🎯", " ")

    text = re.sub(r"\s+", " ", text).strip()

    tags = []

    # --- PTGG ---
    if "PTGG" in text:
        tags.append("PTGG")

    # --- PTO1.5 ---
    if (
        "PT1.5" in text or
        "PTO1.5" in text or
        "PTO15" in text
    ):
        tags.append("PTO15")

    # --- OVER ---
    if "OVER" in text:
        tags.append("OVER")

    # --- BOOST ---
    if "BOOST" in text:
        tags.append("BOOST")

    # --- GOLD ---
    if "GOLD" in text:
        tags.append("GOLD")

    # --- FISH GG ---
    if "🐟G" in raw or "🐟GG" in raw or "FISHG" in text:
        tags.append("FISH_GG")

    # --- FISH OVER ---
    if "🐟O" in raw or "FISHO" in text:
        tags.append("FISH_OVER")

    return tags


def get_fixture_id(row: dict):
    return row.get("Fixture_ID") or row.get("fixture_id")


def normalize_match_row(row: dict) -> dict | None:
    fixture_id = get_fixture_id(row)
    match_date = row.get("Data")
    league = row.get("Lega", "")
    match_name = row.get("Match", "")
    info_raw = row.get("Info", "")

    if not fixture_id or not match_date or not match_name:
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
    }


def detect_main_match_date(rows: list[dict]) -> str:
    dates = [r.get("Data") for r in rows if isinstance(r, dict) and r.get("Data")]
    if not dates:
        raise ValueError("Nessuna data trovata dentro data_day1.json")

    counts = Counter(dates)
    main_date, _ = counts.most_common(1)[0]
    return main_date


# =========================
# MAIN
# =========================
def main():
    ensure_dir(OUTPUT_DIR)

    data = load_json_file(DAY1_FILE)
    if not isinstance(data, list):
        raise ValueError("Formato inatteso: data_day1.json deve essere una lista")

    main_date = detect_main_match_date(data)

    matches = []
    for row in data:
        if not isinstance(row, dict):
            continue

        if row.get("Data") != main_date:
            continue

        normalized = normalize_match_row(row)
        if normalized:
            matches.append(normalized)

    deduped = {}
    for item in matches:
        deduped[item["fixture_id"]] = item

    final_matches = sorted(
        deduped.values(),
        key=lambda x: (x.get("league", ""), x.get("match", ""))
    )

    created_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    output_data = {
        "audit_date": main_date,
        "created_at": created_at,
        "source_file": "data_day1.json",
        "matches_count": len(final_matches),
        "matches": final_matches,
    }

    output_filename = f"freeze_for_audit_{main_date}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    save_json_file(output_path, output_data)

    print(f"[OK] Freeze creato: {output_path}")
    print(f"[OK] Data freeze: {main_date}")
    print(f"[OK] Match salvati: {len(final_matches)}")


if __name__ == "__main__":
    main()
