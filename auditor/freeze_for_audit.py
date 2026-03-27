import os
import json
import re
import argparse
from datetime import datetime, timedelta, timezone

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "auditordata")

DAY_FILES = [
    "data_day1.json",
    "data_day2.json",
    "data_day3.json",
    "data_day4.json",
    "data_day5.json",
]

# Fuso orario Italia semplificato per naming/created_at
# Per questo uso basta un offset fisso. Se poi vuoi lo rendiamo più raffinato.
ITALY_TZ = timezone(timedelta(hours=1))


# =========================
# HELPERS
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_json_file(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Errore lettura file {path}: {e}")
        return None


def save_json_file(path: str, data) -> None:
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


def extract_tags_from_info(info_raw: str) -> list[str]:
    """
    Estrae e normalizza solo i tag utili da Info.
    Ignora drop, frecce, numeri e testo extra.

    Mappature attuali:
    - PT -> PT
    - PTO1.5 -> PTO15
    - OVER -> OVER
    - BOOST -> BOOST
    - GOLD -> GOLD
    - 🐟GG -> FISH_GG
    - 🐟O -> FISH_OVER
    """

    if not info_raw or not isinstance(info_raw, str):
        return []

    info = info_raw.strip()

    tag_patterns = [
        (r"\bPTO1\.5\b", "PTO15"),
        (r"\bPT\b", "PT"),
        (r"\bOVER\b", "OVER"),
        (r"\bBOOST\b", "BOOST"),
        (r"\bGOLD\b", "GOLD"),
        (r"🐟GG", "FISH_GG"),
        (r"🐟O", "FISH_OVER"),
    ]

    found_tags = []
    for pattern, normalized in tag_patterns:
        if re.search(pattern, info, flags=re.IGNORECASE):
            found_tags.append(normalized)

    # rimuove duplicati mantenendo ordine
    unique_tags = list(dict.fromkeys(found_tags))
    return unique_tags


def get_fixture_id(row: dict):
    return row.get("Fixture_ID") or row.get("fixture_id")


def normalize_match_row(row: dict) -> dict | None:
    """
    Normalizza il record per il freeze.
    Ritorna None se mancano dati minimi.
    """
    fixture_id = get_fixture_id(row)
    match_date = row.get("Data")
    league = row.get("Lega", "")
    match_name = row.get("Match", "")
    info_raw = row.get("Info", "")

    if not fixture_id or not match_date or not match_name:
        return None

    tags = extract_tags_from_info(info_raw)

    # Salviamo solo match con almeno un tag utile
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


def collect_matches_for_date(target_date: str):
    """
    Scorre tutti i data_dayN.json e raccoglie i match della data target.
    Ritorna:
    - matches normalizzati
    - file sorgente coinvolti
    """
    collected = []
    source_files = []

    for filename in DAY_FILES:
        path = os.path.join(DATA_DIR, filename)
        data = load_json_file(path)

        if data is None:
            print(f"[INFO] File non trovato o non leggibile: {filename}")
            continue

        if not isinstance(data, list):
            print(f"[WARN] Formato inatteso in {filename}: attesa lista")
            continue

        file_matches = []
        for row in data:
            if not isinstance(row, dict):
                continue

            if row.get("Data") != target_date:
                continue

            normalized = normalize_match_row(row)
            if normalized:
                file_matches.append(normalized)

        if file_matches:
            collected.extend(file_matches)
            source_files.append(filename)

    # Deduplica per fixture_id, nel caso una partita compaia in più file
    deduped = {}
    for item in collected:
        deduped[item["fixture_id"]] = item

    return list(deduped.values()), source_files


# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(description="Freeze match tagged for auditor")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data target in formato YYYY-MM-DD. Se assente usa ieri.",
    )
    args = parser.parse_args()

    ensure_dir(OUTPUT_DIR)

    target_date = get_target_date(args.date)
    matches, source_files = collect_matches_for_date(target_date)

    created_at = datetime.now(ITALY_TZ).strftime("%Y-%m-%dT%H:%M:%S")

    output_data = {
        "audit_date": target_date,
        "created_at": created_at,
        "source_files": source_files,
        "matches_count": len(matches),
        "matches": sorted(matches, key=lambda x: (x.get("league", ""), x.get("match", ""))),
    }

    output_filename = f"freeze_for_audit_{target_date}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    save_json_file(output_path, output_data)

    print(f"[OK] Freeze creato: {output_path}")
    print(f"[OK] Data target: {target_date}")
    print(f"[OK] File sorgente usati: {source_files if source_files else 'nessuno'}")
    print(f"[OK] Match salvati: {len(matches)}")


if __name__ == "__main__":
    main()
