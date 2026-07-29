#!/usr/bin/env python3
"""Runner separato per ArabSniper Quote Engine V1.1.3.

Non ruota né sovrascrive i file legacy data_day1...day5.
Usa file dedicati arab_quote_*.json.

Esempi:
    python arab_quote_runner.py --fast
    python arab_quote_runner.py --night
    python arab_quote_runner.py --catalog
    python arab_quote_runner.py --night --github
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    ROME_TZ = ZoneInfo("Europe/Rome")
except Exception:  # pragma: no cover
    ROME_TZ = None

SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent if SRC_DIR.name.lower() == "src" else SRC_DIR
DATA_DIR = PROJECT_ROOT / "data"
ENGINE_CANDIDATES = (
    SRC_DIR / "3appdays.py",
    SRC_DIR / "3appdays(3).py",  # fallback solo per copie scaricate dal browser
)
RUN_STATE_FILE = DATA_DIR / "arab_quote_run_state.json"


def resolve_engine_file() -> Path:
    for candidate in ENGINE_CANDIDATES:
        if candidate.exists():
            return candidate
    return ENGINE_CANDIDATES[0]


def now_iso() -> str:
    now = datetime.now(ROME_TZ) if ROME_TZ else datetime.now()
    return now.isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{now_iso()}] {message}", flush=True)


def atomic_write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def build_command(mode: str, github: bool = False, days: int = 5, audit_date: str = "") -> list[str]:
    python_exe = sys.executable or "python"
    command = [python_exe, str(resolve_engine_file())]
    if mode == "fast":
        command.append("--today")
    elif mode == "night":
        command.extend(["--night", "--days", str(days)])
    elif mode == "catalog":
        command.append("--catalog")
    elif mode == "audit":
        command.append("--audit-markets")
        if audit_date:
            command.extend(["--audit-date", audit_date])
    else:
        raise ValueError(f"Modalità non valida: {mode}")
    if github:
        command.append("--github")
    return command


def run(mode: str, github: bool = False, days: int = 5, audit_date: str = "") -> int:
    engine_file = resolve_engine_file()
    if not engine_file.exists():
        log(f"❌ Motore non trovato. Cercati: {', '.join(str(path) for path in ENGINE_CANDIDATES)}")
        return 2

    if engine_file.name != "3appdays.py":
        log(f"⚠️ Uso copia fallback {engine_file.name}; online è preferibile rinominarla 3appdays.py")
    command = build_command(mode, github=github, days=days, audit_date=audit_date)
    log(f"🚀 Avvio: {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")

    report = {
        "mode": mode,
        "github": github,
        "days": days,
        "audit_date": audit_date,
        "command": command,
        "returncode": result.returncode,
        "status": "ok" if result.returncode == 0 else "error",
        "generated_at": now_iso(),
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
    }
    atomic_write_json(RUN_STATE_FILE, report)

    if result.returncode == 0:
        log("✅ Workflow completato")
    else:
        log(f"❌ Workflow fallito, codice {result.returncode}")
    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runner ArabSniper Quote Engine")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--fast", action="store_true", help="Scansiona solo oggi")
    modes.add_argument("--night", action="store_true", help="Scansiona rolling più giorni")
    modes.add_argument("--catalog", action="store_true", help="Aggiorna catalogo mercati API")
    modes.add_argument("--audit-markets", action="store_true", help="Esegue l'audit dei mercati quota")
    parser.add_argument("--audit-date", type=str, default="", help="Data audit YYYY-MM-DD, default oggi")
    parser.add_argument("--days", type=int, default=5, help="Giorni del night, massimo 7")
    parser.add_argument("--github", action="store_true", help="Pubblica i JSON dedicati")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mode = "fast" if args.fast else "night" if args.night else "catalog" if args.catalog else "audit"
    raise SystemExit(run(
        mode, github=args.github, days=max(1, min(args.days, 7)), audit_date=args.audit_date,
    ))


if __name__ == "__main__":
    main()
