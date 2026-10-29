#!/usr/bin/env python3
"""Runner separato per ArabSniper Quote Engine V1.1.4.

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
import signal
import subprocess
import sys
import threading
import time
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
    SRC_DIR / "3appdays(5).py",
    SRC_DIR / "3appdays(4).py",
    SRC_DIR / "3appdays(3).py",  # fallback solo per copie scaricate dal browser
)
RUN_STATE_FILE = DATA_DIR / "arab_quote_run_state.json"
OUTPUT_TAIL_CHARS = 4000


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
        command.append("--fast")
    elif mode == "night":
        command.extend(["--night", "--days", "5"])
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


def stream_process(command: list[str], *, heartbeat_seconds: float = 60,
                   termination_grace_seconds: float = 10) -> subprocess.CompletedProcess:
    """Stream both pipes immediately, retaining only bounded diagnostic tails.

    A quiet engine still gets a liveness message, not a claim of progress.
    Cancellation remains a failure even if the child handles SIGTERM cleanly.
    """
    started = time.monotonic()
    last_output = started
    tails = {"stdout": "", "stderr": ""}
    lock = threading.Lock()
    stopped = {"signal": None, "at": None}
    old_handlers = {}
    process = subprocess.Popen(
        command, cwd=str(PROJECT_ROOT), text=True, encoding="utf-8",
        errors="replace", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        start_new_session=(os.name == "posix"),
    )

    def send_signal(signum):
        if process.poll() is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signum)
            else:
                process.send_signal(signum)
        except ProcessLookupError:
            pass

    def stop(signum, _frame):
        if stopped["signal"] is None:
            stopped.update(signal=signum, at=time.monotonic())
        send_signal(signum)

    def drain(pipe, target, name):
        nonlocal last_output
        try:
            # The size bound also handles very long lines without growing memory.
            for chunk in iter(lambda: pipe.readline(4096), ""):
                with lock:
                    tails[name] = (tails[name] + chunk)[-OUTPUT_TAIL_CHARS:]
                    last_output = time.monotonic()
                target.write(chunk)
                target.flush()
        finally:
            pipe.close()

    readers = [threading.Thread(target=drain, args=(pipe, target, name), daemon=True)
               for pipe, target, name in ((process.stdout, sys.stdout, "stdout"),
                                          (process.stderr, sys.stderr, "stderr"))]
    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                old_handlers[signum] = signal.signal(signum, stop)
        for reader in readers:
            reader.start()
        next_heartbeat = started + heartbeat_seconds
        while True:
            try:
                code = process.wait(timeout=min(1, heartbeat_seconds))
                break
            except subprocess.TimeoutExpired:
                now = time.monotonic()
                if stopped["at"] is not None:
                    if now - stopped["at"] >= termination_grace_seconds:
                        if os.name == "posix":
                            send_signal(signal.SIGKILL)
                        else:
                            process.kill()
                elif now >= next_heartbeat:
                    with lock:
                        quiet_for = now - last_output
                    log(f"⏳ Motore in esecuzione da {int(now-started)}s; "
                        f"ultimo output {int(quiet_for)}s fa (non indica completamento)")
                    next_heartbeat = now + heartbeat_seconds
    finally:
        if process.poll() is None:
            if os.name == "posix":
                send_signal(signal.SIGKILL)
            else:
                process.kill()
            process.wait()
        for reader in readers:
            if reader.ident is not None:
                reader.join()
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)

    if stopped["signal"] is not None:
        code = 128 + stopped["signal"]
    elif code < 0:
        code = 128 - code
    return subprocess.CompletedProcess(command, code, tails["stdout"], tails["stderr"])


def run(mode: str, github: bool = False, days: int = 5, audit_date: str = "", force_night: bool = False) -> int:
    if mode == "night":
        from night_scan_guard import completed_today
        force_night = force_night or os.getenv("ARAB_FORCE_NIGHT") == "true"
        if not force_night and completed_today(PROJECT_ROOT):
            log("Night Scan già completato oggi (Europe/Rome): nessuna nuova chiamata API")
            return 0
    engine_file = resolve_engine_file()
    if not engine_file.exists():
        log(f"❌ Motore non trovato. Cercati: {', '.join(str(path) for path in ENGINE_CANDIDATES)}")
        return 2

    if engine_file.name != "3appdays.py":
        log(f"⚠️ Uso copia fallback {engine_file.name}; online è preferibile rinominarla 3appdays.py")
    command = build_command(mode, github=github, days=days, audit_date=audit_date)
    log(f"🚀 Avvio: {' '.join(command)}")
    started_at = now_iso()
    report = {
        "mode": mode,
        "github": github,
        "days": 2 if mode == "fast" else 5 if mode == "night" else days,
        "audit_date": audit_date,
        "command": command,
        "returncode": None,
        "status": "running",
        "generated_at": started_at,
        "started_at": started_at,
    }
    atomic_write_json(RUN_STATE_FILE, report)
    result = stream_process(command)
    if mode == "night" and result.returncode == 0:
        from night_scan_guard import record_completion
        try:
            record_completion(PROJECT_ROOT, started_at, now_iso())
        except (ValueError, OSError) as error:
            log(f"❌ Verifica Night Scan fallita: {error}")
            result = subprocess.CompletedProcess(command, 3, result.stdout, str(error))
    report.update(
        returncode=result.returncode,
        status="ok" if result.returncode == 0 else "error",
        generated_at=now_iso(),
        stdout_tail=result.stdout,
        stderr_tail=result.stderr,
    )
    atomic_write_json(RUN_STATE_FILE, report)

    if result.returncode == 0:
        log("✅ Workflow completato")
    else:
        log(f"❌ Workflow fallito, codice {result.returncode}")
    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runner ArabSniper Quote Engine")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--fast", action="store_true", help="Scansiona oggi e domani")
    modes.add_argument("--night", action="store_true", help="Scansiona rolling più giorni")
    modes.add_argument("--catalog", action="store_true", help="Aggiorna catalogo mercati API")
    modes.add_argument("--audit-markets", action="store_true", help="Esegue l'audit dei mercati quota")
    parser.add_argument("--audit-date", type=str, default="", help="Data audit YYYY-MM-DD, default oggi")
    parser.add_argument("--days", type=int, default=5, help="Giorni del night, massimo 7")
    parser.add_argument("--github", action="store_true", help="Pubblica i JSON dedicati")
    parser.add_argument("--force-night", action="store_true", help="Ripeti esplicitamente il Night Scan già completato")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mode = "fast" if args.fast else "night" if args.night else "catalog" if args.catalog else "audit"
    raise SystemExit(run(
        mode, github=args.github, days=max(1, min(args.days, 7)), audit_date=args.audit_date,
        force_night=args.force_night,
    ))


if __name__ == "__main__":
    main()


