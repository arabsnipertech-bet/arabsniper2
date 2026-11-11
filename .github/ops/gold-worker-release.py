"""Publish the tested Gold bundle on the existing Worker, preserving its bindings."""

import email.parser
import email.policy
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

REPOSITORY = "arabsnipertech-bet/arabsniper2"
ORIGIN = "https://arabsniperbet.com"
ACCOUNT_API = "https://api.cloudflare.com/client/v4/accounts/dfdb02b4139b31c0ab4456a50eb371a2"
WORKER = "/workers/scripts/stripe-checker"
DATABASE_ID = "2f9dbeae-4b26-494d-9b88-a3a89a1c4f25"
DATABASE = "/d1/database/" + DATABASE_ID
CAMPAIGN_SHA = "6131f2e83def80f1cce4f2d959ddc7d7630ca01df33a862ec97137c2f19562dd"
ROOT = Path("workers/stripe-access")
BUNDLE = ROOT / "production-bundle.mjs"
SCHEMA = ROOT / "renewal-schema.sql"
GOLD_FLAGS = {"GOLD_RENEWAL_ENABLED", "GOLD_PROMO_ENABLED", "GOLD_RENEWAL_COUPON_ID"}
COUPON = "arabsniper_gold_renewal_10"


class ReleaseError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


CLIENT = urllib.request.build_opener(NoRedirect())


def require(condition, message):
    if not condition:
        raise ReleaseError(message)


def request(url, *, token=None, method="GET", data=None, content_type=None, origin=None, label):
    headers = {"User-Agent": "ArabSniperBet-Gold-release"}
    if token:
        headers["Authorization"] = "Bearer " + token
    if content_type:
        headers["Content-Type"] = content_type
    if origin:
        headers["Origin"] = origin
    req = urllib.request.Request(url, headers=headers, data=data, method=method)
    try:
        with CLIENT.open(req, timeout=25) as result:
            return result.status, result.headers, result.read()
    except urllib.error.HTTPError as exc:
        if token:
            raise ReleaseError(f"{label}: HTTP {exc.code}") from None
        return exc.code, exc.headers, exc.read()
    except (urllib.error.URLError, TimeoutError):
        raise ReleaseError(f"{label}: risposta di rete non verificabile") from None


def cf(path, *, token, method="GET", data=None, content_type=None, label):
    _, _, body = request(ACCOUNT_API + path, token=token, method=method, data=data,
                         content_type=content_type, label=label)
    try:
        response = json.loads(body)
    except (ValueError, UnicodeError):
        raise ReleaseError(f"{label}: risposta non JSON") from None
    if response.get("success") is not True:
        codes = [item["code"] for item in response.get("errors", [])
                 if isinstance(item.get("code"), int)]
        raise ReleaseError(f"{label}: errore API {codes}")
    return response["result"]


def post(path, token, payload, label):
    return cf(path, token=token, method="POST", data=json.dumps(payload).encode(),
              content_type="application/json", label=label)


def query(token, sql):
    result = post(DATABASE + "/query", token, {"sql": sql}, "D1 query")
    require(isinstance(result, list) and result and all(row.get("success") for row in result),
            "D1 non ha confermato tutte le istruzioni")
    return result


def main_matches():
    remote = f"https://github.com/{REPOSITORY}.git"
    head = subprocess.check_output(
        ["git", "ls-remote", "--exit-code", remote, "refs/heads/main"],
        text=True, timeout=25,
    ).split()[0]
    require(re.fullmatch(r"[0-9a-f]{40}", head), "HEAD di main non verificabile")
    for path in (BUNDLE, SCHEMA):
        url = f"https://raw.githubusercontent.com/{REPOSITORY}/{head}/{path.as_posix()}"
        code, _, data = request(url, label="Controllo sorgenti su main")
        require(code == 200 and data == path.read_bytes(),
                f"main ha modificato {path.name} durante il rilascio")


def active_deployment(token):
    deployments = cf(WORKER + "/deployments", token=token, label="Lettura deploy attivo")
    current = max(deployments["deployments"], key=lambda item: item["created_on"])
    versions = current["versions"]
    require(len(versions) == 1 and versions[0]["percentage"] == 100,
            "È in corso un deploy graduale di stripe-checker")
    return current["id"], versions[0]["version_id"]


def latest_version(token):
    versions = cf(WORKER + "/versions?per_page=10", token=token, label="Lettura versioni")
    return max(versions["items"], key=lambda item: item["number"])["id"]


def settings(token):
    return cf(WORKER + "/settings", token=token, label="Lettura impostazioni Worker")


def live_module(token):
    _, headers, body = request(ACCOUNT_API + WORKER + "/content/v2", token=token,
                               label="Lettura bundle in servizio")
    content_type = headers.get("Content-Type", "")
    if content_type.startswith("multipart/"):
        prefix = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()
        message = email.parser.BytesParser(policy=email.policy.default).parsebytes(prefix + body)
        modules = [part.get_payload(decode=True) for part in message.iter_parts()
                   if part.get_content_type() in ("application/javascript", "text/javascript",
                                                 "application/javascript+module")]
        require(len(modules) == 1, "Il Worker attivo non è un bundle con un solo modulo")
        return modules[0]
    return body


def telegram_block(module):
    start = b"const telegramTrialRoute = (() => {\n"
    end = b"\nreturn telegramTrialRoute;\n})();"
    require(module.count(start) == 1 and module.count(end) == 1,
            "Impossibile verificare il modulo Telegram nel bundle")
    return module.split(start, 1)[1].split(end, 1)[0]


def by_name(items):
    require(isinstance(items, list), "Elenco dei binding non verificabile")
    bindings = {item["name"]: item for item in items}
    require(len(bindings) == len(items), "Binding duplicati")
    return bindings


def verify_base_bindings(bindings):
    require(bindings.get("ACCESS_DB", {}).get("id") == DATABASE_ID, "ACCESS_DB non corrisponde al D1 verificato")
    require(bindings.get("GOLD_DATA", {}).get("bucket_name") == "arabsniper-gold",
            "GOLD_DATA non corrisponde al bucket privato previsto")
    for name, expected in {"AUTH_ENABLED": "true", "AUTH_ORIGIN": ORIGIN,
                           "CHECKOUT_ENABLED": "true", "STRIPE_MODE": "live"}.items():
        require(bindings.get(name, {}).get("text") == expected, f"Variabile preesistente {name} inattesa")
    for name in ("OTP_SECRET", "RESEND_API_KEY", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET"):
        require(bindings.get(name, {}).get("type") == "secret_text", f"Binding esistente {name} assente")


def schema_entries(sql):
    with sqlite3.connect(":memory:") as local:
        local.executescript(sql)
        return local.execute("SELECT name, type, sql FROM sqlite_master WHERE sql IS NOT NULL").fetchall()


def verify_schema(d1_token, *, apply):
    sql = SCHEMA.read_text()
    expected = schema_entries(sql)
    names = {name for name, _, _ in expected}
    require(names == {"gold_access_cache", "gold_renewals", "gold_renewal_pending",
                      "gold_renewal_promo_cycle", "gold_renewals_email"},
            "Migrazione Gold inattesa")
    for statement in sql.split(";"):
        cleaned = re.sub(r"(?m)^\s*--[^\n]*", "", statement).strip()
        require(not cleaned or re.match(r"CREATE (?:TABLE|(?:UNIQUE )?INDEX) IF NOT EXISTS ", cleaned),
                "La migrazione contiene un’istruzione diversa da una creazione additiva")
    listing = "SELECT name, type, sql FROM sqlite_master WHERE name IN (" + ",".join(
        "'" + name + "'" for name in sorted(names)) + ")"

    def normalized(entries):
        return {name: (kind, " ".join(definition.split())) for name, kind, definition in entries}

    want = normalized(expected)

    def observed():
        rows = query(d1_token, listing)[0]["results"]
        return normalized((row["name"], row["type"], row["sql"]) for row in rows)

    existing = observed()
    require(all(want.get(name) == value for name, value in existing.items()),
            "D1 contiene oggetti Gold con una definizione diversa")
    if apply and existing != want:
        query(d1_token, sql)
        existing = observed()
    require(existing == want, "Tabelle e indici Gold incompleti")
    print("D1: due tabelle e tre indici Gold verificati.", flush=True)


def flag_values(enabled):
    value = "true" if enabled else "false"
    return {"GOLD_RENEWAL_ENABLED": value, "GOLD_PROMO_ENABLED": value,
            "GOLD_RENEWAL_COUPON_ID": COUPON}


def verify_bindings(before, after, expected):
    require(set(after) == set(before) | set(expected), "Nomi dei binding modificati")
    for name, binding in before.items():
        if name not in expected:
            for key in ("type", "id", "bucket_name", "namespace_id", "text", "json"):
                if key in binding:
                    require(after[name].get(key) == binding[key], f"Binding {name} modificato")
    for name, value in expected.items():
        require(after[name].get("type") == "plain_text" and after[name].get("text") == value,
                f"Variabile Gold {name} non verificata")


def upload(token, current_settings, bindings, bundle, expected, phase):
    manifest = {
        "main_module": "worker.js",
        "compatibility_date": current_settings["compatibility_date"],
        "compatibility_flags": current_settings.get("compatibility_flags", []),
        "bindings": [{"name": name, "type": "inherit"} for name in sorted(bindings)
                     if name not in expected],
        "annotations": {"workers/tag": f"gold-169-{phase}",
                        "workers/message": f"Gold renewal PR 169 {phase}"},
    }
    for optional in ("limits", "placement", "observability", "usage_model"):
        if current_settings.get(optional) is not None:
            manifest[optional] = current_settings[optional]
    manifest["bindings"].extend({"name": name, "type": "plain_text", "text": value}
                                for name, value in expected.items())
    boundary = "gold-release-" + uuid.uuid4().hex

    def part(name, mime, content, filename=""):
        disposition = f'Content-Disposition: form-data; name="{name}"' + filename
        return f"--{boundary}\r\n{disposition}\r\nContent-Type: {mime}\r\n\r\n".encode() + content + b"\r\n"

    body = part("metadata", "application/json", json.dumps(manifest).encode())
    body += part("worker.js", "application/javascript+module", bundle, '; filename="worker.js"')
    body += f"--{boundary}--\r\n".encode()
    result = cf(WORKER + "/versions?bindings_inherit=strict", token=token, method="POST", data=body,
                content_type=f"multipart/form-data; boundary={boundary}", label="Upload versione Worker")
    version_id = str(uuid.UUID(result["id"]))
    print(f"Versione caricata ({phase}): {version_id}", flush=True)
    version = cf(WORKER + "/versions/" + version_id, token=token, label="Verifica versione caricata")
    verify_bindings(bindings, by_name(version["resources"]["bindings"]), expected)
    return version_id


def deploy(token, version_id, label):
    return post(WORKER + "/deployments", token, {
        "strategy": "percentage",
        "versions": [{"version_id": version_id, "percentage": 100}],
        "annotations": {"workers/message": label},
    }, label)


def smoke_test():
    for _ in range(7):
        session_code, _, session = request(ORIGIN + "/api/auth/session", label="Verifica sessione")
        renew_code, _, renewal = request(ORIGIN + "/api/auth/renew", method="POST", data=b"{}",
                                         content_type="application/json", origin=ORIGIN,
                                         label="Verifica rinnovi")
        if session_code == 401 and renew_code == 401 and (
            b'"active":false' in session and b"Accedi con il codice email per rinnovare" in renewal
        ):
            print("HTTP: sessione e rinnovi protetti; nuova route verificata.", flush=True)
            return
        time.sleep(3)
    raise ReleaseError("La route di rinnovo protetta non risponde come previsto")


def release():
    phase = Path(".github/ops/gold-release-phase").read_text().strip()
    require(phase in ("stage", "activate"), "Fase di rilascio sconosciuta")
    worker_token = os.environ.get("CF_WORKER_TOKEN")
    d1_token = os.environ.get("CF_D1_TOKEN")
    require(worker_token and d1_token, "Secret Cloudflare mancanti")
    main_matches()
    previous_deploy = active_deployment(worker_token)
    previous_version = previous_deploy[1]
    require(latest_version(worker_token) == previous_version,
            "È presente una versione Worker preparata da un altro lavoro")
    current = settings(worker_token)
    bindings = by_name(current["bindings"])
    verify_base_bindings(bindings)
    db = cf(DATABASE, token=d1_token, label="Identificazione D1")
    require(db["name"] == "arabsniper-access" and db["uuid"] == DATABASE_ID, "Database D1 inatteso")
    bundle = BUNDLE.read_bytes()
    live = live_module(worker_token)
    same = live.rstrip() == bundle.rstrip()
    require(telegram_block(live) == telegram_block(bundle), "Modulo Telegram in servizio diverso dal bundle")
    if phase == "stage":
        require(same or hashlib.sha256(live).hexdigest() == CAMPAIGN_SHA,
                "Il codice Worker in servizio è cambiato")
        require(all(bindings.get(name, {}).get("text") != "true"
                    for name in ("GOLD_RENEWAL_ENABLED", "GOLD_PROMO_ENABLED")),
                "I rinnovi Gold sono già attivi")
    else:
        require(same, "L’ultima versione in servizio non è il bundle Gold verificato")
        verify_bindings(bindings, bindings, flag_values(False))
        verify_schema(d1_token, apply=False)
    print(f"Configurazione e modulo Telegram verificati; versione attiva {previous_version}.", flush=True)
    expected = flag_values(phase == "activate")
    version_id = upload(worker_token, current, bindings, bundle, expected, phase)
    if phase == "stage":
        verify_schema(d1_token, apply=True)
    main_matches()
    require(active_deployment(worker_token) == previous_deploy and latest_version(worker_token) == version_id,
            "Rilevato un deploy concorrente; il traffico resta sulla versione in servizio")
    try:
        deploy(worker_token, version_id, f"Gold PR 169 {phase}")
        require(active_deployment(worker_token)[1] == version_id, "Deploy Worker non confermato")
        verify_bindings(bindings, by_name(settings(worker_token)["bindings"]), expected)
        smoke_test()
    except Exception:
        if active_deployment(worker_token)[1] == version_id:
            deploy(worker_token, previous_version, "Ripristino automatico Gold PR 169")
            require(active_deployment(worker_token)[1] == previous_version, "Ripristino Worker non confermato")
            print("Versione precedente ripristinata.", flush=True)
        raise
    print(f"Rilascio {phase} riuscito: {version_id}", flush=True)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"Stripe-checker: fase **{phase}** verificata.\n\n")
        summary.write(f"- Versione precedente: `{previous_version}`\n- Versione attiva: `{version_id}`\n")
        summary.write("- D1, binding, modulo Telegram e protezione delle route verificati.\n")


if __name__ == "__main__":
    try:
        release()
    except ReleaseError as error:
        raise SystemExit(str(error)) from None
    except Exception as error:
        # Provider payloads and exception contexts can contain configuration details.
        raise SystemExit("Rilascio interrotto: " + type(error).__name__) from None
