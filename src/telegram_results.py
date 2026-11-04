"""Audit only Telegram predictions confirmed as sent, against API-Football results."""
import argparse
import json
import math
import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
from telegram_slots import WINDOWS
SLOTS = sorted(set(WINDOWS) | {"13:00", "19:00"})
FINISHED = {"FT", "AET", "PEN"}
HALF_FINISHED = FINISHED | {"HT", "2H", "ET", "BT", "P"}
MARKETS = {"over15_ht": ("ht", 2), "over25_ht": ("ht", 3),
           "gg_ht": ("ht", "gg"), "over25_ft": ("ft", 3),
           "over35_ft": ("ft", 4)}


def outcome(market, result):
    """Never mark a prediction lost before its own period is completed."""
    if market not in MARKETS:
        return "pending"
    status = result.get("status")
    period, target = MARKETS[market]
    if status not in (HALF_FINISHED if period == "ht" else FINISHED):
        return "pending"
    score = result.get("halftime" if period == "ht" else "fulltime") or {}
    home, away = score.get("home"), score.get("away")
    if any(type(n) is not int or n < 0 for n in (home, away)):
        return "pending"
    hit = home > 0 and away > 0 if target == "gg" else home + away >= target
    return "won" if hit else "lost"


def audit(day, posts, scores):
    """One posted selection = one frozen outcome, with a separate triple ticket."""
    rows, groups, seen = [], {}, set()
    for slot in SLOTS:
        post = posts.get(slot)
        if not post:
            continue
        selected = post.get("picks") or []
        for p in selected:
            fid = str(p.get("id", ""))
            market = p.get("market")
            key = (fid, market)
            if not fid or market not in MARKETS or key in seen:
                continue
            seen.add(key)
            odd = p.get("odd")
            if not isinstance(odd, (int, float)) or isinstance(odd, bool) or not math.isfinite(odd) or odd <= 1:
                odd = None
            status = outcome(market, scores.get(fid, {}))
            pnl = round(float(odd) - 1, 3) if status == "won" and odd else (-1.0 if status == "lost" else None)
            rows.append(dict(slot=slot, fixture_id=fid, match=p.get("match"),
                             market=market, label=p.get("label"), kickoff=p.get("kickoff"),
                             captured_at=p.get("captured_at"), odd=odd,
                             status=status, pnl_one_unit=pnl,
                             halftime=(scores.get(fid, {}).get("halftime") or {}),
                             fulltime=(scores.get(fid, {}).get("fulltime") or {})))
            groups.setdefault(slot, []).append(rows[-1])
    singles = [r for r in rows if r["slot"] != "13:00"]
    settled = [r for r in rows if r["status"] != "pending"]
    def aggregate(items):
        valid = [r for r in items if r["status"] != "pending"]
        return dict(total=len(items), won=sum(r["status"] == "won" for r in valid),
                    lost=sum(r["status"] == "lost" for r in valid),
                    pending=len(items)-len(valid),
                    accuracy_pct=round(100*sum(r["status"] == "won" for r in valid)/len(valid), 1) if valid else None,
                    theoretical_pnl=round(sum(r["pnl_one_unit"] for r in valid if r["pnl_one_unit"] is not None), 3))
    triple = groups.get("13:00", [])
    if len(triple) != 3:
        ticket = dict(status="not_published", odd=None, pnl_one_unit=None)
    else:
        odd = math.prod(r["odd"] for r in triple) if all(r["odd"] for r in triple) else None
        status = ("lost" if any(r["status"] == "lost" for r in triple)
                  else "won" if all(r["status"] == "won" for r in triple)
                  else "pending")
        ticket = dict(status=status, odd=round(odd, 3) if odd else None,
                      pnl_one_unit=(round(odd-1, 3) if odd else None) if status == "won" else
                      -1.0 if status == "lost" else None)
    return dict(schema_version=1, date=day, source="confirmed_telegram_sent_artifacts",
                posts_found=len(posts), selections=rows, singles=aggregate(singles),
                selections_total=aggregate(rows), triple=ticket)


def get_sent_posts(day):
    repo = os.environ["GITHUB_REPOSITORY"]
    posts = {}
    for slot in SLOTS:
        name = f"tg-{day}-{slot.replace(':','')}-sent"
        raw = subprocess.check_output(
            ["gh", "api", f"repos/{repo}/actions/artifacts?name={name}&per_page=100"],
            text=True)
        items = json.loads(raw).get("artifacts", [])
        item = next((x for x in items if x.get("name") == name and not x.get("expired")), None)
        if item is None:
            continue
        target = Path("receipts") / slot.replace(":", "")
        target.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(["gh", "run", "download", str(item["workflow_run"]["id"]),
                               "-n", name, "-D", str(target)])
        posts[slot] = json.loads((target / "post.json").read_text(encoding="utf-8"))
    return posts


def fetch_official(day, key):
    if not key:
        raise RuntimeError("API_SPORTS_KEY missing: cannot certify outcomes")
    query = urlencode({"date": day, "timezone": "Europe/Rome"})
    request = Request("https://v3.football.api-sports.io/fixtures?" + query,
                      headers={"x-apisports-key": key})
    with urlopen(request, timeout=50) as response:
        payload = json.load(response)
    if payload.get("errors") or not isinstance(payload.get("response"), list):
        raise RuntimeError("Official API returned invalid results")
    result = {}
    for match in payload["response"]:
        fixture = match.get("fixture") or {}
        if fixture.get("id") is None:
            continue
        score = match.get("score") or {}
        result[str(fixture["id"])] = {
            "status": (fixture.get("status") or {}).get("short"),
            "halftime": score.get("halftime") or {},
            "fulltime": score.get("fulltime") or {}}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    now = datetime.now(ROME).date()
    if not 1 <= (now - args.date).days <= 3:
        raise ValueError("Can audit only closed dates from the last three days")
    posts = get_sent_posts(args.date.isoformat())
    scores = fetch_official(args.date.isoformat(), os.environ.get("API_SPORTS_KEY", "")) if posts else {}
    report = audit(args.date.isoformat(), posts, scores)
    report["generated_at"] = datetime.now(ROME).isoformat()
    folder = Path("telegram-audit")
    folder.mkdir(exist_ok=True)
    (folder / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [f"# Telegram audit · {args.date}", "", f"Post realmente inviati: {report['posts_found']}",
             f"Pronostici: {report['selections_total']}", f"Singoli serali: {report['singles']}",
             f"Tripla: {report['triple']}", "", "| Invio | Partita | Mercato | Quota | Esito |",
             "|---|---|---|---|---|"]
    for row in report["selections"]:
        lines.append(f"| {row['slot']} | {row['match']} | {row['label']} | {row['odd']} | {row['status']} |")
    (folder / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Telegram audit for {args.date}: {report['posts_found']} sent posts, {report['selections_total']}")
    print(f"Tripla: {report['triple']['status']}; singles: {report['singles']}")


if __name__ == "__main__":
    main()
