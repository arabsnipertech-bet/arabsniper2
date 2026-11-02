"""Read current private Gold daily board for Telegram, without public projection."""
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from opportunity_policy import goal_market

ROME=ZoneInfo("Europe/Rome")
ALLOWED={"over15_ht","over25_ht","gg_ht","over25_ft","over35_ft"}
POLICY="platinum-flow-v1.1"


def stamp(raw):
    try:
        value=datetime.fromisoformat(str(raw).replace("Z","+00:00"))
        return value if value.tzinfo else None
    except (ValueError,TypeError):
        return None


def good_number(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def from_private_day(day, now):
    """Use only the producer's actual qualifying decisions; never re-grade models."""
    if day.get("date") != now.astimezone(ROME).date().isoformat():
        return []
    updated=stamp(day.get("updated_at"))
    if not updated or not timedelta(0) <= now-updated <= timedelta(minutes=90):
        return []
    choices=[]
    for match in day.get("matches",[]):
        f=match.get("fixture") or {}
        a=match.get("multimarket") or {}
        matrix=a.get("matrix") or {}
        if f.get("status_short") not in ("NS","TBD") or f.get("result"):
            continue
        start=stamp(f.get("kickoff")); captured=stamp(a.get("captured_at"))
        if not start or not captured or not captured <= now < start or now-captured > timedelta(minutes=90):
            continue
        if start.astimezone(ROME).date() != now.astimezone(ROME).date():
            continue
        fid=f.get("fixture_id")
        if fid is None or not str(fid).isdigit():
            continue
        scope=match.get("gold_scope") or {}
        if scope.get("allowed") is not True:
            continue
        rows=matrix.get("rows") or []
        platinum=matrix.get("platinum_policy") or {}
        gold=matrix.get("opportunity_policy") or {}
        choices_for_fixture=[]
        for tier,decision_name,best_id in (
            ("platinum","platinum",platinum.get("best_market_id")),
            ("gold","opportunity",gold.get("best_market_id"))):
            if best_id is None:
                continue
            if tier=="platinum" and platinum.get("id")!=POLICY:
                continue
            for row in rows:
                if row.get("id")!=best_id:
                    continue
                decision=row.get(decision_name) or {}
                market=goal_market(row)
                odd=row.get("odd")
                at=stamp(row.get("provider_updated_at"))
                if (decision.get("eligible") is not True or market not in ALLOWED or
                    row.get("quote_executable") is not True or
                    row.get("quote_status") not in ("recent","cached_stable") or
                    not good_number(odd) or odd <= 1 or
                    not at or not timedelta(0)<= now-at <= timedelta(minutes=90)):
                    continue
                if tier=="platinum" and (
                    row.get("movement_confirmed") is not True or
                    row.get("fair_direction_coherent") is not True or
                    not good_number(row.get("oci")) or row["oci"]<30):
                    continue
                if tier=="gold" and decision.get("policy_id") not in ("gold-goals-v6.1",):
                    continue
                choices_for_fixture.append(dict(
                    fixture_id=str(fid),match=f.get("match",""),league=f.get("league",""),
                    kickoff=start.isoformat(),captured_at=captured.isoformat(),
                    provider_updated_at=at.isoformat(),odd=odd,label=row.get("label") or market,
                    market=market,source_tier=tier,model_probability=row.get("statistical_pct"),
                    oci=row.get("oci"),delta_pct=row.get("delta_pct"),
                    ev_pct=row.get("ev_pct"),bookmaker=row.get("bookmaker"),
                    strict=bool(decision.get("strict_eligible",True)),
                    stable=(decision.get("stability") or {}).get("status")=="strict",
                    market_confirmed=(tier=="platinum" or decision.get("signal")=="convergence"),
                    score=float(row.get("oci") or 0) if tier=="platinum" else float(decision.get("playability_score") or 0),
                    signal=tier if tier=="platinum" else decision.get("signal","unconfirmed")))
        # Platinum wins for this fixture, Gold remains a fallback.
        if choices_for_fixture:
            choices.append(next((x for x in choices_for_fixture if x["source_tier"]=="platinum"),choices_for_fixture[0]))
    return choices


def fetch_current_private(now):
    """The daily Gold feed stays in private R2; errors fail closed."""
    from gold_storage import client
    import os
    s3=client()
    bucket=os.environ["R2_BUCKET"]
    # The dashboard's 'day1' is today's board; do not expose raw payload.
    response=s3.get_object(Bucket=bucket,Key="arab_quote_day1.json")
    raw=response["Body"].read()
    if len(raw)>24*1024*1024:
        raise ValueError("Unexpected oversized private daily feed")
    return from_private_day(json.loads(raw),now)
