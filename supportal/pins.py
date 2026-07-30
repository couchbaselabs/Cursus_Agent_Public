"""
Account / opportunity pinning — a personal "this is mine / I'm watching it" lens.

IMPORTANT — this is an ADDITIVE personal layer, NOT an override. It never
rewrites the SFDC-synced account/opportunity docs; those stay 100% faithful to
Salesforce (see the faithful-mirror rule). Pins live in their own `pins`
collection. When a pin and SFDC disagree, that is surfaced as a signal for a
human to reconcile in Salesforce — never silently masked.

Pin doc shape (key: `pin::<user>::<target_type>::<target_id>`):
    {
      "_type": "pin",
      "user": "<sfdc_user_name>",
      "target_type": "opp" | "account",
      "target_id": "<sfdc opp id or account id>",
      "label": "<human name of the opp/account, for display>",
      "tag": "owned",            # free-form: owned | watching | priority | ...
      "note": "",
      "created_at": <epoch>,
    }
"""
from __future__ import annotations

import time
from datetime import timedelta

from supportal.sfdc_sync import (
    _cb_cfg, _cb_cluster, _ensure_collection, _get_collection,
)

_PINS_COLL = "pins"


def _pin_key(user: str, target_type: str, target_id: str) -> str:
    return f"pin::{user}::{target_type}::{target_id}"


def _cfg_and_cluster():
    cfg = _cb_cfg()
    cluster = _cb_cluster(cfg)
    _ensure_collection(cluster, cfg["bucket"], cfg["scope"], _PINS_COLL)
    return cfg, cluster


def pin_target(user: str, target_id: str, target_type: str = "opp",
               tag: str = "owned", label: str = "", note: str = "") -> dict:
    """Create/update a pin. Returns the stored pin doc."""
    user = (user or "").strip()
    if not user:
        return {"error": "No SFDC user configured — set sfdc_user_name in settings."}
    target_type = (target_type or "opp").strip().lower()
    if target_type not in ("opp", "account"):
        return {"error": "target_type must be 'opp' or 'account'."}
    if not target_id:
        return {"error": "target_id is required."}

    cfg, cluster = _cfg_and_cluster()
    col = _get_collection(cluster, cfg["bucket"], cfg["scope"], _PINS_COLL)
    doc = {
        "_type": "pin",
        "user": user,
        "target_type": target_type,
        "target_id": target_id,
        "label": label or "",
        "tag": (tag or "owned").strip().lower(),
        "note": note or "",
        "created_at": time.time(),
    }
    col.upsert(_pin_key(user, target_type, target_id), doc)
    return doc


def unpin_target(user: str, target_id: str, target_type: str = "opp") -> dict:
    """Remove a pin. Returns {removed: bool}."""
    user = (user or "").strip()
    target_type = (target_type or "opp").strip().lower()
    cfg, cluster = _cfg_and_cluster()
    col = _get_collection(cluster, cfg["bucket"], cfg["scope"], _PINS_COLL)
    try:
        col.remove(_pin_key(user, target_type, target_id))
        return {"removed": True}
    except Exception:
        return {"removed": False, "note": "pin not found"}


def list_pins(user: str, tag: str = "") -> list[dict]:
    """Return the user's pins, optionally filtered by tag."""
    user = (user or "").strip()
    cfg, cluster = _cfg_and_cluster()
    ks = f"`{cfg['bucket']}`.`{cfg['scope']}`.`{_PINS_COLL}`"
    from couchbase.options import QueryOptions
    if tag:
        q = (f"SELECT p.* FROM {ks} p WHERE p._type='pin' AND p.user=$1 "
             f"AND p.tag=$2 ORDER BY p.created_at DESC")
        params = [user, tag.strip().lower()]
    else:
        q = (f"SELECT p.* FROM {ks} p WHERE p._type='pin' AND p.user=$1 "
             f"ORDER BY p.created_at DESC")
        params = [user]
    try:
        return list(cluster.query(q, QueryOptions(positional_parameters=params,
                                                   timeout=timedelta(seconds=20))))
    except Exception:
        return []


def pinned_ids(user: str, target_type: str = "") -> set[str]:
    """Fast lookup set of target_ids this user has pinned (for ★ annotation)."""
    out: set[str] = set()
    for p in list_pins(user):
        if not target_type or p.get("target_type") == target_type:
            out.add(p.get("target_id", ""))
    return out


def reconcile(user: str) -> dict:
    """Compare the user's 'owned' pins against what SFDC says.

    Faithful-mirror governance surface — surfaces disagreements, never fixes
    them. Two mismatch classes:
      - pinned_not_in_sfdc:  user pinned it 'owned' but is NOT the SE on it in
                             the synced SFDC data → confirm/update in Salesforce.
      - sfdc_not_pinned:     SFDC shows the user as SE but they haven't pinned it
                             → confirm ownership.
    """
    cfg, cluster = _cfg_and_cluster()
    ks_a = f"`{cfg['bucket']}`.`{cfg['scope']}`.accounts"
    ks_o = f"`{cfg['bucket']}`.`{cfg['scope']}`.opportunities"
    from couchbase.options import QueryOptions

    pins = [p for p in list_pins(user) if p.get("tag") == "owned"]
    pinned_opp = {p["target_id"] for p in pins if p.get("target_type") == "opp"}
    pinned_acct = {p["target_id"] for p in pins if p.get("target_type") == "account"}

    # SFDC-side: accounts + opps where this user is the SE (synced mirror)
    try:
        sfdc_acct = {r["sfdc_id"] for r in cluster.query(
            f"SELECT a.sfdc_id FROM {ks_a} a WHERE a._type='account' "
            f"AND LOWER(a.se_name)=LOWER($1)",
            QueryOptions(positional_parameters=[user], timeout=timedelta(seconds=20)))}
    except Exception:
        sfdc_acct = set()
    try:
        sfdc_opp = {r["sfdc_opp_id"] for r in cluster.query(
            f"SELECT o.sfdc_opp_id FROM {ks_o} o "
            f"WHERE LOWER(o.se_name)=LOWER($1) OR LOWER(o.supporting_se_name)=LOWER($1)",
            QueryOptions(positional_parameters=[user], timeout=timedelta(seconds=20)))}
    except Exception:
        sfdc_opp = set()

    return {
        "user": user,
        "pinned_not_in_sfdc": {
            "opps":     sorted(pinned_opp - sfdc_opp),
            "accounts": sorted(pinned_acct - sfdc_acct),
        },
        "sfdc_not_pinned": {
            "opps":     sorted(sfdc_opp - pinned_opp),
            "accounts": sorted(sfdc_acct - pinned_acct),
        },
        "in_agreement": {
            "opps":     len(pinned_opp & sfdc_opp),
            "accounts": len(pinned_acct & sfdc_acct),
        },
    }
