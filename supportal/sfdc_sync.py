"""
supportal/sfdc_sync.py — Salesforce → Couchbase sync pipeline.

Harvests Account, AccountTeamMember, Opportunity, OpportunityLineItem,
Contract, and pse__Proj__c into two CB collections:
  • accounts      — one doc per SFDC Account
  • opportunities — one doc per open Opportunity

Field mapping (SFDC API name → role in sync) is stored in CB as
config::sfdc_field_map so it can be updated at runtime via agent tools
without a code change or redeploy.

SFDC credentials are read from the active Strabo profile (settings.json)
with env-var overrides:
  SFDC_TOKEN_HOST, SFDC_CONSUMER_KEY, SFDC_CONSUMER_SECRET, SFDC_AUTH_FLOW
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from datetime import timedelta
from typing import Any

from supportal.constants import SETTINGS_FILE

# Prevents concurrent sync_all() invocations (startup + Save + manual can all fire at once).
_SYNC_LOCK = threading.Lock()

# ── Default field mapping ──────────────────────────────────────────────────────
# Keys are logical names used in this module; values are SFDC API field names.
# Update via update_sfdc_field_mapping() when SFDC renames a field.
DEFAULT_FIELD_MAPPING: dict[str, str] = {
    # Opportunity fields
    "opp_se_primary":       "Primary_SE__c",       # SFDC label: "SE Opp Primary"
    "opp_se_supporting":    "Opp_SE_Supporting__c",  # SFDC label: "SE Opp Supporting SE"
    "opp_arr":              "ACV_Enterprise_Total__c",
    # Closed-won TCV rollup field. Standard Opportunity.Amount, NOT the
    # enterprise-ACV field: verified that Capella-consumption accounts (GoDaddy,
    # DaVita) carry $0 in ACV_Enterprise_Total__c and their real value lives in
    # Amount (which rolls up Cloud + Services + license). Amount is populated for
    # both enterprise and consumption deal types, so it's the faithful TCV total.
    "opp_tcv":              "Amount",
    "opp_renewal_arr":      "ACV_Enterprise_Renewal__c",
    "opp_renewal_date":     "REN_Target_Renewal_Close_Date__c",
    "opp_blocking_cbses":   "Blocking_CBSEs__c",
    "opp_tech_win":         "SE_Technical_Win__c",
    # AccountTeamMember role strings for team enrichment
    "role_ae":              "Account Executive",
    "role_csm":             "Customer Success Manager",
    "role_se":              "Solutions Engineer",
}

_ACCTS_COLL   = "accounts"
_OPPS_COLL    = "opportunities"
_FM_DOC_KEY   = "config::sfdc_field_map"


# ── Settings helpers ───────────────────────────────────────────────────────────

def _load_profile() -> dict:
    try:
        with open(SETTINGS_FILE) as f:
            s = json.load(f)
        last = s.get("__last__", "")
        return s.get(last, {})
    except Exception:
        return {}


def _sfdc_cfg() -> dict:
    p = _load_profile()
    return {
        "token_host":      os.environ.get("SFDC_TOKEN_HOST",      p.get("sfdc_token_host",      "https://couchbase.my.salesforce.com")),
        "consumer_key":    os.environ.get("SFDC_CONSUMER_KEY",    p.get("sfdc_consumer_key",    "")),
        "consumer_secret": os.environ.get("SFDC_CONSUMER_SECRET", p.get("sfdc_consumer_secret", "")),
        "auth_flow":       os.environ.get("SFDC_AUTH_FLOW",        p.get("sfdc_auth_flow",       "client_credentials")),
        "username":        os.environ.get("SFDC_USERNAME",         p.get("sfdc_username",        "")),
        "password":        os.environ.get("SFDC_PASSWORD",         p.get("sfdc_password",        "")),
        "security_token":  os.environ.get("SFDC_SECURITY_TOKEN",   p.get("sfdc_security_token",  "")),
    }


def _cb_cfg() -> dict:
    p = _load_profile()
    return {
        "cb_url":     os.environ.get("CB_URL",        p.get("cb_url",        "couchbase://localhost")),
        "bucket":     os.environ.get("CB_BUCKET",     p.get("cb_bucket",     "rag")),
        "username":   os.environ.get("CB_USER",       p.get("cb_user",       "")),
        "password":   os.environ.get("CB_PASS",       p.get("cb_pass",       "")),
        "use_tls":    os.environ.get("CB_TLS",        str(p.get("cb_tls", False))).lower() == "true",
        "scope":      os.environ.get("CB_SCOPE",      p.get("cb_scope",      "transcripts")),
        "collection": os.environ.get("CB_COLLECTION", p.get("cb_collection", "supportal")),
    }


# ── SFDC client ───────────────────────────────────────────────────────────────

class SFDCClient:
    def __init__(self, cfg: dict | None = None):
        self._cfg = cfg or _sfdc_cfg()
        self._sf = None

    def connect(self):
        import requests
        import urllib.parse as _up
        from simple_salesforce import Salesforce
        c = self._cfg
        flow = c["auth_flow"]
        # Normalize token_host → scheme://host only; user may have stored the full endpoint URL.
        _p = _up.urlparse(c["token_host"])
        _base = f"{_p.scheme}://{_p.netloc}" if _p.netloc else c["token_host"].rstrip("/")
        if flow == "client_credentials":
            data = {"grant_type": "client_credentials",
                    "client_id": c["consumer_key"],
                    "client_secret": c["consumer_secret"]}
        elif flow == "password":
            data = {"grant_type": "password",
                    "client_id": c["consumer_key"],
                    "client_secret": c["consumer_secret"],
                    "username": c["username"],
                    "password": c["password"] + c["security_token"]}
        else:
            raise ValueError(f"Unknown SFDC auth_flow: {flow!r}")
        r = requests.post(f"{_base}/services/oauth2/token", data=data, timeout=30)
        if r.status_code != 200:
            # Truncate HTML responses to a one-liner so callers get a readable message.
            _body = r.text
            for _tag in ("<html", "<table", "<body", "<script"):
                _idx = _body.lower().find(_tag)
                if _idx > 0:
                    _body = _body[:_idx].strip()
                    break
            raise RuntimeError(f"SFDC OAuth failed {r.status_code}: {_body[:200]}")
        tok = r.json()
        self._sf = Salesforce(instance_url=tok["instance_url"], session_id=tok["access_token"])
        return self

    def query_all(self, soql: str) -> list[dict]:
        if not self._sf:
            self.connect()
        result = self._sf.query_all(soql)
        return result.get("records", [])

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self._sf = None


# ── CB helpers ────────────────────────────────────────────────────────────────

def _cb_cluster(cfg: dict):
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from supportal.cb_helpers import _cb_conn_str
    cl = Cluster(
        _cb_conn_str(cfg["cb_url"], cfg["use_tls"]),
        ClusterOptions(PasswordAuthenticator(cfg["username"], cfg["password"])),
    )
    cl.wait_until_ready(timedelta(seconds=15))
    return cl


def _ensure_collection(cluster, bucket_name: str, scope_name: str, coll_name: str):
    """Create collection if it doesn't exist — best-effort, non-fatal."""
    try:
        bm = cluster.bucket(bucket_name).collections()
        existing = {s.name: [c.name for c in s.collections] for s in bm.get_all_scopes()}
        if coll_name not in existing.get(scope_name, []):
            from couchbase.management.collections import CollectionSpec
            bm.create_collection(CollectionSpec(coll_name, scope_name=scope_name))
            time.sleep(1)
    except Exception as e:
        print(f"[sfdc_sync] ensure_collection {coll_name}: {e}")


def _get_collection(cluster, bucket: str, scope: str, coll: str):
    return cluster.bucket(bucket).scope(scope).collection(coll)


def _purge_account_docs(cluster, cb_cfg: dict) -> int:
    """Delete all _type='account' docs before a fresh SE-scoped sync.

    Preserves config/field-map docs (different _type). Creates a primary index
    on the accounts collection if one doesn't already exist so N1QL DELETE works.
    """
    bucket = cb_cfg["bucket"]
    scope  = cb_cfg["scope"]
    coll   = _ACCTS_COLL
    try:
        cluster.query(
            f"CREATE PRIMARY INDEX IF NOT EXISTS `#primary_accounts` "
            f"ON `{bucket}`.`{scope}`.`{coll}`"
        ).execute()
    except Exception as e:
        print(f"[sfdc_sync] primary index create (non-fatal): {e}")
    try:
        rows = list(cluster.query(
            f"DELETE FROM `{bucket}`.`{scope}`.`{coll}` "
            f"WHERE _type = 'account' RETURNING META().id"
        ))
        deleted = len(rows)
        print(f"[sfdc_sync] purged {deleted} stale account docs")
        return deleted
    except Exception as e:
        print(f"[sfdc_sync] purge_account_docs error: {e}")
        return 0


# ── Field mapping ──────────────────────────────────────────────────────────────

def load_field_mapping(cluster=None, cfg: dict | None = None) -> dict:
    """Load field mapping from CB, falling back to DEFAULT_FIELD_MAPPING."""
    if cluster is None:
        cfg = cfg or _cb_cfg()
        try:
            cluster = _cb_cluster(cfg)
        except Exception:
            return dict(DEFAULT_FIELD_MAPPING)
    cfg = cfg or _cb_cfg()
    try:
        col = _get_collection(cluster, cfg["bucket"], cfg["scope"], _ACCTS_COLL)
        doc = col.get(_FM_DOC_KEY).content_as[dict]
        merged = dict(DEFAULT_FIELD_MAPPING)
        merged.update(doc.get("mapping", {}))
        return merged
    except Exception:
        return dict(DEFAULT_FIELD_MAPPING)


def save_field_mapping(mapping: dict, cluster=None, cfg: dict | None = None) -> bool:
    cfg = cfg or _cb_cfg()
    if cluster is None:
        try:
            cluster = _cb_cluster(cfg)
        except Exception as e:
            print(f"[sfdc_sync] save_field_mapping CB error: {e}")
            return False
    try:
        _ensure_collection(cluster, cfg["bucket"], cfg["scope"], _ACCTS_COLL)
        col = _get_collection(cluster, cfg["bucket"], cfg["scope"], _ACCTS_COLL)
        col.upsert(_FM_DOC_KEY, {"_type": "sfdc_field_map", "mapping": mapping, "updated_at": time.time()})
        return True
    except Exception as e:
        print(f"[sfdc_sync] save_field_mapping error: {e}")
        return False


# ── SOQL helpers ──────────────────────────────────────────────────────────────

def _soql_str(s: str) -> str:
    """Safely quote a Python string for a SOQL single-quoted literal."""
    return "'" + s.replace("'", "\\'") + "'"


def _rel_name(field: str) -> str:
    """Return the SOQL relationship name for a custom lookup field.
    Primary_SE__c → Primary_SE__r  (SFDC replaces __c with __r for traversal)
    """
    return field[:-3] + "__r" if field.endswith("__c") else field + "r"


def _se_sfdc_user_id(sfdc: "SFDCClient", se_name: str) -> str | None:
    """Look up a Salesforce User.Id by full name. Returns None if not found."""
    if not se_name:
        return None
    try:
        recs = sfdc.query_all(
            f"SELECT Id FROM User WHERE Name = {_soql_str(se_name)} AND IsActive = true LIMIT 1"
        )
        return recs[0]["Id"] if recs else None
    except Exception as e:
        print(f"[sfdc_sync] _se_sfdc_user_id({se_name!r}) error: {e}")
        return None


def _sfdc_id_list(ids: set) -> str:
    """Format a set of SFDC IDs as a SOQL IN-clause value string ('id1','id2',...)."""
    return ",".join(f"'{i}'" for i in ids)


# ── Org alias fuzzy matching ───────────────────────────────────────────────────

def _normalize(name: str) -> str:
    name = name.lower().strip()
    for sfx in [", inc.", ", inc", " inc.", " inc", ", llc", " llc", ", ltd",
                " ltd.", " limited", " corp.", " corp", " corporation",
                ", lp", " lp", " co.", " co"]:
        if name.endswith(sfx):
            name = name[: -len(sfx)].strip()
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def build_org_aliases(sfdc_name: str, supportal_orgs: list[str]) -> list[str]:
    norm_sfdc = _normalize(sfdc_name)
    aliases: list[str] = []
    for org in supportal_orgs:
        norm_org = _normalize(org)
        if norm_sfdc == norm_org or norm_sfdc in norm_org or norm_org in norm_sfdc:
            aliases.append(org)
    return aliases


def _fetch_supportal_orgs(cluster, cfg: dict) -> list[str]:
    try:
        from couchbase.options import QueryOptions
        coll = cfg.get("collection", "supportal")
        rows = list(cluster.query(
            f"SELECT DISTINCT RAW TOSTRING(t.organization) "
            f"FROM `{cfg['bucket']}`.`{cfg['scope']}`.`{coll}` t "
            f"WHERE t.organization IS NOT MISSING",
            QueryOptions(timeout=timedelta(seconds=30)),
        ))
        return [r for r in rows if r]
    except Exception as e:
        print(f"[sfdc_sync] _fetch_supportal_orgs error: {e}")
        return []


# ── Sync: accounts ────────────────────────────────────────────────────────────

def sync_accounts(sfdc: SFDCClient | None = None, cluster=None,
                  sfdc_cfg: dict | None = None, cb_cfg: dict | None = None,
                  se_user_id: str | None = None,
                  fm: dict | None = None) -> dict:
    cb_cfg = cb_cfg or _cb_cfg()
    if cluster is None:
        cluster = _cb_cluster(cb_cfg)
    _ensure_collection(cluster, cb_cfg["bucket"], cb_cfg["scope"], _ACCTS_COLL)

    if fm is None:
        fm = load_field_mapping(cluster, cb_cfg)
    if sfdc is None:
        sfdc = SFDCClient(sfdc_cfg or _sfdc_cfg()).connect()

    # Resolve SE's account scope: two simple queries merged in Python.
    # SFDC SOQL forbids OR between two semi-joins (IN subqueries), so we fetch
    # each ID set separately and union them before any further queries.
    if se_user_id:
        f_se     = fm["opp_se_primary"]
        f_se_sup = fm["opp_se_supporting"]
        uid = se_user_id

        print("[sfdc_sync] Resolving SE account scope…")
        atm_id_rows = sfdc.query_all(
            f"SELECT AccountId FROM AccountTeamMember "
            f"WHERE UserId = '{uid}' AND AccountId != null"
        )
        # Active-opp book: an account is in the SE's scope only if they are
        # primary/supporting SE on an OPEN opportunity. This is the tight current
        # working book (~8 accounts). Accounts owned only via closed-won opps
        # (e.g. Western Union — 0 open opps with the user as SE) are intentionally
        # NOT in the SFDC book; they are supported/PS accounts covered by the
        # ticket + topology tooling, not the active pipeline. (Do not widen to
        # IsWon here — that pulled in 60+ historical accounts.)
        opp_id_rows = sfdc.query_all(
            f"SELECT AccountId FROM Opportunity "
            f"WHERE ({f_se} = '{uid}' OR {f_se_sup} = '{uid}') "
            f"AND IsClosed = false "
            f"AND AccountId != null"
        )
        se_account_ids: set[str] = (
            {r["AccountId"] for r in atm_id_rows if r.get("AccountId")}
            | {r["AccountId"] for r in opp_id_rows if r.get("AccountId")}
        )
        if not se_account_ids:
            print("[sfdc_sync] No accounts found in SE scope — verify sfdc_user_id matches an active SFDC user")
            return {"accounts_synced": 0}
        id_list = _sfdc_id_list(se_account_ids)
        acct_where = f" AND Id IN ({id_list})"
        atm_where  = f" WHERE AccountId IN ({id_list})"
        ctr_where  = f" AND AccountId IN ({id_list})"
        ps_where   = f" AND pse__Account__c IN ({id_list})"
        print(f"[sfdc_sync] SE scope: {len(se_account_ids)} accounts")
    else:
        acct_where = atm_where = ctr_where = ps_where = ""

    print("[sfdc_sync] Fetching accounts…")
    accounts_raw = sfdc.query_all(
        f"SELECT Id, Name, Type, Industry, OwnerId, Owner.Name, Owner.Email "
        f"FROM Account WHERE IsDeleted = false{acct_where}"
    )
    print(f"[sfdc_sync] {len(accounts_raw)} accounts")

    print("[sfdc_sync] Fetching account team members…")
    atm_raw = sfdc.query_all(
        f"SELECT AccountId, UserId, User.Name, User.Email, TeamMemberRole "
        f"FROM AccountTeamMember{atm_where}"
    )
    # Build per-account team index
    team_idx: dict[str, dict] = {}
    role_ae  = fm["role_ae"]
    role_csm = fm["role_csm"]
    role_se  = fm["role_se"]
    for row in atm_raw:
        aid = row["AccountId"]
        role = row.get("TeamMemberRole", "")
        uname = (row.get("User") or {}).get("Name", "")
        uemail = (row.get("User") or {}).get("Email", "")
        t = team_idx.setdefault(aid, {})
        if role == role_ae and not t.get("ae_name"):
            t["ae_name"] = uname; t["ae_email"] = uemail
        elif role == role_csm and not t.get("csm_name"):
            t["csm_name"] = uname; t["csm_email"] = uemail
        elif role == role_se and not t.get("se_name_atm"):
            t["se_name_atm"] = uname; t["se_email_atm"] = uemail

    print("[sfdc_sync] Fetching active contracts…")
    contracts_raw = sfdc.query_all(
        f"SELECT AccountId, Status, StartDate, EndDate, ContractNumber "
        f"FROM Contract WHERE Status = 'Activated'{ctr_where}"
    )
    contract_idx: dict[str, dict] = {}
    for row in contracts_raw:
        aid = row["AccountId"]
        if aid not in contract_idx:
            contract_idx[aid] = {"contract_status": row.get("Status", ""),
                                  "contract_start_date": row.get("StartDate", ""),
                                  "contract_end_date": row.get("EndDate", ""),
                                  "contract_number": row.get("ContractNumber", "")}

    print("[sfdc_sync] Fetching PS projects…")
    ps_raw = sfdc.query_all(
        f"SELECT pse__Account__c, COUNT(Id) cnt "
        f"FROM pse__Proj__c WHERE pse__Stage__c != 'Closed'{ps_where} "
        f"GROUP BY pse__Account__c"
    )
    ps_idx: dict[str, int] = {r["pse__Account__c"]: int(r.get("cnt", 0)) for r in ps_raw if r.get("pse__Account__c")}

    # Closed-won TCV rollup. The open-opp views (get_account_opportunities etc.)
    # report $0 ARR for mature accounts whose revenue is all closed-won (e.g.
    # Western Union) — that's the empty open-pipeline view, NOT a worthless
    # account. Sum ACV across IsWon opps per account so reports show real value.
    # SUM/COUNT are aggregate exprs, so SOQL field aliases are allowed here.
    print("[sfdc_sync] Rolling up closed-won TCV…")
    f_tcv = fm.get("opp_tcv", "Amount")
    won_where = f" AND AccountId IN ({id_list})" if se_user_id else ""
    won_acv_idx: dict[str, float] = {}
    won_cnt_idx: dict[str, int] = {}
    try:
        won_raw = sfdc.query_all(
            f"SELECT AccountId, SUM({f_tcv}) tcv, COUNT(Id) cnt "
            f"FROM Opportunity WHERE IsWon = true{won_where} GROUP BY AccountId"
        )
        for r in won_raw:
            aid = r.get("AccountId")
            if aid:
                won_acv_idx[aid] = float(r.get("tcv") or 0)
                won_cnt_idx[aid] = int(r.get("cnt") or 0)
    except Exception as e:
        print(f"[sfdc_sync] closed-won TCV rollup (non-fatal): {e}")

    # Build per-account primary/supporting SE name from OPEN opportunities.
    # Scope is already open-opp-only, so every in-scope account has an open opp;
    # deriving SE names from open opps keeps both fields reflecting the current
    # engagement (no stale closed-won SEs bleeding into supporting SE).
    opp_se_idx: dict[str, str] = {}
    opp_sup_se_idx: dict[str, str] = {}
    if se_user_id and accounts_raw:
        f_se     = fm["opp_se_primary"]
        f_se_sup = fm["opp_se_supporting"]
        f_se_rel     = _rel_name(f_se)
        f_se_sup_rel = _rel_name(f_se_sup)
        try:
            opp_se_rows = sfdc.query_all(
                f"SELECT AccountId, {f_se_rel}.Name, {f_se_sup_rel}.Name "
                f"FROM Opportunity WHERE IsClosed = false AND AccountId IN ({id_list})"
            )
            for r in opp_se_rows:
                aid = r.get("AccountId", "")
                if aid:
                    _se_rel_obj  = r.get(f_se_rel) or {}
                    _sup_rel_obj = r.get(f_se_sup_rel) or {}
                    se_nm  = (_se_rel_obj.get("Name") if isinstance(_se_rel_obj, dict) else "") or ""
                    sup_nm = (_sup_rel_obj.get("Name") if isinstance(_sup_rel_obj, dict) else "") or ""
                    if se_nm and aid not in opp_se_idx:
                        opp_se_idx[aid] = se_nm
                    if sup_nm and aid not in opp_sup_se_idx:
                        opp_sup_se_idx[aid] = sup_nm
        except Exception as e:
            print(f"[sfdc_sync] opp se lookup (non-fatal): {e}")

    supportal_orgs = _fetch_supportal_orgs(cluster, cb_cfg)
    # Purge all existing account docs so a rescoped sync doesn't leave stale records.
    _purge_account_docs(cluster, cb_cfg)
    col = _get_collection(cluster, cb_cfg["bucket"], cb_cfg["scope"], _ACCTS_COLL)
    now = time.time()
    upserted = 0
    for acct in accounts_raw:
        aid = acct["Id"]
        team = team_idx.get(aid, {})
        ctr  = contract_idx.get(aid, {})
        doc = {
            "_type":              "account",
            "sfdc_id":            aid,
            "org_name":           acct.get("Name", ""),
            "org_aliases":        build_org_aliases(acct.get("Name", ""), supportal_orgs),
            "account_type":       acct.get("Type", ""),
            "industry":           acct.get("Industry", ""),
            "ae_name":            team.get("ae_name") or (acct.get("Owner") or {}).get("Name", ""),
            "ae_email":           team.get("ae_email") or (acct.get("Owner") or {}).get("Email", ""),
            "csm_name":           team.get("csm_name", ""),
            "csm_email":          team.get("csm_email", ""),
            "se_name":            opp_se_idx.get(aid) or team.get("se_name_atm", ""),
            "se_email":           team.get("se_email_atm", ""),
            "supporting_se_name": opp_sup_se_idx.get(aid, ""),
            "contract_status":    ctr.get("contract_status", ""),
            "contract_end_date":  ctr.get("contract_end_date", ""),
            "contract_number":    ctr.get("contract_number", ""),
            "active_ps_projects": ps_idx.get(aid, 0),
            "closed_won_acv":     won_acv_idx.get(aid, 0),
            "closed_won_count":   won_cnt_idx.get(aid, 0),
            "last_synced":        now,
        }
        try:
            col.upsert(f"account::{aid}", doc)
            upserted += 1
        except Exception as e:
            print(f"[sfdc_sync] upsert account {aid}: {e}")

    print(f"[sfdc_sync] accounts: {upserted} upserted")
    return {"accounts_synced": upserted}


# ── Sync: opportunities ───────────────────────────────────────────────────────

def sync_opportunities(sfdc: SFDCClient | None = None, cluster=None,
                       sfdc_cfg: dict | None = None, cb_cfg: dict | None = None,
                       se_user_id: str | None = None,
                       fm: dict | None = None) -> dict:
    cb_cfg = cb_cfg or _cb_cfg()
    if cluster is None:
        cluster = _cb_cluster(cb_cfg)
    _ensure_collection(cluster, cb_cfg["bucket"], cb_cfg["scope"], _OPPS_COLL)

    if fm is None:
        fm = load_field_mapping(cluster, cb_cfg)
    if sfdc is None:
        sfdc = SFDCClient(sfdc_cfg or _sfdc_cfg()).connect()

    f_se         = fm["opp_se_primary"]
    f_se_sup     = fm["opp_se_supporting"]
    f_se_rel     = _rel_name(f_se)       # Primary_SE__r  (not Primary_SE__cr)
    f_se_sup_rel = _rel_name(f_se_sup)   # Opp_SE_Supporting__r
    f_arr        = fm["opp_arr"]
    f_ren_arr    = fm["opp_renewal_arr"]
    f_ren_dt     = fm["opp_renewal_date"]
    f_cbses      = fm["opp_blocking_cbses"]
    f_tw         = fm["opp_tech_win"]

    if se_user_id:
        uid = se_user_id
        opp_scope = f" AND ({f_se} = '{uid}' OR {f_se_sup} = '{uid}')"
    else:
        opp_scope = ""

    print("[sfdc_sync] Fetching open opportunities…")
    opps_raw = sfdc.query_all(
        f"SELECT Id, Name, AccountId, StageName, CloseDate, IsWon, "
        f"{f_se}, {f_se_rel}.Name, {f_se_rel}.Email, "
        f"{f_se_sup}, {f_se_sup_rel}.Name, {f_se_sup_rel}.Email, "
        f"OwnerId, Owner.Name, Owner.Email, "
        f"{f_arr}, {f_ren_arr}, {f_ren_dt}, {f_cbses}, {f_tw} "
        f"FROM Opportunity WHERE IsClosed = false AND AccountId != null{opp_scope}"
    )
    print(f"[sfdc_sync] {len(opps_raw)} open opportunities")

    # Use the fetched opp IDs directly — avoids a SOQL subquery in OpportunityLineItem.
    print("[sfdc_sync] Fetching opportunity line items (products)…")
    if opps_raw and se_user_id:
        opp_ids_str = _sfdc_id_list({r["Id"] for r in opps_raw})
        oli_raw = sfdc.query_all(
            f"SELECT OpportunityId, Product2.Name "
            f"FROM OpportunityLineItem WHERE Product2.Name != null "
            f"AND OpportunityId IN ({opp_ids_str})"
        )
    else:
        oli_raw = sfdc.query_all(
            "SELECT OpportunityId, Product2.Name FROM OpportunityLineItem WHERE Product2.Name != null"
        )
    products_idx: dict[str, list[str]] = {}
    for row in oli_raw:
        oid = row["OpportunityId"]
        pname = (row.get("Product2") or {}).get("Name", "")
        if pname:
            products_idx.setdefault(oid, [])
            if pname not in products_idx[oid]:
                products_idx[oid].append(pname)

    col = _get_collection(cluster, cb_cfg["bucket"], cb_cfg["scope"], _OPPS_COLL)
    now = time.time()
    upserted = 0
    for opp in opps_raw:
        oid = opp["Id"]
        se_obj     = opp.get(f_se_rel) or {}
        se_sup_obj = opp.get(f_se_sup_rel) or {}
        owner_obj  = opp.get("Owner") or {}
        doc = {
            "_type":               "opportunity",
            "sfdc_opp_id":         oid,
            "sfdc_account_id":     opp.get("AccountId", ""),
            "opp_name":            opp.get("Name", ""),
            "stage":               opp.get("StageName", ""),
            "close_date":          opp.get("CloseDate", ""),
            "se_name":             se_obj.get("Name", ""),
            "se_email":            se_obj.get("Email", ""),
            "supporting_se_name":  se_sup_obj.get("Name", ""),
            "supporting_se_email": se_sup_obj.get("Email", ""),
            "ae_name":             owner_obj.get("Name", ""),
            "ae_email":            owner_obj.get("Email", ""),
            "arr":                 opp.get(f_arr) or 0,
            "renewal_arr":         opp.get(f_ren_arr) or 0,
            "renewal_close_date":  opp.get(f_ren_dt, ""),
            "blocking_cbses":      opp.get(f_cbses, ""),
            "se_technical_win":    opp.get(f_tw, ""),
            "products":            products_idx.get(oid, []),
            "last_synced":         now,
        }
        try:
            col.upsert(f"opportunity::{oid}", doc)
            # Back-fill se_name on the parent account doc if account has none
            _backfill_account_se(cluster, cb_cfg, opp.get("AccountId", ""),
                                 doc["se_name"], doc["se_email"])
            upserted += 1
        except Exception as e:
            print(f"[sfdc_sync] upsert opp {oid}: {e}")

    print(f"[sfdc_sync] opportunities: {upserted} upserted")
    return {"opportunities_synced": upserted}


def _backfill_account_se(cluster, cfg: dict, account_id: str, se_name: str, se_email: str):
    """If the account doc has no se_name, populate it from the opportunity."""
    if not account_id or not se_name:
        return
    try:
        col = _get_collection(cluster, cfg["bucket"], cfg["scope"], _ACCTS_COLL)
        doc = col.get(f"account::{account_id}").content_as[dict]
        if not doc.get("se_name"):
            doc["se_name"] = se_name
            doc["se_email"] = se_email
            col.replace(f"account::{account_id}", doc)
    except Exception:
        pass


def sync_all(sfdc_cfg: dict | None = None, cb_cfg: dict | None = None) -> dict:
    if not _SYNC_LOCK.acquire(blocking=False):
        print("[sfdc_sync] sync_all already running — skipping duplicate invocation")
        return {"skipped": True, "reason": "already running"}
    try:
        cb_cfg  = cb_cfg  or _cb_cfg()
        cluster = _cb_cluster(cb_cfg)
        sfdc    = SFDCClient(sfdc_cfg or _sfdc_cfg()).connect()

        # Resolve SE identity — prefer stored sfdc_user_id, fall back to name lookup.
        profile     = _load_profile()
        se_user_id  = profile.get("sfdc_user_id", "").strip()
        se_name     = profile.get("sfdc_user_name", "").strip()
        if not se_user_id and se_name:
            se_user_id = _se_sfdc_user_id(sfdc, se_name) or ""
        if not se_user_id:
            msg = (
                "No SE identity configured. "
                "Set your name in Settings → Salesforce → Find to scope the sync."
            )
            print(f"[sfdc_sync] {msg}")
            return {"error": msg}

        print(f"[sfdc_sync] Scoping sync to SE: {se_name or se_user_id} ({se_user_id})")
        fm = load_field_mapping(cluster, cb_cfg)
        r1 = sync_accounts(sfdc=sfdc, cluster=cluster, cb_cfg=cb_cfg,
                           se_user_id=se_user_id, fm=fm)
        r2 = sync_opportunities(sfdc=sfdc, cluster=cluster, cb_cfg=cb_cfg,
                                se_user_id=se_user_id, fm=fm)
        result = {**r1, **r2}
        _mark_sync_complete()
        return result
    finally:
        _SYNC_LOCK.release()


def _mark_sync_complete() -> None:
    """Write sfdc_last_sync_at epoch to the active profile in settings.json."""
    try:
        with open(SETTINGS_FILE) as f:
            s = json.load(f)
        last = s.get("__last__", "")
        if last and last in s:
            s[last]["sfdc_last_sync_at"] = time.time()
            with open(SETTINGS_FILE, "w") as f:
                json.dump(s, f, indent=2)
    except Exception:
        pass


def sfdc_sync_age_seconds() -> float | None:
    """Return seconds since last successful sync, or None if never synced."""
    try:
        with open(SETTINGS_FILE) as f:
            s = json.load(f)
        last = s.get("__last__", "")
        ts = s.get(last, {}).get("sfdc_last_sync_at")
        return (time.time() - float(ts)) if ts else None
    except Exception:
        return None


# ── Agent query functions ─────────────────────────────────────────────────────

def query_account_opportunities(organization: str,
                                cb_url: str, bucket: str, username: str,
                                password: str, use_tls: bool, scope: str) -> str:
    """Return markdown table of all open opps for an account."""
    try:
        from couchbase.options import QueryOptions
        cluster = _cb_cluster({"cb_url": cb_url, "bucket": bucket,
                                "username": username, "password": password,
                                "use_tls": use_tls, "scope": scope})
        # Resolve org to sfdc_account_id via aliases
        rows = list(cluster.query(
            f"SELECT o.* FROM `{bucket}`.`{scope}`.`{_OPPS_COLL}` o "
            f"JOIN `{bucket}`.`{scope}`.`{_ACCTS_COLL}` a ON o.sfdc_account_id = a.sfdc_id "
            f"WHERE LOWER(a.org_name) LIKE $1 "
            f"   OR ANY al IN a.org_aliases SATISFIES LOWER(al) LIKE $1 END "
            f"ORDER BY o.close_date ASC",
            QueryOptions(positional_parameters=[f"%{organization.lower()}%"],
                         timeout=timedelta(seconds=20)),
        ))
        if not rows:
            return f"No open opportunities found for '{organization}'."
        lines = ["| Opp ID | Name | Stage | Close Date | SE | ARR | Renewal ARR | Blocking CBSEs |",
                 "|--------|------|-------|------------|----|----|-------------|----------------|"]
        for r in rows:
            lines.append(
                f"| {r.get('sfdc_opp_id','')} "
                f"| {r.get('opp_name','')[:50]} "
                f"| {r.get('stage','')} "
                f"| {r.get('close_date','')[:10]} "
                f"| {r.get('se_name','')} "
                f"| ${r.get('arr') or 0:,.0f} "
                f"| ${r.get('renewal_arr') or 0:,.0f} "
                f"| {r.get('blocking_cbses') or '—'} |"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"query_account_opportunities error: {e}"


def query_se_opportunities(se_name: str,
                           cb_url: str, bucket: str, username: str,
                           password: str, use_tls: bool, scope: str) -> str:
    """Return markdown table of all open opps where se_name is primary or supporting SE."""
    try:
        from couchbase.options import QueryOptions
        cluster = _cb_cluster({"cb_url": cb_url, "bucket": bucket,
                                "username": username, "password": password,
                                "use_tls": use_tls, "scope": scope})
        rows = list(cluster.query(
            f"SELECT o.*, a.org_name, a.org_aliases "
            f"FROM `{bucket}`.`{scope}`.`{_OPPS_COLL}` o "
            f"LEFT JOIN `{bucket}`.`{scope}`.`{_ACCTS_COLL}` a ON o.sfdc_account_id = a.sfdc_id "
            f"WHERE LOWER(o.se_name) LIKE $1 OR LOWER(o.supporting_se_name) LIKE $1 "
            f"ORDER BY o.close_date ASC",
            QueryOptions(positional_parameters=[f"%{se_name.lower()}%"],
                         timeout=timedelta(seconds=20)),
        ))
        if not rows:
            return f"No open opportunities found for SE '{se_name}'."

        # Cross-validate opportunity-level SE against the account-level book.
        # The opps filter matches o.se_name (Salesforce Primary_SE__c), which is
        # known-unreliable for some accounts (it has shown the wrong SE even after
        # a fresh sync). The account mirror (`accounts`) is SE-scoped by account-
        # team membership and is the more trustworthy signal. When the JOIN finds
        # no account row (org_name is null), the opp claims you as SE but the
        # account is NOT in your book — treat it as UNVERIFIED rather than
        # asserting it's yours. Segregate instead of dropping: a missing account
        # row could also just mean the account wasn't synced, so surface it with
        # a caveat and let the user confirm against Salesforce.
        def _fmt(r: dict) -> str:
            role = "Primary SE" if se_name.lower() in (r.get("se_name") or "").lower() else "Supporting SE"
            acct = r.get("org_name") or r.get("sfdc_account_id", "")
            return (
                f"| {acct} "
                f"| {r.get('opp_name','')[:45]} "
                f"| {r.get('stage','')} "
                f"| {r.get('close_date','')[:10]} "
                f"| {role} "
                f"| ${r.get('arr') or 0:,.0f} "
                f"| {r.get('blocking_cbses') or '—'} |"
            )

        confirmed  = [r for r in rows if (r.get("org_name") or "").strip()]
        unverified = [r for r in rows if not (r.get("org_name") or "").strip()]

        hdr = ["| Account | Opp Name | Stage | Close Date | Role | ARR | Blocking CBSEs |",
               "|---------|----------|-------|------------|------|-----|----------------|"]
        out: list[str] = []
        if confirmed:
            out.append(f"**Confirmed — {se_name} on both the account team and the opportunity ({len(confirmed)}):**")
            out.extend(hdr)
            out.extend(_fmt(r) for r in confirmed)
        if unverified:
            if out:
                out.append("")
            out.append(
                f"**⚠ Unverified — opportunity-level SE only ({len(unverified)}):** "
                f"these opportunities list {se_name} as SE, but the account is not in "
                f"your account-team book, so the assignment could not be confirmed. "
                f"Salesforce's opportunity `Primary_SE__c` has been wrong for some "
                f"accounts. Verify each against the opportunity's **SE Related "
                f"Information → SE Opp Primary** panel in Salesforce before treating "
                f"it as yours. (Accounts show as a raw ID because they are not in your book.)"
            )
            out.extend(hdr)
            out.extend(_fmt(r) for r in unverified)
        return "\n".join(out)
    except Exception as e:
        return f"query_se_opportunities error: {e}"


def query_sfdc_accounts(se_name: str = "",
                        cb_url: str = "", bucket: str = "", username: str = "",
                        password: str = "", use_tls: bool = False, scope: str = "") -> str:
    """Return markdown table of accounts, optionally filtered by SE name."""
    try:
        from couchbase.options import QueryOptions
        cfg = _cb_cfg()
        cluster = _cb_cluster({"cb_url": cb_url or cfg["cb_url"],
                                "bucket": bucket or cfg["bucket"],
                                "username": username or cfg["username"],
                                "password": password or cfg["password"],
                                "use_tls": use_tls if cb_url else cfg["use_tls"],
                                "scope": scope or cfg["scope"]})
        _bucket  = bucket or cfg["bucket"]
        _scope   = scope  or cfg["scope"]
        if se_name:
            rows = list(cluster.query(
                f"SELECT a.org_name, a.ae_name, a.se_name, a.csm_name, "
                f"a.contract_end_date, a.active_ps_projects, "
                f"a.closed_won_acv, a.closed_won_count "
                f"FROM `{_bucket}`.`{_scope}`.`{_ACCTS_COLL}` a "
                f"WHERE a._type = 'account' "
                f"  AND (LOWER(a.se_name) LIKE $1 "
                f"    OR LOWER(a.ae_name) LIKE $1 "
                f"    OR LOWER(a.csm_name) LIKE $1) "
                f"ORDER BY a.org_name",
                QueryOptions(positional_parameters=[f"%{se_name.lower()}%"],
                             timeout=timedelta(seconds=20)),
            ))
        else:
            rows = list(cluster.query(
                f"SELECT a.org_name, a.ae_name, a.se_name, a.csm_name, "
                f"a.contract_end_date, a.active_ps_projects, "
                f"a.closed_won_acv, a.closed_won_count "
                f"FROM `{_bucket}`.`{_scope}`.`{_ACCTS_COLL}` a "
                f"WHERE a._type = 'account' "
                f"ORDER BY a.org_name LIMIT 200",
                QueryOptions(timeout=timedelta(seconds=20)),
            ))
        if not rows:
            return "No accounts found." + (f" (filtered by SE: {se_name})" if se_name else "")
        lines = ["| Account | AE | SE | CSM | Contract End | PS Projects | Closed-Won TCV | Won Opps |",
                 "|---------|----|----|-----|-------------|-------------|----------------|----------|"]
        for r in rows:
            _tcv = r.get("closed_won_acv") or 0
            lines.append(
                f"| {r.get('org_name','')} "
                f"| {r.get('ae_name','—')} "
                f"| {r.get('se_name','—')} "
                f"| {r.get('csm_name','—')} "
                f"| {(r.get('contract_end_date') or '—')[:10]} "
                f"| {r.get('active_ps_projects', 0)} "
                f"| ${_tcv:,.0f} "
                f"| {r.get('closed_won_count', 0)} |"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"query_sfdc_accounts error: {e}"


def lookup_account_live(account_name: str, cluster=None) -> str:
    """Ad-hoc, read-only LIVE Salesforce lookup for ANY account by name.

    Unlike the SE-scoped sync (`sync_sfdc_data` / `list_sfdc_accounts`), this
    queries Salesforce directly for any account — not just the current user's
    book — and returns AE (owner), account type, closed-won TCV, and open
    opportunities with their SE. It does NOT write to Couchbase or touch the
    SE-scoped mirror — results are ephemeral (answer-a-question, don't store).
    Read-only; never mutates Salesforce.

    Use for "what's the ARR/deal size for <account>?", "who's the AE on
    <account>?", "how big is <account>?" for accounts you don't own.
    """
    try:
        fm = load_field_mapping(cluster, None)
    except Exception:
        fm = dict(DEFAULT_FIELD_MAPPING)
    f_tcv = fm.get("opp_tcv", "Amount")
    f_se_rel     = _rel_name(fm["opp_se_primary"])
    f_sup_rel    = _rel_name(fm["opp_se_supporting"])

    try:
        client = SFDCClient()
        sf = client.connect()
    except Exception as e:
        return f"Live SFDC lookup unavailable (auth failed): {e}"

    # Base org URL for record links — normalize token_host to scheme://host
    # (the stored value may include the /services/oauth2/token path).
    import urllib.parse as _up
    _pp = _up.urlparse(client._cfg.get("token_host", ""))
    org_base = f"{_pp.scheme}://{_pp.netloc}" if _pp.netloc else "https://couchbase.my.salesforce.com"

    name_q = _soql_str(f"%{account_name}%")
    try:
        accts = sf.query_all(
            f"SELECT Id, Name, Type, Industry, Owner.Name "
            f"FROM Account WHERE Name LIKE {name_q} AND IsDeleted = false "
            f"ORDER BY Name LIMIT 10"
        )
    except Exception as e:
        return f"Live SFDC account query failed: {e}"
    if not accts:
        return f"No Salesforce account found matching '{account_name}'."

    out: list[str] = []
    for a in accts:
        aid = a["Id"]
        owner = (a.get("Owner") or {}).get("Name", "—")
        out.append(f"## {a.get('Name','')}  ({a.get('Type') or '—'}, {a.get('Industry') or '—'})")
        out.append(f"- **AE (owner):** {owner}   ·   SFDC: {org_base}/{aid}")

        # Closed-won TCV
        try:
            won = sf.query_all(
                f"SELECT SUM({f_tcv}) tcv, COUNT(Id) cnt "
                f"FROM Opportunity WHERE IsWon = true AND AccountId = '{aid}'"
            )
            tcv = float((won[0].get("tcv") if won else 0) or 0)
            wcnt = int((won[0].get("cnt") if won else 0) or 0)
            out.append(f"- **Closed-Won TCV:** ${tcv:,.0f} across {wcnt} won opps (lifetime bookings, not annual ARR)")
        except Exception as e:
            out.append(f"- Closed-Won TCV: lookup failed ({e})")

        # Open opps
        try:
            opps = sf.query_all(
                f"SELECT Name, StageName, CloseDate, {f_tcv}, "
                f"{f_se_rel}.Name, {f_sup_rel}.Name "
                f"FROM Opportunity WHERE IsClosed = false AND AccountId = '{aid}' "
                f"ORDER BY CloseDate"
            )
            if opps:
                out.append(f"- **Open opportunities ({len(opps)}):**")
                out.append("")
                out.append("| Opp | Stage | Close | Amount | Primary SE | Supporting SE |")
                out.append("|-----|-------|-------|--------|-----------|---------------|")
                for o in opps:
                    amt = float(o.get(f_tcv) or 0)
                    se  = (o.get(f_se_rel.split('.')[0]) or {}).get("Name") if isinstance(o.get(f_se_rel.split('.')[0]), dict) else None
                    sup = (o.get(f_sup_rel.split('.')[0]) or {}).get("Name") if isinstance(o.get(f_sup_rel.split('.')[0]), dict) else None
                    out.append(f"| {o.get('Name','')[:44]} | {o.get('StageName','')} "
                               f"| {(o.get('CloseDate') or '')[:10]} | ${amt:,.0f} "
                               f"| {se or '—'} | {sup or '—'} |")
                out.append("")
            else:
                out.append("- Open opportunities: none")
        except Exception as e:
            out.append(f"- Open opportunities: lookup failed ({e})")

    out.append("\n_Live read-only Salesforce lookup — not stored locally. Values are lifetime won-bookings TCV, not current annual ARR._")
    return "\n".join(out)


def get_se_opp_worklist(se_user_id: str = "", se_name: str = "",
                        window_quarters: int = 3, behind_days: int = 0,
                        cluster=None) -> str:
    """LIVE, READ-ONLY SFDC worklist for the SE-Section weekly-update tool.

    Returns the current SE's OPEN opportunities in the CQ+`window_quarters`
    window (default CQ+3 = this fiscal quarter + next 3), ranked by SE-Section
    staleness (`SE_Update_Age__c` = "SE Section Days Since Last Update"), with
    the current SE-Section values needed to prepare updates. Read-only; never
    writes to Salesforce or Couchbase.

    Args:
        se_user_id:      SFDC User.Id of the SE (preferred). If empty, resolved
                         from se_name.
        se_name:         SE full name (fallback if se_user_id not given).
        window_quarters: forward fiscal-quarter horizon (default 3 = CQ+3).
        behind_days:     if > 0, only opps whose SE section is at least this
                         many days stale (the "behind / catch-up" list).
    """
    try:
        fm = load_field_mapping(cluster, None)
    except Exception:
        fm = dict(DEFAULT_FIELD_MAPPING)
    f_se = fm["opp_se_primary"]          # Primary_SE__c

    try:
        client = SFDCClient()
        sf = client.connect()
    except Exception as e:
        return f"Live SFDC worklist unavailable (auth failed): {e}"

    uid = (se_user_id or "").strip() or (_se_sfdc_user_id(sf, se_name) or "")
    if not uid:
        return ("No SFDC user resolved. Set sfdc_user_id in settings, or pass a "
                "valid se_name.")

    import urllib.parse as _up
    _pp = _up.urlparse(client._cfg.get("token_host", ""))
    org_base = f"{_pp.scheme}://{_pp.netloc}" if _pp.netloc else "https://couchbase.my.salesforce.com"

    n = max(0, int(window_quarters or 0))
    # SOQL fiscal-quarter literals — no manual date math. CQ..CQ+n.
    if n > 0:
        window = f"(CloseDate = THIS_FISCAL_QUARTER OR CloseDate = NEXT_N_FISCAL_QUARTERS:{n})"
    else:
        window = "CloseDate = THIS_FISCAL_QUARTER"

    try:
        rows = sf.query_all(
            f"SELECT Id, Name, Account.Name, StageName, CloseDate, "
            f"SE_Update_Age__c, Last_updated_SE_Section__c, "
            f"SE_Next_Steps__c, SE_Technical_Risk__c, SE_Technical_Win__c, POC_Stage__c "
            f"FROM Opportunity "
            f"WHERE {f_se} = '{uid}' AND IsClosed = false AND {window} "
            f"ORDER BY SE_Update_Age__c DESC NULLS FIRST"
        )
    except Exception as e:
        return f"SE worklist query failed: {e}"

    if behind_days > 0:
        rows = [r for r in rows if (r.get("SE_Update_Age__c") or 0) >= behind_days]
    if not rows:
        return (f"No open opportunities in the CQ+{n} window"
                + (f" stale ≥ {behind_days}d" if behind_days else "")
                + " where you are SE Opp Primary.")

    def _trunc(v, cap=60):
        s = (v or "").strip().replace("\n", " ")
        return (s[:cap] + "…") if len(s) > cap else (s or "—")

    hdr = ["| Age(d) | Account | Opportunity | Stage | Close | SE Next Steps (current) | Tech Risk | Link |",
           "|-------:|---------|-------------|-------|-------|-------------------------|-----------|------|"]
    out = [f"## SE-Section worklist — CQ+{n}, {len(rows)} open opps"
           + (f" (behind ≥ {behind_days}d)" if behind_days else "")
           + ", ranked by SE-Section staleness\n"]
    out.extend(hdr)
    for r in rows:
        acct = (r.get("Account") or {}).get("Name", "—")
        age  = r.get("SE_Update_Age__c")
        out.append(
            f"| {int(age) if age is not None else '—'} "
            f"| {acct} | {_trunc(r.get('Name'), 40)} "
            f"| {r.get('StageName','')} | {(r.get('CloseDate') or '')[:10]} "
            f"| {_trunc(r.get('SE_Next_Steps__c'))} "
            f"| {_trunc(r.get('SE_Technical_Risk__c'), 22)} "
            f"| {org_base}/{r['Id']} |"
        )
    out.append("\n_Live read-only SFDC. SE_Update_Age is SFDC-computed (do not edit). "
               "Prepare SE_Next_Steps + validate SE_Technical_Risk; apply via the Link. "
               "Never writes to Salesforce._")
    return "\n".join(out)


# ── GATED WRITE (SE-Section field updates) ────────────────────────────────────
# The ONLY SFDC write path in Cursus. Read-only-to-customer-systems is the tenet;
# this is the explicit, gated exception: dry-run by default, field-whitelisted,
# audited. Never writes anything unless dry_run is explicitly False.

# Whitelist of SE-Section fields this tool may write. Computed rollups
# (SE_Update_Age__c, *_days, *Last_Updated*) are intentionally absent — they are
# formula fields and must never be targeted.
_SE_WRITABLE_FIELDS = {
    "SE_Next_Steps__c", "SE_Technical_Risk__c", "SE_Technical_Win__c",
    "SE_Arch_Sizing_Validated__c", "SE_Tech_Decision_Criteria__c", "SE_Use_Case__c",
    "SE_Next_Steps_Date_Planned_On__c", "SE_Technical_Win_Loss_Notes__c",
    "POC_Win_Lost_Notes__c", "POC_Stage__c", "POC_Start_Date__c", "POC_End_Date__c",
    "SE_Date_Tech_Win_is_in_Progress__c", "SE_Technical_Win_Date__c",
    "CBSE_Improvement_Link__c",
}


def apply_se_opp_updates(opp_id: str, fields: dict | str, dry_run: bool = True,
                         cluster=None) -> str:
    """GATED WRITE — update SE-Section fields on ONE opportunity in Salesforce.

    THE ONLY tool in Cursus that writes to a customer system of record. Safe by
    construction:
      - dry_run=True (DEFAULT) → returns the PLAN (current → proposed per field),
        writes NOTHING. You must pass dry_run=False to write.
      - only fields in _SE_WRITABLE_FIELDS are allowed; anything else is rejected
        (never the SFDC-computed rollups).
      - every real write is audited to a `sewrite::<opp>::<ts>` marker doc.

    Args:
        opp_id:  the 15/18-char Opportunity Id.
        fields:  dict (or JSON string) of {SFDC_api_name: value} to set. Only the
                 SE-Section whitelist is permitted.
        dry_run: True (default) plans without writing; False performs the write.
    """
    import json as _json
    if isinstance(fields, str):
        try:
            fields = _json.loads(fields) if fields.strip() else {}
        except Exception as e:
            return _json.dumps({"error": f"fields is not valid JSON: {e}"})
    if not isinstance(fields, dict) or not fields:
        return _json.dumps({"error": "No fields to update."})
    if not opp_id:
        return _json.dumps({"error": "opp_id is required."})

    # Whitelist enforcement — reject anything not an approved SE-Section field.
    rejected = [k for k in fields if k not in _SE_WRITABLE_FIELDS]
    if rejected:
        return _json.dumps({"error": "Refused: fields not in the SE-Section write "
                                     "whitelist (computed/rollup fields are never "
                                     "writable).", "rejected": rejected,
                            "allowed": sorted(_SE_WRITABLE_FIELDS)})

    try:
        client = SFDCClient()
        sf = client.connect()
    except Exception as e:
        return _json.dumps({"error": f"SFDC auth failed: {e}"})

    # Read current values for the plan / audit (before/after).
    cols = ", ".join(fields.keys())
    try:
        cur_rows = sf.query_all(
            f"SELECT Id, Name, {cols} FROM Opportunity WHERE Id = '{opp_id}' LIMIT 1")
        current = cur_rows[0] if cur_rows else {}
    except Exception as e:
        return _json.dumps({"error": f"Could not read current opp values: {e}"})
    if not current:
        return _json.dumps({"error": f"Opportunity '{opp_id}' not found."})

    plan = {k: {"current": current.get(k), "proposed": v} for k, v in fields.items()}
    name = current.get("Name", "")

    if dry_run:
        return _json.dumps({"dry_run": True, "opp_id": opp_id, "opp_name": name,
                            "would_set": plan,
                            "note": "No write performed. Call again with dry_run=False to apply."},
                           default=str)

    # ── Real write ────────────────────────────────────────────────────────────
    try:
        sf.Opportunity.update(opp_id, dict(fields))  # 204 on success
    except Exception as e:
        return _json.dumps({"error": f"SFDC write failed: {e}", "opp_id": opp_id,
                            "attempted": plan}, default=str)

    # Best-effort audit marker.
    audited = False
    try:
        import time as _t
        cfg = _cb_cfg()
        cl = cluster or _cb_cluster(cfg)
        col = _get_collection(cl, cfg["bucket"], cfg["scope"], "markers")
        ts = int(_t.time())
        col.upsert(f"sewrite::{opp_id}::{ts}", {
            "_type": "sewrite", "opp_id": opp_id, "opp_name": name,
            "changes": plan, "written_at": ts, "written_by": "tool:apply_se_opp_updates",
        })
        audited = True
    except Exception:
        pass

    return _json.dumps({"dry_run": False, "written": True, "opp_id": opp_id,
                        "opp_name": name, "changed": plan, "audited": audited},
                       default=str)


def get_field_mapping_text(cb_url: str = "", bucket: str = "",
                           username: str = "", password: str = "",
                           use_tls: bool = False, scope: str = "") -> str:
    cfg = _cb_cfg()
    try:
        cluster = _cb_cluster({"cb_url": cb_url or cfg["cb_url"],
                                "bucket": bucket or cfg["bucket"],
                                "username": username or cfg["username"],
                                "password": password or cfg["password"],
                                "use_tls": use_tls if cb_url else cfg["use_tls"],
                                "scope": scope or cfg["scope"]})
        fm = load_field_mapping(cluster, cfg)
    except Exception:
        fm = dict(DEFAULT_FIELD_MAPPING)
    lines = ["| Logical Key | SFDC API Field / Role Value |",
             "|-------------|----------------------------|"]
    for k, v in sorted(fm.items()):
        lines.append(f"| {k} | {v} |")
    return "\n".join(lines)


def update_field_mapping_entry(logical_key: str, sfdc_value: str,
                               cb_url: str = "", bucket: str = "",
                               username: str = "", password: str = "",
                               use_tls: bool = False, scope: str = "") -> str:
    cfg = _cb_cfg()
    try:
        cluster = _cb_cluster({"cb_url": cb_url or cfg["cb_url"],
                                "bucket": bucket or cfg["bucket"],
                                "username": username or cfg["username"],
                                "password": password or cfg["password"],
                                "use_tls": use_tls if cb_url else cfg["use_tls"],
                                "scope": scope or cfg["scope"]})
        fm = load_field_mapping(cluster, cfg)
        old = fm.get(logical_key, "(not set)")
        fm[logical_key] = sfdc_value
        save_field_mapping(fm, cluster, cfg)
        return f"Updated `{logical_key}`: `{old}` → `{sfdc_value}`. Takes effect on next sync."
    except Exception as e:
        return f"update_field_mapping error: {e}"


def get_account_sfdc_context(organization: str,
                             cb_url: str = "", bucket: str = "",
                             username: str = "", password: str = "",
                             use_tls: bool = False, scope: str = "") -> dict:
    """Return a combined SFDC intelligence dict for a given org name.

    Queries the accounts collection (matching org_name or org_aliases) and then
    joins open opportunities for that account.  Returns an empty dict if no
    account is found.
    """
    try:
        from couchbase.options import QueryOptions
        cfg = _cb_cfg()
        cluster = _cb_cluster({"cb_url": cb_url or cfg["cb_url"],
                                "bucket": bucket or cfg["bucket"],
                                "username": username or cfg["username"],
                                "password": password or cfg["password"],
                                "use_tls": use_tls if cb_url else cfg["use_tls"],
                                "scope": scope or cfg["scope"]})
        _bucket = bucket or cfg["bucket"]
        _scope  = scope  or cfg["scope"]
        param   = f"%{organization.lower()}%"

        # ── Account lookup ──────────────────────────────────────────────────
        acct_rows = list(cluster.query(
            f"SELECT a.org_name, a.sfdc_id, a.arr, a.account_type, "
            f"       a.contract_end_date, a.ae_name, a.se_name, a.csm_name, "
            f"       a.active_ps_projects, a.closed_won_acv, a.closed_won_count "
            f"FROM `{_bucket}`.`{_scope}`.`{_ACCTS_COLL}` a "
            f"WHERE LOWER(a.org_name) LIKE $1 "
            f"   OR ANY al IN a.org_aliases SATISFIES LOWER(al) LIKE $1 END "
            f"LIMIT 1",
            QueryOptions(positional_parameters=[param], timeout=timedelta(seconds=20)),
        ))
        if not acct_rows:
            return {}

        acct = acct_rows[0]
        sfdc_id = acct.get("sfdc_id", "")

        # ── Opportunities lookup ────────────────────────────────────────────
        opp_rows: list[dict] = []
        if sfdc_id:
            opp_rows = list(cluster.query(
                f"SELECT o.opp_name, o.stage, o.close_date, o.arr, "
                f"       o.products, o.supporting_se_name "
                f"FROM `{_bucket}`.`{_scope}`.`{_OPPS_COLL}` o "
                f"WHERE o.sfdc_account_id = $1 "
                f"ORDER BY o.close_date ASC",
                QueryOptions(positional_parameters=[sfdc_id], timeout=timedelta(seconds=20)),
            ))

        # ── Deduplicated product union across all open opps ─────────────────
        seen: set[str] = set()
        licensed_products: list[str] = []
        for o in opp_rows:
            for p in (o.get("products") or []):
                if p and p not in seen:
                    seen.add(p)
                    licensed_products.append(p)

        open_opportunities = [
            {
                "opp_name":           o.get("opp_name", ""),
                "stage":              o.get("stage", ""),
                "close_date":         o.get("close_date", ""),
                "arr":                o.get("arr") or 0.0,
                "products":           o.get("products") or [],
                "supporting_se_name": o.get("supporting_se_name", ""),
            }
            for o in opp_rows
        ]

        return {
            "org_name":           acct.get("org_name", ""),
            "arr":                acct.get("arr") or 0.0,
            "account_type":       acct.get("account_type", ""),
            "contract_end_date":  acct.get("contract_end_date", ""),
            "ae_name":            acct.get("ae_name", ""),
            "se_name":            acct.get("se_name", ""),
            "csm_name":           acct.get("csm_name", ""),
            "active_ps_projects": acct.get("active_ps_projects") or 0,
            "closed_won_acv":     acct.get("closed_won_acv") or 0.0,
            "closed_won_count":   acct.get("closed_won_count") or 0,
            "licensed_products":  licensed_products,
            "open_opportunities": open_opportunities,
        }
    except Exception:
        return {}
