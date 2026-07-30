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
    "opp_se_primary":       "Primary_SE__c",
    "opp_se_supporting":    "Opp_SE_Supporting__c",
    "opp_arr":              "ACV_Enterprise_Total__c",
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
        opp_id_rows = sfdc.query_all(
            f"SELECT AccountId FROM Opportunity "
            f"WHERE ({f_se} = '{uid}' OR {f_se_sup} = '{uid}') "
            f"AND IsClosed = false AND AccountId != null"
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

    # Build per-account SE name from open opportunities (Primary_SE__r.Name).
    # This covers accounts that are in scope via opportunity but have no ATM SE entry.
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
                f"a.contract_end_date, a.active_ps_projects "
                f"FROM `{_bucket}`.`{_scope}`.`{_ACCTS_COLL}` a "
                f"WHERE LOWER(a.se_name) LIKE $1 "
                f"   OR LOWER(a.ae_name) LIKE $1 "
                f"   OR LOWER(a.csm_name) LIKE $1 "
                f"ORDER BY a.org_name",
                QueryOptions(positional_parameters=[f"%{se_name.lower()}%"],
                             timeout=timedelta(seconds=20)),
            ))
        else:
            rows = list(cluster.query(
                f"SELECT a.org_name, a.ae_name, a.se_name, a.csm_name, "
                f"a.contract_end_date, a.active_ps_projects "
                f"FROM `{_bucket}`.`{_scope}`.`{_ACCTS_COLL}` a "
                f"ORDER BY a.org_name LIMIT 200",
                QueryOptions(timeout=timedelta(seconds=20)),
            ))
        if not rows:
            return "No accounts found." + (f" (filtered by SE: {se_name})" if se_name else "")
        lines = ["| Account | AE | SE | CSM | Contract End | PS Projects |",
                 "|---------|----|----|-----|-------------|-------------|"]
        for r in rows:
            lines.append(
                f"| {r.get('org_name','')} "
                f"| {r.get('ae_name','—')} "
                f"| {r.get('se_name','—')} "
                f"| {r.get('csm_name','—')} "
                f"| {(r.get('contract_end_date') or '—')[:10]} "
                f"| {r.get('active_ps_projects', 0)} |"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"query_sfdc_accounts error: {e}"


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
            f"       a.active_ps_projects "
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
            "licensed_products":  licensed_products,
            "open_opportunities": open_opportunities,
        }
    except Exception:
        return {}
