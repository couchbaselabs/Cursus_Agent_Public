"""
Chainlit data layer backed by Couchbase.

Threads, steps, elements, users, and feedback are stored in a dedicated
`chat` scope within the user's configured CB bucket. Collections and
primary indexes are created automatically on first connection.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from chainlit.data.base import BaseDataLayer
from chainlit.data.utils import queue_until_user_message
from chainlit.logger import logger
from chainlit.types import (
    Feedback,
    PageInfo,
    PaginatedResponse,
    Pagination,
    ThreadDict,
    ThreadFilter,
)
from chainlit.user import PersistedUser, User

if TYPE_CHECKING:
    from chainlit.element import Element, ElementDict
    from chainlit.step import StepDict

_SCOPE = "chat"
_USERS    = "users"
_THREADS  = "threads"
_STEPS    = "steps"
_ELEMENTS = "elements"
_FEEDBACK = "feedback"
_ASSETS   = "assets"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CouchbaseDataLayer(BaseDataLayer):
    """
    Chainlit persistence backed by Couchbase.
    The `chat` scope is created automatically inside the configured bucket.
    """

    def __init__(self, cb_url: str, cb_bucket: str, cb_user: str, cb_pass: str, cb_tls: bool = False):
        self._url    = cb_url
        self._bucket_name = cb_bucket
        self._user   = cb_user
        self._pass   = cb_pass
        self._tls    = cb_tls
        self._cluster = None
        self._bucket  = None
        self._ready   = False

    # ── Connection / setup ────────────────────────────────────────────────────

    def _connect(self) -> None:
        """Synchronous connect + ensure collections exist (called once)."""
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions

        auth = PasswordAuthenticator(self._user, self._pass)
        opts = ClusterOptions(
            auth,
            timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10)),
        )
        self._cluster = Cluster(self._url, opts)
        self._cluster.wait_until_ready(timedelta(seconds=15))
        self._bucket = self._cluster.bucket(self._bucket_name)
        self._ensure_collections()
        self._ready = True

    def _ensure_collections(self) -> None:
        try:
            from couchbase.management.collections import CollectionSpec
            cm = self._bucket.collections()
            existing = {
                s.name: {c.name for c in s.collections}
                for s in cm.get_all_scopes()
            }
            if _SCOPE not in existing:
                cm.create_scope(_SCOPE)
                existing[_SCOPE] = set()
            for col in [_USERS, _THREADS, _STEPS, _ELEMENTS, _FEEDBACK, _ASSETS]:
                if col not in existing[_SCOPE]:
                    cm.create_collection(CollectionSpec(col, scope_name=_SCOPE))
            # Primary indexes (small data set — sufficient for this use case)
            for col in [_USERS, _THREADS, _STEPS, _ELEMENTS, _ASSETS]:
                try:
                    self._cluster.query(
                        f"CREATE PRIMARY INDEX IF NOT EXISTS "
                        f"ON `{self._bucket_name}`.`{_SCOPE}`.`{col}`"
                    ).execute()
                except Exception:
                    pass
        except Exception as exc:
            logger.warning(f"[CB DataLayer] collection setup warning: {exc}")

    def _setup(self) -> None:
        if not self._ready:
            self._connect()

    def _col(self, name: str):
        self._setup()
        return self._bucket.scope(_SCOPE).collection(name)

    def _q(self, sql: str, **params) -> list:
        self._setup()
        from couchbase.options import QueryOptions
        result = self._cluster.query(sql, QueryOptions(named_parameters=params))
        return list(result)

    async def _run(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    # ── Users ─────────────────────────────────────────────────────────────────

    async def get_user(self, identifier: str) -> Optional[PersistedUser]:
        def _get():
            try:
                doc = self._col(_USERS).get(f"user::{identifier}").content_as[dict]
                return PersistedUser(
                    id=doc["id"],
                    identifier=doc["identifier"],
                    metadata=doc.get("metadata", {}),
                    createdAt=doc["createdAt"],
                )
            except Exception:
                return None
        return await self._run(_get)

    async def create_user(self, user: User) -> Optional[PersistedUser]:
        def _create():
            key = f"user::{user.identifier}"
            # Preserve existing UUID so threads stay linked across logins
            try:
                existing = self._col(_USERS).get(key).content_as[dict]
                existing["metadata"] = user.metadata or existing.get("metadata", {})
                self._col(_USERS).upsert(key, existing)
                return PersistedUser(
                    id=existing["id"],
                    identifier=existing["identifier"],
                    metadata=existing.get("metadata", {}),
                    createdAt=existing["createdAt"],
                )
            except Exception:
                pass
            uid = str(uuid.uuid4())
            now = _now()
            doc = {
                "id": uid,
                "identifier": user.identifier,
                "metadata": user.metadata or {},
                "createdAt": now,
            }
            self._col(_USERS).upsert(key, doc)
            return PersistedUser(
                id=uid, identifier=user.identifier,
                metadata=user.metadata or {}, createdAt=now,
            )
        return await self._run(_create)

    # ── Threads ───────────────────────────────────────────────────────────────

    async def get_thread(self, thread_id: str) -> Optional[ThreadDict]:
        def _get():
            try:
                doc = self._col(_THREADS).get(f"thread::{thread_id}").content_as[dict]
            except Exception:
                return None
            steps = self._q(
                f"SELECT s.* FROM `{self._bucket_name}`.`{_SCOPE}`.`{_STEPS}` s "
                f"WHERE s.threadId = $tid ORDER BY s.createdAt ASC",
                tid=thread_id,
            )
            elements = self._q(
                f"SELECT e.* FROM `{self._bucket_name}`.`{_SCOPE}`.`{_ELEMENTS}` e "
                f"WHERE e.threadId = $tid",
                tid=thread_id,
            )
            return ThreadDict(
                id=doc["id"],
                createdAt=doc["createdAt"],
                name=doc.get("name"),
                userId=doc.get("userId"),
                userIdentifier=doc.get("userIdentifier"),
                tags=doc.get("tags"),
                metadata=doc.get("metadata"),
                steps=steps,
                elements=elements,
            )
        return await self._run(_get)

    async def get_thread_author(self, thread_id: str) -> str:
        def _get():
            try:
                doc = self._col(_THREADS).get(f"thread::{thread_id}").content_as[dict]
                if doc.get("userIdentifier"):
                    return doc["userIdentifier"]
                # Older threads only stored userId (UUID) — look up the identifier
                uid = doc.get("userId")
                if uid:
                    try:
                        rows = self._q(
                            f"SELECT u.identifier FROM `{self._bucket_name}`.`{_SCOPE}`.`{_USERS}` u "
                            f"WHERE u.id = $uid LIMIT 1",
                            uid=uid,
                        )
                        if rows:
                            return rows[0].get("identifier", "")
                    except Exception:
                        pass
                return ""
            except Exception:
                return ""
        return await self._run(_get)

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        def _update():
            key = f"thread::{thread_id}"
            try:
                doc = self._col(_THREADS).get(key).content_as[dict]
            except Exception:
                doc = {"id": thread_id, "createdAt": _now()}
            if name is not None:
                doc["name"] = name
            if user_id is not None:
                doc["userId"] = user_id
                # Resolve and cache the username string so get_thread_author works
                if not doc.get("userIdentifier"):
                    try:
                        rows = self._q(
                            f"SELECT u.identifier FROM `{self._bucket_name}`.`{_SCOPE}`.`{_USERS}` u "
                            f"WHERE u.id = $uid LIMIT 1",
                            uid=user_id,
                        )
                        if rows:
                            doc["userIdentifier"] = rows[0].get("identifier", "")
                    except Exception:
                        pass
            if metadata is not None:
                doc["metadata"] = metadata
            if tags is not None:
                doc["tags"] = tags
            self._col(_THREADS).upsert(key, doc)
        await self._run(_update)

    async def delete_thread(self, thread_id: str):
        def _delete():
            try:
                self._col(_THREADS).remove(f"thread::{thread_id}")
            except Exception:
                pass
        await self._run(_delete)

    async def list_threads(
        self, pagination: Pagination, filters: ThreadFilter
    ) -> PaginatedResponse[ThreadDict]:
        def _list():
            first  = pagination.first
            cursor = pagination.cursor

            where_parts = []
            params: Dict[str, Any] = {}

            if filters.userId:
                where_parts.append("t.userId = $userId")
                params["userId"] = filters.userId
            if filters.search:
                where_parts.append("LOWER(t.`name`) LIKE $search")
                params["search"] = f"%{filters.search.lower()}%"

            # Keyset pagination — page backward through createdAt DESC
            if cursor:
                try:
                    cur_doc = self._col(_THREADS).get(f"thread::{cursor}").content_as[dict]
                    params["cursorTs"] = cur_doc.get("createdAt", "")
                    where_parts.append("t.createdAt < $cursorTs")
                except Exception:
                    pass

            where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
            sql = (
                f"SELECT t.* FROM `{self._bucket_name}`.`{_SCOPE}`.`{_THREADS}` t "
                f"{where} ORDER BY t.createdAt DESC LIMIT {first + 1}"
            )
            rows = self._q(sql, **params)

            has_next = len(rows) > first
            rows = rows[:first]

            threads = [
                ThreadDict(
                    id=r["id"],
                    createdAt=r["createdAt"],
                    name=r.get("name"),
                    userId=r.get("userId"),
                    userIdentifier=r.get("userIdentifier"),
                    tags=r.get("tags"),
                    metadata=r.get("metadata"),
                    steps=[],
                    elements=[],
                )
                for r in rows
            ]
            return PaginatedResponse(
                pageInfo=PageInfo(
                    hasNextPage=has_next,
                    startCursor=threads[0]["id"] if threads else None,
                    endCursor=threads[-1]["id"] if threads else None,
                ),
                data=threads,
            )
        return await self._run(_list)

    # ── Steps ─────────────────────────────────────────────────────────────────

    @queue_until_user_message()
    async def create_step(self, step_dict: "StepDict"):
        def _create():
            sid = step_dict.get("id") or str(uuid.uuid4())
            step_dict["id"] = sid
            if not step_dict.get("createdAt"):
                step_dict["createdAt"] = _now()
            self._col(_STEPS).upsert(f"step::{sid}", dict(step_dict))
        await self._run(_create)

    @queue_until_user_message()
    async def update_step(self, step_dict: "StepDict"):
        def _update():
            sid = step_dict.get("id")
            if not sid:
                return
            try:
                existing = self._col(_STEPS).get(f"step::{sid}").content_as[dict]
                existing.update({k: v for k, v in step_dict.items() if v is not None})
                self._col(_STEPS).upsert(f"step::{sid}", existing)
            except Exception:
                self._col(_STEPS).upsert(f"step::{sid}", dict(step_dict))
        await self._run(_update)

    @queue_until_user_message()
    async def delete_step(self, step_id: str):
        def _delete():
            try:
                self._col(_STEPS).remove(f"step::{step_id}")
            except Exception:
                pass
        await self._run(_delete)

    # ── Elements ──────────────────────────────────────────────────────────────

    @queue_until_user_message()
    async def create_element(self, element: "Element"):
        def _create():
            eid = getattr(element, "id", None) or str(uuid.uuid4())
            element.id = eid
            doc = element.to_dict() if hasattr(element, "to_dict") else {}
            self._col(_ELEMENTS).upsert(f"element::{eid}", doc)
        await self._run(_create)

    async def get_element(
        self, thread_id: str, element_id: str
    ) -> Optional["ElementDict"]:
        def _get():
            try:
                return self._col(_ELEMENTS).get(f"element::{element_id}").content_as[dict]
            except Exception:
                return None
        return await self._run(_get)

    @queue_until_user_message()
    async def delete_element(self, element_id: str, thread_id: Optional[str] = None):
        def _delete():
            try:
                self._col(_ELEMENTS).remove(f"element::{element_id}")
            except Exception:
                pass
        await self._run(_delete)

    # ── Feedback ──────────────────────────────────────────────────────────────

    async def upsert_feedback(self, feedback: Feedback) -> str:
        def _upsert():
            fid = feedback.id or str(uuid.uuid4())
            doc = {
                "id": fid,
                "forId": feedback.forId,
                "threadId": feedback.threadId,
                "value": feedback.value,
                "comment": feedback.comment,
            }
            self._col(_FEEDBACK).upsert(f"feedback::{fid}", doc)
            return fid
        return await self._run(_upsert)

    async def delete_feedback(self, feedback_id: str) -> bool:
        def _delete():
            try:
                self._col(_FEEDBACK).remove(f"feedback::{feedback_id}")
                return True
            except Exception:
                return False
        return await self._run(_delete)

    # ── Assets (charts / tables stored as base64-encoded JSON blobs) ──────────

    async def save_asset(self, asset: dict) -> str:
        def _save():
            aid = asset.get("id") or str(uuid.uuid4())
            asset["id"] = aid
            if not asset.get("created_at"):
                asset["created_at"] = _now()
            self._col(_ASSETS).upsert(f"asset::{aid}", dict(asset))
            return aid
        return await self._run(_save)

    async def get_assets_for_thread(self, thread_id: str) -> list:
        def _get():
            try:
                return self._q(
                    f"SELECT a.* FROM `{self._bucket_name}`.`{_SCOPE}`.`{_ASSETS}` a "
                    f"WHERE a.thread_id = $tid ORDER BY a.created_at ASC",
                    tid=thread_id,
                )
            except Exception:
                return []
        return await self._run(_get)

    # ── Favorites ─────────────────────────────────────────────────────────────

    async def get_favorite_steps(self, user_id: str) -> List["StepDict"]:
        def _get():
            try:
                return self._q(
                    f"SELECT s.* FROM `{self._bucket_name}`.`{_SCOPE}`.`{_STEPS}` s "
                    f"WHERE s.metadata.favorite = true "
                    f"AND s.threadId IN ("
                    f"  SELECT RAW t.id FROM `{self._bucket_name}`.`{_SCOPE}`.`{_THREADS}` t "
                    f"  WHERE t.userId = $uid"
                    f")",
                    uid=user_id,
                )
            except Exception:
                return []
        return await self._run(_get)

    # ── Misc ──────────────────────────────────────────────────────────────────

    async def build_debug_url(self) -> str:
        return ""

    async def close(self) -> None:
        if self._cluster:
            try:
                self._cluster.close()
            except Exception:
                pass
        self._ready = False
