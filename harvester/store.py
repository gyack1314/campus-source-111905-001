"""收割状态存储（SQLite）。

核心原则：

* ``raw_responses`` 保存每一次 OAI 响应的原始字节和请求 URL/参数，只增不改。
  工作人员可以据此还原任意一次专题缩减的来历。
* ``record_formats`` 按 (identifier, metadataPrefix) 保存**每一种格式的原文**，
  同一 identifier 的两个格式同时存在，互不覆盖；删除头不会清掉历史原文。
* ``memberships`` 按 (setSpec, identifier) 保存成员关系及观察来源（全量/增量/
  GetRecord）。专题移出只改成员关系，不碰记录本身的删除状态。
* ``tombstones`` 保存撤回事实（来源保留删除头）。撤回对所有格式生效，但历史
  格式原文继续保留在 ``record_formats`` 中，供解释与审计。
* ``runs`` / ``pages`` 记录每次收割的覆盖范围与页链是否完成；未完成页链不会
  被当成完整清单。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
    source_url TEXT PRIMARY KEY,
    repository_name TEXT,
    protocol_version TEXT,
    deleted_record TEXT,
    granularity TEXT,
    earliest_datestamp TEXT,
    identify_raw_id INTEGER,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sets (
    source_url TEXT NOT NULL,
    set_spec TEXT NOT NULL,
    set_name TEXT,
    description TEXT,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (source_url, set_spec)
);
CREATE TABLE IF NOT EXISTS metadata_formats (
    source_url TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    schema_url TEXT,
    namespace TEXT,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (source_url, metadata_prefix)
);
CREATE TABLE IF NOT EXISTS raw_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL,
    run_id TEXT,
    verb TEXT NOT NULL,
    params TEXT NOT NULL,
    request_url TEXT NOT NULL,
    status INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    body BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_responses_source ON raw_responses(source_url, id);
CREATE TABLE IF NOT EXISTS record_formats (
    source_url TEXT NOT NULL,
    identifier TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    datestamp TEXT,
    header_sets TEXT NOT NULL,
    header_status TEXT,
    metadata TEXT,
    metadata_namespace TEXT,
    raw_id INTEGER,
    content_sha256 TEXT,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY (source_url, identifier, metadata_prefix)
);
CREATE INDEX IF NOT EXISTS record_formats_identifier
    ON record_formats(source_url, identifier);
CREATE TABLE IF NOT EXISTS tombstones (
    source_url TEXT NOT NULL,
    identifier TEXT NOT NULL,
    datestamp TEXT NOT NULL,
    set_spec TEXT,
    raw_id INTEGER,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (source_url, identifier)
);
CREATE TABLE IF NOT EXISTS current_availability (
    source_url TEXT NOT NULL,
    identifier TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    available INTEGER NOT NULL,
    reason TEXT NOT NULL,
    evidence_raw_id INTEGER,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (source_url, identifier, metadata_prefix)
);
CREATE TABLE IF NOT EXISTS memberships (
    source_url TEXT NOT NULL,
    set_spec TEXT NOT NULL,
    identifier TEXT NOT NULL,
    state TEXT NOT NULL,
    observed_via TEXT NOT NULL,
    evidence_run TEXT,
    evidence_raw_id INTEGER,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY (source_url, set_spec, identifier)
);
CREATE INDEX IF NOT EXISTS memberships_set ON memberships(source_url, set_spec, state);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    mode TEXT NOT NULL,
    set_spec TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    date_from TEXT,
    date_until TEXT,
    status TEXT NOT NULL,
    chain_complete INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    requests INTEGER NOT NULL DEFAULT 0,
    response_bytes INTEGER NOT NULL DEFAULT 0,
    complete_list_size INTEGER,
    detail TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    verb TEXT NOT NULL,
    raw_id INTEGER,
    status INTEGER,
    error_code TEXT,
    resumption_token TEXT,
    complete_list_size INTEGER,
    cursor INTEGER,
    record_count INTEGER,
    max_datestamp TEXT,
    fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_items (
    run_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    identifier TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    datestamp TEXT,
    header_sets TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    of_interest INTEGER NOT NULL DEFAULT 1,
    raw_id INTEGER,
    PRIMARY KEY (run_id, identifier, metadata_prefix)
);
CREATE TABLE IF NOT EXISTS watermarks (
    source_url TEXT NOT NULL,
    set_spec TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    datestamp TEXT NOT NULL,
    run_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source_url, set_spec, metadata_prefix)
);
CREATE TABLE IF NOT EXISTS record_format_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL,
    identifier TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    datestamp TEXT,
    header_status TEXT,
    content_sha256 TEXT,
    raw_id INTEGER,
    observed_at TEXT NOT NULL,
    run_id TEXT
);
CREATE INDEX IF NOT EXISTS record_format_history_key
    ON record_format_history(source_url, identifier, metadata_prefix, id);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    source_url TEXT NOT NULL,
    kind TEXT NOT NULL,
    set_spec TEXT,
    identifier TEXT,
    metadata_prefix TEXT,
    reason TEXT,
    detail TEXT NOT NULL DEFAULT '{}',
    raw_id INTEGER,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_set ON events(source_url, set_spec, id);
CREATE INDEX IF NOT EXISTS events_identifier ON events(source_url, identifier, id);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Store:
    """线程安全的 SQLite 存储。默认每次操作自动提交。"""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        parent = os.path.dirname(self.path)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, check_same_thread=False,
                                          isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self.connection.executescript(SCHEMA)
            row = self.connection.execute(
                "SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row is None:
                self.connection.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),))
            elif int(row["value"]) > SCHEMA_VERSION:
                raise RuntimeError(
                    f"存储结构版本 {row['value']} 高于本程序支持的 {SCHEMA_VERSION}")

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- 低层 ---------------------------------------------------------------

    def execute(self, sql: str, params=()):
        with self._lock:
            return self.connection.execute(sql, params)

    def executemany(self, sql: str, rows):
        with self._lock:
            return self.connection.executemany(sql, rows)

    def query(self, sql: str, params=()) -> list:
        with self._lock:
            return self.connection.execute(sql, params).fetchall()

    def query_one(self, sql: str, params=()):
        with self._lock:
            return self.connection.execute(sql, params).fetchone()

    # -- 来源描述 -----------------------------------------------------------

    def upsert_source(self, source_url: str, identify: dict) -> None:
        now = utcnow()
        with self._lock:
            self.connection.execute(
                """INSERT INTO sources(source_url, repository_name, protocol_version,
                       deleted_record, granularity, earliest_datestamp, identify_raw_id,
                       first_seen, last_seen)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_url) DO UPDATE SET
                       repository_name=excluded.repository_name,
                       protocol_version=excluded.protocol_version,
                       deleted_record=excluded.deleted_record,
                       granularity=excluded.granularity,
                       earliest_datestamp=excluded.earliest_datestamp,
                       identify_raw_id=excluded.identify_raw_id,
                       last_seen=excluded.last_seen""",
                (source_url, identify.get("repositoryName"),
                 identify.get("protocolVersion"), identify.get("deletedRecord"),
                 identify.get("granularity"), identify.get("earliestDatestamp"),
                 identify.get("_rawId"), now, now))

    def source(self, source_url: str):
        return self.query_one("SELECT * FROM sources WHERE source_url=?", (source_url,))

    def replace_sets(self, source_url: str, sets: list) -> None:
        """更新已知专题清单。已消失的专题不再被列举，但其历史成员关系保留。"""
        now = utcnow()
        with self._lock:
            self.connection.execute("BEGIN")
            try:
                self.connection.execute("DELETE FROM sets WHERE source_url=?", (source_url,))
                self.connection.executemany(
                    "INSERT INTO sets(source_url, set_spec, set_name, description, observed_at)"
                    " VALUES(?,?,?,?,?)",
                    [(source_url, s["setSpec"], s["setName"], s["description"], now)
                     for s in sets])
                self.connection.execute("COMMIT")
            except Exception:
                self.connection.execute("ROLLBACK")
                raise

    def replace_formats(self, source_url: str, formats: list) -> None:
        now = utcnow()
        with self._lock:
            self.connection.execute("BEGIN")
            try:
                self.connection.execute(
                    "DELETE FROM metadata_formats WHERE source_url=?", (source_url,))
                self.connection.executemany(
                    "INSERT INTO metadata_formats(source_url, metadata_prefix, schema_url,"
                    " namespace, observed_at) VALUES(?,?,?,?,?)",
                    [(source_url, f["metadataPrefix"], f["schema"],
                      f["metadataNamespace"], now) for f in formats])
                self.connection.execute("COMMIT")
            except Exception:
                self.connection.execute("ROLLBACK")
                raise

    # -- 原始响应 -----------------------------------------------------------

    def add_raw(self, source_url: str, run_id: Optional[str], verb: str,
                params: dict, request_url: str, status: int,
                sha256: str, body: bytes) -> int:
        with self._lock:
            cursor = self.connection.execute(
                """INSERT INTO raw_responses(source_url, run_id, verb, params, request_url,
                       status, fetched_at, sha256, body) VALUES(?,?,?,?,?,?,?,?,?)""",
                (source_url, run_id, verb,
                 json.dumps(params, ensure_ascii=False, sort_keys=True),
                 request_url, status, utcnow(), sha256, body))
            return int(cursor.lastrowid)

    def raw(self, raw_id: int):
        return self.query_one("SELECT * FROM raw_responses WHERE id=?", (raw_id,))

    # -- 记录与格式 ---------------------------------------------------------

    def upsert_format(self, source_url: str, record: dict, metadata_prefix: str,
                      raw_id: Optional[int], content_sha256: Optional[str]) -> None:
        header = record["header"]
        now = utcnow()
        with self._lock:
            self.connection.execute(
                """INSERT INTO record_formats(source_url, identifier, metadata_prefix,
                       datestamp, header_sets, header_status, metadata, metadata_namespace,
                       raw_id, content_sha256, first_seen, last_seen)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_url, identifier, metadata_prefix) DO UPDATE SET
                       datestamp=excluded.datestamp,
                       header_sets=excluded.header_sets,
                       header_status=excluded.header_status,
                       metadata=COALESCE(excluded.metadata, record_formats.metadata),
                       metadata_namespace=COALESCE(excluded.metadata_namespace,
                                                   record_formats.metadata_namespace),
                       raw_id=excluded.raw_id,
                       content_sha256=COALESCE(excluded.content_sha256,
                                               record_formats.content_sha256),
                       last_seen=excluded.last_seen""",
                (source_url, header["identifier"], metadata_prefix,
                 header["datestamp"], json.dumps(header["setSpecs"]),
                 header["status"], record.get("metadata"),
                 record.get("metadataNamespace"), raw_id, content_sha256, now, now))

    def formats_for(self, source_url: str, identifier: str) -> list:
        return self.query(
            "SELECT * FROM record_formats WHERE source_url=? AND identifier=?"
            " ORDER BY metadata_prefix", (source_url, identifier))

    def mark_format_deleted(self, source_url: str, identifier: str,
                            metadata_prefix: str, datestamp: Optional[str],
                            header_sets: list, raw_id: Optional[int]) -> None:
        """记录删除头观察；保留该格式最后一次已知原文，不删除历史。"""
        now = utcnow()
        previous = self.query_one(
            "SELECT datestamp, header_status FROM record_formats"
            " WHERE source_url=? AND identifier=? AND metadata_prefix=?",
            (source_url, identifier, metadata_prefix))
        self.execute(
            """INSERT INTO record_formats(source_url, identifier, metadata_prefix,
                   datestamp, header_sets, header_status, metadata, metadata_namespace,
                   raw_id, content_sha256, first_seen, last_seen)
               VALUES(?,?,?,?,?,'deleted',NULL,NULL,?,NULL,?,?)
               ON CONFLICT(source_url, identifier, metadata_prefix) DO UPDATE SET
                   header_status='deleted',
                   datestamp=excluded.datestamp,
                   header_sets=excluded.header_sets,
                   raw_id=excluded.raw_id,
                   last_seen=excluded.last_seen""",
            (source_url, identifier, metadata_prefix, datestamp,
             json.dumps(header_sets), raw_id, now, now))
        if (previous is not None and previous["header_status"] == "deleted"
                and previous["datestamp"] == datestamp):
            return
        self.add_format_history(source_url, identifier, metadata_prefix, datestamp,
                                "deleted", None, raw_id, None)
    def add_format_history(self, source_url: str, identifier: str, metadata_prefix: str,
                           datestamp: Optional[str], header_status: Optional[str],
                           content_sha256: Optional[str], raw_id: Optional[int],
                           run_id: Optional[str]) -> None:
        self.execute(
            """INSERT INTO record_format_history(source_url, identifier, metadata_prefix,
                   datestamp, header_status, content_sha256, raw_id, observed_at, run_id)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (source_url, identifier, metadata_prefix, datestamp, header_status,
             content_sha256, raw_id, utcnow(), run_id))

    def format_history(self, source_url: str, identifier: str,
                       metadata_prefix: Optional[str] = None) -> list:
        if metadata_prefix:
            return self.query(
                """SELECT * FROM record_format_history WHERE source_url=? AND identifier=?
                       AND metadata_prefix=? ORDER BY id""",
                (source_url, identifier, metadata_prefix))
        return self.query(
            "SELECT * FROM record_format_history WHERE source_url=? AND identifier=?"
            " ORDER BY id", (source_url, identifier))

    # -- 当前可用性 ---------------------------------------------------------

    def set_availability(self, source_url: str, identifier: str, metadata_prefix: str,
                         available: bool, reason: str, raw_id: Optional[int]) -> None:
        with self._lock:
            self.connection.execute(
                """INSERT INTO current_availability(source_url, identifier, metadata_prefix,
                       available, reason, evidence_raw_id, observed_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(source_url, identifier, metadata_prefix) DO UPDATE SET
                       available=excluded.available,
                       reason=excluded.reason,
                       evidence_raw_id=excluded.evidence_raw_id,
                       observed_at=excluded.observed_at""",
                (source_url, identifier, metadata_prefix, 1 if available else 0,
                 reason, raw_id, utcnow()))

    def availability_for(self, source_url: str, identifier: str) -> dict:
        rows = self.query(
            "SELECT * FROM current_availability WHERE source_url=? AND identifier=?",
            (source_url, identifier))
        return {r["metadata_prefix"]: r for r in rows}

    # -- 撤回 ---------------------------------------------------------------

    def add_tombstone(self, source_url: str, identifier: str, datestamp: str,
                      set_spec: Optional[str], raw_id: Optional[int]) -> None:
        with self._lock:
            self.connection.execute(
                """INSERT INTO tombstones(source_url, identifier, datestamp, set_spec,
                       raw_id, observed_at) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(source_url, identifier) DO UPDATE SET
                       datestamp=excluded.datestamp,
                       set_spec=COALESCE(excluded.set_spec, tombstones.set_spec),
                       raw_id=excluded.raw_id,
                       observed_at=excluded.observed_at""",
                (source_url, identifier, datestamp, set_spec, raw_id, utcnow()))

    def tombstone(self, source_url: str, identifier: str):
        return self.query_one(
            "SELECT * FROM tombstones WHERE source_url=? AND identifier=?",
            (source_url, identifier))

    # -- 成员关系 -----------------------------------------------------------

    def set_membership(self, source_url: str, set_spec: str, identifier: str,
                       state: str, observed_via: str, evidence_run: Optional[str],
                       evidence_raw_id: Optional[int]) -> None:
        now = utcnow()
        with self._lock:
            self.connection.execute(
                """INSERT INTO memberships(source_url, set_spec, identifier, state,
                       observed_via, evidence_run, evidence_raw_id, first_seen, last_seen)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_url, set_spec, identifier) DO UPDATE SET
                       state=excluded.state,
                       observed_via=excluded.observed_via,
                       evidence_run=excluded.evidence_run,
                       evidence_raw_id=excluded.evidence_raw_id,
                       last_seen=excluded.last_seen""",
                (source_url, set_spec, identifier, state, observed_via,
                 evidence_run, evidence_raw_id, now, now))

    def membership(self, source_url: str, set_spec: str, identifier: str):
        return self.query_one(
            "SELECT * FROM memberships WHERE source_url=? AND set_spec=? AND identifier=?",
            (source_url, set_spec, identifier))

    def members(self, source_url: str, set_spec: str, state: str = "present") -> list:
        return self.query(
            "SELECT * FROM memberships WHERE source_url=? AND set_spec=? AND state=?"
            " ORDER BY identifier", (source_url, set_spec, state))

    # -- 运行与页 -----------------------------------------------------------

    def start_run(self, run_id: str, source_url: str, mode: str, set_spec: str,
                  metadata_prefix: str, date_from: Optional[str],
                  date_until: Optional[str]) -> None:
        self.execute(
            """INSERT INTO runs(run_id, source_url, mode, set_spec, metadata_prefix,
                   date_from, date_until, status, started_at) VALUES(?,?,?,?,?,?,?,?,?)""",
            (run_id, source_url, mode, set_spec, metadata_prefix, date_from,
             date_until, "running", utcnow()))

    def set_run_window(self, run_id: str, date_from: Optional[str],
                       date_until: Optional[str]) -> None:
        self.execute("UPDATE runs SET date_from=?, date_until=? WHERE run_id=?",
                     (date_from, date_until, run_id))

    def add_page(self, run_id: str, sequence: int, page, attempt: int = 1) -> int:
        fact = page.fact
        stamps = [r["header"]["datestamp"] for r in page.records
                  if r["header"].get("datestamp")]
        with self._lock:
            cursor = self.connection.execute(
                """INSERT INTO pages(run_id, sequence, attempt, verb, raw_id, status,
                       error_code, resumption_token, complete_list_size, cursor,
                       record_count, max_datestamp, fetched_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, sequence, attempt, fact.verb, page.raw_id, fact.status,
                 fact.error_code, fact.resumption_token, fact.complete_list_size,
                 fact.cursor, len(page.records), max(stamps) if stamps else None,
                 utcnow()))
            self.connection.execute(
                "UPDATE runs SET requests=requests+1, response_bytes=response_bytes+? WHERE run_id=?",
                (len(page.raw), run_id))
            return int(cursor.lastrowid)

    def finish_run(self, run_id: str, status: str, chain_complete: bool,
                   complete_list_size: Optional[int], detail: dict) -> None:
        self.execute(
            """UPDATE runs SET status=?, chain_complete=?, finished_at=?,
                   complete_list_size=?, detail=? WHERE run_id=?""",
            (status, 1 if chain_complete else 0, utcnow(), complete_list_size,
             json.dumps(detail, ensure_ascii=False), run_id))

    def run(self, run_id: str):
        return self.query_one("SELECT * FROM runs WHERE run_id=?", (run_id,))

    def newest_observed_datestamp(self, run_id: str) -> Optional[str]:
        row = self.query_one(
            "SELECT MAX(max_datestamp) AS stamp FROM pages WHERE run_id=? AND attempt="
            "(SELECT MAX(attempt) FROM pages WHERE run_id=?)", (run_id, run_id))
        if row is None or row["stamp"] is None:
            return None
        return row["stamp"]

    def pages(self, run_id: str) -> list:
        return self.query(
            "SELECT * FROM pages WHERE run_id=? ORDER BY attempt, sequence", (run_id,))

    def run_items(self, run_id: str) -> list:
        return self.query("SELECT * FROM run_items WHERE run_id=?", (run_id,))

    def add_run_item(self, run_id: str, source_url: str, identifier: str,
                     metadata_prefix: str, record: dict, raw_id: Optional[int],
                     attempt: int = 1, of_interest: bool = True) -> None:
        header = record["header"]
        self.execute(
            """INSERT INTO run_items(run_id, source_url, identifier, metadata_prefix,
                   datestamp, header_sets, deleted, of_interest, raw_id)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id, identifier, metadata_prefix) DO UPDATE SET
                   datestamp=excluded.datestamp, header_sets=excluded.header_sets,
                   deleted=excluded.deleted, of_interest=excluded.of_interest,
                   raw_id=excluded.raw_id""",
            (run_id, source_url, identifier, metadata_prefix, header["datestamp"],
             json.dumps(header["setSpecs"]), 1 if record["deleted"] else 0,
             1 if of_interest else 0, raw_id))
        if attempt > 1:
            self.add_event(
                source_url, "item-reobserved-after-restart", run_id=run_id,
                identifier=identifier, metadata_prefix=metadata_prefix,
                reason="resumptionRestart",
                detail={"attempt": attempt, "datestamp": header["datestamp"]},
                raw_id=raw_id)

    def latest_run(self, source_url: str, mode: str, set_spec: str,
                   metadata_prefix: str):
        return self.query_one(
            """SELECT * FROM runs WHERE source_url=? AND mode=? AND set_spec=?
                   AND metadata_prefix=? AND status='completed'
               ORDER BY finished_at DESC, started_at DESC LIMIT 1""",
            (source_url, mode, set_spec, metadata_prefix))

    # -- 水位 ---------------------------------------------------------------

    def watermark(self, source_url: str, set_spec: str, metadata_prefix: str):
        return self.query_one(
            "SELECT * FROM watermarks WHERE source_url=? AND set_spec=? AND metadata_prefix=?",
            (source_url, set_spec, metadata_prefix))

    def set_watermark(self, source_url: str, set_spec: str, metadata_prefix: str,
                      datestamp: str, run_id: Optional[str]) -> None:
        self.execute(
            """INSERT INTO watermarks(source_url, set_spec, metadata_prefix, datestamp,
                   run_id, updated_at) VALUES(?,?,?,?,?,?)
               ON CONFLICT(source_url, set_spec, metadata_prefix) DO UPDATE SET
                   datestamp=excluded.datestamp, run_id=excluded.run_id,
                   updated_at=excluded.updated_at""",
            (source_url, set_spec, metadata_prefix, datestamp, run_id, utcnow()))

    # -- 事件 ---------------------------------------------------------------

    def add_event(self, source_url: str, kind: str, *, run_id: Optional[str] = None,
                  set_spec: Optional[str] = None, identifier: Optional[str] = None,
                  metadata_prefix: Optional[str] = None, reason: Optional[str] = None,
                  detail: Optional[dict] = None, raw_id: Optional[int] = None) -> int:
        with self._lock:
            cursor = self.connection.execute(
                """INSERT INTO events(run_id, source_url, kind, set_spec, identifier,
                       metadata_prefix, reason, detail, raw_id, at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (run_id, source_url, kind, set_spec, identifier, metadata_prefix,
                 reason, json.dumps(detail or {}, ensure_ascii=False), raw_id, utcnow()))
            return int(cursor.lastrowid)

    def events(self, source_url: str, *, set_spec: Optional[str] = None,
               identifier: Optional[str] = None, kind: Optional[str] = None,
               since_id: int = 0, limit: int = 200) -> list:
        clauses = ["source_url=?", "id>?"]
        params: list = [source_url, since_id]
        if set_spec:
            clauses.append("set_spec=?")
            params.append(set_spec)
        if identifier:
            clauses.append("identifier=?")
            params.append(identifier)
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        params.append(limit)
        return self.query(
            "SELECT * FROM events WHERE " + " AND ".join(clauses) +
            " ORDER BY id LIMIT ?", params)
