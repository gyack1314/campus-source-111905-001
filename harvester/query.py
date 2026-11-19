"""面向工作人员的只读视图：解释"专题为什么变小了"。

这些查询只读本地状态，不访问来源，也不使用来源的管理接口：

* ``explain_set``      专题现行成员 / 移出 / 撤回 / 待补采集。
* ``explain_record``   单个成果的原始响应、格式原文历史和成员关系历史。
* ``coverage_status``  各 (专题, 格式) 的已确认范围与待补范围。
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Optional

from .store import Store


def _qname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _row(row) -> Optional[dict]:
    if row is None:
        return None
    out = {k: row[k] for k in row.keys()}
    for key in ("detail", "header_sets"):
        if key in out and out[key] is not None:
            try:
                out[key] = json.loads(out[key])
            except (TypeError, ValueError):
                pass
    return out


def _rows(rows) -> list[dict]:
    return [_row(r) for r in rows]


def _metadata_snapshot(body: bytes, identifier: str) -> Optional[dict]:
    """从保存的原始响应中取出该成果当次的内容与 setSpec；找不到返回 None。"""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    for record in root.iter():
        if _qname(record.tag) != "record":
            continue
        header = None
        metadata = None
        for child in record:
            if _qname(child.tag) == "header":
                header = child
            elif _qname(child.tag) == "metadata":
                metadata = child
        if header is None:
            continue
        found = None
        sets = []
        for child in header:
            if _qname(child.tag) == "identifier":
                found = (child.text or "").strip()
            elif _qname(child.tag) == "setSpec":
                sets.append((child.text or "").strip())
        if found != identifier:
            continue
        content = None
        if metadata is not None and list(metadata):
            content = ET.tostring(list(metadata)[0], encoding="unicode")
        return {"metadata": content, "setSpecs": sets}
    return None


def explain_set(store: Store, source_url: str, set_spec: str) -> dict:
    """解释一个专题的现行构成，并把每种缺席归类到可核验的原因。"""
    memberships = _rows(store.query(
        "SELECT * FROM memberships WHERE source_url=? AND set_spec=?"
        " ORDER BY identifier", (source_url, set_spec)))
    effective, withdrawn, removed = [], [], []
    for membership in memberships:
        identifier = membership["identifier"]
        tombstone = store.tombstone(source_url, identifier)
        if tombstone is not None:
            withdrawn.append({**membership,
                              "membershipState": membership["state"],
                              "withdrawalDatestamp": tombstone["datestamp"],
                              "withdrawalRawId": tombstone["raw_id"]})
        elif membership["state"] == "removed":
            removed.append(membership)
        else:
            effective.append(membership)

    # 一条记录可能只在某一格式的完整采集中出现过；另一个格式尚未覆盖到它。
    # 这属于待补范围，不是删除。只按"实际采集过（至少一次完整结束）"的格式
    # 判断待补，避免把从未请求的格式也算成每条记录的缺口。
    advertised = [row["metadata_prefix"] for row in store.query(
        "SELECT DISTINCT metadata_prefix FROM runs WHERE source_url=?"
        " AND set_spec=? AND status='completed' ORDER BY metadata_prefix",
        (source_url, set_spec))]
    for membership in effective:
        identifier = membership["identifier"]
        availability = store.availability_for(source_url, identifier)
        formats = sorted(set(advertised) |
                         {row["metadata_prefix"] for row in
                          store.formats_for(source_url, identifier)})
        missing = [prefix for prefix in formats
                   if prefix not in availability
                   or not availability[prefix]["available"]]
        membership["formatsMissing"] = missing

    # 待补范围：某个格式没有完整结束过的采集，或该记录从未被任一完整格式见过。
    pending_formats = _rows(store.query(
        """SELECT metadata_prefix, MAX(finished_at) AS last_finished
             FROM runs
            WHERE source_url=? AND set_spec=? AND status NOT IN ('completed')
         GROUP BY metadata_prefix""", (source_url, set_spec)))
    incomplete_runs = _rows(store.query(
        """SELECT run_id, mode, metadata_prefix, status, started_at, detail
             FROM runs
            WHERE source_url=? AND set_spec=? AND status='incomplete'
         ORDER BY started_at DESC LIMIT 20""", (source_url, set_spec)))
    drafted = _rows(store.query(
        """SELECT run_id, set_spec, identifier, reason, detail, at FROM events
            WHERE source_url=? AND kind='membership-removal-deferred'
         ORDER BY id DESC LIMIT 50""", (source_url,)))
    return {
        "sourceUrl": source_url,
        "setSpec": set_spec,
        "effective": [
            {"identifier": m["identifier"],
             "observedVia": m["observed_via"],
             "evidenceRun": m["evidence_run"],
             "evidenceRawId": m["evidence_raw_id"],
             "formatsMissing": m.get("formatsMissing", [])}
            for m in effective
        ],
        "removed": [
            {"identifier": m["identifier"], "evidenceRun": m["evidence_run"],
             "lastSeen": m["last_seen"]}
            for m in removed
        ],
        "withdrawn": [
            {"identifier": m["identifier"],
             "membershipState": m["membershipState"],
             "withdrawalDatestamp": m["withdrawalDatestamp"],
             "withdrawalRawId": m["withdrawalRawId"]}
            for m in withdrawn
        ],
        "pendingCoverage": {
            "formatsWithoutCompleteHarvest": [p["metadata_prefix"]
                                              for p in pending_formats],
            "incompleteRuns": incomplete_runs,
            "deferredRemovals": drafted,
        },
        "totals": {
            "memberships": len(memberships),
            "effective": len(effective),
            "removed": len(removed),
            "withdrawn": len(withdrawn),
            "withMissingFormats": sum(1 for m in effective if m["formatsMissing"]),
        },
    }


def explain_record(store: Store, source_url: str, identifier: str) -> dict:
    formats = _rows(store.query(
        "SELECT * FROM record_formats WHERE source_url=? AND identifier=?"
        " ORDER BY metadata_prefix", (source_url, identifier)))
    availability = _rows(store.query(
        "SELECT * FROM current_availability WHERE source_url=? AND identifier=?"
        " ORDER BY metadata_prefix", (source_url, identifier)))
    history = _rows(store.query(
        "SELECT * FROM record_format_history WHERE source_url=? AND identifier=?"
        " ORDER BY id", (source_url, identifier)))
    # 历史表只保存内容摘要；把每一步的原文快照从原始响应里还原出来。
    for entry in history:
        raw = store.raw(entry["raw_id"]) if entry["raw_id"] else None
        entry["rawRequestUrl"] = raw["request_url"] if raw is not None else None
        entry["rawSha256"] = raw["sha256"] if raw is not None else None
        entry["rawBytes"] = len(raw["body"]) if raw is not None else None
        snapshot = _metadata_snapshot(raw["body"], identifier) if raw is not None else None
        entry["metadataSnapshot"] = snapshot["metadata"] if snapshot else None
        entry["setSpecsSnapshot"] = snapshot["setSpecs"] if snapshot else None
    memberships = _rows(store.query(
        "SELECT * FROM memberships WHERE source_url=? AND identifier=?"
        " ORDER BY set_spec", (source_url, identifier)))
    events = _rows(store.query(
        "SELECT * FROM events WHERE source_url=? AND identifier=? ORDER BY id",
        (source_url, identifier)))
    raw_ids = sorted({row["raw_id"] for row in history if row["raw_id"]} |
                     {row["evidence_raw_id"] for row in memberships
                      if row["evidence_raw_id"]} |
                     {row["raw_id"] for row in events if row["raw_id"]})
    raws = []
    for raw_id in raw_ids:
        row = store.raw(raw_id)
        if row is not None:
            raws.append({"id": raw_id, "verb": row["verb"],
                         "requestUrl": row["request_url"],
                         "fetchedAt": row["fetched_at"],
                         "sha256": row["sha256"],
                         "bytes": len(row["body"])})
    tombstone = _row(store.tombstone(source_url, identifier))
    return {
        "sourceUrl": source_url,
        "identifier": identifier,
        "withdrawn": tombstone is not None,
        "tombstone": tombstone,
        "formats": formats,
        "availability": availability,
        "formatHistory": history,
        "memberships": memberships,
        "events": events,
        "rawResponses": raws,
    }


def coverage_status(store: Store, source_url: str, set_spec: str) -> dict:
    """每个格式最近一次完整与不完整采集，以及水位。"""
    out = {}
    formats = [r["metadata_prefix"] for r in store.query(
        "SELECT DISTINCT metadata_prefix FROM runs WHERE source_url=? AND set_spec=?"
        " ORDER BY metadata_prefix", (source_url, set_spec))]
    for prefix in formats:
        confirmed = _row(store.query_one(
            """SELECT run_id, mode, finished_at, complete_list_size
                 FROM runs WHERE source_url=? AND set_spec=? AND metadata_prefix=?
                   AND status='completed'
              ORDER BY finished_at DESC LIMIT 1""",
            (source_url, set_spec, prefix)))
        pending = _rows(store.query(
            """SELECT run_id, mode, started_at, detail
                 FROM runs WHERE source_url=? AND set_spec=? AND metadata_prefix=?
                   AND status NOT IN ('completed')
              ORDER BY started_at DESC LIMIT 10""",
            (source_url, set_spec, prefix)))
        watermark = _row(store.watermark(source_url, set_spec, prefix))
        pending_members = store.query_one(
            """SELECT COUNT(*) AS n FROM memberships m
                WHERE m.source_url=? AND m.set_spec=? AND m.state='present'
                  AND EXISTS (SELECT 1 FROM current_availability a
                               WHERE a.source_url=m.source_url
                                 AND a.identifier=m.identifier
                                 AND a.metadata_prefix=?
                                 AND a.available=1)""",
            (source_url, set_spec, prefix))
        effective = store.query_one(
            """SELECT COUNT(*) AS n FROM memberships m
                WHERE m.source_url=? AND m.set_spec=? AND m.state='present'
                  AND NOT EXISTS (SELECT 1 FROM tombstones t
                                   WHERE t.source_url=m.source_url
                                     AND t.identifier=m.identifier)""",
            (source_url, set_spec))
        out[prefix] = {
            "lastConfirmed": confirmed,
            "pending": pending,
            # 已确认范围 vs 待补范围：水位之后的观察尚未完整结束。
            "confirmedRange": {
                "watermark": watermark["datestamp"] if watermark else None,
                "membersWithUsableFormat": (pending_members["n"]
                                            if pending_members else 0),
                "effectiveMembers": effective["n"] if effective else 0,
            },
            "pendingRange": {
                "openWindowStartsAt": watermark["datestamp"] if watermark else None,
                "incompleteRuns": [p["run_id"] for p in pending],
                "note": ("水位之前的内容已由完整页链确认；水位之后的增量尚未在"
                         "本次完整结束，属于待补范围" if pending else None),
            },
            "watermark": watermark,
        }
    return {"sourceUrl": source_url, "setSpec": set_spec, "formats": out}
