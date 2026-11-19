"""收割引擎：把 OAI-PMH 协议事实变成可解释、可续跑的采集状态。

三种运行模式：

``initial``
    首次建库。对选定专题做全量列举（不设日期上下限），建立各格式原文、
    专题成员关系与水位。只有完整结束的页链才会推进水位。

``update``
    继续更新。按 (专题, 格式) 水位做增量列举，日期下界回退
    ``overlap_seconds`` 以覆盖同一秒内的重复更新；上界默认开放。
    列举范围是整个来源（不只是本专题），因为撤回是全局事实，而已经移出
    专题的记录不会再出现在专题列举里。增量中没有出现的旧记录**不会**被
    当成删除，专题移出也不会在这里判定。

``reconcile``
    专题核对。再次全量列举（列表范围与 ``update`` 相同，在整个来源内观察，
    按专题归属过滤），只有在全部请求格式页链都完整结束时，才把"在完整列举中
    未出现"的旧成员判为移出，而且只影响这一个专题。撤回（删除头）是全局事实，
    会同时使所有格式失效；移出与撤回分别记录。

无论哪种模式，原始响应、每个格式的原文、成员关系和现行可用性都分别落盘，
并通过 run/raw id 相互关联。
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from .client import OAIClient, Page, HarvestError
from .store import Store, utcnow

MODES = ("initial", "update", "reconcile")
STAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _sha256(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_stamp(value: str) -> datetime:
    return datetime.strptime(value, STAMP_FORMAT).replace(tzinfo=timezone.utc)


def _format_stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(STAMP_FORMAT)


class Harvester:
    def __init__(self, base_url: str, state, *, formats: Optional[Iterable[str]] = None,
                 timeout: float = 30.0, retries: int = 3, backoff: float = 0.25,
                 overlap_seconds: int = 5, max_restarts: int = 2,
                 session=None):
        self.base_url = base_url.rstrip("/")
        if isinstance(state, Store):
            self.store = state
            self._owns_store = False
        else:
            path = os.path.abspath(str(state))
            if os.path.isdir(path) or not os.path.splitext(path)[1]:
                os.makedirs(path, exist_ok=True)
                path = os.path.join(path, "harvest.sqlite3")
            self.store = Store(path)
            self._owns_store = True
        self.formats = list(formats) if formats else None
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.overlap_seconds = max(0, int(overlap_seconds))
        self.max_restarts = max(0, int(max_restarts))
        self.session = session
        self._active_run: Optional[str] = None
        self._active_job: Optional[str] = None
        self.client = OAIClient(self.base_url, timeout=timeout, retries=retries,
                                backoff=backoff, session=session,
                                recorder=self._record_raw)

    # -- 生命周期 -----------------------------------------------------------

    def close(self) -> None:
        if self._owns_store:
            self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _record_raw(self, params: dict, verb: str, status: int, url: str,
                    body: bytes) -> int:
        digest = hashlib.sha256(body).hexdigest()
        run_id = self._active_run or self._active_job
        return self.store.add_raw(self.store_source, run_id, verb, params, url,
                                  status, digest, body)

    @property
    def store_source(self) -> str:
        return self.base_url

    # -- 公共入口 -----------------------------------------------------------

    def run(self, mode: str = "initial", *, set_spec: str,
            formats: Optional[Iterable[str]] = None,
            date_until: Optional[str] = None) -> dict:
        if mode not in MODES:
            raise ValueError(f"未知模式 {mode!r}，应为 {', '.join(MODES)}")
        if not set_spec:
            raise ValueError("必须指定专题 setSpec")
        if date_until is not None:
            try:
                _parse_stamp(date_until)
            except ValueError as exc:
                raise ValueError("日期上界必须是 YYYY-MM-DDThh:mm:ssZ") from exc
        if mode in ("initial", "reconcile") and date_until is not None:
            raise ValueError(f"{mode} 必须是全量列举，不能设置日期上界")

        job_id = self._new_job_id(mode, set_spec)
        self._active_job = job_id
        self._active_run = None
        started = utcnow()
        warnings: list[str] = []

        identify = self.client.identify()
        self.store.upsert_source(self.base_url, identify)
        if identify.get("deletedRecord") != "persistent":
            warnings.append(
                "来源未声明 persistent 删除语义；撤回判定的历史完整性需人工复核")

        sets = {s["setSpec"]: s for s in self.client.list_sets()}
        self.store.replace_sets(self.base_url, list(sets.values()))
        if set_spec not in sets:
            # 保留本次发现阶段的原始响应，报告里能解释为什么没有开始列举。
            report = {
                "jobId": job_id,
                "source": {"baseUrl": self.base_url,
                           "repositoryName": identify.get("repositoryName")},
                "set": {"setSpec": set_spec, "setName": None},
                "mode": mode,
                "error": {"code": "unknownSet",
                          "message": f"来源的 ListSets 未声明专题 {set_spec!r}",
                          "availableSets": sorted(sets)},
                "startedAt": started,
                "finishedAt": utcnow(),
                "coverage": {"confirmed": [], "pending": [],
                             "membership": {"removed": [], "applied": False}},
                "warnings": warnings,
                "reports": self._report_path(job_id),
            }
            self._save_report(report)
            self._active_job = None
            raise HarvestError(
                f"来源没有专题 {set_spec!r}",
                code="unknownSet",
                detail={"setSpec": set_spec, "available": sorted(sets),
                        "report": report["reports"]},
            )
        set_name = sets[set_spec]["setName"]

        advertised = {f["metadataPrefix"]: f for f in self.client.list_metadata_formats()}
        self.store.replace_formats(self.base_url, list(advertised.values()))
        requested = list(formats) if formats else (self.formats or list(advertised))
        # 去重但保持顺序
        requested = list(dict.fromkeys(requested))

        # 增量与核对按整个来源列举：撤回是全局事实，且已移出专题的记录不会出现在
        # 专题列举里；只有全来源列举才能看到它们的删除头。首次建库只需专题范围。
        scope = "set" if mode == "initial" else "repository"
        runs = []
        for prefix in requested:
            try:
                runs.append(self._harvest_format(
                    job_id, prefix, mode, set_spec, advertised, date_until,
                    warnings, scope))
            except HarvestError as exc:
                run_id = f"{job_id}:{prefix}"
                if self.store.run(run_id) is None:
                    self.store.start_run(run_id, self.base_url, mode, set_spec,
                                         prefix, None, date_until)
                self.store.finish_run(run_id, "failed", False, None, {
                    "errorCode": exc.code,
                    "error": str(exc),
                    "note": "请求失败，本次页链未开始或未完成",
                })
                self.store.add_event(
                    self.base_url, "harvest-failed", run_id=run_id,
                    set_spec=set_spec, metadata_prefix=prefix, reason=exc.code,
                    detail={"error": str(exc), **exc.detail})
                warnings.append(
                    f"格式 {prefix} 请求失败（{exc.code}）；不推进水位、不判定移出")
                runs.append(self._run_summary(run_id, prefix, "failed", False, []))

        # 专题核对：只有能观察该专题的格式都完整结束，才能确认移出。
        membership_changes = {"removed": [], "applied": False}
        if mode == "reconcile":
            membership_changes = self._apply_reconcile_removals(
                job_id, set_spec, runs, warnings, started)

        started_counts = {
            "events": self.store.query_one(
                "SELECT COUNT(*) AS n FROM events WHERE source_url=? AND run_id LIKE ?",
                (self.base_url, job_id + "%"))["n"],
        }
        report = self._build_report(
            job_id, mode, set_spec, set_name, identify, requested, advertised,
            runs, date_until, started, warnings, membership_changes,
            started_counts)
        self._save_report(report)
        self._active_job = None
        self._active_run = None
        return report

    def _earliest_datestamp(self) -> Optional[str]:
        source = self.store.source(self.base_url)
        if source is None or not source["earliest_datestamp"]:
            return None
        return source["earliest_datestamp"]

    # -- 单个格式的页链 -----------------------------------------------------

    def _new_job_id(self, mode: str, set_spec: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{stamp}-{mode}-{set_spec}-{uuid.uuid4().hex[:6]}"

    def _harvest_format(self, job_id: str, prefix: str, mode: str, set_spec: str,
                        advertised: dict, date_until: Optional[str],
                        warnings: list, scope: str = "set") -> dict:
        run_id = f"{job_id}:{prefix}"
        self._active_run = run_id

        date_from = None
        bootstrap = False
        if mode == "update":
            line = self.store.watermark(self.base_url, set_spec, prefix)
            if line is None:
                bootstrap = True
                warnings.append(
                    f"格式 {prefix} 在专题 {set_spec} 没有水位，本次按全量列举建立基线")
            else:
                floor = self._earliest_datestamp() or line["datestamp"]
                low = _parse_stamp(line["datestamp"]) - timedelta(
                    seconds=self.overlap_seconds)
                lowest = _format_stamp(low)
                # 无论怎样回退，都不应越过来源声明的最早日期。
                date_from = max(lowest, floor)

        self.store.start_run(run_id, self.base_url, mode, set_spec, prefix,
                             date_from, date_until)

        if mode == "update" and bootstrap:
            # 没有水位时不能假装这是"增量"：明确按全量列举，日期上下界为空，
            # 并在报告中说明本次是建立基线。
            date_from = None
            self.store.set_run_window(run_id, None, date_until)
            self.store.add_event(
                self.base_url, "bootstrap-baseline", run_id=run_id,
                set_spec=set_spec, metadata_prefix=prefix, reason="noWatermark")

        if prefix not in advertised:
            self.store.finish_run(run_id, "unsupported", False, None, {
                "reason": "ListMetadataFormats 未声明该格式",
                "errorCode": "cannotDisseminateFormat",
            })
            self.store.add_event(
                self.base_url, "format-unsupported", run_id=run_id, set_spec=set_spec,
                metadata_prefix=prefix, reason="notAdvertised",
                detail={"requested": prefix, "advertised": sorted(advertised)})
            return self._run_summary(run_id, prefix, "unsupported", False, [])
        if advertised[prefix].get("metadataNamespace") is None:
            warnings.append(f"格式 {prefix} 未声明元数据命名空间")

        restarts = 0
        raw_ids: list[int] = []
        window: list[str] = []
        while True:
            page = self.client.list_records(
                prefix,
                set_spec=set_spec if scope == "set" else None,
                date_from=date_from, until=date_until)
            self._note_raw(raw_ids, page)
            self._note_date(window, page)
            attempt = restarts + 1
            self.store.add_page(run_id, 1, page, attempt)

            if page.is_token_expired:
                if restarts < self.max_restarts:
                    restarts += 1
                    self._token_restart(run_id, prefix, set_spec, page, restarts)
                    continue  # 从第一页重来；写入均为幂等 upsert
                return self._finish_failed(
                    run_id, prefix, mode, set_spec, page, restarts, raw_ids,
                    "badResumptionToken", warnings)

            if page.has_error and not page.is_empty and not page.is_unsupported:
                return self._finish_failed(
                    run_id, prefix, mode, set_spec, page, restarts, raw_ids,
                    page.fact.error_code, warnings)

            if page.is_unsupported:
                self.store.finish_run(run_id, "unsupported", False, None, {
                    "reason": "来源拒绝该格式（不等同于成果撤回）",
                    "errorCode": page.fact.error_code,
                    "errorText": page.fact.error_text,
                })
                self.store.add_event(
                    self.base_url, "format-unsupported", run_id=run_id,
                    set_spec=set_spec, metadata_prefix=prefix,
                    reason="cannotDisseminateFormat",
                    detail={"error": page.fact.error_text}, raw_id=page.raw_id)
                warnings.append(
                    f"格式 {prefix} 在来源不可分发；相关记录保持原可用性，不算撤回")
                return self._run_summary(run_id, prefix, "unsupported", False, raw_ids)

            self._ingest_page(run_id, prefix, mode, set_spec, page, scope, attempt)

            restarted = False
            while page.fact.resumption_token:
                token = page.fact.resumption_token
                page = self.client.resumption_page("ListRecords", token)
                self._note_raw(raw_ids, page)
                self._note_date(window, page)
                self.store.add_page(run_id, self._next_sequence(run_id, attempt),
                                    page, attempt)
                if page.is_token_expired:
                    if restarts < self.max_restarts:
                        restarts += 1
                        self._token_restart(run_id, prefix, set_spec, page, restarts)
                        restarted = True
                        break
                    return self._finish_failed(
                        run_id, prefix, mode, set_spec, page, restarts, raw_ids,
                        "badResumptionToken", warnings)
                if page.has_error:
                    return self._finish_failed(
                        run_id, prefix, mode, set_spec, page, restarts, raw_ids,
                        page.fact.error_code, warnings)
                if page.fact.resumption_token == token:
                    return self._finish_failed(
                        run_id, prefix, mode, set_spec, page, restarts, raw_ids,
                        "resumptionTokenLoop", warnings)
                self._ingest_page(run_id, prefix, mode, set_spec, page, scope, attempt)
            if restarted:
                continue
            # 页链自然结束（末页没有 token），才能声明完整清单
            break

        observed = self._observed_identifiers(run_id)
        # 水位按"本页链观察到的最大 datestamp"推进（包括不属于本专题的记录），
        # 而不是只按收集到的记录；这样下次增量的窗口才和协议范围一致。
        newest = self.store.newest_observed_datestamp(run_id)

        if page.is_empty:
            detail = {"reason": "范围内没有记录（noRecordsMatch，不是删除）",
                      "restarts": restarts}
            self.store.finish_run(run_id, "completed", True, 0, detail)
        else:
            self._advance_watermark(set_spec, prefix, newest, mode, run_id, page)
            detail = {"restarts": restarts,
                      "completeListSize": page.fact.complete_list_size,
                      "note": "completeListSize 只代表该次响应匹配到的记录数，"
                              "不代表本程序已收集的专题成果数"}
            self.store.finish_run(run_id, "completed", True,
                                  page.fact.complete_list_size, detail)
        self.store.add_event(
            self.base_url, "format-harvested", run_id=run_id, set_spec=set_spec,
            metadata_prefix=prefix, reason=mode,
            detail={"collected": len(observed),
                    "listed": len(self._listed_identifiers(run_id)),
                    "restarts": restarts, "dateFrom": date_from,
                    "dateUntil": date_until, "bootstrap": bootstrap,
                    "scope": scope})
        listed = self._listed_identifiers(run_id)
        return self._run_summary(run_id, prefix, "completed", True, raw_ids,
                                 observed=len(observed), bootstrap=bootstrap,
                                 listed=len(listed),
                                 date_from=date_from, date_until=date_until,
                                 restarts=restarts,
                                 complete_list_size=page.fact.complete_list_size,
                                 window=window)

    def _finish_failed(self, run_id, prefix, mode, set_spec, page, restarts,
                       raw_ids, code, warnings) -> dict:
        reason = page.fact.error_text or code
        self.store.finish_run(run_id, "incomplete", False,
                              page.fact.complete_list_size, {
                                  "errorCode": code, "errorText": reason,
                                  "restarts": restarts,
                                  "note": "页链未结束，本次不得作为完整清单"})
        self.store.add_event(
            self.base_url, "harvest-incomplete", run_id=run_id, set_spec=set_spec,
            metadata_prefix=prefix, reason=code,
            detail={"error": reason, "restarts": restarts}, raw_id=page.raw_id)
        warnings.append(
            f"格式 {prefix} 的页链未完整结束（{code}）；已观察到的正事实保留，"
            "但水位、成员移出和完整性声明都不更新")
        return self._run_summary(run_id, prefix, "incomplete", False, raw_ids,
                                 restarts=restarts)

    @staticmethod
    def _note_raw(raw_ids: list, page: Page) -> None:
        if page.raw_id is not None and page.raw_id not in raw_ids:
            raw_ids.append(page.raw_id)

    def _next_sequence(self, run_id: str, attempt: int = 1) -> int:
        row = self.store.query_one(
            "SELECT COALESCE(MAX(sequence), 0) AS n FROM pages"
            " WHERE run_id=? AND attempt=?", (run_id, attempt))
        return int(row["n"]) + 1

    def _token_restart(self, run_id: str, prefix: str, set_spec: str,
                       page: Page, restarts: int) -> None:
        self.store.add_event(
            self.base_url, "token-expired", run_id=run_id, set_spec=set_spec,
            metadata_prefix=prefix, reason="restarting",
            detail={"restart": restarts, "url": page.fact.url},
            raw_id=page.raw_id)

    @staticmethod
    def _note_date(window: list, page: Page) -> None:
        if page.response_date and page.response_date not in window:
            window.append(page.response_date)

    def _run_summary(self, run_id, prefix, status, complete, raw_ids, *, observed=0,
                     bootstrap=False, date_from=None, date_until=None, restarts=0,
                     complete_list_size=None, window=None, listed=0,
                     failed=False) -> dict:
        raw_ids = list(dict.fromkeys(raw_ids))
        return {
            "runId": run_id,
            "metadataPrefix": prefix,
            "status": status,
            "chainComplete": bool(complete),
            "collectedIdentifiers": observed,
            "listedIdentifiers": listed,
            "rawResponseIds": raw_ids,
            "dateFrom": date_from,
            "dateUntil": date_until,
            "restarts": restarts,
            "completeListSize": complete_list_size,
            "bootstrap": bootstrap,
            # 只有"来源明确不支持该格式"才不参与成员判定；请求失败或页链
            # 中断必须阻塞移出判定（它们仍是能观察该专题的格式，只是这次没完成）。
            "supportsSet": status != "unsupported",
            "responseDates": window or [],
        }

    # -- 单页入库 -----------------------------------------------------------

    def _ingest_page(self, run_id: str, prefix: str, mode: str, set_spec: str,
                     page: Page, scope: str = "set", attempt: int = 1) -> None:
        for record in page.records:
            header = record["header"]
            identifier = header["identifier"]
            # 增量/核对列举整个来源，但只收集与选定专题有关的成果（本次落在该
            # 专题，或曾被记录在该专题）。这样既能看见撤回（撤回是全来源事实），
            # 又不会把整个来源的其他专题尽收进来。
            interesting = (scope != "repository"
                           or self._of_interest(identifier, header["setSpecs"],
                                                set_spec))
            self.store.add_run_item(run_id, self.base_url, identifier, prefix,
                                    record, page.raw_id, attempt, interesting)
            if not interesting:
                continue
            if record["deleted"]:
                self._record_withdrawal(run_id, prefix, set_spec, record, page.raw_id,
                                        mode)
                continue
            if self.store.tombstone(self.base_url, identifier) is not None:
                # 撤回是持久事实；之后若又看到正记录，只能报告矛盾，不能复活。
                self.store.add_event(
                    self.base_url, "post-withdrawal-present", run_id=run_id,
                    set_spec=set_spec, identifier=identifier,
                    metadata_prefix=prefix, reason="sourceContradiction",
                    detail={"datestamp": header["datestamp"],
                            "setSpecs": header["setSpecs"]},
                    raw_id=page.raw_id)
                continue
            self._record_format(run_id, prefix, record, page.raw_id)
            self._record_memberships(run_id, mode, set_spec, record, page.raw_id,
                                     scope)

    def _of_interest(self, identifier: str, header_sets: list,
                     set_spec: str) -> bool:
        """该成果是否与选定专题有关：本次在专题中，或曾经被记录在专题中。"""
        if set_spec in header_sets:
            return True
        return self.store.membership(self.base_url, set_spec,
                                     identifier) is not None

    def _record_format(self, run_id: str, prefix: str, record: dict,
                       raw_id: Optional[int]) -> None:
        header = record["header"]
        digest = _sha256(record.get("metadata"))
        previous = self.store.query_one(
            """SELECT * FROM record_formats WHERE source_url=? AND identifier=?
                   AND metadata_prefix=?""",
            (self.base_url, header["identifier"], prefix))
        self.store.upsert_format(self.base_url, record, prefix, raw_id, digest)
        changed = (previous is None
                   or previous["datestamp"] != header["datestamp"]
                   or previous["header_status"] != header["status"]
                   or previous["header_sets"] != json.dumps(header["setSpecs"])
                   or previous["content_sha256"] != digest)
        if changed:
            self.store.add_format_history(
                self.base_url, header["identifier"], prefix, header["datestamp"],
                header["status"], digest, raw_id, run_id)
            self.store.add_event(
                self.base_url, "format-updated", run_id=run_id,
                identifier=header["identifier"], metadata_prefix=prefix,
                reason="firstObservation" if previous is None else "contentChanged",
                detail={"datestamp": header["datestamp"],
                        "contentSha256": digest}, raw_id=raw_id)
        # 撤回是持久事实；只有先确认没有撤回时，才可以把该格式标回"现行"。
        if self.store.tombstone(self.base_url, header["identifier"]) is None:
            self.store.set_availability(self.base_url, header["identifier"], prefix,
                                        True, "current", raw_id)

    def _record_memberships(self, run_id: str, mode: str, set_spec: str,
                            record: dict, raw_id: Optional[int],
                            scope: str = "set") -> None:
        header = record["header"]
        # 头部给出的 setSpec 是成员关系的直接证据；照实记录每个专题。
        # "没有出现"只能由完整结束的全量专题列举来判定，与这里分开。
        for spec in header["setSpecs"]:
            before = self.store.membership(self.base_url, spec, header["identifier"])
            self.store.set_membership(
                self.base_url, spec, header["identifier"], "present",
                observed_via=mode, evidence_run=run_id, evidence_raw_id=raw_id)
            if before is None or before["state"] != "present":
                self.store.add_event(
                    self.base_url, "membership-present", run_id=run_id, set_spec=spec,
                    identifier=header["identifier"], reason=mode,
                    detail={"datestamp": header["datestamp"]}, raw_id=raw_id)

    def _record_withdrawal(self, run_id: str, prefix: str, set_spec: str,
                           record: dict, raw_id: Optional[int], mode: str) -> None:
        header = record["header"]
        identifier = header["identifier"]
        sets = header["setSpecs"]
        tombstone_set = set_spec if set_spec in sets else (sets[0] if sets else set_spec)
        existing = self.store.tombstone(self.base_url, identifier)
        self.store.add_tombstone(self.base_url, identifier, header["datestamp"],
                                 tombstone_set, raw_id)
        # 删除头本身也要落成该格式的观察事实；历史原文继续保留。
        self.store.mark_format_deleted(self.base_url, identifier, prefix,
                                       header["datestamp"], sets, raw_id)
        # 撤回是全局事实：已知的所有格式都不再是有效成果。
        known = {r["metadata_prefix"] for r in
                 self.store.formats_for(self.base_url, identifier)}
        known.add(prefix)
        for known_prefix in sorted(known):
            self.store.set_availability(self.base_url, identifier, known_prefix,
                                        False, "withdrawn", raw_id)
        # 删除头不等于"移出专题"：删除头里的 setSpec 说明来源仍把它归于这些专题，
        # 只是成果本身撤回。成员关系照实记录，现行有效性另由撤回状态决定。
        # （若没带 setSpec，说明这份删除头没有专题证据，不能据此增删任何成员关系。）
        for spec in sets:
            self.store.set_membership(
                self.base_url, spec, identifier, "present", observed_via=mode,
                evidence_run=run_id, evidence_raw_id=raw_id)
        if existing is None:
            self.store.add_event(
                self.base_url, "withdrawn", run_id=run_id, set_spec=tombstone_set,
                identifier=identifier, metadata_prefix=prefix,
                reason="deletedHeader", detail={"datestamp": header["datestamp"],
                                                "setSpecs": sets,
                                                "formatsInvalidated": sorted(known)},
                raw_id=raw_id)
            self._verify_withdrawal_other_formats(run_id, identifier, prefix,
                                                  record, set_spec)
        else:
            self.store.add_event(
                self.base_url, "withdrawal-reobserved", run_id=run_id,
                set_spec=tombstone_set, identifier=identifier,
                metadata_prefix=prefix, reason="deletedHeader",
                detail={"datestamp": header["datestamp"]}, raw_id=raw_id)

    def _verify_withdrawal_other_formats(self, run_id: str, identifier: str,
                                         observed_prefix: str, record: dict,
                                         set_spec: str) -> None:
        """撤回时用 GetRecord 确认其它已声明格式；这不是替代原文，而是交叉核对。"""
        prefixes = [f["metadata_prefix"] for f in
                    self.store.query("SELECT metadata_prefix FROM metadata_formats"
                                     " WHERE source_url=?", (self.base_url,))]
        for prefix in prefixes:
            if prefix == observed_prefix:
                continue
            try:
                page = self.client.get_record(identifier, prefix)
            except HarvestError as exc:
                self.store.add_event(
                    self.base_url, "withdrawal-check-failed", run_id=run_id,
                    set_spec=set_spec, identifier=identifier, metadata_prefix=prefix,
                    reason=exc.code, detail={"error": str(exc)})
                continue
            if page.has_error:
                # 格式不可分发既不是撤回，也不是重新有效；保留撤回状态，单独注明。
                self.store.add_event(
                    self.base_url, "withdrawal-check", run_id=run_id, set_spec=set_spec,
                    identifier=identifier, metadata_prefix=prefix,
                    reason=page.fact.error_code,
                    detail={"error": page.fact.error_text,
                            "note": "格式不可分发不等于成果仍有效"},
                    raw_id=page.raw_id)
                continue
            if not page.records:
                continue
            other = page.records[0]
            if other["deleted"]:
                self.store.mark_format_deleted(
                    self.base_url, identifier, prefix, other["header"]["datestamp"],
                    other["header"]["setSpecs"], page.raw_id)
                self.store.set_availability(self.base_url, identifier, prefix,
                                            False, "withdrawn", page.raw_id)
                self.store.add_event(
                    self.base_url, "withdrawal-confirmed", run_id=run_id,
                    set_spec=set_spec, identifier=identifier, metadata_prefix=prefix,
                    reason="deletedHeader", raw_id=page.raw_id)
            else:
                # 同一成果一个格式撤回、另一个仍给出正记录：撤回优先，并保留这次
                # 原始响应作为矛盾证据，供工作人员人工核对来源。
                self.store.set_availability(self.base_url, identifier, prefix,
                                            False, "withdrawn", page.raw_id)
                self.store.add_event(
                    self.base_url, "withdrawal-format-conflict", run_id=run_id,
                    set_spec=set_spec, identifier=identifier, metadata_prefix=prefix,
                    reason="otherFormatStillPresent",
                    detail={"note": "撤回优先，另一格式不得继续当作有效成果",
                            "datestamp": other["header"]["datestamp"],
                            "rawId": page.raw_id},
                    raw_id=page.raw_id)

    # -- 汇总与核对 ---------------------------------------------------------

    def _observed_identifiers(self, run_id: str) -> set:
        rows = self.store.query(
            "SELECT DISTINCT identifier FROM run_items WHERE run_id=?"
            " AND of_interest=1", (run_id,))
        return {r["identifier"] for r in rows}

    def _listed_identifiers(self, run_id: str) -> set:
        rows = self.store.query(
            "SELECT DISTINCT identifier FROM run_items WHERE run_id=?", (run_id,))
        return {r["identifier"] for r in rows}

    @staticmethod
    def _observed_for_membership(run_ids: list, set_spec: str, store) -> set:
        """专题核对的"出现"= 该专题列举中出现过；全来源列举中出现在别的专题不算。"""
        if not run_ids:
            return set()
        marks = ",".join("?" for _ in run_ids)
        # run_items 的 header_sets 是该记录当时的完整 setSpec 列表。
        rows = store.query(
            f"SELECT identifier, header_sets FROM run_items WHERE run_id IN ({marks})",
            tuple(run_ids))
        out = set()
        for row in rows:
            if set_spec in json.loads(row["header_sets"]):
                out.add(row["identifier"])
        return out

    def _advance_watermark(self, set_spec: str, prefix: str, newest: Optional[str],
                           mode: str, run_id: str, page: Page) -> None:
        if newest is None:
            return
        line = self.store.watermark(self.base_url, set_spec, prefix)
        if line is not None and newest < line["datestamp"]:
            return
        self.store.set_watermark(self.base_url, set_spec, prefix, newest, run_id)

    def _apply_reconcile_removals(self, job_id: str, set_spec: str, runs: list,
                                  warnings: list, started_at: str) -> dict:
        # 只有"能观察到该专题"的格式全部完整结束，才能判断成员是否出现。
        # 一个声称不支持该专题的格式不该阻塞核对；但请求失败或页链中断会阻塞。
        observable = [r for r in runs if r.get("supportsSet", True)]
        failed = [r for r in observable if not r["chainComplete"]]
        if not observable or failed:
            warnings.append(
                "专题核对没有在所有请求格式上完整结束；本次不判定任何移出")
            return {"applied": False, "removed": [],
                    "reason": "incompleteFormatCoverage",
                    "unfinishedFormats": [r["metadataPrefix"] for r in failed]}
        run_ids = [r["runId"] for r in observable]
        observed = self._observed_for_membership(run_ids, set_spec, self.store)
        watermark = self._reconcile_watermark(run_ids)
        present = self.store.members(self.base_url, set_spec, "present")
        if present and not self._any_records_listed(run_ids):
            # 全量列举整体返回 noRecordsMatch，却声称专题里还有成员。这更可能是
            # 来源状态或参数异常；在没有其它证据前，不把所有成员判为移出。
            warnings.append(
                "专题核对的全部列举都为空（noRecordsMatch），本次不判定任何移出")
            self.store.add_event(
                self.base_url, "reconcile-empty-source", run_id=run_ids[0],
                set_spec=set_spec, reason="noRecordsMatch",
                detail={"presentMembers": len(present)})
            return {"applied": False, "removed": [],
                    "reason": "emptyListingWithPresentMembers",
                    "observed": 0, "watermark": watermark}
        removed = []
        deferred = []
        for member in self.store.members(self.base_url, set_spec, "present"):
            identifier = member["identifier"]
            if identifier in observed:
                continue
            # 若在列举覆盖的时点之后、某次完整采集又见到它属于本专题，说明来源
            # 在核对过程中仍有变化。此时"本次未出现"不足以判移出，先记为待补。
            seen_later = self.store.query_one(
                """SELECT r.finished_at FROM runs r
                     JOIN run_items i ON i.run_id=r.run_id
                    WHERE i.source_url=? AND i.identifier=? AND r.status='completed'
                      AND r.finished_at > ?
                      AND r.run_id NOT IN (%s)
                      AND i.header_sets LIKE ?
                    LIMIT 1""" % ",".join("?" for _ in run_ids),
                (self.base_url, identifier, started_at, *run_ids,
                 '%"' + set_spec + '"%'))
            if seen_later is not None:
                deferred.append(identifier)
                self.store.add_event(
                    self.base_url, "membership-removal-deferred", run_id=run_ids[0],
                    set_spec=set_spec, identifier=identifier,
                    reason="observedDuringReconcile",
                    detail={"observedAt": seen_later["finished_at"],
                            "reconcileWatermark": watermark})
                continue
            self.store.set_membership(
                self.base_url, set_spec, identifier, "removed",
                observed_via="reconcile", evidence_run=run_ids[0],
                evidence_raw_id=None)
            removed.append(identifier)
            self.store.add_event(
                self.base_url, "membership-removed", run_id=run_ids[0],
                set_spec=set_spec, identifier=identifier,
                reason="absentFromCompletedFullListing",
                detail={"formats": [r["metadataPrefix"] for r in observable],
                        "withdrawn": self.store.tombstone(
                            self.base_url, identifier) is not None})
        self.store.add_event(
            self.base_url, "reconcile-completed", run_id=run_ids[0], set_spec=set_spec,
            reason="fullListing", detail={"observed": len(observed),
                                          "removed": len(removed),
                                          "deferred": len(deferred),
                                          "watermark": watermark,
                                          "formats": [r["metadataPrefix"]
                                                      for r in observable]})
        return {"applied": True, "removed": sorted(removed),
                "deferred": sorted(deferred), "observed": len(observed),
                "watermark": watermark,
                "formats": [r["metadataPrefix"] for r in observable]}

    def _any_records_listed(self, run_ids: list) -> bool:
        if not run_ids:
            return False
        marks = ",".join("?" for _ in run_ids)
        row = self.store.query_one(
            f"SELECT COUNT(*) AS n FROM run_items WHERE run_id IN ({marks})",
            tuple(run_ids))
        return bool(row and row["n"])

    def _reconcile_watermark(self, run_ids: list) -> Optional[str]:
        """本次全量列举覆盖到的最新 datestamp，随核对结果一起报告。"""
        stamps = [self.store.newest_observed_datestamp(run_id) for run_id in run_ids]
        stamps = [s for s in stamps if s]
        return max(stamps) if stamps else None

    # -- 报告 ---------------------------------------------------------------

    def _build_report(self, job_id, mode, set_spec, set_name, identify, requested,
                      advertised, runs, date_until, started, warnings,
                      membership_changes, started_counts=None) -> dict:
        pending = [r for r in runs if not r["chainComplete"]]
        confirmed = [r for r in runs if r["chainComplete"]]
        source = self.store.source(self.base_url)
        members = self.store.members(self.base_url, set_spec, "present")
        active_members = [m for m in members
                          if self.store.tombstone(self.base_url,
                                                  m["identifier"]) is None]
        removed_members = self.store.members(self.base_url, set_spec, "removed")
        per_run_raw = self.store.query(
            "SELECT run_id, COUNT(*) AS n FROM raw_responses WHERE run_id IN (" +
            ",".join("?" for _ in runs) + ") GROUP BY run_id",
            tuple(r["runId"] for r in runs)) if runs else []
        per_run = {row["run_id"]: row["n"] for row in per_run_raw}
        discovery_raw = self.store.query_one(
            "SELECT COUNT(*) AS n FROM raw_responses WHERE run_id=? AND verb != "
            "'ListRecords'", (job_id,))
        all_raw = self.store.query_one(
            "SELECT COUNT(*) AS n, COALESCE(SUM(LENGTH(body)),0) AS b"
            " FROM raw_responses WHERE run_id=? OR run_id LIKE ?",
            (job_id, job_id + ":%"))
        date_range = {
            "from": min((r["dateFrom"] for r in runs if r["dateFrom"]), default=None),
            "until": date_until,
            "inclusive": True,
            "overlapSeconds": self.overlap_seconds if mode == "update" else None,
            "mode": mode,
            "perFormat": {
                r["metadataPrefix"]: {"from": r["dateFrom"], "until": r["dateUntil"]}
                for r in runs
            },
        }
        if mode in ("initial", "reconcile"):
            date_range["from"] = None
            date_range["note"] = "全量列举，不设日期上下界"
            date_range["perFormat"] = {
                r["metadataPrefix"]: {"from": None, "until": None} for r in runs
            }
        bootstrap_formats = [r["metadataPrefix"] for r in runs if r["bootstrap"]]
        if bootstrap_formats:
            date_range["bootstrapFormats"] = bootstrap_formats
            date_range["note"] = (
                "以下格式没有水位，本次在全来源范围全量列举以建立基线："
                + "、".join(bootstrap_formats))

        report = {
            "jobId": job_id,
            "source": {
                "baseUrl": self.base_url,
                "repositoryName": identify.get("repositoryName"),
                "protocolVersion": identify.get("protocolVersion"),
                "deletedRecord": identify.get("deletedRecord"),
                "granularity": identify.get("granularity"),
                "earliestDatestamp": identify.get("earliestDatestamp"),
            },
            "set": {"setSpec": set_spec, "setName": set_name},
            "listingScope": "set" if mode == "initial" else "repository",
            "mode": mode,
            "formats": requested,
            "dateRange": date_range,
            "startedAt": started,
            "finishedAt": utcnow(),
            "coverage": {
                "confirmed": [{
                    "format": r["metadataPrefix"], "runId": r["runId"],
                    "completeListSize": r["completeListSize"],
                    "collectedIdentifiers": r["collectedIdentifiers"],
                    "listedIdentifiers": r["listedIdentifiers"],
                } for r in confirmed],
                "pending": [{
                    "format": r["metadataPrefix"], "runId": r["runId"],
                    "status": r["status"],
                    "reason": ("页链未完整结束：本次不能声明完整清单"
                               if r["status"] != "unsupported"
                               else "格式不可分发：不等于成果撤回，也不能据此判定移出"),
                } for r in pending],
                "membership": membership_changes,
            },
            "counts": {
                "eventCount": (started_counts or {}).get("events", 0),
                "reconnaissanceRequests": discovery_raw["n"] if discovery_raw else 0,
                "requestsPerFormat": {r["metadataPrefix"]: per_run.get(r["runId"], 0)
                                      for r in runs},
                "rawResponses": all_raw["n"] if all_raw else 0,
                "rawBytes": all_raw["b"] if all_raw else 0,
                "presentMembers": len(members),
                "effectiveMembers": len(active_members),
                "removedMembers": len(removed_members),
                "formatsHarvested": len(confirmed),
            },
            "observedWindows": {
                r["metadataPrefix"]: {"first": r["responseDates"][0],
                                      "last": r["responseDates"][-1]}
                for r in runs if r.get("responseDates")
            },
            "runs": runs,
            "warnings": warnings,
            "reports": self._report_path(job_id),
        }
        return report

    # -- 报告落盘 -----------------------------------------------------------

    def _reports_dir(self) -> Optional[str]:
        if self.store.path == ":memory:":
            return None
        directory = os.path.join(os.path.dirname(self.store.path), "reports")
        os.makedirs(directory, exist_ok=True)
        return directory

    def _report_path(self, job_id: str) -> Optional[str]:
        directory = self._reports_dir()
        return os.path.join(directory, f"{job_id}.json") if directory else None

    def _save_report(self, report: dict) -> None:
        path = report.get("reports")
        if path:
            temporary = path + ".tmp"
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(report, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.replace(temporary, path)
