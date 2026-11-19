"""收割能力的验收测试：用本地合成来源演练建库、更新、核对与撤回。"""

import json
import os
import re
import shutil
import tempfile
import unittest

from harvester import Harvester
from harvester.query import coverage_status, explain_record, explain_set
from source.server import running_source
from tests.support import SourceCase


class HarvesterCase(SourceCase):
    def setUp(self):
        super().setUp()
        self.state = tempfile.mkdtemp(prefix="harvest-")
        self.addCleanup(shutil.rmtree, self.state, True)
        self.harvester = Harvester(
            self.base + "/oai", self.state, retries=1, backoff=0.01,
            overlap_seconds=5, max_restarts=1)
        self.addCleanup(self.harvester.close)

    def harvest(self, mode, set_spec="teaching", **kwargs):
        return self.harvester.run(mode, set_spec=set_spec, **kwargs)

    def formats(self, identifier):
        return {row["metadata_prefix"]: dict(row) for row in
                self.harvester.store.formats_for(self.base + "/oai", identifier)}

    def availability(self, identifier):
        return {prefix: row["available"] for prefix, row in
                self.harvester.store.availability_for(
                    self.base + "/oai", identifier).items()}

    def memberships(self, set_spec):
        return {row["identifier"]: row["state"] for row in
                self.harvester.store.query(
                    "SELECT identifier, state FROM memberships"
                    " WHERE source_url=? AND set_spec=?", (self.base + "/oai", set_spec))}


class InitialHarvest(HarvesterCase):
    def test_both_formats_for_same_identifier_are_kept(self):
        report = self.harvest("initial")
        self.assertEqual(sorted(report["formats"]), ["oai_dc", "research"])
        formats = self.formats("oai:campus:p1")
        self.assertEqual(sorted(formats), ["oai_dc", "research"])
        # 两种格式保留各自原文，不是互相替代
        self.assertIn("dc:title", formats["oai_dc"]["metadata"])
        self.assertIn("urn:campus:research", formats["research"]["metadata"])
        self.assertNotEqual(formats["oai_dc"]["metadata"],
                            formats["research"]["metadata"])

    def test_report_declares_source_set_formats_and_date_range(self):
        report = self.harvest("initial")
        self.assertEqual(report["source"]["repositoryName"], "校园成果库")
        self.assertEqual(report["source"]["deletedRecord"], "persistent")
        self.assertEqual(report["set"]["setSpec"], "teaching")
        self.assertEqual(report["set"]["setName"], "教学创新")
        self.assertIn("oai_dc", report["formats"])
        self.assertIn("research", report["formats"])
        self.assertEqual(report["dateRange"]["mode"], "initial")
        self.assertIsNone(report["dateRange"]["from"])
        self.assertIsNone(report["dateRange"]["until"])
        self.assertTrue(report["dateRange"]["inclusive"])
        for run in report["runs"]:
            self.assertTrue(run["chainComplete"])
            self.assertTrue(run["rawResponseIds"])
        self.assertEqual(len(report["coverage"]["confirmed"]), 2)
        self.assertEqual(report["coverage"]["pending"], [])

    def test_raw_responses_are_preserved_with_provenance(self):
        report = self.harvest("initial")
        for run in report["runs"]:
            for raw_id in run["rawResponseIds"]:
                row = self.harvester.store.raw(raw_id)
                self.assertIsNotNone(row)
                self.assertIn(b"<OAI-PMH", row["body"])
                self.assertIn("verb=ListRecords", row["request_url"])
                self.assertEqual(row["run_id"], run["runId"])

    def test_initial_is_idempotent(self):
        self.harvest("initial")
        self.harvest("initial")
        rows = self.harvester.store.query(
            "SELECT identifier, metadata_prefix, COUNT(*) AS n FROM record_formats"
            " GROUP BY identifier, metadata_prefix")
        self.assertTrue(all(row["n"] == 1 for row in rows))
        self.assertEqual(len(rows), 6)  # 3 个教学成果 × 2 种格式

    def test_no_records_match_is_not_deletion(self):
        report = self.harvest("initial", set_spec="teaching")
        before = dict(self.availability("oai:campus:p2"))
        self.control(0)
        empty = self.harvester.client.list_records(
            "research", set_spec="teaching", date_from="2030-01-01T00:00:00Z")
        self.assertEqual(empty.fact.error_code, "noRecordsMatch")
        self.assertTrue(empty.is_empty)
        self.assertFalse(empty.has_error and not empty.is_empty)
        self.assertEqual(before, dict(self.availability("oai:campus:p2")))
        self.assertIsNone(self.harvester.store.tombstone(
            self.base + "/oai", "oai:campus:p2"))

    def test_unknown_set_is_reported_not_silently_empty(self):
        with self.assertRaises(Exception) as context:
            self.harvest("initial", set_spec="absent")
        self.assertEqual(getattr(context.exception, "code", None), "unknownSet")


class UpdateAndReconcile(HarvesterCase):
    def setUp(self):
        super().setUp()
        self.harvest("initial")
        self.harvest("initial", set_spec="industry")

    def test_incremental_missing_record_is_not_deletion(self):
        self.control(2)
        report = self.harvest("update")
        self.assertIsNone(self.harvester.store.tombstone(
            self.base + "/oai", "oai:campus:p1"))
        # 增量里 p1 没有出现在 teaching，但不能据此判移出
        self.assertEqual(self.memberships("teaching")["oai:campus:p1"], "present")
        self.assertEqual(report["coverage"]["membership"]["applied"], False)

    def test_reconcile_removes_only_from_that_set(self):
        self.control(2)
        report = self.harvest("reconcile")
        self.assertTrue(report["coverage"]["membership"]["applied"])
        self.assertEqual(report["coverage"]["membership"]["removed"],
                         ["oai:campus:p1"])
        self.assertEqual(self.memberships("teaching")["oai:campus:p1"], "removed")
        self.assertEqual(self.memberships("industry")["oai:campus:p1"], "present")
        # 移出专题不改变成果本身的可用性
        self.assertEqual(self.availability("oai:campus:p1"),
                         {"oai_dc": 1, "research": 1})
        self.assertIsNone(self.harvester.store.tombstone(
            self.base + "/oai", "oai:campus:p1"))

    def test_incomplete_full_listing_never_confirms_removal(self):
        self.control(2)
        self.source.options["expire"] = True
        report = self.harvest("reconcile")
        self.assertEqual(report["coverage"]["membership"]["applied"], False)
        self.assertEqual(self.memberships("teaching")["oai:campus:p1"], "present")
        self.assertTrue(report["coverage"]["pending"])
        self.assertTrue(all(not run["chainComplete"] for run in report["runs"]))

    def test_same_second_update_is_covered_by_overlap(self):
        self.control(1)  # p1 在 10:00:00 同一秒补充，时间戳不前进
        self.harvest("update")
        formats = self.formats("oai:campus:p1")
        self.assertIn("补充版", formats["research"]["metadata"])
        self.assertIn("补充版", formats["oai_dc"]["metadata"])

    def test_overlapping_windows_do_not_duplicate_records(self):
        self.harvest("update")
        self.harvest("update")
        self.harvest("update", date_until="2026-11-16T10:00:02Z")
        rows = self.harvester.store.query(
            "SELECT identifier, metadata_prefix, COUNT(*) AS n FROM record_formats"
            " GROUP BY identifier, metadata_prefix")
        self.assertTrue(all(row["n"] == 1 for row in rows))


class Withdrawal(HarvesterCase):
    def setUp(self):
        super().setUp()
        self.harvest("initial")
        self.harvest("initial", set_spec="industry")

    def test_withdrawal_invalidates_every_known_format(self):
        self.control(3)
        report = self.harvest("update")
        identifier = "oai:campus:p1"
        self.assertIsNotNone(self.harvester.store.tombstone(self.base + "/oai",
                                                           identifier))
        self.assertEqual(self.availability(identifier),
                         {"oai_dc": 0, "research": 0})
        stored = self.formats(identifier)
        for prefix, row in stored.items():
            self.assertEqual(row["header_status"], "deleted")
            # 撤回不删除历史原文：工作人员仍能解释曾经发布过什么
            self.assertTrue(row["metadata"])

        events = [dict(e) for e in self.harvester.store.events(
            self.base + "/oai", identifier=identifier)]
        kinds = [e["kind"] for e in events]
        self.assertIn("withdrawn", kinds)
        self.assertNotIn("membership-removed", kinds)
        self.assertTrue(any(e["kind"] == "withdrawn" for e in events))
        confirmed = [e for e in events if e["kind"] == "withdrawal-confirmed"]
        self.assertEqual([e["metadata_prefix"] for e in confirmed], ["research"])

    def test_withdrawal_is_reported_as_withdrawal_not_removal(self):
        self.control(3)
        self.harvest("reconcile")
        teaching = explain_set(self.harvester.store, self.base + "/oai", "teaching")
        self.assertEqual([w["identifier"] for w in teaching["withdrawn"]],
                         ["oai:campus:p1"])
        self.assertEqual(teaching["removed"], [])
        industry = explain_set(self.harvester.store, self.base + "/oai", "industry")
        self.assertEqual([w["identifier"] for w in industry["withdrawn"]],
                         ["oai:campus:p1"])

    def test_positive_record_after_withdrawal_cannot_resurrect(self):
        self.control(3)
        self.harvest("update")
        # 模拟来源后来又在旧阶段给出了正记录（与撤回矛盾）
        self.control(0)
        self.harvest("reconcile")
        identifier = "oai:campus:p1"
        self.assertIsNotNone(self.harvester.store.tombstone(self.base + "/oai",
                                                           identifier))
        self.assertEqual(self.availability(identifier),
                         {"oai_dc": 0, "research": 0})
        events = [e["kind"] for e in self.harvester.store.events(
            self.base + "/oai", identifier=identifier)]
        self.assertIn("post-withdrawal-present", events)

    def test_restart_after_expired_token_is_safe_and_reported(self):
        self.source.options["expire"] = True
        report = self.harvest("update")
        self.assertTrue(all(not run["chainComplete"] for run in report["runs"]))
        self.assertTrue(report["warnings"])
        self.assertIsNone(self.harvester.store.tombstone(
            self.base + "/oai", "oai:campus:p3"))
        # 凭证恢复后继续更新能补齐，且不会产生重复记录
        self.source.options["expire"] = False
        self.control(3)
        healed = self.harvest("update")
        self.assertTrue(all(run["chainComplete"] for run in healed["runs"]))
        rows = self.harvester.store.query(
            "SELECT identifier, metadata_prefix, COUNT(*) AS n FROM record_formats"
            " GROUP BY identifier, metadata_prefix")
        self.assertTrue(all(row["n"] == 1 for row in rows))
        self.assertEqual(self.availability("oai:campus:p1"),
                         {"oai_dc": 0, "research": 0})

    def test_format_support_is_not_deletion(self):
        report = self.harvest("initial", formats=["research", "no_such_format"])
        no_such = [r for r in report["runs"]
                   if r["metadataPrefix"] == "no_such_format"][0]
        self.assertEqual(no_such["status"], "unsupported")
        self.assertIn("no_such_format", [p["format"]
                                         for p in report["coverage"]["pending"]])
        self.assertEqual(self.availability("oai:campus:p2"),
                         {"oai_dc": 1, "research": 1})

    def test_record_explanation_links_raw_and_history(self):
        self.control(3)
        self.harvest("update")
        explanation = explain_record(self.harvester.store, self.base + "/oai",
                                     "oai:campus:p1")
        self.assertTrue(explanation["withdrawn"])
        self.assertEqual(len(explanation["formats"]), 2)
        self.assertTrue(explanation["rawResponses"])
        self.assertTrue(all(r["requestUrl"].startswith(self.base)
                            for r in explanation["rawResponses"]))
        changed = [h for h in explanation["formatHistory"]
                   if h["header_status"] == "deleted"]
        self.assertEqual(len(changed), 2)
        # 历史每一步都能从原始响应还原当时的内容与 setSpec
        before = [h for h in explanation["formatHistory"]
                  if h["header_status"] is None and h["datestamp"]
                  == "2026-11-16T10:00:00Z"]
        self.assertTrue(before)
        for entry in before:
            self.assertIn("场景化软件实践", entry["metadataSnapshot"])
            self.assertEqual(entry["setSpecsSnapshot"], ["teaching", "industry"])
            self.assertIsNotNone(entry["rawRequestUrl"])
        for entry in changed:
            self.assertEqual(entry["setSpecsSnapshot"], ["industry"])
            self.assertIsNone(entry["metadataSnapshot"])


class CoverageView(HarvesterCase):
    def test_confirmed_and_pending_are_separated(self):
        self.harvest("initial")
        self.control(1)
        self.source.options["expire"] = True
        self.harvest("update")
        status = coverage_status(self.harvester.store, self.base + "/oai",
                                 "teaching")
        for prefix, entry in status["formats"].items():
            self.assertIsNotNone(entry["lastConfirmed"])
            self.assertTrue(entry["pending"])
            self.assertIn("errorCode", entry["pending"][0]["detail"])

    def test_harvester_never_uses_management_endpoints(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        offenders = []
        for name in os.listdir(os.path.join(root, "harvester")):
            if not name.endswith(".py"):
                continue
            with open(os.path.join(root, "harvester", name), encoding="utf-8") as f:
                text = f.read()
            if re.search(r"/__control|/__log", text):
                offenders.append(name)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
