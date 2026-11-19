"""客户端层测试：传输失败、5xx 重试与协议错误分类（不依赖来源服务）。"""

import unittest

import requests

from harvester.client import HarvestError, OAIClient

ENVELOPE = (
    '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
    '<responseDate>2026-11-16T10:00:05Z</responseDate>'
    '<request verb="ListRecords">/oai</request>%s</OAI-PMH>')


class FakeResponse:
    def __init__(self, body, status=200):
        self.content = body.encode() if isinstance(body, str) else body
        self.status_code = status


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class ClientFacts(unittest.TestCase):
    def make(self, responses, **kwargs):
        session = FakeSession(responses)
        client = OAIClient("http://source.invalid/oai", session=session,
                           retries=kwargs.pop("retries", 2), backoff=0, **kwargs)
        return client, session

    def test_no_records_match_is_a_protocol_fact_not_an_error(self):
        body = ENVELOPE % '<error code="noRecordsMatch">范围内无记录</error>'
        client, _ = self.make([FakeResponse(body)])
        page = client.list_records("research", set_spec="teaching")
        self.assertTrue(page.is_empty)
        self.assertFalse(page.is_unsupported)
        self.assertEqual(page.fact.error_code, "noRecordsMatch")
        self.assertEqual(page.fact.verb, "ListRecords")

    def test_cannot_disseminate_is_not_deletion(self):
        body = ENVELOPE % ('<error code="cannotDisseminateFormat">'
                           '未支持格式</error>')
        client, _ = self.make([FakeResponse(body)])
        page = client.list_records("bogus")
        self.assertTrue(page.is_unsupported)
        self.assertFalse(page.is_empty)
        self.assertFalse(page.records)

    def test_transport_failure_is_retried_then_classified(self):
        client, session = self.make(
            [requests.ConnectionError("refused"), requests.ConnectionError("again"),
             FakeResponse(ENVELOPE % (
                 '<ListRecords><resumptionToken /></ListRecords>'))])
        page = client.list_records("research")
        self.assertEqual(len(session.calls), 3)
        self.assertIsNone(page.fact.error_code)

    def test_exhausted_transport_failure_raises_transport_error(self):
        client, _ = self.make([requests.ConnectionError("refused")] * 3)
        with self.assertRaises(HarvestError) as context:
            client.list_records("research")
        self.assertEqual(context.exception.code, "transportError")

    def test_server_error_is_retried_and_then_transport_error(self):
        client, session = self.make([FakeResponse("oops", 503)] * 3)
        with self.assertRaises(HarvestError) as context:
            client.list_records("research")
        self.assertEqual(context.exception.code, "transportError")
        self.assertEqual(len(session.calls), 3)

    def test_client_error_is_not_retried(self):
        client, session = self.make([FakeResponse("bad", 404)])
        with self.assertRaises(HarvestError) as context:
            client.list_records("research")
        self.assertEqual(context.exception.code, "httpError")
        self.assertEqual(len(session.calls), 1)

    def test_resumption_request_carries_only_verb_and_token(self):
        first = ENVELOPE % (
            '<ListRecords><resumptionToken completeListSize="9" cursor="2">'
            'abc</resumptionToken></ListRecords>')
        second = ENVELOPE % (
            '<ListRecords><resumptionToken /></ListRecords>')
        client, session = self.make([FakeResponse(first), FakeResponse(second)])
        page = client.list_records("research", set_spec="teaching",
                                   date_from="2026-11-16T10:00:00Z")
        self.assertEqual(page.fact.complete_list_size, 9)
        self.assertEqual(page.fact.cursor, 2)
        page = client.resumption_page("ListRecords", "abc")
        last = session.calls[-1]
        self.assertIn("verb=ListRecords", last)
        self.assertIn("resumptionToken=abc", last)
        self.assertNotIn("metadataPrefix", last)
        self.assertTrue(page.is_last)

    def test_malformed_xml_is_parse_error(self):
        client, _ = self.make([FakeResponse("<html>not oai</html>")])
        with self.assertRaises(HarvestError) as context:
            client.list_records("research")
        self.assertEqual(context.exception.code, "parseError")

    def test_raw_recorder_receives_every_response(self):
        seen = []

        def recorder(params, verb, status, url, body):
            seen.append((verb, url, body))
            return len(seen)

        client, _ = self.make([FakeResponse(ENVELOPE % (
            '<ListRecords><resumptionToken /></ListRecords>'))],
            recorder=recorder)
        page = client.list_records("research")
        self.assertEqual(len(seen), 1)
        self.assertEqual(page.raw_id, 1)
        self.assertIn(b"<OAI-PMH", seen[0][2])


if __name__ == "__main__":
    unittest.main()
