"""OAI-PMH 客户端层：只做请求、协议解析和协议事实分类，不把事实写成采集结果。

设计要点：

* 每个请求都保留原始响应字节、请求 URL 和 HTTP 状态，作为可追溯的原始材料。
  ``recorder`` 回调由收割层提供，用来把每一份原始响应落盘。
* 区分互不相同的协议事实：
    - ``noRecordsMatch``          范围内没有记录（不是删除）
    - ``cannotDisseminateFormat`` 某格式不支持（不是删除）
    - ``badResumptionToken``      续页凭证失效（本次页链未完成）
    - 网络 / 5xx / XML 解析失败    单次请求失败（可重试，不能冒充完整清单）
* 日期参数原样发送，由来源按秒粒度解释；续页请求只携带 verb 与 token。
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

OAI_NS = "http://www.openarchives.org/OAI/2.0/"

#: 来源返回的、代表"本次查询没有结果"的协议错误码（不是删除）。
EMPTY_CODES = {"noRecordsMatch"}
#: 来源返回的、代表"格式不可分发"的协议错误码（不是删除）。
UNSUPPORTED_CODES = {"cannotDisseminateFormat"}
#: 来源返回的、代表"续页凭证失效"的协议错误码。
TOKEN_CODES = {"badResumptionToken"}

Recorder = Callable[[dict, str, int, str, bytes], int]


class HarvestError(Exception):
    """收割过程无法安全继续时抛出；``detail`` 附带协议事实与请求信息。"""

    def __init__(self, message: str, *, code: str = "harvestError", detail=None):
        super().__init__(message)
        self.code = code
        self.detail = detail or {}


@dataclass
class ProtocolFact:
    """一次来源响应的协议层事实。"""

    verb: str
    url: str
    status: int
    error_code: Optional[str] = None
    error_text: Optional[str] = None
    resumption_token: Optional[str] = None
    complete_list_size: Optional[int] = None
    cursor: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "verb": self.verb,
            "url": self.url,
            "status": self.status,
            "errorCode": self.error_code,
            "errorText": self.error_text,
            "resumptionToken": self.resumption_token,
            "completeListSize": self.complete_list_size,
            "cursor": self.cursor,
        }


@dataclass
class Page:
    """一页已解析响应。"""

    fact: ProtocolFact
    headers: list = field(default_factory=list)
    records: list = field(default_factory=list)
    raw: bytes = b""
    raw_id: Optional[int] = None
    response_date: Optional[str] = None

    @property
    def is_empty(self) -> bool:
        return self.fact.error_code in EMPTY_CODES

    @property
    def is_unsupported(self) -> bool:
        return self.fact.error_code in UNSUPPORTED_CODES

    @property
    def is_token_expired(self) -> bool:
        return self.fact.error_code in TOKEN_CODES

    @property
    def has_error(self) -> bool:
        return self.fact.error_code is not None

    @property
    def is_last(self) -> bool:
        return not self.fact.resumption_token


def _qname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _child(element, name):
    for child in element:
        if _qname(child.tag) == name:
            return child
    return None


def _children(element, name):
    return [child for child in element if _qname(child.tag) == name]


def _text(element, name, default=None):
    child = _child(element, name)
    return default if child is None else (child.text or "")


class OAIClient:
    """薄封装：一次调用对应一个 OAI 请求，绝不在客户端偷偷分页或合并。"""

    def __init__(self, base_url: str, *, timeout: float = 30.0,
                 session: Optional[requests.Session] = None,
                 retries: int = 3, backoff: float = 0.25,
                 recorder: Optional[Recorder] = None):
        parts = urlsplit(base_url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError("来源地址必须是有效的 http(s) URL")
        self.base_url = base_url
        self.timeout = timeout
        self.session = session or requests.Session()
        self.retries = retries
        self.backoff = backoff
        self.recorder = recorder

    # -- 请求 ---------------------------------------------------------------

    def endpoint(self, params: dict) -> str:
        parts = urlsplit(self.base_url)
        path = parts.path or "/oai"
        if path.endswith("/"):
            path = path + "oai"
        return urlunsplit((parts.scheme, parts.netloc, path,
                           urlencode(params, doseq=True), parts.fragment))

    def _get(self, params: dict) -> tuple[bytes, int, str, Optional[int]]:
        """发送一次请求。网络错误与 5xx 按 ``retries`` 重试。

        协议错误码由来源以 200 返回，不做重试；无法恢复的传输错误以
        ``HarvestError(code="transportError")`` 结束，调用方据此标记待补范围。
        """
        url = self.endpoint(params)
        verb = params.get("verb", "")
        last = None
        for attempt in range(self.retries + 1):
            if attempt:
                time.sleep(self.backoff * attempt)
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:  # 网络层失败
                last = f"{type(exc).__name__}: {exc}"
                continue
            body = response.content
            if response.status_code >= 500:
                last = f"HTTP {response.status_code}"
                continue
            if response.status_code != 200:
                raise HarvestError(
                    f"来源返回 HTTP {response.status_code}",
                    code="httpError",
                    detail={"url": url, "status": response.status_code,
                            "body": body[:2048].decode("utf-8", "replace")},
                )
            raw_id = None
            if self.recorder is not None:
                raw_id = self.recorder(params, verb, response.status_code, url, body)
            return body, response.status_code, url, raw_id
        raise HarvestError(f"请求失败：{last}", code="transportError",
                           detail={"url": url, "attempts": self.retries + 1})

    # -- 解析 ---------------------------------------------------------------

    @staticmethod
    def parse(body: bytes) -> tuple[ET.Element, Optional[str]]:
        """解析 OAI-PMH 信封，返回 (根节点, responseDate)。"""
        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            raise HarvestError("来源响应不是合法 XML", code="parseError",
                               detail={"detail": str(exc)}) from exc
        if _qname(root.tag) != "OAI-PMH":
            raise HarvestError("响应根元素不是 OAI-PMH", code="parseError",
                               detail={"root": _qname(root.tag)})
        return root, _text(root, "responseDate")

    @staticmethod
    def error_of(root: ET.Element) -> Optional[tuple[str, str]]:
        error = _child(root, "error")
        if error is None:
            return None
        return error.get("code") or "", "".join(error.itertext()).strip()

    def _request(self, params: dict) -> tuple[ET.Element, bytes, int, str, Optional[int]]:
        body, status, url, raw_id = self._get(params)
        try:
            root, _ = self.parse(body)
        except HarvestError as exc:
            exc.detail.setdefault("url", url)
            exc.detail.setdefault("rawId", raw_id)
            raise
        return root, body, status, url, raw_id

    # -- 动词 ---------------------------------------------------------------

    def identify(self) -> dict:
        root, body, status, url, raw_id = self._request({"verb": "Identify"})
        err = self.error_of(root)
        if err:
            raise HarvestError(f"Identify 失败：{err[0]}", code=err[0],
                               detail={"url": url, "rawId": raw_id})
        node = _child(root, "Identify")
        if node is None:
            raise HarvestError("Identify 响应缺少 Identify 元素", code="parseError")
        version = _text(node, "protocolVersion")
        if version and version != "2.0":
            raise HarvestError(f"不支持的协议版本 {version}", code="protocolError")
        return {
            "repositoryName": _text(node, "repositoryName"),
            "baseURL": _text(node, "baseURL"),
            "protocolVersion": version,
            "deletedRecord": _text(node, "deletedRecord"),
            "granularity": _text(node, "granularity"),
            "earliestDatestamp": _text(node, "earliestDatestamp"),
            "_requestUrl": url,
            "_raw": body,
            "_rawId": raw_id,
        }

    def list_sets(self) -> list[dict]:
        sets: list[dict] = []
        params = {"verb": "ListSets"}
        seen_tokens: set = set()
        while True:
            root, _body, _status, _url, _raw_id = self._request(params)
            err = self.error_of(root)
            if err and err[0] != "noSetHierarchy":
                raise HarvestError(f"ListSets 失败：{err[0]}", code=err[0])
            if err:
                return []
            node = _child(root, "ListSets")
            if node is None:
                raise HarvestError("ListSets 响应缺少负载", code="parseError")
            for item in _children(node, "set"):
                sets.append({
                    "setSpec": _text(item, "setSpec"),
                    "setName": _text(item, "setName"),
                    "description": _text(item, "setDescription"),
                })
            token = self._token_of_root(root)
            if not token:
                return sets
            if token in seen_tokens:
                raise HarvestError("ListSets 续页凭证循环", code="badResumptionToken")
            seen_tokens.add(token)
            params = {"verb": "ListSets", "resumptionToken": token}

    def list_metadata_formats(self, identifier: Optional[str] = None) -> list[dict]:
        params = {"verb": "ListMetadataFormats"}
        if identifier:
            params["identifier"] = identifier
        root, _body, _status, _url, _raw_id = self._request(params)
        err = self.error_of(root)
        if err:
            raise HarvestError(f"ListMetadataFormats 失败：{err[0]}", code=err[0])
        node = _child(root, "ListMetadataFormats")
        if node is None:
            raise HarvestError("ListMetadataFormats 响应缺少负载", code="parseError")
        return [{
            "metadataPrefix": _text(f, "metadataPrefix"),
            "schema": _text(f, "schema"),
            "metadataNamespace": _text(f, "metadataNamespace"),
        } for f in _children(node, "metadataFormat")]

    def get_record(self, identifier: str, metadata_prefix: str) -> Page:
        params = {"verb": "GetRecord", "identifier": identifier,
                  "metadataPrefix": metadata_prefix}
        return self._page(params, "GetRecord")

    def list_records(self, metadata_prefix: str, *, set_spec: Optional[str] = None,
                     date_from: Optional[str] = None, until: Optional[str] = None) -> Page:
        params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
        if set_spec:
            params["set"] = set_spec
        if date_from:
            params["from"] = date_from
        if until:
            params["until"] = until
        return self._page(params, "ListRecords")

    def resumption_page(self, verb: str, token: str) -> Page:
        return self._page({"verb": verb, "resumptionToken": token}, verb)

    # -- 页解析 -------------------------------------------------------------

    @staticmethod
    def _token_of_root(root: ET.Element) -> Optional[str]:
        for child in root.iter():
            if _qname(child.tag) == "resumptionToken":
                value = (child.text or "").strip()
                return value or None
        return None

    def _page(self, params: dict, verb: str) -> Page:
        root, body, status, url, raw_id = self._request(params)
        fact = ProtocolFact(verb=verb, url=url, status=status)
        err = self.error_of(root)
        node = _child(root, verb)
        if err:
            fact.error_code, fact.error_text = err
            return Page(fact=fact, raw=body, raw_id=raw_id,
                        response_date=_text(root, "responseDate"))
        if node is None:
            raise HarvestError(f"{verb} 响应缺少负载元素", code="parseError",
                               detail={"url": url, "rawId": raw_id})
        token_node = _child(node, "resumptionToken")
        if token_node is not None:
            fact.resumption_token = (token_node.text or "").strip() or None
            if token_node.get("completeListSize") is not None:
                try:
                    fact.complete_list_size = int(token_node.get("completeListSize"))
                except ValueError:
                    fact.complete_list_size = None
            if token_node.get("cursor") is not None:
                try:
                    fact.cursor = int(token_node.get("cursor"))
                except ValueError:
                    fact.cursor = None
        records = [self._parse_record(item) for item in _children(node, "record")]
        return Page(fact=fact, headers=[r["header"] for r in records],
                    records=records, raw=body, raw_id=raw_id,
                    response_date=_text(root, "responseDate"))

    @staticmethod
    def _parse_record(record: ET.Element) -> dict:
        header = _child(record, "header")
        if header is None:
            raise HarvestError("record 缺少 header", code="parseError")
        identifier = (_text(header, "identifier") or "").strip()
        datestamp = (_text(header, "datestamp") or "").strip()
        if not identifier or not datestamp:
            raise HarvestError("record header 缺少 identifier 或 datestamp",
                               code="parseError", detail={"identifier": identifier})
        sets = [(s.text or "").strip() for s in _children(header, "setSpec")]
        status = header.get("status")
        metadata = _child(record, "metadata")
        content = None
        namespace = None
        if metadata is not None:
            children = list(metadata)
            if children:
                payload = children[0]
                namespace = payload.tag[1:].split("}")[0] if payload.tag.startswith("{") else None
                content = ET.tostring(payload, encoding="unicode")
        about = [_qname(x) for x in _children(record, "about")]
        return {
            "header": {"identifier": identifier, "datestamp": datestamp,
                       "setSpecs": sets, "status": status},
            "metadata": content,
            "metadataNamespace": namespace,
            "about": about,
            "deleted": status == "deleted",
        }
