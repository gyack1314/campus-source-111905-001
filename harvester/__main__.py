"""命令行入口。

    python -m harvester --source http://127.0.0.1:8765/oai --state ./harvest \\
        initial|update|reconcile --set teaching [--format research ...]

工作人员视图：

    python -m harvester --source ... --state ... show --set teaching
    python -m harvester --source ... --state ... show --set teaching --record oai:campus:p1
    python -m harvester --source ... --state ... show --set teaching --coverage
    python -m harvester --source ... --state ... events --set teaching
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .engine import Harvester, HarvestError
from .query import coverage_status, explain_record, explain_set
from .store import Store


def _default_state(url: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in url)
    return f"./harvest-state/{safe}"


def _dump(value) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def _add_common(parser: argparse.ArgumentParser, *, required: bool = True) -> None:
    # 子命令解析器用 SUPPRESS 默认值：没写的选项不会覆盖写在子命令之前的同名选项。
    head = {} if required else {"default": argparse.SUPPRESS}
    parser.add_argument("--source", required=False, help=argparse.SUPPRESS if not required
                        else "来源的 OAI-PMH 基础地址，例如 http://127.0.0.1:8765/oai",
                        **head)
    parser.add_argument("--state", default=argparse.SUPPRESS,
                        help="本地状态目录（默认按来源地址生成）")
    parser.add_argument("--format", dest="formats", action="append",
                        default=argparse.SUPPRESS,
                        help="要收割的 metadataPrefix，可重复；默认使用来源声明的全部格式")
    parser.add_argument("--set", dest="set_spec", required=False,
                        help=argparse.SUPPRESS if not required else "专题 setSpec",
                        **head)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m harvester",
        description="按专题收割 OAI-PMH 成果，保留原始响应、格式原文与成员关系。"
                    "选项写在子命令之后。")
    common = argparse.ArgumentParser(add_help=False)
    _add_common(common, required=False)
    tuning = argparse.ArgumentParser(add_help=False)
    tuning.add_argument("--overlap-seconds", type=int, default=argparse.SUPPRESS,
                        help="增量采集时水位回退的秒数（覆盖同秒多次更新）")
    tuning.add_argument("--timeout", type=float, default=argparse.SUPPRESS)
    tuning.add_argument("--retries", type=int, default=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command")
    # 选项也可以写在子命令之前（子命令里的同名选项优先）。子命令解析出的值
    # 为 None/默认值时回退到这些全局默认值。
    _add_common(parser, required=True)
    parser.add_argument("--overlap-seconds", type=int, default=5,
                        help="增量采集时水位回退的秒数（覆盖同秒多次更新）")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=3)

    for mode in ("initial", "update", "reconcile"):
        child = sub.add_parser(mode, parents=[common, tuning], help={
            "initial": "首次建库（专题全量列举）",
            "update": "继续更新（按水位增量，观察撤回）",
            "reconcile": "专题核对（全量确认移出）",
        }[mode])
        child.add_argument("--until", dest="date_until", default=None,
                           help="增量采集的日期上界（YYYY-MM-DDThh:mm:ssZ，含端点）")

    show = sub.add_parser("show", parents=[common], help="只读解释视图")
    show.add_argument("--record", default=None, help="解释单个 identifier")
    show.add_argument("--coverage", action="store_true", help="显示各格式覆盖状态")

    events = sub.add_parser("events", parents=[common], help="按专题查看事件流")
    events.add_argument("--kind", default=None)
    events.add_argument("--since-id", type=int, default=0)
    events.add_argument("--limit", type=int, default=200)

    raw = sub.add_parser("raw", parents=[common], help="导出一条原始响应")
    raw.add_argument("raw_id", type=int)
    raw.add_argument("--out", default=None)
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2
    missing = [name for name in ("source", "set_spec") if not getattr(args, name, None)]
    if missing:
        parser.error("缺少必需参数：" + "、".join(
            "--source" if n == "source" else "--set" for n in missing))
    state = getattr(args, "state", None) or _default_state(args.source)
    formats = getattr(args, "formats", None)

    if args.command in ("initial", "update", "reconcile"):
        harvester = Harvester(
            args.source, state, formats=formats,
            timeout=getattr(args, "timeout", 30.0),
            retries=getattr(args, "retries", 3),
            overlap_seconds=getattr(args, "overlap_seconds", 5))
        try:
            report = harvester.run(
                args.command, set_spec=args.set_spec,
                formats=formats, date_until=getattr(args, "date_until", None))
            _dump(report)
            code = 0 if not report["coverage"]["pending"] else 1
            return code
        except HarvestError as exc:
            _dump({"error": str(exc), "code": exc.code, "detail": exc.detail})
            return 2
        finally:
            harvester.close()

    db_path = state if state.endswith(".sqlite3") else state + "/harvest.sqlite3"
    if not os.path.exists(db_path):
        _dump({"error": "状态库不存在，请先运行 initial",
               "state": state})
        return 2
    store = Store(db_path)
    try:
        source = harvester_source(store, args.source)
        known_sets = {r["set_spec"] for r in store.query(
            "SELECT DISTINCT set_spec FROM runs WHERE source_url=?", (source,))}
        if args.set_spec not in known_sets:
            _dump({"error": "状态库没有该专题的采集记录",
                   "setSpec": args.set_spec, "knownSets": sorted(known_sets)})
            return 2
        if args.command == "show":
            if args.coverage:
                _dump(coverage_status(store, source, args.set_spec))
            elif args.record:
                _dump(explain_record(store, source, args.record))
            else:
                _dump(explain_set(store, source, args.set_spec))
        elif args.command == "events":
            from .query import _row
            rows = store.events(source, set_spec=args.set_spec, kind=args.kind,
                                since_id=args.since_id, limit=args.limit)
            _dump([_row(r) for r in rows])
        elif args.command == "raw":
            row = store.raw(args.raw_id)
            if row is None:
                _dump({"error": "找不到原始响应"})
                return 1
            if args.out:
                with open(args.out, "wb") as handle:
                    handle.write(row["body"])
                _dump({"written": args.out, "bytes": len(row["body"]),
                       "requestUrl": row["request_url"]})
            else:
                sys.stdout.buffer.write(row["body"])
                sys.stdout.buffer.write(b"\n")
        return 0
    finally:
        store.close()


def harvester_source(store: Store, source: str) -> str:
    """状态库中记录的来源标识就是规范化后的 base URL。"""
    normalized = source.rstrip("/")
    row = store.source(normalized)
    if row is None:
        known = store.query("SELECT source_url FROM sources")
        raise SystemExit(
            "该状态库中没有来源 %s；已有：%s"
            % (normalized, ", ".join(r["source_url"] for r in known) or "无"))
    return normalized


if __name__ == "__main__":
    raise SystemExit(main())
