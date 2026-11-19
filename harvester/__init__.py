"""可追溯的 OAI-PMH 成果收割器。

公开入口：

    from harvester import Harvester

    harvester = Harvester("http://127.0.0.1:8765", "./harvest-state")
    result = harvester.run(mode="initial", set_spec="teaching")
    result = harvester.run(mode="update", set_spec="teaching")
    result = harvester.run(mode="reconcile", set_spec="teaching")

``result`` 是可直接序列化的字典。完整命令行见 ``python -m harvester --help``。
"""

from .engine import Harvester, HarvestError
from .store import Store

__all__ = ["Harvester", "HarvestError", "Store"]
__version__ = "1.0.0"
