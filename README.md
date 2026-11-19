# 成果库合成资料源与收割器

此仓库提供一个本地授权演练用的 OAI-PMH 合成来源，以及按专题收集成果的收割器。
全部机构、课程、账号、作品及观测均为合成资料。使用前阅读
[来源契约](docs/来源契约.md) 与 [收割器说明](docs/收割器.md)。

## 来源服务

Python 3.11及以上。安装固定依赖后，运行：

    python -m pip install -r requirements.txt
    python -m source.server --port 8765

服务默认绑定本机环境的8765端口，健康检查地址为http://127.0.0.1:8765/health。
浏览器门户入口为契约所列资料端点。服务重启会重置演练状态。

## 收割器

收割器按专题收集可追溯的成果记录：两种元数据格式各自保留原文，专题移出与
成果撤回分开记录，未完成的页链不会被当成完整清单。

    python -m harvester initial   --source http://127.0.0.1:8765/oai --set teaching
    python -m harvester update    --source http://127.0.0.1:8765/oai --set teaching
    python -m harvester reconcile --source http://127.0.0.1:8765/oai --set teaching
    python -m harvester show      --source http://127.0.0.1:8765/oai --set teaching

`initial` 首次建库，`update` 按水位增量更新（含同秒重叠窗口，并观察撤回），
`reconcile` 做全量专题核对、确认移出。完整说明见 [收割器说明](docs/收割器.md)。

## 测试

来源契约测试：

    python -m unittest discover -s tests -v

收割器测试（使用同一合成来源演练建库、更新、核对、撤回与凭证失效；几乎全部
耗时花在来源的 HTTP 往返上）：

    python -m unittest discover -s tests -t . -v

## 容器

独立容器可用 docker build -t local-source . 构建，docker run --rm -p 8765:8765
local-source 启动。测试也可在构建镜像中运行：
docker run --rm --network none local-source python -m unittest discover -s tests -t . -v。
容器使用非root用户，测试仅连接同容器回环源。

来源测试只检查来源如何发布资料与响应页面操作。控制接口仅供本地演练，收割器
不访问控制接口，也不依赖它生成采集结果。文档与代码没有真实服务凭据。
