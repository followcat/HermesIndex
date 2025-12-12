HermesIndex
===========

实现一个离线向量化 + 在线语义搜索的最小可用版本，分为 GPU 推理节点与 CPU 同步/搜索节点。参考 `docs/init.md` 要求。

目录结构
--------
- `docs/init.md`：原始需求说明。
- `configs/example.yaml`：多数据源配置模板。
- `src/gpu_service/`：GPU 节点（`core/` 模型与推理，`main.py` FastAPI 入口）。
- `src/cpu/`：CPU 节点，分层：
  - `api/`：搜索 API。
  - `clients/`：外部服务客户端（GPU 节点）。
  - `core/`：基础能力（本地 embedder、工具）。
  - `repositories/`：PG 与向量库适配器。
  - `services/`：同步任务与作业入口。
- `sql/sync_state.sql`：同步状态表 schema。
- `requirements.txt`：依赖清单。

快速开始（开发）
---------------
1) 安装依赖（建议 Python 3.10+，虚拟环境）：
```
pip install -r requirements.txt
```
2) 准备 PostgreSQL：执行 `sql/sync_state.sql`。
3) 填写 `configs/example.yaml`，指定 PG 连接、数据源表与向量索引存储路径。

启动 GPU 推理服务
----------------
```
export MODEL_NAME=BAAI/bge-small-zh-v1.5  # 可选，或使用默认 mini 模型
PYTHONPATH=src uvicorn gpu_service.main:app --host 0.0.0.0 --port 8001
```
- `POST /infer`：同时返回 embedding 与 NSFW 分数。
- `POST /embed` 与 `POST /classify`：拆分接口。
- 支持批量输入，默认最大长度截断，可通过环境变量配置。

运行同步任务（CPU 节点）
-----------------------
```
PYTHONPATH=src python -m cpu.services.sync_runner --config configs/example.yaml
```
- 读取 PG，检测未同步/变更的记录。
- 批量调用 GPU 推理，写入向量索引与 `sync_state`。
- 幂等：重复执行只更新变更记录。

启动搜索 API（CPU 节点）
----------------------
```
PYTHONPATH=src uvicorn cpu.api.search:app --host 0.0.0.0 --port 8000 --reload
```
- `GET /search?q=...&topk=20&exclude_nsfw=true`
- 查询向量化后执行 ANN 检索，再回查 PG 返回结果。

关键设计说明
-----------
- 分离 GPU/CPU：GPU 仅做推理；CPU 负责存储、索引与在线查询。
- 配置驱动：sources 中指定 PG 表/字段、索引参数、同步批大小与并发。
- 向量索引：默认 HNSW（hnswlib），支持持久化；元数据存本地 JSONL，并在 `sync_state` 中记录 hash 与版本。
- 增量同步：依据文本哈希或 `updated_at` 字段；支持重试与断点续跑。
- NSFW 过滤：推理返回 `nsfw_score`，CPU 端按阈值标记并在搜索时过滤。

常见问题
-------
- 模型下载：`MODEL_NAME` 可指向已本地缓存的模型；无模型时会回退到轻量随机嵌入（仅用于联调）。
- 向量维度：与所选模型一致，需与配置 `dim` 保持一致；变更模型需重建索引。
- 数据量：首版面向百万级；HNSW 参数可在 `config` 中调整。

后续可拓展
---------
- 支持 Qdrant/Milvus 等服务化向量库。
- 增量任务调度器（celery/apscheduler）。
- 更丰富的标签与多模型版本共存。
