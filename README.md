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
1) 安装依赖（建议 Python 3.11+，虚拟环境）：
```
pip install -r requirements.txt
```
> `requirements.txt` 包含可选依赖（Qdrant、Milvus、Celery）。如只用本地 HNSW，可在安装时移除对应包以减轻体积。
2) 准备 PostgreSQL：执行 `sql/sync_state.sql`。
3) 填写 `configs/example.yaml`，指定 PG 连接、数据源表与向量索引存储路径：
   - 本地索引：`vector_store.type=hnsw`（默认）。
   - Qdrant：`vector_store.type=qdrant`，配置 `url`/`collection`（需安装 `qdrant-client`）。
   - Milvus：`vector_store.type=milvus`，配置 `uri`/`collection`（需安装 `pymilvus`）。
   - 模型默认 `BAAI/bge-m3`（多语），如需更轻可改回 `bge-small-zh-v1.5` 并同步调整 `dim`。

bitmagnet 插件（可选）
--------------------
如果要对接 bitmagnet 数据库，可在配置中启用 `bitmagnet` 并自动创建必要的 view：
```
PYTHONPATH=src python -m cpu.services.bitmagnet_setup --config configs/example.yaml
```
示例配置已使用 `127.0.0.1` 占位，按需替换为实际地址或 DSN。

自动创建的 view（bitmagnet）
---------------------------
- `hermes.torrent_files_view`：为 `torrent_files` 生成唯一 `file_id`
- `hermes.content_view`：为 `content` 生成唯一 `content_uid`，并聚合 `search_text` 以便语义检索

TMDB 自动扩展（可选）
--------------------
`tmdb.auto_enrich=true` 时，同步 `bitmagnet_content` 会自动拉取 TMDB 信息（演员/导演/别名/关键词/剧情/类型）并写入 `hermes.tmdb_enrichment`。

bitmagnet 索引建议
-----------------
同步性能优先依赖 `updated_at` 索引，建议执行：
```
psql "$BITMAGNET_DSN" -f sql/bitmagnet_indexes.sql
```

短文本检索优化
--------------
- 同步/查询会做基础文本清洗（去除分辨率/编码/格式等噪声标记）。
- 对 BGE 模型默认加检索提示前缀以提升短文本语义召回。
- 支持在 Qdrant payload 中基于类型/语言/字幕进行过滤。

运行脚本（scripts/）
-------------------
- `scripts/run_gpu_multi.sh`：单进程多卡 GPU 服务（默认 `GPU_DEVICES=0,1,2,3`）
- `scripts/run_gpu_single.sh`：单进程单卡 GPU 服务
- `scripts/run_cpu_service.sh`：CPU 搜索服务
- `scripts/run_sync_all.sh`：同步任务 + TMDB enrich 并行（loop）
- `scripts/purge_hermes_data.sh`：清空 hermes schema 与向量库（需要二次确认）
- `scripts/entry.sh`：统一入口（`gpu-multi|gpu-single|cpu|sync-all|purge`）

前端（Vue）
-----------
位于 `web/`，默认走 `/api` 代理到 `http://127.0.0.1:8000`。

```
cd web
npm install
npm run dev
```

访问 `http://127.0.0.1:5173`，搜索结果支持详情展示与磁力链接复制。

认证与用户管理
--------------
开启后，前端会显示登录页，管理员可创建/删除用户（文件存储）。

```
auth:
  enabled: true
  admin_user: "admin"
  admin_password: "CHANGE_ME"
  user_store_path: "data/users.json"
  token_ttl_seconds: 86400
```

说明：
- `user_store_path` 保存轻量用户列表（JSON），不要提交到仓库。
- 登录后使用 `Bearer` token 访问接口（前端已自动处理）。
- 管理员可在右侧面板添加用户，新增账号即可登录。

启动 GPU 推理服务
----------------
```
export MODEL_NAME=BAAI/bge-m3  # 多语模型；如需轻量可改回 bge-small-zh-v1.5
export MAX_TOKEN_LENGTH=256
export BATCH_SIZE=16
export DEVICE=cuda
export GPU_DEVICES=0,1,2,3  # 单进程多卡推理
PYTHONPATH=src uvicorn gpu_service.main:app --host 0.0.0.0 --port 8001
```
- `POST /infer`：同时返回 embedding 与 NSFW 分数。
- `POST /embed` 与 `POST /classify`：拆分接口。
- 支持批量输入，默认最大长度截断，可通过环境变量配置。

运行同步任务（CPU 节点）
------------------------
```
PYTHONPATH=src python -m cpu.services.sync_runner --config configs/example.yaml
```
- 读取 PG，检测未同步/变更的记录。
- 批量调用 GPU 推理，写入向量索引与 `sync_state`。
- 幂等：重复执行只更新变更记录。
- 指定单个 source 同步：`--source torrents`

使用 Celery 调度增量同步（可选）
-----------------------------
```
CONFIG_PATH=configs/example.yaml \
PYTHONPATH=src \
celery -A cpu.services.celery_app worker --loglevel=INFO --concurrency=1

CONFIG_PATH=configs/example.yaml \
PYTHONPATH=src \
celery -A cpu.services.celery_app beat --loglevel=INFO
```
- `celery.schedule_seconds`（配置项）> 0 时自动启用定时全量增量同步。
- 对本地 HNSW 索引建议 worker `--concurrency=1` 避免并发写冲突；服务化向量库可并发更高。

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
- 向量索引：默认本地 HNSW（hnswlib），支持持久化；元数据存本地 JSONL，并在 `sync_state` 中记录 hash 与版本。可切换 Qdrant/Milvus 作为服务化向量库（配置 `vector_store.type`）。
- 增量同步：依据文本哈希或 `updated_at` 字段；支持重试与断点续跑。
- NSFW 过滤：推理返回 `nsfw_score`，CPU 端按阈值标记并在搜索时过滤。

常见问题
-------
- 模型下载：`MODEL_NAME` 可指向已本地缓存的模型；无模型时会回退到轻量随机嵌入（仅用于联调）。
- 向量维度：与所选模型一致，需与配置 `dim` 保持一致；变更模型需重建索引。
- 数据量：首版面向百万级；HNSW 参数可在 `config` 中调整。
- Celery + 本地 HNSW：因索引文件写入非并发安全，使用单 worker 或外部向量库（Qdrant/Milvus）以提升并发。
- 可选依赖：仅本地 HNSW 时可跳过 `qdrant-client`/`pymilvus`；不使用 Celery 可跳过 `celery`。

后续可拓展
---------
- 增量同步的分布式锁/去重（多 worker 或多实例）。
- 查询层缓存与热点结果缓存。
- 更丰富的标签、多模型版本共存与灰度迁移。
