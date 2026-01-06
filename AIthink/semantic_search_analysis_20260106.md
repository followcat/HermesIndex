# 语义搜索慢问题分析 (2026-01-06)

## 问题现象
用户反馈线上 `/api/search` 很慢，甚至触发 nginx 504 Gateway Timeout。

## 日志分析

从用户提供的日志可以看到请求链路：
```
10:13:07,036 GraphQL 200 OK
10:13:07,245 WARNING tmdb query_expand failed ... error=syntax error at or near "$1"
10:13:07,380 HTTP /embed 200 OK          (~135ms)
10:13:07,459 HTTP Qdrant GET collection  (~79ms)
10:13:07,550 HTTP Qdrant search          (~91ms)
```

### 关键发现

1. **tmdb_expand 语法错误已修复**
   - 错误原因：psycopg3 的 `SET LOCAL statement_timeout = %s` 不支持参数化占位符
   - 修复：改用 f-string 直接拼接数值（已确认安全，因为值来自 int 转换）

2. **实际各阶段耗时（从日志推算）**
   - `tmdb_expand`: ~209ms（含失败降级）
   - `embed`: ~135ms
   - `qdrant`: ~170ms（含 collection check + search）
   - **总计**: ~514ms（这次请求）

3. **请求已经能正常返回**
   日志最后一行显示 `/search` 返回了 200 OK。

## 2026-01-06 11:10 新的 debug 数据分析

用户用 `debug=true` 获取了详细的 timing：

```json
{
  "tmdb_expand": 0.0,      // 已禁用 tmdb_expand=false
  "embed": 147.09,         // ~147ms - 正常
  "qdrant": 1742.39,       // ~1.7s - 较慢！
  "total": 132648.6        // ~132s - 异常！
}
```

### 问题根因

1. **Qdrant 查询慢 (1.7s)**
   - 向量库有 **4,002,689** 条向量
   - 对 400 万级向量，1.7s 偏高（正常应 <500ms）
   - 可能原因：Qdrant 资源不足、HNSW 参数未优化、或 payload 过大

2. **total 异常大 (132s)**
   - `tmdb_expand + embed + qdrant` 加起来只有 ~2s
   - 说明有未被计入的慢点
   - **最可能是 PG 回查**，尤其是 `bitmagnet_content` 这个 source（有 14 条 id 但没出现在 `pg_sources` 里）

3. **缺失的 source**
   - `ids_by_source` 显示 `bitmagnet_content: 14, bitmagnet_content_tpdb_movie: 9, bitmagnet_content_tpdb_jav: 8`
   - 但 `pg_sources` 只有后两者
   - 说明 `bitmagnet_content` 在 `source_map` 中不存在，被跳过了——但这不解释 132s

4. **新增 pg_loop timing**
   - 已添加 `timing_ms.pg_loop` 来精确测量 PG 回查循环总耗时
   - 部署后再次测试即可确认

## 之前 504 的原因推测

1. **tmdb_expand 失败时没有超时**
   - 修复前 `SET LOCAL statement_timeout = %s` 会抛语法错误
   - 但因为有 try/except，不会卡住，只是 warning 然后降级
   
2. **可能是 Qdrant/GPU 服务不稳定**
   - 日志显示这次请求很快，但之前可能有请求卡在 embed 或 qdrant

3. **nginx proxy_read_timeout 设置**
   - 默认通常是 60s，如果后端在 60s 内没返回就会 504

## 配置建议

### 1. 降低超时配置，快速失败
```yaml
search:
  gpu_timeout_seconds: 15

vector_store:
  type: qdrant
  timeout_seconds: 15
  http_timeout_seconds: 10

tmdb:
  query_expand_timeout_ms: 1500
```

### 2. 优化 Qdrant 性能（400 万向量）
```yaml
# 在 Qdrant 配置中调整 HNSW 参数
vector_store:
  type: qdrant
  # 降低 ef_search 可以提速但牺牲召回率
  # 默认 ef_search=128，可尝试 64
```

或者在 Qdrant 服务端配置：
```yaml
# qdrant 配置文件
service:
  max_search_threads: 4  # 根据 CPU 核数调整
```

### 3. 如果 tmdb_expand 慢，添加索引
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_aka_trgm 
  ON hermes.tmdb_enrichment USING gin (aka gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_keywords_trgm 
  ON hermes.tmdb_enrichment USING gin (keywords gin_trgm_ops);
```

### 4. 或直接关闭 tmdb_expand
在配置或请求中禁用：
```yaml
tmdb:
  query_expand: false
```
或请求时带 `tmdb_expand=false`。

## 新增的可调参数

| 参数 | 位置 | 默认值 | 说明 |
|------|------|--------|------|
| `tmdb_expand` | 请求参数 | true | 是否启用 TMDB 扩展 |
| `tmdb.query_expand_timeout_ms` | 配置 | 1500 | TMDB 扩展查询超时(ms) |
| `search.gpu_timeout_seconds` | 配置 | 60 | GPU embed 服务超时(s) |
| `vector_store.timeout_seconds` | 配置 | 60 | Qdrant client 超时(s) |
| `vector_store.http_timeout_seconds` | 配置 | 30 | Qdrant HTTP fallback 超时(s) |

## timing_ms 字段说明

| 字段 | 说明 |
|------|------|
| `tmdb_expand` | TMDB aka/keywords 扩展查询耗时 |
| `embed` | 调用 GPU/local embedder 耗时 |
| `qdrant` | Qdrant 向量检索耗时 |
| `pg_loop` | PG 回查循环总耗时（新增） |
| `total` | 整个 /search 端到端耗时 |

## 排查脚本

使用 `AIthink/benchmark_semantic_search.py`：
```bash
python3 AIthink/benchmark_semantic_search.py \
  --base 'http://127.0.0.1:8000' \
  --q 'jojo奇妙冒险' \
  --n 5 \
  --token 'YOUR_TOKEN'
```

输出会显示 `_debug.timing_ms`，包含各阶段耗时。

## 修复提交

1. `Fix SET LOCAL statement_timeout syntax for psycopg3` - 修复 tmdb_expand 语法错误
2. `Add tmdb_expand switch and timeout` - 添加 tmdb_expand 参数和超时
3. `Make GPU/Qdrant timeouts configurable` - 让 GPU/Qdrant 超时可配置
4. `Add pg_loop timing to debug output` - 添加 PG 回查循环计时
5. **`Skip keyword_search in semantic search path`** - 修复 136s 的 pg_loop 慢问题

## 根因分析 (136s pg_loop)

通过 `timing_ms.pg_loop` 定位到问题：
- `pg_sources` 显示两个 source 的 `pg_fetch_ms` 各 ~280ms
- 但 `pg_loop` 总计 136s

**原因**：source 配置了 `keyword_search: true`，导致语义搜索路径也会调用 `search_by_keyword()`，对 `hermes.content_view` 执行 `ILIKE '%jojo奇妙冒险%'` 全表扫描。

**修复**：在 `/search`（语义搜索）路径下跳过 `keyword_search` 调用。语义搜索已经有向量命中，不需要再做关键词补充。`keyword_search` 主要用于 `/search_keyword` 端点。

## 下一步

1. 线上部署最新代码
2. 用 `debug=true` 再次测试，贴出 `_debug.timing_ms`（含 `pg_loop`）确认各阶段耗时
3. 如果 `pg_loop` 很大，检查 `pg_sources` 中各 source 的 `pg_fetch_ms`
4. 如果 `qdrant` 仍然慢，考虑：
   - 降低 topk/fetch_k
   - 优化 Qdrant HNSW 参数
   - 检查 Qdrant 服务资源（CPU/内存/IO）
