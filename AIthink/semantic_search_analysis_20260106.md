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

### 2. 如果 tmdb_expand 慢，添加索引
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_aka_trgm 
  ON hermes.tmdb_enrichment USING gin (aka gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_keywords_trgm 
  ON hermes.tmdb_enrichment USING gin (keywords gin_trgm_ops);
```

### 3. 或直接关闭 tmdb_expand
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

## 下一步

1. 线上部署最新代码
2. 用 `debug=true` 再次测试，贴出 `_debug.timing_ms` 确认各阶段耗时
3. 如果某阶段仍慢，针对性优化
