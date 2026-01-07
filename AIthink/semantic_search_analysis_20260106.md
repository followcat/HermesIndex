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

## 根因分析 (搜索结果几乎全是 TMDB 数据)

用户反馈：语义搜索 "jojo奇妙冒险" 返回的几乎全是 TMDB content，没有普通种子。

### 原因 1：JAV gating 正则太宽泛（已修复）

原正则：`\b([A-Z]{2,6})[-_ ]?(\d{2,5})\b`

这会误匹配：
- `HD 720`, `MP4 1080` (视频格式)
- `S01 E05` (剧集编号)
- `DTS 5.1` (音频格式)
- `X264`, `H265` (编码器)

导致这些普通种子因为没有 `tpdb_id` 而被跳过 embedding。

**修复**：添加 false positive 前缀黑名单。

### 原因 2：跨语言语义匹配局限性（已改进）

测试发现：
- 中文查询 "jojo奇妙冒险" → 只返回 TMDB content（中文元数据）
- 英文查询 "JoJo Bizarre Adventure" → 返回 bitmagnet_torrents（英文文件名）

BGE-M3 虽然是多语言模型，但中文查询与英文 torrent 名称的向量距离仍然较远。

**改进**：增强 `tmdb_expand` 查询扩展逻辑：
- 原逻辑：只在 aka/keywords 中搜索查询词
- 新逻辑：**也搜索 content.title**，找到匹配的中文标题后，取其英文 aka 来扩展查询

这样 "jojo奇妙冒险" 会先匹配到 TMDB content 的中文标题，然后取其 aka（如 "JoJo's Bizarre Adventure"）来扩展，从而也能匹配英文 torrent。

**注意**：修复后需要重新同步 `bitmagnet_torrents` 和 `bitmagnet_torrent_files` 才能让普通种子入库。

## 下一步

1. 线上部署最新代码
2. 用 `debug=true` 再次测试，贴出 `_debug.timing_ms`（含 `pg_loop`）确认各阶段耗时
3. 如果 `pg_loop` 很大，检查 `pg_sources` 中各 source 的 `pg_fetch_ms`
4. 如果 `qdrant` 仍然慢，考虑：
   - 降低 topk/fetch_k
   - 优化 Qdrant HNSW 参数
   - 检查 Qdrant 服务资源（CPU/内存/IO）

## 2026-01-07 更新：限制查询扩展 tokens

### 问题
`tmdb_expand` 返回的 aka/keywords 太多（如 JoJo 有 20+ 别名），导致扩展后的查询串太长，embedding 质量下降。

### 修复

1. **`expand_query` 函数改进**：
   - 限制 TMDB 扩展 tokens 最多 8 个
   - 优先选择 ASCII/英文 tokens（利于跨语言匹配）
   - 过滤太短的 tokens（< 2 字符）

2. **`search_tmdb_expansions` 分隔符改进**：
   - 不再用空格分割（会破坏 "JoJo's Bizarre Adventure"）
   - 只用 `，,|/·` 作为分隔符
   - 保留完整的标题字符串

### 效果

修复前：
```
final_query: "jojo奇妙冒险 JoJo's Bizarre Adventure JoJo no Kimyō na Bōken ジョジョの奇妙な冒険 ..."（50+ tokens）
```

修复后：
```
final_query: "jojo奇妙冒险 JoJo's Bizarre Adventure JoJo no Kimyou na Bouken Stone Ocean Golden Wind"（约 10 tokens）
```

优先选择的是英文/ASCII tokens，这样更容易匹配英文 torrent 文件名。

## 2026-01-07 更新：跨语言混合搜索

### 问题
即使用 `tmdb_expand` 扩展了英文别名，中文查询 "jojo奇妙冒险" 仍然只返回 TMDB content，没有 torrents。

**原因**：混合中英文的查询串（如 "jojo奇妙冒险 JoJo's Bizarre Adventure ..."）在 BGE-M3 embedding 后的向量质量较差，无法很好地匹配纯英文的 torrent 文件名。

### 解决方案：双路搜索

1. **主搜索**：用原始/扩展查询搜索（命中 TMDB content）
2. **英文副搜索**：如果原始查询是非 ASCII（如中文），且有英文扩展词，则用纯英文扩展词再搜一次（命中英文 torrents）
3. **合并结果**：去重后按 score 排序

### 实现

新增 `extract_english_expansion()` 函数：
- 从 TMDB 扩展中提取 top 3 英文词
- 要求：纯 ASCII、≥3 字符

搜索流程改进：
```python
# 主搜索
results = vector_store.query(query_vec, ...)

# 如果原始查询非 ASCII 且有英文扩展
if english_expansion and not cleaned_query.isascii():
    english_vec = embed_query(english_expansion)
    english_results = vector_store.query(english_vec, ...)
    # 合并结果
    for r in english_results:
        if r not in results:
            results.append(r)
```

### Debug 输出新增

- `english_expansion`: 用于英文副搜索的查询词（如 "JoJo's Bizarre Adventure"）
- `timing_ms.english_search`: 英文副搜索耗时

### 预期效果

中文查询 "jojo奇妙冒险" 应该能同时返回：
- TMDB content（来自主搜索的中文匹配）
- Torrents（来自英文副搜索的英文匹配）

测试命令：
```bash
python3 /tmp/test_semantic_search.py 'http://127.0.0.1:8000' "$TOKEN"
```

## 2026-01-07 更新：修复结果合并排序问题

### 问题
英文副搜索执行了并返回了结果，但最终只显示 content，没有 torrents。

**原因**：合并逻辑有 bug
- 主搜索返回 100 个低分 content 结果（score ~0.5）
- 英文副搜索返回 100 个高分 torrent 结果（score ~0.9）
- 合并后直接 append，没有重新排序
- 导致低分 content 在前，高分 torrents 在后
- 后续只取前 100 个，所以全是 content

### 修复
在合并后按 score 降序重新排序，然后只取 top fetch_k：
```python
results.sort(key=lambda x: float(x.get("score", 0)), reverse=True)
results = results[:fetch_k]
```

### 预期效果
中文查询 "jojo奇妙冒险" 应该返回：
- 高分的 torrents（来自英文副搜索，score ~0.9）
- 低分的 TMDB content（来自主搜索，score ~0.5）

按 score 排序后，torrents 应该排在前面。

## 2026-01-07 Cross-Language Search Fix

### Problem
Chinese queries (e.g., "jojo奇妙冒险") were only returning TMDB content, no torrents, despite implementing English secondary search.

### Root Cause Analysis

1. **Initial Hypothesis (WRONG)**: English expansion string was being transformed differently
   - Added debug logging to compare embedding inputs
   - Applied same `expand_query()`, `normalize_title_text()`, and `query_prefix` to both searches
   - Result: Still no torrents from English search

2. **True Root Cause (FOUND)**: Metadata filter was blocking torrent results
   - Primary search extracted query filters including genres (e.g., "冒险" → `['冒险', 'Adventure']`)
   - English secondary search reused same `metadata_filter` with genre filter
   - **Problem**: Raw torrents (`bitmagnet_torrents`) don't have TMDB enrichment metadata (genres, tmdb_id)
   - Genre filter only matches content sources (`bitmagnet_content_*`) which have TMDB data
   - Result: English search returned 20 content results, 0 torrent results

### Solution

Modified English secondary search to use minimal metadata filter:
- **Before**: Applied full metadata filter including `genres`, `has_tmdb`, etc.
- **After**: Only apply `size_min` filter (torrents have size field)
- Removed genre/tmdb filters that require enrichment metadata

```python
# For English secondary search, only apply size filter (not genre/tmdb filters)
# because raw torrents don't have TMDB enrichment metadata
english_metadata_filter = None
if size_min_bytes:
    english_metadata_filter = {"size_min": size_min_bytes}
```

### Test Results (2026-01-07 15:33)

✅ **Test 1**: Chinese "jojo奇妙冒险" with TMDB expansion
- Results: 19 torrents
- Top score: 1.000 (perfect match)
- Top result: "JoJo's Bizarre Adventure"

✅ **Test 2**: Direct English "JoJo's Bizarre Adventure"
- Results: 19 torrents
- Top score: 1.000 (identical to Chinese query)

✅ **Test 3**: Chinese "海贼王" (One Piece)
- Results: 20 torrents (mix of English and Chinese titles)
- Top score: 0.713

### Architecture Summary

**Cross-Language Hybrid Search Flow**:
1. **Primary Search**: Chinese query → embed → Qdrant → returns TMDB content (with metadata)
2. **TMDB Expansion**: Extract English title from aka/keywords (e.g., "JoJo's Bizarre Adventure")
3. **Secondary Search**: English expansion → embed → Qdrant (no genre filter) → returns raw torrents
4. **Merge & Sort**: Combine results, dedupe, sort by score descending
5. **Result**: Chinese query gets both TMDB content AND English torrents with high scores

### Key Insights

1. **Metadata filters must match data availability**
   - Content sources: Have TMDB enrichment (genres, tmdb_id, etc.)
   - Torrent sources: Only have basic fields (title, size, etc.)
   - Can't apply enrichment-based filters to raw data

2. **Cross-language search requires careful filter design**
   - Primary search can use rich filters (for enriched content)
   - Secondary search should use minimal filters (for raw data)
   - Merge step combines both result types

3. **Debug logging was essential**
   - Tracked sources returned by each search
   - Identified that English search returned wrong source types
   - Led to metadata filter investigation

### Performance Impact

- English secondary search: ~2s (GPU embed + Qdrant query)
- Total overhead: ~2s when triggered (non-ASCII queries with TMDB expansion)
- No impact on ASCII queries (secondary search skipped)

### Files Modified

1. `src/cpu/api/search.py` (lines 703-728)
   - Implemented English secondary search with minimal metadata filter
   - Only applies size filter, skips genre/tmdb filters for raw torrent matching

### Configuration Recommendations

No configuration changes needed. The fix is automatic:
- ASCII queries: Single search (no overhead)
- Non-ASCII queries with TMDB expansion: Hybrid search (finds both content and torrents)
- Non-ASCII queries without TMDB: Single search (no English expansion)

