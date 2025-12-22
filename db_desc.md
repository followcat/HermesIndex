Bitmagnet 数据库可用数据总结
============================

概览
----
本项目可从 bitmagnet 获取三类信息：
1) 可向量化检索的主文本数据（推荐做 source）
2) 标签/属性补充（用于搜索结果 metadata 或过滤）
3) 字典/来源映射（仅展示用途）

推荐主 source
-------------
1) torrents（种子级）
- 表：`public.torrents`
- 主键建议：`info_hash`（bytea，会在 SQL 中 ::text）
- 文本字段：`name`
- 增量字段：`updated_at`
- 推荐 extra_fields：`size`, `private`, `files_count`, `extension`
- 适用：种子标题检索、主入口检索

2) torrent_files（文件级）
- 表：`public.torrent_files`
- 注意：原表无唯一主键，必须构造唯一 ID（推荐 view）
- 推荐 view：
  - `file_id = encode(info_hash, 'hex') || ':' || index::text`
  - 视图：`hermes.torrent_files_view`
- 文本字段：`path`
- 增量字段：`updated_at`
- 推荐 extra_fields：`info_hash`, `index`, `extension`, `size`
- 适用：文件名检索、细粒度命中

3) content（内容级）
- 表：`public.content`
- 注意：无单列主键，建议用组合键构造唯一 ID
- 推荐 view：
  - `content_uid = type || ':' || source || ':' || id`
  - 视图：`hermes.content_view`
- 文本字段：`title`（可扩展到 `original_title`/`overview`）
- 增量字段：`updated_at`
- 推荐 extra_fields：`type`, `source`, `id`, `original_title`, `overview`, `adult`, `release_year`
- 适用：内容级语义检索（电影/剧集等）

标签/属性补充（推荐 join）
---------------------------
1) torrent_tags
- 表：`public.torrent_tags`
- 字段：`info_hash`, `name`
- 建议：用 `array_agg` 聚合为 `tags` 数组
- 用途：标签展示/过滤

2) torrent_hints
- 表：`public.torrent_hints`
- 常用字段：`title`, `release_year`, `video_resolution`, `video_codec`, `languages`, `episodes`
- 建议：作为 metadata 字段返回
- 用途：更丰富的展示/过滤

3) content_attributes
- 表：`public.content_attributes`
- 字段：`content_type`, `content_source`, `content_id`, `key`, `value`
- 建议：聚合为 key/value 数组（或只取感兴趣 key）
- 用途：附加属性（如 imdb id、poster_path）

4) content_collections / content_collections_content
- 表：`public.content_collections`, `public.content_collections_content`
- 关系：内容 -> collection（常见为 genre）
- 建议：join 后聚合为 `collection_names` / `collection_ids`
- 用途：类型/题材/合集标签

字典/来源映射（可选）
---------------------
1) torrent_sources
- 表：`public.torrent_sources`
- 字段：`key`, `name`
- 用途：来源 key 映射为展示名

2) metadata_sources
- 表：`public.metadata_sources`
- 字段：`key`, `name`
- 用途：元数据来源展示

实施注意点
----------
- HermesIndex 需要写 `sync_state` 表；建议用 `search_path=hermes,public` 并在 `hermes` schema 写入。
- `torrent_files` 和 `content` 需要通过 view 构造唯一 ID，避免同步状态冲突。
- join 多表时：
  - 1:many 表要用聚合（如 `array_agg`），否则会产生重复行。
  - 1:1 表可以直接字段展开。

建议的接入顺序
--------------
1) `torrents`（主入口）
2) `torrent_files`（文件名搜索）
3) `content`（内容级搜索）
4) 标签与 hint 作为 metadata 补充
