<template>
  <div class="page">
    <section>
      <div class="hero">
        <h1>HermesIndex</h1>
        <p>向量化搜索你的种子、文件与内容库。支持 TMDB 扩展与 Qdrant 检索。</p>
        <div class="search-bar">
          <input
            v-model="query"
            type="text"
            placeholder="输入关键词，例如电影名、别名或演员"
            @keyup.enter="runSearch(true)"
          />
          <button @click="runSearch(true)" :disabled="loading">{{ loading ? "搜索中..." : "搜索" }}</button>
        </div>
        <div class="filters">
          <label class="chip">
            <input v-model="excludeNsfw" type="checkbox" />
            排除 NSFW
          </label>
          <label class="chip">
            <input v-model="tmdbOnly" type="checkbox" />
            仅 TMDB 记录
          </label>
          <span class="chip">每页: {{ pageSize }}</span>
          <input v-model.number="pageSize" type="range" min="5" max="50" />
        </div>
      </div>

      <div class="results">
        <div v-if="filteredResults.length === 0" class="empty">
          {{ emptyMessage }}
        </div>
        <div v-if="filteredResults.length" class="pager">
          <button class="action-btn" @click="prevPage" :disabled="page === 1 || loading">
            上一页
          </button>
          <span>第 {{ page }} / {{ totalPages }} 页</span>
          <button
            class="action-btn"
            @click="nextPage"
            :disabled="page >= totalPages || loading"
          >
            下一页
          </button>
        </div>
        <article
          v-for="item in filteredResults"
          :key="itemKey(item)"
          class="result-card"
          @click="selectItem(item)"
        >
          <h3>{{ item.title || "(无标题)" }}</h3>
          <div class="meta">
            <span class="badge">{{ item.source }}</span>
            <span>相似度 {{ item.score.toFixed(3) }}</span>
            <span v-if="item.metadata.release_year">{{ item.metadata.release_year }}</span>
            <span v-if="item.metadata.video_resolution">{{ item.metadata.video_resolution }}</span>
            <span v-if="item.metadata.collection_names">{{ formatList(item.metadata.collection_names) }}</span>
          </div>
          <div class="meta">
            <span v-if="item.metadata.tags">标签: {{ formatList(item.metadata.tags) }}</span>
            <span v-if="item.metadata.video_codec">编码: {{ item.metadata.video_codec }}</span>
            <span v-if="item.metadata.size">大小: {{ prettySize(item.metadata.size) }}</span>
          </div>
        </article>
      </div>

      <div class="latest">
        <div class="latest-header">
          <h2>最新 TMDB 收录</h2>
          <button class="action-btn" @click="fetchLatestTmdb" :disabled="latestLoading">
            {{ latestLoading ? "刷新中..." : "刷新" }}
          </button>
        </div>
        <div v-if="latestLoading" class="empty">加载中...</div>
        <div v-else-if="latestTmdb.length === 0" class="empty">暂无记录</div>
        <div v-else class="latest-list">
          <div v-for="item in latestTmdb" :key="item.content_uid" class="latest-item">
            <div class="latest-title">
              {{ item.title }}
              <span v-if="item.release_year">({{ item.release_year }})</span>
            </div>
            <div class="meta">
              <span class="badge">TMDB {{ item.tmdb_id }}</span>
              <span v-if="item.type">{{ item.type }}</span>
              <span v-if="item.genre">{{ item.genre }}</span>
              <span v-if="item.updated_at">更新 {{ formatDate(item.updated_at) }}</span>
            </div>
          </div>
        </div>
      </div>
    </section>

    <aside class="detail">
      <template v-if="selected">
        <h2>{{ selected.title || "未命名" }}</h2>
        <p>{{ detailSummary }}</p>
        <p class="empty">文件列表：{{ fileListSummary }}</p>
        <div class="actions">
          <button class="action-btn primary" @click="copyMagnet" :disabled="!magnetLink">
            复制磁力链接
          </button>
          <a v-if="magnetLink" class="action-btn" :href="magnetLink">直接下载</a>
          <button class="action-btn" @click="clearSelection">清空详情</button>
        </div>
        <div class="file-list" v-if="selectedFiles.length">
          <div v-for="file in selectedFiles" :key="file.index" class="file-item">
            <span class="file-index">#{{ file.index }}</span>
            <span class="file-path">{{ file.path }}</span>
            <span class="file-size">{{ prettySize(file.size) }}</span>
          </div>
        </div>
        <div class="kv">
          <span>来源</span>
          <div>{{ selected.source }}</div>
          <span>PG ID</span>
          <div class="mono">{{ selected.pg_id }}</div>
          <span v-if="selected.metadata.tmdb_id">TMDB</span>
          <div v-if="selected.metadata.tmdb_id">{{ selected.metadata.tmdb_id }}</div>
          <span v-if="selected.metadata.release_year">年份</span>
          <div v-if="selected.metadata.release_year">{{ selected.metadata.release_year }}</div>
          <span v-if="selected.metadata.video_resolution">分辨率</span>
          <div v-if="selected.metadata.video_resolution">{{ selected.metadata.video_resolution }}</div>
          <span v-if="selected.metadata.video_codec">编码</span>
          <div v-if="selected.metadata.video_codec">{{ selected.metadata.video_codec }}</div>
          <span v-if="selected.metadata.tags">标签</span>
          <div v-if="selected.metadata.tags">{{ formatList(selected.metadata.tags) }}</div>
          <span v-if="selected.metadata.actors">演员</span>
          <div v-if="selected.metadata.actors">{{ selected.metadata.actors }}</div>
          <span v-if="selected.metadata.directors">导演</span>
          <div v-if="selected.metadata.directors">{{ selected.metadata.directors }}</div>
          <span v-if="selected.metadata.aka">别名</span>
          <div v-if="selected.metadata.aka">{{ selected.metadata.aka }}</div>
        </div>
      </template>
      <template v-else>
        <h2>详情</h2>
        <p class="empty">选择一条结果查看详情与磁力下载。</p>
      </template>
      <div class="footer">API: {{ apiBase }}</div>
    </aside>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from "vue";

const apiBase = import.meta.env.VITE_API_BASE || "/api";
const query = ref("");
const pageSize = ref(20);
const page = ref(1);
const total = ref(0);
const excludeNsfw = ref(true);
const tmdbOnly = ref(false);
const loading = ref(false);
const results = ref([]);
const selected = ref(null);
const selectedFiles = ref([]);
const filesLoading = ref(false);
const latestTmdb = ref([]);
const latestLoading = ref(false);

const emptyMessage = computed(() => {
  if (loading.value) return "搜索中...";
  if (!query.value) return "输入关键词开始搜索";
  return "没有找到结果";
});

const totalPages = computed(() => {
  if (!total.value) return 1;
  return Math.max(1, Math.ceil(total.value / pageSize.value));
});

const filteredResults = computed(() => results.value);

const detailSummary = computed(() => {
  if (!selected.value) return "";
  const meta = selected.value.metadata || {};
  return meta.overview || meta.hint_title || meta.title || "暂无简介";
});

const fileListSummary = computed(() => {
  if (filesLoading.value) return "加载中...";
  if (!selectedFiles.value.length) return "暂无文件列表";
  return `${selectedFiles.value.length} 个文件`;
});

const magnetLink = computed(() => {
  if (!selected.value) return "";
  const meta = selected.value.metadata || {};
  const infoHash = normalizeInfoHash(meta.info_hash || selected.value.pg_id);
  if (!infoHash) return "";
  const name = encodeURIComponent(selected.value.title || "torrent");
  return `magnet:?xt=urn:btih:${infoHash}&dn=${name}`;
});

function itemKey(item) {
  return `${item.source}:${item.pg_id}`;
}

function normalizeInfoHash(raw) {
  if (!raw) return "";
  const text = String(raw).trim();
  const hex = text.startsWith("\\x") ? text.slice(2) : text;
  if (!/^[0-9a-fA-F]{40,64}$/.test(hex)) return "";
  return hex.toLowerCase();
}

function formatList(value) {
  if (Array.isArray(value)) return value.filter(Boolean).join(" · ");
  return value || "";
}

function prettySize(size) {
  const num = Number(size);
  if (!Number.isFinite(num)) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let idx = 0;
  let current = num;
  while (current >= 1024 && idx < units.length - 1) {
    current /= 1024;
    idx += 1;
  }
  return `${current.toFixed(1)} ${units[idx]}`;
}

async function runSearch(resetPage = false) {
  if (!query.value.trim()) return;
  if (resetPage) {
    page.value = 1;
  }
  loading.value = true;
  try {
    const params = new URLSearchParams({
      q: query.value.trim(),
      topk: String(pageSize.value),
      exclude_nsfw: String(excludeNsfw.value),
      tmdb_only: String(tmdbOnly.value),
      page: String(page.value),
      page_size: String(pageSize.value),
    });
    const resp = await fetch(`${apiBase}/search?${params.toString()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    results.value = data.results || [];
    total.value = data.total || results.value.length;
    if (filteredResults.value.length) {
      selected.value = filteredResults.value[0];
    }
  } catch (err) {
    console.error(err);
  } finally {
    loading.value = false;
  }
}

function formatDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString();
}

async function fetchLatestTmdb() {
  latestLoading.value = true;
  try {
    const resp = await fetch(`${apiBase}/tmdb_latest?limit=50`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    latestTmdb.value = data.results || [];
  } catch (err) {
    console.error(err);
    latestTmdb.value = [];
  } finally {
    latestLoading.value = false;
  }
}

onMounted(() => {
  fetchLatestTmdb();
});
function prevPage() {
  if (page.value <= 1) return;
  page.value -= 1;
  runSearch();
}

function nextPage() {
  if (page.value >= totalPages.value) return;
  page.value += 1;
  runSearch();
}

function selectItem(item) {
  selected.value = item;
  fetchTorrentFiles(item);
}

function clearSelection() {
  selected.value = null;
  selectedFiles.value = [];
}

async function fetchTorrentFiles(item) {
  const meta = item?.metadata || {};
  const infoHash = normalizeInfoHash(meta.info_hash || item.pg_id);
  if (!infoHash) {
    selectedFiles.value = [];
    return;
  }
  filesLoading.value = true;
  try {
    const resp = await fetch(`${apiBase}/torrent_files?info_hash=\\x${infoHash}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    selectedFiles.value = data.files || [];
  } catch (err) {
    console.error(err);
    selectedFiles.value = [];
  } finally {
    filesLoading.value = false;
  }
}

async function copyMagnet() {
  if (!magnetLink.value) return;
  try {
    await navigator.clipboard.writeText(magnetLink.value);
  } catch (err) {
    console.error(err);
  }
}
</script>

<style scoped>
.mono {
  font-family: "SFMono-Regular", "Menlo", "Consolas", monospace;
  word-break: break-all;
}

input[type="range"] {
  accent-color: var(--accent);
  width: 160px;
}
</style>
