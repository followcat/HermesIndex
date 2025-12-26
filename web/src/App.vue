<template>
  <div v-if="!isAuthenticated" class="login-page">
    <div class="login-card">
      <h1>HermesIndex</h1>
      <p>请登录以继续</p>
      <div class="login-form">
        <input v-model="loginForm.username" type="text" placeholder="用户名" />
        <input v-model="loginForm.password" type="password" placeholder="密码" />
        <button class="action-btn primary" @click="doLogin" :disabled="authLoading">
          {{ authLoading ? "登录中..." : "登录" }}
        </button>
        <div v-if="authError" class="login-error">{{ authError }}</div>
      </div>
    </div>
  </div>

  <div v-else class="page">
    <section>
      <div class="hero">
        <div class="hero-title">
          <h1>HermesIndex</h1>
          <button class="theme-toggle" @click="toggleTheme">
            {{ themeLabel }}
          </button>
        </div>
        <p>向量化搜索你的种子、文件与内容库。支持 TMDB 扩展与 Qdrant 检索。</p>
        <form class="search-bar" autocomplete="off" @submit.prevent="runSearch(true)">
          <input
            v-model="query"
            type="search"
            name="search-query"
            autocomplete="off"
            inputmode="search"
            aria-autocomplete="none"
            autocorrect="off"
            autocapitalize="off"
            spellcheck="false"
            placeholder="输入关键词，例如电影名、别名或演员"
            @keyup.enter="runSearch(true)"
          />
          <button type="submit" :disabled="loading">{{ loading ? "搜索中..." : "搜索" }}</button>
        </form>
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
          <button class="action-btn" @click="prevPage" :disabled="!cursorStack.length || loading">
            上一页
          </button>
          <span>第 {{ currentPage }} 页</span>
          <button
            class="action-btn"
            @click="nextPage"
            :disabled="!nextCursor || loading"
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
          <template v-if="isTmdbResult(item)">
            <div class="tmdb-result">
              <img
                v-if="tmdbPosterUrl(item)"
                :src="tmdbPosterUrl(item)"
                alt="TMDB Poster"
                class="tmdb-result-poster"
              />
              <div class="tmdb-result-body">
                <div class="latest-title">
                  {{ tmdbTitle(item) }}
                  <span v-if="item.metadata.release_year">({{ item.metadata.release_year }})</span>
                </div>
                <div class="meta">
                  <span v-if="item.metadata.tmdb_id" class="badge">TMDB {{ item.metadata.tmdb_id }}</span>
                  <span v-else class="badge">{{ item.source }}</span>
                  <span v-if="item.metadata.type">{{ item.metadata.type }}</span>
                  <span v-if="item.metadata.genre">{{ item.metadata.genre }}</span>
                  <span v-if="item.metadata.updated_at">更新 {{ formatDate(item.metadata.updated_at) }}</span>
                </div>
              </div>
            </div>
          </template>
          <template v-else>
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
          </template>
          <div
            v-if="selected && itemKey(selected) === itemKey(item)"
            class="result-detail"
            @click.stop
          >
            <p class="result-detail-summary">{{ detailSummary }}</p>
            <p class="empty">文件列表：{{ fileListSummary }}</p>
            <div class="actions">
              <button class="action-btn primary" @click.stop="copyMagnet" :disabled="!magnetLink">
                复制磁力链接
              </button>
              <a v-if="magnetLink" class="action-btn" :href="magnetLink" @click.stop>直接下载</a>
              <button class="action-btn" @click.stop="clearSelection">清空详情</button>
              <span v-if="copyMessage" class="copy-hint">{{ copyMessage }}</span>
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
              <span v-if="selected.metadata.directors">导演</span>
              <div v-if="selected.metadata.directors">{{ selected.metadata.directors }}</div>
            </div>
            <div class="kv-collapses">
              <details v-if="selected.metadata.actors" class="kv-collapse">
                <summary>演员</summary>
                <div class="kv-collapse-body">{{ selected.metadata.actors }}</div>
              </details>
              <details v-if="selected.metadata.aka" class="kv-collapse">
                <summary>别名</summary>
                <div class="kv-collapse-body">{{ selected.metadata.aka }}</div>
              </details>
              <details v-if="selected.metadata.keywords" class="kv-collapse">
                <summary>关键词</summary>
                <div class="kv-collapse-body">{{ selected.metadata.keywords }}</div>
              </details>
            </div>
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
          <div
            v-for="item in latestTmdb"
            :key="item.content_uid"
            class="latest-item"
            :class="{ active: selectedLatest?.content_uid === item.content_uid }"
            @click="selectLatest(item)"
          >
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
            <div
              v-if="selectedLatest?.content_uid === item.content_uid"
              class="latest-detail"
              @click.stop
            >
              <div v-if="latestDetailLoading" class="empty">加载详情...</div>
              <div v-else-if="!latestDetail" class="empty">暂无详情</div>
              <div v-else class="latest-detail-body">
                <div class="latest-detail-grid">
                  <div v-if="latestDetail.poster_url" class="latest-poster">
                    <img :src="latestDetail.poster_url" alt="TMDB Poster" />
                  </div>
                  <div class="latest-plot">
                    <div class="latest-detail-title">{{ latestTitleSummary }}</div>
                    <div class="latest-meta-top">
                      <div v-if="latestDetail.type" class="latest-meta-row">
                        <span class="latest-meta-label">类型</span>
                        <button class="link-btn tag-btn" @click="searchFromText(latestDetail.type)">
                          {{ latestDetail.type }}
                        </button>
                      </div>
                      <div v-if="latestDetail.genre" class="latest-meta-row">
                        <span class="latest-meta-label">风格</span>
                        <div class="tag-list">
                          <button
                            v-for="(token, idx) in splitTokens(latestDetail.genre, 'default')"
                            :key="`top-genre-${idx}-${token}`"
                            class="link-btn tag-btn"
                            @click="searchFromText(token)"
                          >
                            {{ token }}
                          </button>
                        </div>
                      </div>
                      <div v-if="latestDetail.directors" class="latest-meta-row">
                        <span class="latest-meta-label">导演</span>
                        <div class="tag-list">
                          <button
                            v-for="(token, idx) in splitTokens(latestDetail.directors, 'person')"
                            :key="`top-directors-${idx}-${token}`"
                            class="link-btn tag-btn"
                            @click="searchFromText(token)"
                          >
                            {{ token }}
                          </button>
                        </div>
                      </div>
                      <div v-if="latestDetail.aka" class="latest-meta-row">
                        <span class="latest-meta-label">别名</span>
                        <span>{{ akaPreview(latestDetail.aka) }}</span>
                      </div>
                      <div v-if="latestPlotPreview" class="latest-meta-row">
                        <span class="latest-meta-label">简介</span>
                        <span>{{ latestPlotPreview }}</span>
                      </div>
                    </div>
                    <p class="latest-detail-plot">{{ latestPlotSummary }}</p>
                    <div class="latest-actions" v-if="latestMagnetLink">
                      <button class="action-btn primary" @click.stop="copyLatestMagnet">
                        复制磁力链接
                      </button>
                      <a class="action-btn" :href="latestMagnetLink" @click.stop>直接下载</a>
                      <span v-if="latestCopyMessage" class="copy-hint">{{ latestCopyMessage }}</span>
                    </div>
                    <div class="latest-actions" v-else-if="latestMagnetLoading">
                      <span class="empty">磁力链接加载中...</span>
                    </div>
                    <div class="latest-actions" v-else-if="latestMagnetError">
                      <span class="empty">未找到磁力链接</span>
                    </div>
                  </div>
                </div>
                <div class="latest-detail-info">
                    <details v-if="latestDetail.actors" class="latest-kv" :open="!isMobile" @click.stop>
                      <summary @click.stop>
                        演员
                        <span class="kv-preview">{{ actorPreview(latestDetail.actors) }}</span>
                      </summary>
                      <div class="kv-value">
                        <div class="tag-list">
                          <button
                            v-for="(token, idx) in splitTokens(latestDetail.actors, 'person')"
                            :key="`actors-${idx}-${token}`"
                            class="link-btn tag-btn"
                            @click="searchFromText(token)"
                          >
                            {{ token }}
                          </button>
                        </div>
                      </div>
                    </details>
                    <details v-if="latestDetail.aka" class="latest-kv" :open="!isMobile" @click.stop>
                      <summary @click.stop>
                        别名
                        <span class="kv-preview">{{ akaPreview(latestDetail.aka) }}</span>
                      </summary>
                      <div class="kv-value">
                        <div class="tag-list">
                          <button
                            v-for="(token, idx) in splitTokens(latestDetail.aka, 'person')"
                            :key="`aka-${idx}-${token}`"
                            class="link-btn tag-btn"
                            @click="searchFromText(token)"
                          >
                            {{ token }}
                          </button>
                        </div>
                      </div>
                    </details>
                    <details v-if="latestDetail.keywords" class="latest-kv" :open="!isMobile" @click.stop>
                      <summary @click.stop>关键词</summary>
                      <div class="kv-value">
                        <div class="tag-list">
                          <button
                            v-for="(token, idx) in splitTokens(latestDetail.keywords, 'default')"
                            :key="`keywords-${idx}-${token}`"
                            class="link-btn tag-btn"
                            @click="searchFromText(token)"
                          >
                            {{ token }}
                          </button>
                        </div>
                      </div>
                    </details>
                  </div>
                </div>
              </div>
            </div>
        </div>
      </div>
    </section>

    <div class="page-extra">
      <div v-if="isAuthenticated" class="password-panel">
        <h3>修改密码</h3>
        <div class="admin-actions">
          <input v-model="pwdForm.old_password" type="password" placeholder="旧密码" />
          <input v-model="pwdForm.new_password" type="password" placeholder="新密码" />
          <button class="action-btn primary" @click="changePassword" :disabled="pwdLoading">
            {{ pwdLoading ? "提交中..." : "更新密码" }}
          </button>
        </div>
        <div v-if="pwdMessage" class="login-error">{{ pwdMessage }}</div>
      </div>
      <div v-if="isAdmin" class="admin-panel">
        <h3>用户管理</h3>
        <div class="admin-actions">
          <input v-model="newUser.username" type="text" placeholder="用户名" />
          <input v-model="newUser.password" type="password" placeholder="密码" />
          <select v-model="newUser.role">
            <option value="user">用户</option>
            <option value="admin">管理员</option>
          </select>
          <button class="action-btn primary" @click="createUser" :disabled="userLoading">
            添加
          </button>
        </div>
        <div v-if="userError" class="login-error">{{ userError }}</div>
        <div class="admin-list">
          <div v-for="user in users" :key="user.username" class="admin-item">
            <span>{{ user.username }}</span>
            <span class="badge">{{ user.role }}</span>
            <button class="action-btn" @click="deleteUser(user.username)">删除</button>
          </div>
        </div>
      </div>
      <div class="footer">
        <span>API: {{ apiBase }}</span>
        <span v-if="currentUser" class="mono">用户: {{ currentUser.username }} ({{ currentUser.role }})</span>
        <button class="action-btn" @click="logout">退出</button>
      </div>
    </div>
  </div>
  <footer v-if="isAuthenticated" class="status-footer">
    <div class="status-title">同步状态</div>
    <div v-if="statusLoading" class="status-line">加载中...</div>
    <div v-else-if="!syncStatus" class="status-line">暂无状态</div>
    <div v-else class="status-lines">
      <div class="status-line">
        TMDB 内容: {{ syncStatus.tmdb_content_total }}
        <span v-if="syncStatus.tmdb_content_latest">（最新 {{ formatDateTime(syncStatus.tmdb_content_latest) }}）</span>
      </div>
      <div class="status-line">
        TMDB Enrichment: {{ syncStatus.tmdb_enrichment_total }}
        <span v-if="syncStatus.tmdb_enrichment_latest">（最新 {{ formatDateTime(syncStatus.tmdb_enrichment_latest) }}）</span>
        <span>缺失 aka/keywords: {{ syncStatus.tmdb_enrichment_missing }}</span>
      </div>
      <div class="status-grid">
        <div v-for="source in syncStatus.sources" :key="source.name" class="status-card">
          <div class="status-card-title">{{ source.name }}</div>
          <div class="status-card-body">
            <div>表: {{ source.table }}</div>
            <div>总量: {{ source.total_rows }}</div>
            <div>已同步: {{ source.synced_rows }}</div>
            <div v-if="source.max_updated_at">最新更新时间: {{ formatDateTime(source.max_updated_at) }}</div>
            <div v-if="source.last_sync_updated_at">上次同步: {{ formatDateTime(source.last_sync_updated_at) }}</div>
            <div v-if="source.max_synced_updated_at">最新同步数据: {{ formatDateTime(source.max_synced_updated_at) }}</div>
            <div>错误: {{ source.errors }}</div>
          </div>
        </div>
      </div>
      <div class="status-line">每 60 秒自动刷新</div>
    </div>
  </footer>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref } from "vue";

const apiBase = import.meta.env.VITE_API_BASE || "/api";
const authToken = ref(localStorage.getItem("auth_token") || "");
const authRequired = ref(true);
const currentUser = ref(null);
const theme = ref(localStorage.getItem("theme") || "light");
const isMobile = ref(false);
let mobileQuery = null;
let mobileQueryHandler = null;
const authLoading = ref(false);
const authError = ref("");
const loginForm = ref({ username: "", password: "" });
const query = ref("");
const pageSize = ref(20);
const cursor = ref(0);
const nextCursor = ref(null);
const cursorStack = ref([]);
const excludeNsfw = ref(true);
const tmdbOnly = ref(true);
const loading = ref(false);
const results = ref([]);
const selected = ref(null);
const selectedFiles = ref([]);
const filesLoading = ref(false);
const latestTmdb = ref([]);
const latestLoading = ref(false);
const selectedLatest = ref(null);
const latestDetail = ref(null);
const latestDetailLoading = ref(false);
const latestMagnetLink = ref("");
const latestMagnetLoading = ref(false);
const latestMagnetError = ref(false);
const copyMessage = ref("");
const latestCopyMessage = ref("");
let copyMessageTimer = null;
let latestCopyMessageTimer = null;
const syncStatus = ref(null);
const statusLoading = ref(false);
let statusTimer = null;
const users = ref([]);
const userLoading = ref(false);
const userError = ref("");
const newUser = ref({ username: "", password: "", role: "user" });
const pwdForm = ref({ old_password: "", new_password: "" });
const pwdLoading = ref(false);
const pwdMessage = ref("");

const emptyMessage = computed(() => {
  if (loading.value) return "搜索中...";
  if (!query.value) return "输入关键词开始搜索";
  return "没有找到结果";
});

const currentPage = computed(() => cursorStack.value.length + 1);

const filteredResults = computed(() => {
  const seen = new Set();
  const output = [];
  for (const item of results.value) {
    const key = resultKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }
  return output;
});
const isAuthenticated = computed(() => !authRequired.value || !!authToken.value);
const isAdmin = computed(() => currentUser.value?.role === "admin");
const themeLabel = computed(() => (theme.value === "dark" ? "浅色" : "深色"));

const detailSummary = computed(() => {
  if (!selected.value) return "";
  const meta = selected.value.metadata || {};
  return meta.plot || meta.overview || meta.hint_title || meta.title || "暂无简介";
});

const latestPlotSummary = computed(() => {
  const detail = latestDetail.value || {};
  return detail.plot || detail.overview || detail.title || "暂无简介";
});

const latestPlotPreview = computed(() => {
  const detail = latestDetail.value || {};
  const text = detail.plot || detail.overview || "";
  const trimmed = String(text).trim();
  if (!trimmed) return "";
  return trimmed.length > 100 ? `${trimmed.slice(0, 100)}...` : trimmed;
});

const latestTitleSummary = computed(() => {
  const detail = latestDetail.value || {};
  return detail.title || selectedLatest.value?.title || "未命名";
});

const fileListTotalSize = computed(() => {
  if (!selectedFiles.value.length) return "";
  let total = 0;
  for (const file of selectedFiles.value) {
    const size = Number(file?.size || 0);
    if (Number.isFinite(size) && size > 0) total += size;
  }
  if (!Number.isFinite(total) || total <= 0) return "";
  return prettySize(total);
});

const fileListSummary = computed(() => {
  if (filesLoading.value) return "加载中...";
  if (!selectedFiles.value.length) return "暂无文件列表";
  const total = fileListTotalSize.value;
  if (total) return `${selectedFiles.value.length} 个文件 · 合计 ${total}`;
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

function resultKey(item) {
  if (!item) return "";
  const meta = item.metadata || {};
  const infoHash = normalizeInfoHash(meta.info_hash || item.pg_id);
  if (infoHash) return `hash:${infoHash}`;
  const title = String(item.title || meta.title || "").trim().toLowerCase();
  const year = meta.release_year ? String(meta.release_year).trim() : "";
  if (title) return `title:${title}:${year}`;
  return itemKey(item);
}

function isTmdbResult(item) {
  const meta = item?.metadata || {};
  return Boolean(meta.tmdb_id || meta.source === "tmdb" || item?.source === "tmdb");
}

function tmdbTitle(item) {
  const meta = item?.metadata || {};
  return meta.title || item?.title || meta.original_title || "(无标题)";
}

function tmdbPosterUrl(item) {
  const meta = item?.metadata || {};
  if (meta.poster_url) return String(meta.poster_url);
  if (meta.poster_path) return `https://image.tmdb.org/t/p/w185${meta.poster_path}`;
  return "";
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

async function apiFetch(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (authToken.value) {
    headers.set("Authorization", `Bearer ${authToken.value}`);
  }
  const resp = await fetch(url, { ...options, headers });
  if (resp.status === 401) {
    authToken.value = "";
    localStorage.removeItem("auth_token");
    authRequired.value = true;
    currentUser.value = null;
  }
  return resp;
}

function applyTheme(value) {
  const next = value === "light" ? "light" : "dark";
  theme.value = next;
  localStorage.setItem("theme", next);
  document.documentElement.setAttribute("data-theme", next);
  document.documentElement.classList.toggle("theme-light", next === "light");
  if (document.body) {
    document.body.setAttribute("data-theme", next);
    document.body.classList.toggle("theme-light", next === "light");
  }
  const appRoot = document.getElementById("app");
  if (appRoot) {
    appRoot.setAttribute("data-theme", next);
    appRoot.classList.toggle("theme-light", next === "light");
  }
}

function toggleTheme() {
  applyTheme(theme.value === "dark" ? "light" : "dark");
}

function setupMobileQuery() {
  if (typeof window === "undefined" || !window.matchMedia) return;
  mobileQuery = window.matchMedia("(max-width: 900px)");
  const update = () => {
    isMobile.value = mobileQuery.matches;
  };
  mobileQueryHandler = update;
  update();
  if (mobileQuery.addEventListener) {
    mobileQuery.addEventListener("change", update);
  } else if (mobileQuery.addListener) {
    mobileQuery.addListener(update);
  }
}

function teardownMobileQuery() {
  if (!mobileQuery || !mobileQueryHandler) return;
  if (mobileQuery.removeEventListener) {
    mobileQuery.removeEventListener("change", mobileQueryHandler);
  } else if (mobileQuery.removeListener) {
    mobileQuery.removeListener(mobileQueryHandler);
  }
  mobileQuery = null;
  mobileQueryHandler = null;
}

async function loadMe() {
  authLoading.value = true;
  authError.value = "";
  try {
    const resp = await apiFetch(`${apiBase}/auth/me`);
    if (resp.status === 400) {
      authRequired.value = false;
      currentUser.value = { username: "guest", role: "guest" };
      fetchLatestTmdb();
      fetchSyncStatus();
      statusTimer = window.setInterval(fetchSyncStatus, 60000);
      return;
    }
    if (!resp.ok) {
      authRequired.value = true;
      currentUser.value = null;
      return;
    }
    const data = await resp.json();
    currentUser.value = data;
    if (data?.role === "guest") {
      authRequired.value = false;
    } else {
      authRequired.value = true;
      if (currentUser.value?.role === "admin") {
        fetchUsers();
      }
    }
    fetchLatestTmdb();
    fetchSyncStatus();
    statusTimer = window.setInterval(fetchSyncStatus, 60000);
  } catch (err) {
    console.error(err);
  } finally {
    authLoading.value = false;
  }
}

async function doLogin() {
  authLoading.value = true;
  authError.value = "";
  try {
    const resp = await fetch(`${apiBase}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(loginForm.value),
    });
    if (!resp.ok) {
      authError.value = "登录失败";
      return;
    }
    const data = await resp.json();
    authToken.value = data.token;
    localStorage.setItem("auth_token", data.token);
    currentUser.value = { username: data.username, role: data.role };
    authRequired.value = true;
    if (currentUser.value?.role === "admin") {
      fetchUsers();
    }
    fetchLatestTmdb();
    fetchSyncStatus();
    statusTimer = window.setInterval(fetchSyncStatus, 60000);
  } catch (err) {
    console.error(err);
    authError.value = "登录失败";
  } finally {
    authLoading.value = false;
  }
}

function logout() {
  authToken.value = "";
  localStorage.removeItem("auth_token");
  currentUser.value = null;
  authRequired.value = true;
  if (statusTimer) {
    window.clearInterval(statusTimer);
    statusTimer = null;
  }
}

async function changePassword() {
  if (!pwdForm.value.old_password || !pwdForm.value.new_password) {
    pwdMessage.value = "请输入旧密码和新密码";
    return;
  }
  pwdLoading.value = true;
  pwdMessage.value = "";
  try {
    const resp = await apiFetch(`${apiBase}/auth/password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(pwdForm.value),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    pwdForm.value = { old_password: "", new_password: "" };
    pwdMessage.value = "密码已更新";
  } catch (err) {
    console.error(err);
    pwdMessage.value = "修改失败";
  } finally {
    pwdLoading.value = false;
  }
}

async function fetchUsers() {
  if (!isAdmin.value) return;
  userLoading.value = true;
  userError.value = "";
  try {
    const resp = await apiFetch(`${apiBase}/auth/users`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    users.value = data.users || [];
  } catch (err) {
    console.error(err);
    userError.value = "获取用户失败";
  } finally {
    userLoading.value = false;
  }
}

async function createUser() {
  if (!isAdmin.value) return;
  userLoading.value = true;
  userError.value = "";
  try {
    const payload = { ...newUser.value };
    const resp = await apiFetch(`${apiBase}/auth/users`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    newUser.value = { username: "", password: "", role: "user" };
    fetchUsers();
  } catch (err) {
    console.error(err);
    userError.value = "添加失败";
  } finally {
    userLoading.value = false;
  }
}

async function deleteUser(username) {
  if (!isAdmin.value) return;
  userLoading.value = true;
  userError.value = "";
  try {
    const resp = await apiFetch(`${apiBase}/auth/users/${encodeURIComponent(username)}`, {
      method: "DELETE",
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    fetchUsers();
  } catch (err) {
    console.error(err);
    userError.value = "删除失败";
  } finally {
    userLoading.value = false;
  }
}

async function runSearch(resetPage = false) {
  if (!query.value.trim()) return;
  if (resetPage) {
    cursor.value = 0;
    nextCursor.value = null;
    cursorStack.value = [];
  }
  loading.value = true;
  try {
    const params = new URLSearchParams({
      q: query.value.trim(),
      topk: String(pageSize.value),
      exclude_nsfw: String(excludeNsfw.value),
      tmdb_only: String(tmdbOnly.value),
      page_size: String(pageSize.value),
      cursor: String(cursor.value || 0),
    });
    const resp = await apiFetch(`${apiBase}/search?${params.toString()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    results.value = data.results || [];
    nextCursor.value = data.next_cursor ?? null;
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

function formatDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

async function fetchLatestTmdb() {
  latestLoading.value = true;
  try {
    const resp = await apiFetch(`${apiBase}/tmdb_latest?limit=50`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    latestTmdb.value = data.results || [];
    if (latestTmdb.value.length && !selectedLatest.value) {
      selectLatest(latestTmdb.value[0]);
    }
  } catch (err) {
    console.error(err);
    latestTmdb.value = [];
  } finally {
    latestLoading.value = false;
  }
}

function selectLatest(item) {
  if (selectedLatest.value?.content_uid === item.content_uid) {
    selectedLatest.value = null;
    latestDetail.value = null;
    latestMagnetLink.value = "";
    latestMagnetError.value = false;
    return;
  }
  selectedLatest.value = item;
  latestDetail.value = null;
  latestMagnetLink.value = "";
  latestMagnetError.value = false;
  fetchLatestDetail(item);
  fetchLatestMagnet(item);
}

async function fetchLatestDetail(item) {
  if (!item?.tmdb_id) {
    latestDetail.value = {
      title: item?.title,
      release_year: item?.release_year,
      type: item?.type,
      genre: item?.genre,
    };
    return;
  }
  latestDetailLoading.value = true;
  try {
    const params = new URLSearchParams({
      tmdb_id: item.tmdb_id,
      content_type: item.type || "movie",
    });
    const resp = await apiFetch(`${apiBase}/tmdb_detail?${params.toString()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    latestDetail.value = data.detail
      ? {
          ...latestDetail.value,
          ...data.detail,
          title: latestDetail.value?.title || item?.title,
          release_year: latestDetail.value?.release_year || item?.release_year,
          type: latestDetail.value?.type || item?.type,
          genre: data.detail?.genre || latestDetail.value?.genre,
        }
      : latestDetail.value;
  } catch (err) {
    console.error(err);
    latestDetail.value = latestDetail.value || {
      title: item?.title,
      release_year: item?.release_year,
      type: item?.type,
      genre: item?.genre,
    };
  } finally {
    latestDetailLoading.value = false;
  }
}

function buildMagnetLink(item) {
  const meta = item?.metadata || {};
  const infoHash = normalizeInfoHash(meta.info_hash || item?.pg_id);
  if (!infoHash) return "";
  const name = encodeURIComponent(item?.title || meta.title || "torrent");
  return `magnet:?xt=urn:btih:${infoHash}&dn=${name}`;
}

async function fetchLatestMagnet(item) {
  const keyword = String(item?.title || item?.original_title || "").trim();
  if (!keyword) {
    latestMagnetError.value = true;
    return;
  }
  latestMagnetLoading.value = true;
  latestMagnetError.value = false;
  latestMagnetLink.value = "";
  try {
    const params = new URLSearchParams({
      q: keyword,
      topk: "10",
      exclude_nsfw: String(excludeNsfw.value),
      tmdb_only: "false",
      page_size: "10",
      cursor: "0",
    });
    const resp = await apiFetch(`${apiBase}/search?${params.toString()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    const hits = Array.isArray(data.results) ? data.results : [];
    for (const hit of hits) {
      const magnet = buildMagnetLink(hit);
      if (magnet) {
        latestMagnetLink.value = magnet;
        return;
      }
    }
    latestMagnetError.value = true;
  } catch (err) {
    console.error(err);
    latestMagnetError.value = true;
  } finally {
    latestMagnetLoading.value = false;
  }
}

function showCopyMessage(text) {
  copyMessage.value = text;
  if (copyMessageTimer) window.clearTimeout(copyMessageTimer);
  copyMessageTimer = window.setTimeout(() => {
    copyMessage.value = "";
  }, 2000);
}

function showLatestCopyMessage(text) {
  latestCopyMessage.value = text;
  if (latestCopyMessageTimer) window.clearTimeout(latestCopyMessageTimer);
  latestCopyMessageTimer = window.setTimeout(() => {
    latestCopyMessage.value = "";
  }, 2000);
}

async function copyText(text) {
  if (!text) return false;
  if (navigator?.clipboard?.writeText && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.error(err);
    }
  }
  try {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.top = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(textarea);
    return ok;
  } catch (err) {
    console.error(err);
    return false;
  }
}

async function copyLatestMagnet() {
  if (!latestMagnetLink.value) return;
  const ok = await copyText(latestMagnetLink.value);
  showLatestCopyMessage(ok ? "已复制到剪贴板" : "复制失败");
}

function searchFromText(text) {
  const keyword = String(text || "").trim();
  if (!keyword) return;
  tmdbOnly.value = true;
  query.value = keyword;
  runSearch(true);
}

function splitTokens(text, mode = "default") {
  const raw = String(text || "");
  const pattern =
    mode === "person" ? /[，,;/|\\n]+/ : /[，,;/·|\\n]+/;
  return raw
    .split(pattern)
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function actorPreview(text) {
  const tokens = splitTokens(text, "person");
  return tokens.slice(0, 3).join(" · ");
}

function akaPreview(text) {
  const tokens = splitTokens(text, "person");
  return tokens.slice(0, 3).join(" · ");
}

function directorsPreview(text) {
  const tokens = splitTokens(text, "person");
  return tokens.slice(0, 3).join(" · ");
}

async function fetchSyncStatus() {
  statusLoading.value = true;
  try {
    const resp = await apiFetch(`${apiBase}/sync_status`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    syncStatus.value = await resp.json();
  } catch (err) {
    console.error(err);
    syncStatus.value = null;
  } finally {
    statusLoading.value = false;
  }
}

onMounted(() => {
  applyTheme(theme.value);
  setupMobileQuery();
  loadMe();
});

onUnmounted(() => {
  if (statusTimer) {
    window.clearInterval(statusTimer);
    statusTimer = null;
  }
  if (copyMessageTimer) {
    window.clearTimeout(copyMessageTimer);
    copyMessageTimer = null;
  }
  if (latestCopyMessageTimer) {
    window.clearTimeout(latestCopyMessageTimer);
    latestCopyMessageTimer = null;
  }
  teardownMobileQuery();
});
function prevPage() {
  if (!cursorStack.value.length) return;
  cursor.value = cursorStack.value.pop();
  nextCursor.value = null;
  runSearch();
}

function nextPage() {
  if (!nextCursor.value) return;
  cursorStack.value.push(cursor.value);
  cursor.value = nextCursor.value;
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
    const resp = await apiFetch(`${apiBase}/torrent_files?info_hash=\\x${infoHash}`);
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
  const ok = await copyText(magnetLink.value);
  showCopyMessage(ok ? "已复制到剪贴板" : "复制失败");
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
