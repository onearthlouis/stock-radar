const state = {
  items: [],
  siteFilter: "",
  query: "",
  generatedAt: null,
};

const statsEl       = document.getElementById("stats");
const siteSelectEl  = document.getElementById("siteSelect");
const sitePillsEl   = document.getElementById("sitePills");
const newsListEl    = document.getElementById("newsList");
const updatedAtEl   = document.getElementById("updatedAt");
const searchInputEl = document.getElementById("searchInput");
const resultCountEl = document.getElementById("resultCount");
const itemTpl       = document.getElementById("itemTpl");
const hotListEl     = document.getElementById("hotList");
const hotUpdatedEl  = document.getElementById("hotUpdatedAt");

function fmtNumber(n) {
  return new Intl.NumberFormat("zh-CN").format(n || 0);
}

function fmtTime(iso) {
  if (!iso) return "时间未知";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "时间未知";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(d);
}

// ── 统计卡片 ────────────────────────────────────────────────────────────────
function setStats(payload) {
  const cards = [
    ["24h 资讯", fmtNumber(payload.total_items)],
    ["来源站点", fmtNumber(payload.site_count)],
    ["来源分组", fmtNumber(payload.source_count)],
    ["归档总量", fmtNumber(payload.archive_total || 0)],
  ];
  statsEl.innerHTML = "";
  cards.forEach(([k, v]) => {
    const node = document.createElement("div");
    node.className = "stat";
    node.innerHTML = `<div class="k">${k}</div><div class="v">${v}</div>`;
    statsEl.appendChild(node);
  });
}

// ── 热度榜 ──────────────────────────────────────────────────────────────────
const CATEGORY_COLORS = {
  "板块": "#d94825",
  "市场": "#0f6f7f",
  "港股": "#7c3aed",
  "宏观": "#d97706",
};

function renderHotTopics(payload) {
  if (!payload || !payload.hot_topics) return;
  hotUpdatedEl.textContent = `更新：${fmtTime(payload.generated_at)}`;

  const topics = payload.hot_topics.slice(0, 24);
  const maxCount = topics.length > 0 ? topics[0].count : 1;
  hotListEl.innerHTML = "";

  topics.forEach((topic, i) => {
    const color = CATEGORY_COLORS[topic.category] || "#d94825";
    const intensity = Math.max(0.15, topic.count / maxCount);
    const btn = document.createElement("button");
    btn.className = "hot-chip";
    btn.style.cssText = `--c:${color};--a:${intensity.toFixed(2)}`;
    btn.innerHTML =
      `<span class="rank">${i + 1}</span>` +
      `<span class="kw">${topic.keyword}</span>` +
      `<span class="cnt">${topic.count}</span>` +
      `<span class="cat">${topic.category}</span>`;
    btn.title = topic.sample_titles ? topic.sample_titles.slice(0, 2).join("\n") : "";
    btn.addEventListener("click", () => {
      state.query = topic.keyword;
      searchInputEl.value = topic.keyword;
      renderList();
      document.querySelector(".list-wrap")?.scrollIntoView({ behavior: "smooth" });
    });
    hotListEl.appendChild(btn);
  });
}

// ── 来源筛选 ────────────────────────────────────────────────────────────────
function computeSiteStats() {
  const m = new Map();
  state.items.forEach((item) => {
    if (!m.has(item.site_id)) {
      m.set(item.site_id, { site_id: item.site_id, site_name: item.site_name, count: 0 });
    }
    m.get(item.site_id).count += 1;
  });
  return Array.from(m.values()).sort((a, b) => b.count - a.count);
}

function renderSiteFilters() {
  const stats = computeSiteStats();
  siteSelectEl.innerHTML = '<option value="">全部来源</option>';
  stats.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s.site_id;
    opt.textContent = `${s.site_name} (${s.count})`;
    siteSelectEl.appendChild(opt);
  });
  siteSelectEl.value = state.siteFilter;

  sitePillsEl.innerHTML = "";
  const allPill = document.createElement("button");
  allPill.className = `pill ${state.siteFilter === "" ? "active" : ""}`;
  allPill.textContent = "全部";
  allPill.addEventListener("click", () => {
    state.siteFilter = "";
    renderSiteFilters();
    renderList();
  });
  sitePillsEl.appendChild(allPill);

  stats.forEach((s) => {
    const btn = document.createElement("button");
    btn.className = `pill ${state.siteFilter === s.site_id ? "active" : ""}`;
    btn.textContent = `${s.site_name} ${s.count}`;
    btn.addEventListener("click", () => {
      state.siteFilter = s.site_id;
      renderSiteFilters();
      renderList();
    });
    sitePillsEl.appendChild(btn);
  });
}

// ── 新闻列表 ────────────────────────────────────────────────────────────────
function getFilteredItems() {
  const q = state.query.trim().toLowerCase();
  return state.items.filter((item) => {
    if (state.siteFilter && item.site_id !== state.siteFilter) return false;
    if (!q) return true;
    const hay = `${item.title || ""} ${item.site_name || ""} ${item.source || ""}`.toLowerCase();
    return hay.includes(q);
  });
}

function renderItemNode(item) {
  const node = itemTpl.content.firstElementChild.cloneNode(true);
  node.querySelector(".site").textContent = item.site_name;
  node.querySelector(".source").textContent = item.source || "";
  node.querySelector(".time").textContent = fmtTime(item.published_at || item.first_seen_at);
  const titleEl = node.querySelector(".title");
  titleEl.textContent = item.title;
  titleEl.href = item.url;
  return node;
}

function groupBySource(items) {
  const m = new Map();
  items.forEach((item) => {
    const key = item.source || "综合";
    if (!m.has(key)) m.set(key, []);
    m.get(key).push(item);
  });
  return Array.from(m.entries()).sort((a, b) => b[1].length - a[1].length);
}

function renderList() {
  const filtered = getFilteredItems();
  resultCountEl.textContent = `${fmtNumber(filtered.length)} 条`;
  newsListEl.innerHTML = "";

  if (!filtered.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = state.query ? `没有包含「${state.query}」的结果` : "当前没有数据";
    newsListEl.appendChild(empty);
    return;
  }

  // 按站点分组，再按来源分组
  const siteMap = new Map();
  filtered.forEach((item) => {
    if (!siteMap.has(item.site_id)) {
      siteMap.set(item.site_id, { siteName: item.site_name, items: [] });
    }
    siteMap.get(item.site_id).items.push(item);
  });

  const sites = Array.from(siteMap.entries()).sort((a, b) => b[1].items.length - a[1].items.length);
  const frag = document.createDocumentFragment();

  sites.forEach(([, site]) => {
    const siteSection = document.createElement("section");
    siteSection.className = "site-group";
    siteSection.innerHTML = `
      <header class="site-group-head">
        <h3>${site.siteName}</h3>
        <span>${fmtNumber(site.items.length)} 条</span>
      </header>
      <div class="site-group-list"></div>
    `;
    const siteListEl = siteSection.querySelector(".site-group-list");
    groupBySource(site.items).forEach(([source, groupItems]) => {
      const section = document.createElement("section");
      section.className = "source-group";
      section.innerHTML = `
        <header class="source-group-head">
          <h3>${source}</h3>
          <span>${fmtNumber(groupItems.length)} 条</span>
        </header>
        <div class="source-group-list"></div>
      `;
      const listEl = section.querySelector(".source-group-list");
      groupItems.forEach((item) => listEl.appendChild(renderItemNode(item)));
      siteListEl.appendChild(section);
    });
    frag.appendChild(siteSection);
  });

  newsListEl.appendChild(frag);
}

// ── 初始化 ──────────────────────────────────────────────────────────────────
async function init() {
  const [newsResult, hotResult] = await Promise.allSettled([
    fetch(`./data/latest-24h.json?t=${Date.now()}`).then((r) => r.json()),
    fetch(`./data/hot-topics.json?t=${Date.now()}`).then((r) => r.json()),
  ]);

  if (newsResult.status === "fulfilled") {
    const payload = newsResult.value;
    state.items = payload.items || payload.items_all || [];
    state.generatedAt = payload.generated_at;
    setStats(payload);
    renderSiteFilters();
    renderList();
    updatedAtEl.textContent = `更新：${fmtTime(state.generatedAt)}`;
  } else {
    updatedAtEl.textContent = "数据加载失败";
    newsListEl.innerHTML = `<div class="empty">数据加载失败，请稍后刷新重试</div>`;
  }

  if (hotResult.status === "fulfilled") {
    renderHotTopics(hotResult.value);
  } else {
    hotUpdatedEl.textContent = "暂无数据";
  }
}

// ── 事件绑定 ────────────────────────────────────────────────────────────────
searchInputEl.addEventListener("input", (e) => {
  state.query = e.target.value;
  renderList();
});

siteSelectEl.addEventListener("change", (e) => {
  state.siteFilter = e.target.value;
  renderSiteFilters();
  renderList();
});

init();
