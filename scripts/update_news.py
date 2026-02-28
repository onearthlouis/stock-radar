#!/usr/bin/env python3
"""股市信息雷达 - A股 + 港股信息聚合，基于 ai-news-radar 改编"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import feedparser
except ModuleNotFoundError:
    feedparser = None

UTC = timezone.utc
CST = ZoneInfo("Asia/Shanghai")

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# ── 热度追踪关键词 ──────────────────────────────────────────────────────────
HOT_KEYWORDS = [
    # 行业板块
    {"keyword": "新能源", "category": "板块"},
    {"keyword": "半导体", "category": "板块"},
    {"keyword": "芯片", "category": "板块"},
    {"keyword": "人工智能", "category": "板块"},
    {"keyword": "光伏", "category": "板块"},
    {"keyword": "储能", "category": "板块"},
    {"keyword": "机器人", "category": "板块"},
    {"keyword": "医药", "category": "板块"},
    {"keyword": "医疗", "category": "板块"},
    {"keyword": "消费", "category": "板块"},
    {"keyword": "白酒", "category": "板块"},
    {"keyword": "军工", "category": "板块"},
    {"keyword": "国防", "category": "板块"},
    {"keyword": "金融", "category": "板块"},
    {"keyword": "银行", "category": "板块"},
    {"keyword": "保险", "category": "板块"},
    {"keyword": "券商", "category": "板块"},
    {"keyword": "地产", "category": "板块"},
    {"keyword": "房地产", "category": "板块"},
    {"keyword": "汽车", "category": "板块"},
    {"keyword": "新能源车", "category": "板块"},
    {"keyword": "煤炭", "category": "板块"},
    {"keyword": "钢铁", "category": "板块"},
    {"keyword": "有色", "category": "板块"},
    {"keyword": "化工", "category": "板块"},
    {"keyword": "石油", "category": "板块"},
    {"keyword": "天然气", "category": "板块"},
    {"keyword": "5G", "category": "板块"},
    {"keyword": "云计算", "category": "板块"},
    {"keyword": "大数据", "category": "板块"},
    {"keyword": "量子", "category": "板块"},
    {"keyword": "低空经济", "category": "板块"},
    # 市场动态
    {"keyword": "涨停", "category": "市场"},
    {"keyword": "跌停", "category": "市场"},
    {"keyword": "连板", "category": "市场"},
    {"keyword": "北向资金", "category": "市场"},
    {"keyword": "外资", "category": "市场"},
    {"keyword": "融资融券", "category": "市场"},
    {"keyword": "IPO", "category": "市场"},
    {"keyword": "退市", "category": "市场"},
    {"keyword": "减持", "category": "市场"},
    {"keyword": "增持", "category": "市场"},
    {"keyword": "回购", "category": "市场"},
    {"keyword": "分红", "category": "市场"},
    # 港股
    {"keyword": "港股", "category": "港股"},
    {"keyword": "恒指", "category": "港股"},
    {"keyword": "恒生科技", "category": "港股"},
    {"keyword": "南向资金", "category": "港股"},
    # 宏观政策
    {"keyword": "降准", "category": "宏观"},
    {"keyword": "降息", "category": "宏观"},
    {"keyword": "LPR", "category": "宏观"},
    {"keyword": "CPI", "category": "宏观"},
    {"keyword": "PMI", "category": "宏观"},
    {"keyword": "GDP", "category": "宏观"},
    {"keyword": "通胀", "category": "宏观"},
    {"keyword": "美联储", "category": "宏观"},
    {"keyword": "央行", "category": "宏观"},
    {"keyword": "证监会", "category": "宏观"},
    {"keyword": "国务院", "category": "宏观"},
]

# ── 内置 RSS 数据源 ─────────────────────────────────────────────────────────
BUILTIN_RSS_SOURCES = [
    {
        "site_id": "wallstreetcn",
        "site_name": "华尔街见闻",
        "url": "https://wallstreetcn.com/rss",
        "source": "华尔街见闻",
    },
    {
        "site_id": "36kr",
        "site_name": "36氪",
        "url": "https://36kr.com/feed",
        "source": "36氪",
    },
    {
        "site_id": "huxiu",
        "site_name": "虎嗅网",
        "url": "https://www.huxiu.com/rss/0.xml",
        "source": "虎嗅网",
    },
    {
        "site_id": "sina_finance",
        "site_name": "新浪财经",
        "url": "https://rss.sina.com.cn/news/fin_roll/01.xml",
        "source": "财经滚动",
    },
    {
        "site_id": "stcn",
        "site_name": "证券时报",
        "url": "https://www.stcn.com/rss/index.xml",
        "source": "证券时报",
    },
    {
        "site_id": "cs",
        "site_name": "中国证券报",
        "url": "https://www.cs.com.cn/rss/rss.xml",
        "source": "中国证券报",
    },
    {
        "site_id": "tmtpost",
        "site_name": "钛媒体",
        "url": "https://www.tmtpost.com/rss",
        "source": "钛媒体",
    },
    {
        "site_id": "jiemian",
        "site_name": "界面新闻",
        "url": "https://www.jiemian.com/rss/index.xml",
        "source": "界面新闻",
    },
]


@dataclass
class RawItem:
    site_id: str
    site_name: str
    source: str
    title: str
    url: str
    published_at: datetime | None
    meta: dict[str, Any]


# ── 工具函数 ────────────────────────────────────────────────────────────────

def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def iso(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_iso(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        dt = dtparser.parse(dt_str)
    except Exception:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def normalize_url(raw_url: str) -> str:
    try:
        parsed = urlparse(raw_url.strip())
        if not parsed.scheme:
            return raw_url.strip()
        query = []
        for k, v in parse_qsl(parsed.query, keep_blank_values=True):
            lk = k.lower()
            if lk.startswith("utm_") or lk in {"ref", "spm", "fbclid"}:
                continue
            query.append((k, v))
        parsed = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment="",
            query=urlencode(query, doseq=True),
        )
        return urlunparse(parsed).rstrip("/")
    except Exception:
        return raw_url.strip()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": BROWSER_UA,
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ── RSS 抓取器 ──────────────────────────────────────────────────────────────

def fetch_rss(
    session: requests.Session,
    site_id: str,
    site_name: str,
    source: str,
    url: str,
    cutoff: datetime,
) -> tuple[list[RawItem], dict]:
    status = {"site_id": site_id, "site_name": site_name, "ok": False, "item_count": 0}
    items: list[RawItem] = []
    if feedparser is None:
        status["error"] = "feedparser 未安装"
        return items, status
    try:
        resp = session.get(url, timeout=25)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        for entry in feed.entries:
            title = (getattr(entry, "title", "") or "").strip()
            link = (getattr(entry, "link", "") or "").strip()
            if not title or not link:
                continue
            published_at: datetime | None = None
            for attr in ("published_parsed", "updated_parsed"):
                val = getattr(entry, attr, None)
                if val:
                    try:
                        published_at = datetime(*val[:6], tzinfo=UTC)
                    except Exception:
                        pass
                    break
            if published_at and published_at < cutoff:
                continue
            # 尝试从 tags 获取分类
            tags = getattr(entry, "tags", [])
            if tags:
                tag_obj = tags[0]
                tag_str = tag_obj.get("term", "") if isinstance(tag_obj, dict) else getattr(tag_obj, "term", "")
            else:
                tag_str = ""
            items.append(RawItem(
                site_id=site_id,
                site_name=site_name,
                source=tag_str or source,
                title=title,
                url=normalize_url(link),
                published_at=published_at,
                meta={},
            ))
        status["ok"] = True
        status["item_count"] = len(items)
    except Exception as e:
        status["error"] = str(e)
    return items, status


# ── 自定义抓取器 ─────────────────────────────────────────────────────────────

def fetch_eastmoney_flash(session: requests.Session, cutoff: datetime) -> tuple[list[RawItem], dict]:
    """东方财富快讯（HTML 抓取）"""
    site_id, site_name = "eastmoney", "东方财富"
    status = {"site_id": site_id, "site_name": site_name, "ok": False, "item_count": 0}
    items: list[RawItem] = []
    try:
        url = "https://kuaixun.eastmoney.com/"
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for li in soup.select(".news-item, .kuaixun-item, li[data-time]")[:50]:
            a = li.find("a")
            if not a:
                continue
            title = a.get_text(strip=True)
            link = a.get("href", "")
            if not title or not link:
                continue
            if not link.startswith("http"):
                link = "https://kuaixun.eastmoney.com" + link
            ts_str = li.get("data-time", "") or ""
            published_at: datetime | None = None
            if ts_str:
                try:
                    dt = dtparser.parse(ts_str)
                    if not dt.tzinfo:
                        dt = dt.replace(tzinfo=CST)
                    published_at = dt.astimezone(UTC)
                except Exception:
                    pass
            if published_at and published_at < cutoff:
                continue
            items.append(RawItem(
                site_id=site_id,
                site_name=site_name,
                source="快讯",
                title=title,
                url=normalize_url(link),
                published_at=published_at,
                meta={},
            ))
        status["ok"] = True
        status["item_count"] = len(items)
    except Exception as e:
        status["error"] = str(e)
    return items, status


def fetch_eastmoney(session: requests.Session, cutoff: datetime) -> tuple[list[RawItem], dict]:
    """东方财富快讯"""
    site_id, site_name = "eastmoney", "东方财富"
    status = {"site_id": site_id, "site_name": site_name, "ok": False, "item_count": 0}
    items: list[RawItem] = []
    try:
        url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_115_ajaxResult_1_15_.html"
        resp = session.get(url, timeout=20)
        data = resp.json()
        news_list = data.get("LiveList") or []
        for item in news_list:
            title = (item.get("title") or item.get("digest") or "").strip()
            link = item.get("url") or ""
            if not title:
                continue
            if not link:
                uid = item.get("UniqueID", "")
                link = f"https://stock.eastmoney.com/a/c{uid}.html" if uid else "https://www.eastmoney.com"
            ts_str = item.get("showtime")
            published_at: datetime | None = None
            if ts_str:
                try:
                    dt = dtparser.parse(ts_str)
                    if not dt.tzinfo:
                        dt = dt.replace(tzinfo=CST)
                    published_at = dt.astimezone(UTC)
                except Exception:
                    pass
            if published_at and published_at < cutoff:
                continue
            items.append(RawItem(
                site_id=site_id,
                site_name=site_name,
                source=item.get("tag") or item.get("column") or "快讯",
                title=title,
                url=normalize_url(link),
                published_at=published_at,
                meta={},
            ))
        status["ok"] = True
        status["item_count"] = len(items)
    except Exception as e:
        status["error"] = str(e)
    return items, status


def fetch_cls_telegraph(session: requests.Session, cutoff: datetime) -> tuple[list[RawItem], dict]:
    """财联社电报"""
    site_id, site_name = "cls", "财联社"
    status = {"site_id": site_id, "site_name": site_name, "ok": False, "item_count": 0}
    items: list[RawItem] = []
    try:
        url = "https://www.cls.cn/v1/bullet/flow/list"
        params = {"app": "CLS", "os": "web", "sv": "7.7.5"}
        headers = {
            "Referer": "https://www.cls.cn/telegraph",
            "Origin": "https://www.cls.cn",
        }
        resp = session.get(url, params=params, headers=headers, timeout=20)
        data = resp.json()
        roll_list = (data.get("data") or {}).get("roll_data") or []
        for item in roll_list:
            content = item.get("brief") or item.get("content") or ""
            if "<" in content:
                content = BeautifulSoup(content, "html.parser").get_text(" ", strip=True)
            content = content.strip()
            if not content:
                continue
            article_id = str(item.get("id", ""))
            link = f"https://www.cls.cn/detail/{article_id}" if article_id else "https://www.cls.cn/telegraph"
            ts = item.get("ctime")
            published_at: datetime | None = None
            if ts:
                try:
                    published_at = datetime.fromtimestamp(int(ts), tz=UTC)
                except Exception:
                    pass
            if published_at and published_at < cutoff:
                continue
            items.append(RawItem(
                site_id=site_id,
                site_name=site_name,
                source="电报",
                title=content,
                url=normalize_url(link),
                published_at=published_at,
                meta={},
            ))
        status["ok"] = True
        status["item_count"] = len(items)
    except Exception as e:
        status["error"] = str(e)
    return items, status


# ── OPML 解析 ───────────────────────────────────────────────────────────────

def load_opml_feeds(opml_path: str) -> list[dict]:
    feeds = []
    try:
        tree = ET.parse(opml_path)
        for outline in tree.getroot().iter("outline"):
            url = outline.get("xmlUrl", "")
            title = outline.get("title", "") or outline.get("text", "") or url
            if url:
                feeds.append({"url": url, "title": title})
    except Exception as e:
        print(f"OPML 解析错误: {e}")
    return feeds


# ── 热度榜计算 ──────────────────────────────────────────────────────────────

def compute_hot_topics(items: list[dict], window_hours: int) -> dict:
    counts: dict[str, int] = {}
    samples: dict[str, list[str]] = {}
    for item in items:
        text = (item.get("title") or "") + " " + (item.get("source") or "")
        for kw_info in HOT_KEYWORDS:
            kw = kw_info["keyword"]
            if kw in text:
                counts[kw] = counts.get(kw, 0) + 1
                if kw not in samples:
                    samples[kw] = []
                if len(samples[kw]) < 3:
                    samples[kw].append(item.get("title", ""))

    hot_topics = []
    for kw_info in HOT_KEYWORDS:
        kw = kw_info["keyword"]
        cnt = counts.get(kw, 0)
        if cnt > 0:
            hot_topics.append({
                "keyword": kw,
                "category": kw_info["category"],
                "count": cnt,
                "sample_titles": samples.get(kw, []),
            })
    hot_topics.sort(key=lambda x: x["count"], reverse=True)
    return {
        "generated_at": iso(utc_now()),
        "window_hours": window_hours,
        "total_items_analyzed": len(items),
        "hot_topics": hot_topics[:30],
    }


# ── 去重 & 归档 ─────────────────────────────────────────────────────────────

def item_uid(url: str, title: str) -> str:
    key = f"{normalize_url(url)}|{title[:80]}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def raw_to_record(item: RawItem, now: datetime) -> dict:
    uid = item_uid(item.url, item.title)
    return {
        "uid": uid,
        "site_id": item.site_id,
        "site_name": item.site_name,
        "source": item.source,
        "title": item.title,
        "url": item.url,
        "published_at": iso(item.published_at),
        "first_seen_at": iso(now),
        "last_seen_at": iso(now),
        "meta": item.meta,
    }


def dedupe_items(records: list[dict]) -> list[dict]:
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    result = []
    for r in records:
        url_key = normalize_url(r.get("url", ""))
        title_key = re.sub(r"\W+", "", (r.get("title") or "").lower())[:40]
        if url_key in seen_urls:
            continue
        if title_key and title_key in seen_titles:
            continue
        seen_urls.add(url_key)
        if title_key:
            seen_titles.add(title_key)
        result.append(r)
    return result


# ── 主函数 ──────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="股市信息雷达数据更新")
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--window-hours", type=int, default=24)
    parser.add_argument("--rss-opml", default=None)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    now = utc_now()
    cutoff = now - timedelta(hours=args.window_hours)
    session = make_session()

    # 加载归档
    archive_path = output_dir / "archive.json"
    archive: dict[str, dict] = {}
    if archive_path.exists():
        try:
            data = json.loads(archive_path.read_text(encoding="utf-8"))
            for rec in data.get("items", []):
                archive[rec["uid"]] = rec
        except Exception as e:
            print(f"归档加载错误: {e}")

    # 构建任务列表
    tasks: list[tuple] = []

    # 自定义抓取器
    for fn, name in [
        (fetch_eastmoney_flash, "eastmoney"),
        (fetch_cls_telegraph, "cls"),
    ]:
        tasks.append(("custom", name, fn, session, cutoff))

    # 内置 RSS
    for src in BUILTIN_RSS_SOURCES:
        tasks.append(("rss", src["site_id"], fetch_rss, session,
                      src["site_id"], src["site_name"], src["source"], src["url"], cutoff))

    # OPML 用户自定义 RSS
    if args.rss_opml:
        opml_feeds = load_opml_feeds(args.rss_opml)
        print(f"OPML: 加载 {len(opml_feeds)} 个 RSS 订阅")
        for feed in opml_feeds:
            sid = "rss_" + hashlib.md5(feed["url"].encode()).hexdigest()[:8]
            tasks.append(("rss", sid, fetch_rss, session, sid, feed["title"], feed["title"], feed["url"], cutoff))

    raw_items: list[RawItem] = []
    statuses: list[dict] = []

    def run_task(task):
        kind = task[0]
        if kind == "custom":
            _, name, fn, *fn_args = task
            try:
                return fn(*fn_args)
            except Exception as e:
                return [], {"site_id": name, "ok": False, "error": str(e), "item_count": 0}
        else:
            _, name, fn, *fn_args = task
            try:
                return fn(*fn_args)
            except Exception as e:
                return [], {"site_id": name, "ok": False, "error": str(e), "item_count": 0}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(run_task, t): t[1] for t in tasks}
        for future in as_completed(futures):
            result_items, status = future.result()
            raw_items.extend(result_items)
            statuses.append(status)
            ok_str = "OK" if status.get("ok") else f"FAIL({status.get('error', '')[:60]})"
            print(f"  {status['site_id']}: {ok_str} ({status.get('item_count', 0)} items)")

    print(f"原始条目: {len(raw_items)} 条")

    # 转换为记录 & 更新归档
    all_records: list[dict] = []
    for ri in raw_items:
        rec = raw_to_record(ri, now)
        all_records.append(rec)
        uid = rec["uid"]
        if uid in archive:
            archive[uid]["last_seen_at"] = rec["last_seen_at"]
        else:
            archive[uid] = rec

    # 时间窗过滤
    def in_window(rec: dict) -> bool:
        t = parse_iso(rec.get("published_at") or rec.get("first_seen_at"))
        return t is None or t >= cutoff

    latest = [r for r in all_records if in_window(r)]
    latest.sort(
        key=lambda x: parse_iso(x.get("published_at") or x.get("first_seen_at"))
                      or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    latest_deduped = dedupe_items(latest)

    # 清理旧归档（保留 7 天）
    archive_cutoff = now - timedelta(days=7)
    archive = {
        uid: rec for uid, rec in archive.items()
        if (parse_iso(rec.get("last_seen_at")) or now) >= archive_cutoff
    }

    # 站点统计
    site_stats: dict[str, dict] = {}
    for rec in latest_deduped:
        sid = rec["site_id"]
        if sid not in site_stats:
            site_stats[sid] = {"site_id": sid, "site_name": rec["site_name"], "count": 0}
        site_stats[sid]["count"] += 1

    # 热度榜
    hot_topics_payload = compute_hot_topics(latest, args.window_hours)

    # 构建输出
    latest_payload = {
        "generated_at": iso(now),
        "window_hours": args.window_hours,
        "total_items": len(latest_deduped),
        "total_items_raw": len(latest),
        "total_items_all_mode": len(latest_deduped),
        "archive_total": len(archive),
        "site_count": len(site_stats),
        "source_count": len({f"{r['site_id']}::{r['source']}" for r in latest_deduped}),
        "site_stats": sorted(site_stats.values(), key=lambda x: x["count"], reverse=True),
        "items": latest_deduped,
        "items_all": latest_deduped,
        "items_all_raw": latest,
    }
    archive_payload = {
        "generated_at": iso(now),
        "total_items": len(archive),
        "items": sorted(
            archive.values(),
            key=lambda x: parse_iso(x.get("last_seen_at")) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        ),
    }
    status_payload = {
        "generated_at": iso(now),
        "sites": statuses,
        "successful_sites": sum(1 for s in statuses if s.get("ok")),
        "failed_sites": [s["site_id"] for s in statuses if not s.get("ok")],
        "fetched_raw_items": len(raw_items),
    }

    (output_dir / "latest-24h.json").write_text(
        json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "archive.json").write_text(
        json.dumps(archive_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "source-status.json").write_text(
        json.dumps(status_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "hot-topics.json").write_text(
        json.dumps(hot_topics_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"✓ latest-24h.json: {len(latest_deduped)} 条（去重后）")
    print(f"✓ archive.json: {len(archive)} 条")
    print(f"✓ hot-topics.json: {len(hot_topics_payload['hot_topics'])} 个热词")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
