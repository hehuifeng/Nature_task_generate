#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rss_common.py — 共享工具/数据库/主题分类/翻译
"""

from __future__ import annotations
import os, re, io, json, time, hashlib, sqlite3, html, requests
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from bs4 import BeautifulSoup

# ---------- TZ ----------
try:
    from zoneinfo import ZoneInfo
    DEFAULT_TZ = ZoneInfo(os.getenv("PIPELINE_TZ", "Asia/Beijing"))
except Exception:
    DEFAULT_TZ = None

def now_dt():
    return datetime.now(DEFAULT_TZ) if DEFAULT_TZ else datetime.now().astimezone()

def now_ts() -> str:
    return now_dt().isoformat(timespec="seconds")

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def sha256(b: bytes | str) -> str:
    if isinstance(b, str):
        b = b.encode("utf-8", "ignore")
    return hashlib.sha256(b).hexdigest()

def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

HEADERS = {"User-Agent": "rss-nature-extractor/3.0 (+https://example.com)"}

# ---------------- DB schema （统一库 + tag + 运行日志） ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS feeds (
  feed_url TEXT PRIMARY KEY,
  etag TEXT,
  last_modified TEXT,
  last_checked_at TEXT
);
CREATE TABLE IF NOT EXISTS seen (
  uid TEXT PRIMARY KEY,
  feed_url TEXT NOT NULL,
  article_url TEXT,
  doi TEXT,
  pub_date TEXT,
  first_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS articles (
  uid TEXT PRIMARY KEY,
  feed_url TEXT NOT NULL,
  journal TEXT,
  title_en TEXT,
  title_cn TEXT,
  type TEXT,
  pub_date TEXT,
  doi TEXT,
  article_url TEXT,
  abstract_en TEXT,
  abstract_cn TEXT,
  raw_jsonld TEXT,
  fetched_at TEXT,
  last_updated_at TEXT,
  topic_tag TEXT
);
CREATE TABLE IF NOT EXISTS runs_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  script_name TEXT,
  started_at TEXT,
  finished_at TEXT,
  status TEXT,
  notes TEXT,
  rows_processed INTEGER
);
"""

def db_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA)
    _ensure_columns(conn)
    return conn

def _ensure_columns(conn: sqlite3.Connection):
    # 迁移：若 articles 表缺少列则补上
    cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
    need = {
        "topic_tag": "ALTER TABLE articles ADD COLUMN topic_tag TEXT;",
        "title_cn": "ALTER TABLE articles ADD COLUMN title_cn TEXT;",
        "abstract_cn": "ALTER TABLE articles ADD COLUMN abstract_cn TEXT;",
        "raw_jsonld": "ALTER TABLE articles ADD COLUMN raw_jsonld TEXT;"
    }
    for col, sql in need.items():
        if col not in cols:
            conn.execute(sql)
            conn.commit()

# ---------------- HTTP / XML ----------------
def fetch(url: str, headers: Dict[str, str], timeout: int = 25) -> requests.Response:
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r

def fetch_feed(url: str, etag: Optional[str], last_modified: Optional[str], timeout: int = 25) -> Tuple[int, Dict[str,str], Optional[bytes]]:
    h = dict(HEADERS)
    if etag: h["If-None-Match"] = etag
    if last_modified: h["If-Modified-Since"] = last_modified
    r = requests.get(url, headers=h, timeout=timeout)
    if r.status_code == 304:
        return 304, r.headers, None
    r.raise_for_status()
    return r.status_code, r.headers, r.content

def xml_root(xml_bytes: bytes):
    from xml.etree import ElementTree as ET
    return ET.fromstring(xml_bytes)

NS = {
    'rdf': "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    'rss': "http://purl.org/rss/1.0/",
    'dc': "http://purl.org/dc/elements/1.1/",
    'content': "http://purl.org/rss/1.0/modules/content/",
    'prism': "http://prismstandard.org/namespaces/basic/2.0/",
    'atom': "http://www.w3.org/2005/Atom",
}

def xml_text(node):
    return node.text.strip() if (node is not None and node.text) else None

def guess_doi(*cands) -> Optional[str]:
    for s in cands:
        if not s: continue
        m = re.search(r'\b(10\.\d{4,9}/[^\s<>"\']+)', str(s))
        if m:
            return m.group(1).rstrip(').,;')
    return None

def parse_feed(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = xml_root(xml_bytes)
    tag = root.tag.split('}',1)[-1] if '}' in root.tag else root.tag
    items = []

    def add(d):
        d["title"] = clean_text(d.get("title"))
        d["link"] = d.get("link")
        d["id_like"] = d.get("id_like")
        d["pub_date"] = clean_text(d.get("pub_date"))
        d["doi"] = clean_text(d.get("doi"))
        d["journal"] = clean_text(d.get("journal"))
        items.append(d)

    # RSS1.0
    if tag == "RDF" or root.find('rss:channel', NS) is not None:
        for it in root.findall('rss:item', NS):
            title = xml_text(it.find('rss:title', NS)) or xml_text(it.find('dc:title', NS))
            link = xml_text(it.find('rss:link', NS))
            about = it.get(f'{{{NS["rdf"]}}}about')
            dc_date = xml_text(it.find('dc:date', NS))
            prism_doi = xml_text(it.find('prism:doi', NS))
            prism_pub = xml_text(it.find('prism:publicationName', NS))
            dc_ident = xml_text(it.find('dc:identifier', NS))
            add({"title":title,"link":link,"id_like":about or dc_ident or link,
                 "pub_date":dc_date,"doi":prism_doi or guess_doi(dc_ident,link),
                 "journal":prism_pub})
        return items

    # RSS2.0
    if tag == "rss" or root.find('channel') is not None:
        ch = root.find('channel')
        for it in (ch.findall('item') if ch is not None else []):
            title = xml_text(it.find('title')) or xml_text(it.find('dc:title', NS))
            link = xml_text(it.find('link'))
            guid = xml_text(it.find('guid'))
            pubDate = xml_text(it.find('pubDate')) or xml_text(it.find('dc:date', NS))
            desc = xml_text(it.find('description'))
            add({"title":title,"link":link,"id_like":guid or link,"pub_date":pubDate,
                 "doi":guess_doi(guid,desc,link),"journal":None})
        return items

    # Atom
    if tag == "feed" or root.find('atom:feed', NS) is not None:
        feed = root
        for entry in feed.findall('atom:entry', NS) or feed.findall('entry'):
            title = xml_text(entry.find('atom:title', NS)) or xml_text(entry.find('title'))
            link_el = entry.find('atom:link[@rel=\"alternate\"]', NS) or entry.find('link')
            link = link_el.get('href') if link_el is not None else None
            entry_id = xml_text(entry.find('atom:id', NS)) or xml_text(entry.find('id'))
            published = xml_text(entry.find('atom:published', NS)) or xml_text(entry.find('published'))
            updated = xml_text(entry.find('atom:updated', NS)) or xml_text(entry.find('updated'))
            add({"title":title,"link":link,"id_like":entry_id or link,"pub_date":published or updated,
                 "doi":guess_doi(entry_id,link),"journal":None})
        return items
    return items

# --------------- 文章页面解析 ---------------
def soupify(html_text: str) -> BeautifulSoup:
    return BeautifulSoup(html_text, "lxml")

def parse_jsonld(soup: BeautifulSoup) -> List[dict]:
    out=[]
    for tag in soup.find_all("script", attrs={"type":"application/ld+json"}):
        try:
            data=json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list): out.extend(data)
        elif isinstance(data, dict): out.append(data)
    return out

def pick_article_obj(jsonlds: List[dict]) -> Optional[dict]:
    for obj in jsonlds:
        t = obj.get("@type") or obj.get("type")
        if isinstance(t, list): t = t[0]
        if t in ("Article","ScholarlyArticle","NewsArticle"):
            return obj
    return None

def extract_from_jsonld(obj: dict) -> Dict[str, Optional[str]]:
    if not obj: return {}
    def _getid(v):
        if isinstance(v, dict):
            return v.get("value") or v.get("@id")
        return v
    title = obj.get("headline") or obj.get("name")
    abstract = obj.get("abstract") or obj.get("description")
    date_published = obj.get("datePublished") or obj.get("dateCreated")
    journal = None
    atype = obj.get("articleSection") or obj.get("type")
    doi = None
    ident = obj.get("identifier")
    if isinstance(ident, list):
        for it in ident:
            s=_getid(it)
            if s and "10." in s:
                m=re.search(r'10\.\d{4,9}/\S+', s)
                if m: doi=m.group(0); break
    elif isinstance(ident, str) and "10." in ident:
        m=re.search(r'10\.\d{4,9}/\S+', ident)
        if m: doi=m.group(0)
    ispart = obj.get("isPartOf")
    if isinstance(ispart, dict):
        journal = ispart.get("name")
    publisher = obj.get("publisher")
    if isinstance(publisher, dict):
        journal = journal or publisher.get("name")
    return {
        "title": clean_text(title),
        "abstract": clean_text(abstract),
        "date_published": clean_text(date_published),
        "journal": clean_text(journal),
        "type": clean_text(atype),
        "doi": clean_text(doi),
    }

ABSTRACT_HINTS = [
    {"css": 'section[id*="abstract"], div[id*="abstract"], article[id*="abstract"]'},
    {"css": 'section[class*="abstract"], div[class*="abstract"], article[class*="abstract"]'},
    {"css": 'div#Abs1-content, div#Abs1, div#Abs1-section'},
    {"css": 'section#abstract, div#abstract, section.abstract, div.abstract'},
    {"css": 'div.Citation__abstract, div.article__abstract'},
    {"css": 'section.ArticleBody_abstract, section.Abstract'},
]

def _first_text_by_selectors(soup: BeautifulSoup, selectors: list[str]) -> Optional[str]:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = clean_text(el.get_text(" "))
            if txt and len(txt) > 40:
                return txt
    return None

def _meta(soup: BeautifulSoup, name: str) -> Optional[str]:
    tag = soup.find("meta", attrs={"name": name})
    if tag and tag.get("content"):
        return clean_text(tag["content"])
    return None

def extract_article_fields(html_text: str, url: str) -> Dict[str, Optional[str]]:
    # --- Nature 专用解析优先 ---
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if host.endswith("nature.com"):
        rec = _extract_nature_fields(html_text, url)
        if rec:  # 命中就直接返回
            return rec
    soup = soupify(html_text)
    jsonlds = parse_jsonld(soup)
    jsonld_obj = pick_article_obj(jsonlds)
    base = extract_from_jsonld(jsonld_obj) if jsonld_obj else {}
    # 兜底
    base.setdefault("journal", _meta(soup, "citation_journal_title"))
    base.setdefault("title", _meta(soup, "citation_title"))
    base.setdefault("doi", _meta(soup, "citation_doi"))
    base.setdefault("date_published", _meta(soup, "citation_publication_date"))
    base.setdefault("type", _meta(soup, "citation_article_type"))

    # 摘要优先级
    abstract = base.get("abstract")
    if not abstract:
        abstract = _meta(soup, "dc.description") or _meta(soup, "description")
    if not abstract:
        abstract = _first_text_by_selectors(soup, [h["css"] for h in ABSTRACT_HINTS])
    if not abstract:
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            abstract = clean_text(og["content"])
    if not abstract:
        tw = soup.find("meta", attrs={"name": "twitter:description"})
        if tw and tw.get("content"):
            abstract = clean_text(tw["content"])

    base["abstract"] = abstract
    base["article_url"] = url
    # 将所有 JSON-LD 原样保存（字符串）
    try:
        base["raw_jsonld"] = json.dumps(jsonlds, ensure_ascii=False)
    except Exception:
        base["raw_jsonld"] = None
    return base

# ---------------- OpenAI（可选） ----------------
def _openai_chat(messages: List[Dict[str,str]], cfg: Dict[str,Any]) -> Optional[str]:
    ocfg = (cfg.get("openai") or {})
    api_key = ocfg.get("api_key"); base_url = (ocfg.get("base_url") or "https://api.openai.com/v1").rstrip("/")
    model = ocfg.get("model") or "gpt-4o-mini"
    if not api_key: return None
    url = f"{base_url}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=cfg.get("http_timeout",60))
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

# 批量英->中翻译（可选）
def translate_batch_en2zh(texts: List[str], cfg: Dict[str,Any]) -> List[Optional[str]]:
    ocfg = cfg.get("openai") or {}
    if not ocfg.get("api_key"):
        return [None if t else None for t in texts]
    out=[]
    for t in texts:
        if not t:
            out.append(None)
            continue
        msg = [
            {"role":"system","content":"You are a precise EN->ZH translator. Return only the translation in Simplified Chinese, no extra text."},
            {"role":"user","content":f"请将以下英文准确翻译为简体中文：\n\n{t}"}
        ]
        zh = _openai_chat(msg, cfg)
        out.append(zh or None)
    return out

# ---------------- 主题分类 ----------------
TOPICS = ["生命科学","人工智能","3D打印和增材制造","其他"]

def classify_topic_heu(text: str) -> str:
    t = text.lower()

    # ---- 生命科学 ----
    if re.search(
        r"\b("
        r"genome|genomic|transcriptome|metagenome|proteome|"
        r"cellular|stem\s*cell|tissue|organ|neuron|neuro|"
        r"protein|rna|mrna|dna|"
        r"immun|immune|immunology|virus|viral|"
        r"cancer|tumor|oncology|pathway|biology|biological|biomed|biomedical|life\s*science|"
        r"biochemistry|molecular|"
        r"clinical trial|patient|disease|therapy"
        r")\b",
        t,
    ):
        return "生命科学"

    # ---- 人工智能 ----
    if re.search(
        r"\b("
        r"ai|artificial\s*intelligence|machine\s*learning|ml|deep\s*learning|dl|"
        r"neural\s*network|cnn|rnn|gan|gpt|transformer|bert|llm|foundation\s*model|"
        r"reinforcement\s*learning|rl|q-learning|"
        r"self\-supervised|supervised\s*learning|unsupervised\s*learning|few\-shot|zero\-shot|"
        r"transfer\s*learning|representation\s*learning|backpropagation|"
        r"computer\s*vision|image\s*recognition|object\s*detection|segmentation|"
        r"natural\s*language\s*processing|nlp|language\s*model|speech\s*recognition|"
        r"recommendation\s*system|knowledge\s*graph|ai\s*agent|autonomous|multi\-agent"
        r")\b",
        t,
    ):
        return "人工智能"

    # ---- 3D打印和增材制造 ----
    if re.search(
        r"\b("
        r"3d\s*print|3d\-print|additive\s*manufactur|rapid\s*prototyping|"
        r"fused\s*deposition\s*modeling|fdm|fused\s*filament\s*fabrication|fff|"
        r"stereolithography|sla|selective\s*laser\s*sintering|sls|"
        r"powder\s*bed|binder\s*jetting|direct\s*energy\s*deposition|ded|"
        r"electron\s*beam\s*melting|ebm|inkjet\s*printing|bioprinting|bio\-printing|"
        r"extrusion\s*printing|laser\s*melting|metal\s*printing|metal\s*additive"
        r")\b",
        t,
    ):
        return "3D打印和增材制造"

    return "其他"


def classify_topic(title: str, abstract: Optional[str], cfg: Dict[str,Any]) -> str:
    ocfg = (cfg.get("openai") or {})
    if ocfg.get("classifier", False) and ocfg.get("api_key"):
        prompt = (
            "请把下面论文按主题打一个且仅一个标签，候选集合："
            "['生命科学','人工智能','3D打印和增材制造','其他']。"
            "只输出标签本身，不要解释。\n"
            f"标题：{title}\n摘要：{abstract or ''}"
        )
        out = _openai_chat(
            [{"role":"system","content":"You are a precise tagger."},
             {"role":"user","content":prompt}], cfg)
        if out:
            out = out.strip().replace("'", "").replace(" ", "")
            for k in TOPICS:
                if k in out:
                    return k
    return classify_topic_heu(f"{title}\n{abstract or ''}")

# ---------------- 日志 ----------------
def log_run(conn: sqlite3.Connection, script_name: str, started_at: str, status: str, rows: int, notes: str=""):
    conn.execute(
        "INSERT INTO runs_log(script_name, started_at, finished_at, status, notes, rows_processed) VALUES (?,?,?,?,?,?)",
        (script_name, started_at, now_ts(), status, notes, rows)
    )
    conn.commit()


def parse_topic_tags(v: Optional[str]) -> List[str]:
    if not v: return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else [str(v)]
    except Exception:
        return [v]
    

# --- add imports if missing ---
from urllib.parse import urlparse
from bs4 import BeautifulSoup  # 确保已安装 bs4
import json as _json
import re as _re

_MONTHS = {
    'january':'01','february':'02','march':'03','april':'04','may':'05','june':'06',
    'july':'07','august':'08','september':'09','october':'10','november':'11','december':'12'
}

def _parse_date_human(s: str) -> str | None:
    """
    将 '19 August 2025' / '29 August 2025' 这类日期转为 '2025-08-19'
    """
    if not s: return None
    m = _re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', s)
    if not m: return None
    d, mon, y = m.group(1), m.group(2).lower(), m.group(3)
    mm = _MONTHS.get(mon)
    if not mm: return None
    return f"{y}-{mm}-{int(d):02d}"

def _first_text(el):
    return (el.get_text(" ", strip=True) if el else "").strip() or None

def _get_meta(soup, *names, prop=False):
    """
    取 meta[name=...] 或 meta[property=...] 的 content
    """
    for n in names:
        tag = soup.find("meta", attrs={("property" if prop else "name"): n})
        if tag and tag.get("content"): return tag["content"].strip()
    return None

def _extract_jsonld_candidates(soup: BeautifulSoup) -> list[dict]:
    """
    解析 <script type="application/ld+json">，返回 dict 列表
    """
    out = []
    for sc in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = _json.loads(sc.string or sc.text or "")
            if isinstance(data, list):
                out.extend([x for x in data if isinstance(x, dict)])
            elif isinstance(data, dict):
                out.append(data)
        except Exception:
            continue
    return out

def _jsonld_pick_article(objs: list[dict]) -> dict | None:
    """
    从 JSON-LD 里挑 ScholarlyArticle/NewsArticle/Article
    """
    for o in objs:
        t = (o.get("@type") or o.get("type") or "")
        if isinstance(t, list):
            tset = {str(x).lower() for x in t}
        else:
            tset = {str(t).lower()}
        if tset & {"scholarlyarticle","newsarticle","article","blogposting","reviewarticle"}:
            return o
    return None

def _jsonld_get(o: dict, *paths) -> str | None:
    """
    从 JSON-LD 对象里走多路径拿值；例如 ("isPartOf","name") 或 ("journal","name")
    """
    for p in paths:
        cur = o
        ok = True
        for key in p if isinstance(p, (list,tuple)) else (p,):
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False; break
        if ok and isinstance(cur, (str,int,float)):
            return str(cur).strip()
    return None
def _extract_nature_fields(html: str, url: str) -> dict | None:
    """
    专用解析：Nature 主站/子刊的 News/Research Highlight/Article。
    返回与你现有 extract_article_fields 一致的字段键：
      title, abstract, journal, type, date_published, doi, article_url, raw_jsonld
    """
    art_type = ""   # 避免后续引用未赋值
    soup = BeautifulSoup(html, "html.parser")

    # --- 小工具：判定日期完整性/规范化 ---
    def _is_full_ymd(s: str | None) -> bool:
        if not s:
            return False
        return _re.match(r'^\d{4}-\d{2}-\d{2}$', s) is not None

    def _is_incomplete_date(s: str | None) -> bool:
        # 为空、仅到年-月、或非标准形式，都视为“不完整”
        if not s:
            return True
        # 常见：YYYY/MM、YYYY-MM、YYYY.MM、YYYY年MM月
        if _re.match(r'^\d{4}[-/\.]\d{2}$', s):
            return True
        if _re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            return False
        # 其它杂格式交给 _parse_date_human 再判
        maybe = _parse_date_human(s.replace('/', '-'))
        return not _is_full_ymd(maybe)

    def _normalize_date_keep_precision(s: str | None) -> str | None:
        """
        仅做轻度规范化：把分隔符统一成 '-'，尝试解析。
        - 若能解析出 YYYY-MM-DD，返回该格式
        - 若只能解析到年或年-月，不强行猜测“日”，保持原样（但替换为 '-'）
        """
        if not s:
            return None
        s2 = s.strip().replace('/', '-').replace('.', '-')
        # 先直接命中 YYYY-MM-DD
        if _is_full_ymd(s2):
            return s2
        # 尝试人类日期解析（如 "27 August 2025"）
        maybe = _parse_date_human(s2)
        if _is_full_ymd(maybe):
            return maybe
        # 无法补齐“日”，尽量保持原意（到月/到年）
        # 规整成 YYYY-MM 或 YYYY
        m = _re.match(r'^(\d{4})-(\d{2})$', s2)
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        m = _re.match(r'^(\d{4})$', s2)
        if m:
            return m.group(1)
        return s2 or None

    # 1) 先尝试 meta[citation_*]（Nature Portfolio 很常见）
    title = _get_meta(soup, "citation_title") or _get_meta(soup, "dc.title")
    abstract = (_get_meta(soup, "dc.description") or
                _get_meta(soup, "description") or
                None)
    doi = (_get_meta(soup, "citation_doi") or
           _get_meta(soup, "dc.identifier") or
           None)
    journal = (_get_meta(soup, "citation_journal_title") or
               _get_meta(soup, "prism.publicationName") or
               None)
    art_type = (_get_meta(soup, "citation_article_type") or
                None)
    date_pub = (_get_meta(soup, "citation_online_date") or
                _get_meta(soup, "prism.publicationDate") or
                _get_meta(soup, "article:published_time", prop=True) or
                None)

    # 2) JSON-LD（可给出 @type / datePublished / isPartOf.name / identifier 等）
    raw_jsonld = None
    try:
        jsonlds = _extract_jsonld_candidates(soup)
        if jsonlds:
            raw_jsonld = _json.dumps(jsonlds, ensure_ascii=False)
            art = _jsonld_pick_article(jsonlds)
            if art:
                title = title or art.get("headline") or art.get("name")
                # 仅当现有 date_pub 为空或不完整时，才用 JSON-LD 的 datePublished 升级
                jp = art.get("datePublished")
                if jp and _is_incomplete_date(date_pub):
                    date_pub = jp
                # 期刊/站点名：isPartOf.name / publisher.name / journal.name
                journal = journal or _jsonld_get(art, ("isPartOf","name")) or _jsonld_get(art, ("publisher","name")) or _jsonld_get(art, ("journal","name"))
                # DOI：identifier 可能是字符串或对象
                cand_doi = art.get("identifier")
                if isinstance(cand_doi, str):
                    doi = doi or cand_doi
                elif isinstance(cand_doi, dict):
                    doi = doi or cand_doi.get("value") or cand_doi.get("doi")
                # 类型
                jtype = art.get("@type") or art.get("type")
                if isinstance(jtype, list):
                    jtype = ",".join([str(x) for x in jtype])
                art_type = str(jtype or "").lower()
            else:
                art_type = ""
    except Exception:
        art_type = ""

    # 3) 从正文顶部提取“类型 + 人类可读日期”（cookies_not_supported 时尤其需要）
    body_text = soup.get_text("\n", strip=True)

    # 类型（优先识别强标签）
    type_badge = None
    badge = soup.find(string=_re.compile(r'^(NEWS|RESEARCH HIGHLIGHT|EDITORIAL|COMMENT|CAREERS|NEWS & VIEWS)$', _re.I))
    if badge:
        type_badge = badge.strip().title()
    else:
        m_badge = _re.search(r'\b(NEWS|RESEARCH HIGHLIGHT|EDITORIAL|COMMENT|CAREERS|NEWS & VIEWS)\b', body_text, _re.I)
        if m_badge:
            type_badge = m_badge.group(1).title()

    # 3.a Published 文本（允许冒号可选）
    if _is_incomplete_date(date_pub):
        m_pub = _re.search(r'Published:?\s+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', body_text, _re.I)
        if m_pub:
            parsed = _parse_date_human(m_pub.group(1))
            if _is_full_ymd(parsed):
                date_pub = parsed

    # 3.b 直接利用 <time datetime="YYYY-MM-DD">（更稳）
    if _is_incomplete_date(date_pub):
        # 优先定位“Published …”项里的 time[datetime]
        time_candidate = None
        # 针对 Nature 页面结构的出版信息区块
        hist_list = soup.find("ul", attrs={"data-test": _re.compile(r"publication-history", _re.I)})
        if hist_list:
            # 在包含 "Published" 文本的项里找 time[datetime]
            for li in hist_list.find_all("li"):
                txt = li.get_text(" ", strip=True)
                if _re.search(r'\bPublished\b', txt, _re.I):
                    t = li.find("time", attrs={"datetime": True})
                    if t and t.get("datetime"):
                        time_candidate = t.get("datetime").strip()
                        break
        # 如果上面没找到，就退而在全页找一个 time[datetime]
        if not time_candidate:
            t = soup.find("time", attrs={"datetime": True})
            if t and t.get("datetime"):
                time_candidate = t.get("datetime").strip()

        if time_candidate:
            # 只取 YYYY-MM-DD 或者前 10 位
            d = time_candidate[:10] if len(time_candidate) >= 10 else time_candidate
            if _is_full_ymd(d):
                date_pub = d

    # DOI 兜底：正文可能有 “DOI: https://doi.org/…”
    if not doi:
        m_doi = _re.search(r'\b10\.\d{4,9}/\S+\b', body_text)
        if m_doi:
            doi = m_doi.group(0)

    # 期刊兜底：面包屑/页眉里通常会出现 “Nature Biotechnology”/“Nature”
    if not journal:
        # 取面包屑里第二个或页面大标题区域的期刊名
        crumb = soup.find("ol", attrs={"aria-label": _re.compile("breadcrumb", _re.I)}) or soup.find("nav", attrs={"aria-label": _re.compile("breadcrumb", _re.I)})
        if crumb:
            items = [a.get_text(" ", strip=True) for a in crumb.find_all("a")]
            if len(items) >= 2:
                # 例如 ["nature", "nature biotechnology", "news"] -> 取第 2 个做期刊名
                cand = (items[1] or "").strip()
                if cand:
                    journal = cand.title()
        # 兜底：页脚里常显示 “Nature Biotechnology (Nat Biotechnol)”
        if not journal:
            foot = soup.find(string=_re.compile(r'Nature\s+[A-Za-z ]+\s+\(Nat', _re.I)) or soup.find(string=_re.compile(r'^Nature\s*$', _re.I))
            if foot:
                journal = foot.strip()

    # 文章摘要兜底：正文第一段
    abstract = None
    if not abstract:
        art = soup.find("article") or soup
        first_p = art.find("p")
        abstract = _first_text(first_p)

    # 类型最终确定：JSON-LD 的类型 + 可视化徽章二选一
    type_final = (type_badge or art_type or None)
    if type_final:
        # 标准化几个常见值
        t = type_final.lower()
        if "research highlight" in t:
            type_final = "Research Highlight"
        elif "news" in t:
            type_final = "News"
        elif "editorial" in t:
            type_final = "Editorial"
        elif "comment" in t:
            type_final = "Comment"
        elif "careers" in t:
            type_final = "Careers"

    # 规范 DOI
    if doi and doi.lower().startswith("doi:"):
        doi = doi[4:].strip()
    if doi and doi.lower().startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/"):]

    # 最终日期规范化：尽力转成 YYYY-MM-DD；否则保持到月/到年
    date_pub = _normalize_date_keep_precision(date_pub)

    rec = {
        "title": title,
        "abstract": abstract,
        "journal": journal,
        "type": type_final,
        "date_published": date_pub,
        "doi": doi,
        "article_url": url,
        "raw_jsonld": raw_jsonld,
    }

    # 如果核心字段几乎都空，返回 None 让泛化解析继续处理
    core = any([rec.get("title"), rec.get("date_published"), rec.get("doi"), rec.get("journal")])
    return rec if core else None



# with open("page.html", "w", encoding="utf-8") as f:
#     f.write(r.text)