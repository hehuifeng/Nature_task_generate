#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_verify_interactive.py

[...原始头注释保留...]

新增能力：
  - 通过 --doi / --doi_file 指定 DOI 列表进行“按 DOI 模式”的全量重建：
    * 匹配 articles 中的对应词条（优先 doi 列；并兼容 article_url 中含 doi.org/<doi> 的情况）
    * 回源解析 + 元数据复核（同原逻辑）
    * 主题打标：强制重打（等价 --reclassify）
    * 翻译：强制重译 title_cn 与 abstract_cn（覆盖已有）
"""

from __future__ import annotations
import os, sys, re, json, time, argparse, sqlite3
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse

from rss_common import (
    db_connect, now_ts, translate_batch_en2zh, classify_topic, fetch,
    extract_article_fields, log_run, TOPICS, HEADERS
)

# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="按日期范围交互式回填翻译/主题标签，并复核元数据")
    ap.add_argument("--start", type=str, default=None, help="起始日期（含），格式 YYYY-MM-DD；默认不限")
    ap.add_argument("--end", type=str, default=None, help="结束日期（含），格式 YYYY-MM-DD；默认不限")
    ap.add_argument("--limit", type=int, default=0, help="最多加载多少条（0 表示不限）")

    # 翻译与打标
    ap.add_argument("--batch_size", type=int, default=20, help="翻译批大小（默认20）")
    ap.add_argument("--reclassify", action="store_true", help="对已有 topic_tag 也重新打标并覆盖")

    # 回源抓取参数
    ap.add_argument("--http_timeout", type=int, default=25, help="HTTP 超时秒数（默认25）")
    ap.add_argument("--per_domain_rps", type=float, default=1.5, help="每域限速 req/s（默认1.5）")
    ap.add_argument("--max_retries", type=int, default=3, help="抓取/翻译最大重试（默认3）")
    ap.add_argument("--backoff_base", type=float, default=0.6, help="指数退避基数（默认0.6）")
    ap.add_argument("--backoff_factor", type=float, default=2.0, help="指数退避因子（默认2.0）")
    ap.add_argument("--sleep_between", type=float, default=0.1, help="抓取间隔最小停顿（默认0.1s）")

    # 交互 & 运行
    ap.add_argument("--dry_run", action="store_true", help="只预览拟更新，不写库")
    ap.add_argument("--yes_all", action="store_true", help="无需交互，对所有拟更新直接写入（非 dry_run）")

    # >>> NEW: DOI 模式
    ap.add_argument("--doi", action="append", help="指定 DOI（可多次传入，或用逗号分隔多个）")
    ap.add_argument("--doi_file", type=str, default=None, help="包含 DOI 的文本文件（每行一个，支持#注释）")

    return ap.parse_args()

# -----------------------------
# 工具
# -----------------------------

DATE_YM_RE = re.compile(r"^\d{4}-\d{2}$")
DATE_YMD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TEXT_HTML_RE = re.compile(r"text/html", re.I)
_last_request_at: Dict[str, float] = {}

DOI_URL_PREFIX_RE = re.compile(r"^(?:https?://)?(?:dx\.)?doi\.org/", re.I)  # >>> NEW

def _has_openai(cfg: Dict[str, Any]) -> bool:
    ocfg = cfg.get("openai") or {}
    return bool(ocfg.get("api_key"))

def _domain_of(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return None

def _rate_limit(url: Optional[str], per_domain_rps: float):
    if not url or per_domain_rps <= 0:
        return
    domain = _domain_of(url)
    if not domain:
        return
    now = time.time()
    min_interval = 1.0 / per_domain_rps
    last = _last_request_at.get(domain, 0.0)
    delta = now - last
    if delta < min_interval:
        time.sleep(min_interval - delta)
    _last_request_at[domain] = time.time()

def _with_retries(fn, url_for_limit: Optional[str], per_domain_rps: float,
                  max_retries: int, backoff_base: float, backoff_factor: float):
    ex: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            _rate_limit(url_for_limit, per_domain_rps)
            return fn()
        except Exception as e:
            ex = e
            if attempt >= max_retries:
                break
            sleep_s = backoff_base * (backoff_factor ** attempt)
            time.sleep(sleep_s)
    assert ex is not None
    raise ex

def _content_type_is_html(resp) -> bool:
    try:
        ctype = resp.headers.get("Content-Type", "")
    except Exception:
        ctype = ""
    return bool(TEXT_HTML_RE.search(ctype or ""))

def _is_incomplete_pub_date(v: Optional[str]) -> bool:
    if not v:
        return True
    v = v.strip()
    if DATE_YMD_RE.match(v):
        return False
    if DATE_YM_RE.match(v):
        return True
    return len(v) < 10

def _fmt(v: Any) -> str:
    if v is None:
        return "(NULL)"
    s = str(v)
    return s if s.strip() else "(EMPTY)"

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _is_better_abstract(old_abs: Optional[str], new_abs: Optional[str]) -> bool:
    """认为 new 更好：old 为空；或 new 去空白后明显更长（长度≥old且差值>=20）；或 old 很短（<80）但内容不同。"""
    o = _norm(old_abs)
    n = _norm(new_abs)
    if not n:
        return False
    if not o:
        return True
    if n == o:
        return False
    if len(n) >= len(o) and (len(n) - len(o) >= 20):
        return True
    if len(o) < 80 and n != o:
        return True
    return False

# >>> NEW: DOI 工具函数
def _normalize_doi(s: str) -> str:
    """归一化为纯 DOI 主体，统一小写，不带 doi.org/ 前缀。"""
    s = _norm(s)
    s = DOI_URL_PREFIX_RE.sub("", s)
    s = s.replace("doi:", "")
    return s.strip().strip("/").lower()

def _expand_doi_match_patterns(dois: List[str]) -> Tuple[List[str], List[str]]:
    """
    返回 (doi_list_normalized, url_like_patterns)
      - doi_list_normalized: 纯 DOI 主体（用于和表里的 doi 列做等值匹配，lower 比较）
      - url_like_patterns: ['%doi.org/<doi>%', ...] （用于从 article_url 兜底匹配）
    """
    pure = [_normalize_doi(d) for d in dois if _norm(d)]
    urlp = [f"%doi.org/{d}%" for d in pure]
    return pure, urlp

# -----------------------------
# 查询候选
# -----------------------------

def _build_range_clause(start: Optional[str], end: Optional[str]) -> Tuple[str, List[Any]]:
    wheres: List[str] = []
    params: List[Any] = []
    if start:
        wheres.append("fetched_at >= ?")
        params.append(datetime.fromisoformat(start).strftime("%Y-%m-%d 00:00:00"))
    if end:
        wheres.append("fetched_at <= ?")
        params.append(datetime.fromisoformat(end).strftime("%Y-%m-%d 23:59:59"))
    where_sql = (" AND ".join(wheres)) if wheres else "1=1"
    return where_sql, params

def load_candidates(cur: sqlite3.Cursor, start: Optional[str], end: Optional[str], limit: int) -> List[sqlite3.Row]:
    where_sql, params = _build_range_clause(start, end)
    lim_sql = " LIMIT ?" if limit and limit > 0 else ""
    if lim_sql:
        params2 = params + [limit]
    else:
        params2 = params
    cur.execute(f"""
        SELECT uid, article_url, fetched_at,
               title_en, abstract_en, title_cn, abstract_cn,
               topic_tag, type, journal, pub_date,
               doi                                   -- >>> NEW: 选出 doi，若无该列也不影响运行（SQLite 忽略未知列会报错；若你的 schema 没有 doi，请去掉本行）
          FROM articles
         WHERE {where_sql}
         ORDER BY fetched_at ASC
         {lim_sql}
    """, params2)
    cur.row_factory = sqlite3.Row
    return cur.fetchall()

# >>> NEW: 按 DOI 载入
def load_by_dois(cur: sqlite3.Cursor, dois: List[str]) -> List[sqlite3.Row]:
    pure, urlp = _expand_doi_match_patterns(dois)
    if not pure and not urlp:
        return []

    # 兼容没有 doi 列的场景：只用 article_url LIKE 兜底
    has_doi_col = False
    try:
        cur.execute("PRAGMA table_info(articles)")
        cols = [row[1].lower() for row in cur.fetchall()]
        has_doi_col = "doi" in cols
    except Exception:
        pass

    params: List[Any] = []
    where_clauses: List[str] = []

    if has_doi_col and pure:
        marks = ",".join(["?"] * len(pure))
        where_clauses.append(f"LOWER(doi) IN ({marks})")
        params.extend(pure)

    if urlp:
        url_or = " OR ".join(["article_url LIKE ?"] * len(urlp))
        where_clauses.append(f"({url_or})")
        params.extend(urlp)

    where_sql = " OR ".join(where_clauses) if where_clauses else "1=0"

    cur.execute(f"""
        SELECT uid, article_url, fetched_at,
               title_en, abstract_en, title_cn, abstract_cn,
               topic_tag, type, journal, pub_date,
               { "doi," if has_doi_col else "" }  uid as _dummy
          FROM articles
         WHERE {where_sql}
         ORDER BY fetched_at ASC
    """, params)
    cur.row_factory = sqlite3.Row
    return cur.fetchall()

# -----------------------------
# 翻译（批），基于回源后的“有效英文内容”
# -----------------------------

def plan_translations(rows: List[sqlite3.Row],
                      meta_plans: Dict[str, Dict[str, Any]],
                      cfg: Dict[str, Any],
                      batch_size: int,
                      max_retries: int,
                      force_all_translate: bool = False  # >>> NEW: DOI 模式下强制重译覆盖
                      ) -> Dict[str, Dict[str, Optional[str]]]:
    """
    返回 {uid: {title_cn?, abstract_cn?}}：
      - title_cn：在 title_cn 缺失且存在 title_en 时补齐；（不重翻已有）
      - abstract_cn：若 abstract_en 将被更新（meta_plans 含 abstract_en），强制用新摘要重译并覆盖；
                     否则仅在 abstract_cn 缺失且存在 abstract_en 时补齐。
      - 当 force_all_translate=True 时：title_cn/abstract_cn 均按“有效英文内容”**强制重译并覆盖**。
    """
    plans: Dict[str, Dict[str, Optional[str]]] = {}
    if not _has_openai(cfg):
        return plans

    titles_src: List[str] = []
    abstracts_src: List[str] = []
    need_title_mask: List[bool] = []
    need_abs_mask: List[bool] = []

    for r in rows:
        uid = r["uid"]
        # 有效英文标题：仅来自原 title_en（回源不负责标题）
        t_en = _norm(r["title_en"])

        # 有效英文摘要：优先使用回源更新后的 abstract_en
        mp = meta_plans.get(uid) or {}
        new_abs_en = _norm(mp.get("abstract_en"))
        current_abs_en = _norm(r["abstract_en"])
        src_abs = new_abs_en or current_abs_en

        # 标题是否需要翻译
        need_t = (force_all_translate and bool(t_en)) or ((not _norm(r["title_cn"])) and bool(t_en))
        # 摘要是否需要翻译
        force_retranslate_abs = bool(new_abs_en) or force_all_translate
        need_abs = force_retranslate_abs or ((not _norm(r["abstract_cn"])) and bool(src_abs))

        titles_src.append(t_en if need_t else "")
        abstracts_src.append(src_abs if need_abs else "")
        need_title_mask.append(need_t)
        need_abs_mask.append(need_abs)

    def _call_translate(arr: List[str]) -> List[Optional[str]]:
        if not any(arr):
            return [None for _ in arr]
        for attempt in range(max_retries + 1):
            out = translate_batch_en2zh(arr, cfg)
            if out and any(out):
                return out
            if attempt < max_retries:
                time.sleep(1.5)
        return [None for _ in arr]

    def _batched_translate(arr: List[str]) -> List[Optional[str]]:
        res: List[Optional[str]] = [None] * len(arr)
        for i in range(0, len(arr), batch_size):
            batch = arr[i:i+batch_size]
            outs = _call_translate(batch) if any(batch) else [None for _ in batch]
            res[i:i+batch_size] = outs
        return res

    titles_zh = _batched_translate(titles_src)
    abstracts_zh = _batched_translate(abstracts_src)

    for i, r in enumerate(rows):
        uid = r["uid"]
        plan: Dict[str, Optional[str]] = {}
        if need_title_mask[i] and titles_zh[i]:
            plan["title_cn"] = titles_zh[i]
        if need_abs_mask[i] and abstracts_zh[i]:
            plan["abstract_cn"] = abstracts_zh[i]
        if plan:
            plans[uid] = plan

    return plans

# -----------------------------
# 打标计划（使用回源后的“有效内容”）
# -----------------------------

def plan_topic_tags(rows: List[sqlite3.Row],
                    meta_plans: Dict[str, Dict[str, Any]],
                    cfg: Dict[str, Any],
                    reclassify: bool) -> Dict[str, str]:
    plans: Dict[str, str] = {}
    for r in rows:
        uid = r["uid"]
        have = _norm(r["topic_tag"])
        need = reclassify or (not have)
        if not need:
            continue
        eff_abs_en = _norm((meta_plans.get(uid) or {}).get("abstract_en")) or _norm(r["abstract_en"])
        title_src = _norm(r["title_en"]) or _norm(r["title_cn"])
        abs_src = eff_abs_en or _norm(r["abstract_cn"])
        tag = classify_topic(title_src, abs_src, cfg={**cfg, "openai": cfg.get("openai") or {}}) or "其他"
        if reclassify or (not have and tag):
            plans[uid] = tag
    return plans

# -----------------------------
# 元数据复核计划（回源）
# -----------------------------

def probe_new_values(url: str, http_timeout: int, per_domain_rps: float,
                      max_retries: int, backoff_base: float, backoff_factor: float) -> Optional[Dict[str, Any]]:
    def _do_fetch():
        return fetch(url, HEADERS, timeout=http_timeout)
    r = _with_retries(_do_fetch, url_for_limit=url, per_domain_rps=per_domain_rps,
                      max_retries=max_retries, backoff_base=backoff_base, backoff_factor=backoff_factor)
    if not _content_type_is_html(r):
        return None
    fields = extract_article_fields(r.text, url)
    abs_en = fields.get("abstract_en") or fields.get("abstract") or fields.get("description")
    return {
        "type": fields.get("type"),
        "journal": fields.get("journal"),
        "date_published": fields.get("date_published"),
        "abstract_en": abs_en,
    }

def plan_metadata_updates(rows: List[sqlite3.Row], http_timeout: int, per_domain_rps: float,
                          max_retries: int, backoff_base: float, backoff_factor: float,
                          sleep_between: float) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    updates: Dict[str, Dict[str, Any]] = {}
    no_improve: List[Dict[str, Any]] = []

    for i, r in enumerate(rows, 1):
        print(f"\r[PROBE] {i}/{len(rows)} UID={r['uid']} URL={r['article_url']}", end='', flush=True)
        uid = r["uid"]
        url = r["article_url"]
        need_type = not _norm(r["type"])
        need_journal = not _norm(r["journal"])
        need_pub = _is_incomplete_pub_date(r["pub_date"])

        if not url:
            no_improve.append({
                "uid": uid, "url": "(missing)",
                "missing_fields": [x for x in ("type" if need_type else None, "journal" if need_journal else None, "pub_date" if need_pub else None) if x]
            })
            continue
        try:
            newvals = probe_new_values(url, http_timeout, per_domain_rps, max_retries, backoff_base, backoff_factor)
        except Exception as e:
            no_improve.append({
                "uid": uid, "url": url,
                "missing_fields": [x for x in ("type" if need_type else None, "journal" if need_journal else None, "pub_date" if need_pub else None, "abstract_en") if x],
                "reason": f"fetch/extract failed: {e}"
            })
            continue

        if not newvals:
            no_improve.append({
                "uid": uid, "url": url,
                "missing_fields": [x for x in ("type" if need_type else None, "journal" if need_journal else None, "pub_date" if need_pub else None, "abstract_en") if x],
                "reason": "non-HTML or parser returned None"
            })
            continue

        changes: Dict[str, Any] = {}
        if newvals.get("type") and newvals.get("type") != r["type"]:
            changes["type"] = newvals.get("type")
        if newvals.get("journal") and newvals.get("journal") != r["journal"]:
            changes["journal"] = newvals.get("journal")
        cand_pub = newvals.get("date_published")
        if cand_pub and (cand_pub != r["pub_date"] or _is_incomplete_pub_date(r["pub_date"])):
            changes["pub_date"] = cand_pub

        cand_abs = newvals.get("abstract_en")
        if _is_better_abstract(r["abstract_en"], cand_abs):
            changes["abstract_en"] = _norm(cand_abs)

        if changes:
            updates[uid] = changes

        time.sleep(sleep_between)

    return updates, no_improve

# -----------------------------
# 汇总打印与交互
# -----------------------------

def print_summary(rows: List[sqlite3.Row],
                  trans_plans: Dict[str, Dict[str, Optional[str]]],
                  tag_plans: Dict[str, str],
                  meta_plans: Dict[str, Dict[str, Any]]):
    print("\n" + "="*120)
    print("拟更新项汇总：")
    print("-"*120)
    header = f"{'UID':<36} {'CHANGES'}"
    print(header)
    print("-"*120)

    total = 0
    for r in rows:
        uid = r["uid"]
        changes: List[str] = []
        tp = trans_plans.get(uid)
        if tp:
            if tp.get("title_cn"):
                changes.append(f"title_cn: {_fmt(r['title_cn'])} → {_fmt(tp['title_cn'])}")
            if tp.get("abstract_cn"):
                changes.append(f"abstract_cn: {_fmt(r['abstract_cn'])} → {_fmt(tp['abstract_cn'])}")
        tag = tag_plans.get(uid)
        if tag:
            changes.append(f"topic_tag: {_fmt(r['topic_tag'])} → {tag}")
        mp = meta_plans.get(uid)
        if mp:
            for k in ("type","journal","pub_date","abstract_en"):
                if k in mp:
                    changes.append(f"{k}: {_fmt(r[k])} → {_fmt(mp[k])}")
        if changes:
            total += 1
            print(f"{uid:<36} " + "; ".join(changes))

    if total == 0:
        print("没有任何拟更新项。")
    else:
        print("-"*120)
        print(f"合计 {total} 条记录存在拟更新。")

def interactive_apply(conn: sqlite3.Connection, rows: List[sqlite3.Row],
                       trans_plans: Dict[str, Dict[str, Optional[str]]],
                       tag_plans: Dict[str, str],
                       meta_plans: Dict[str, Dict[str, Any]],
                       yes_all: bool = False, dry_run: bool = False) -> Tuple[int, int]:
    cur = conn.cursor()
    applied = 0
    skipped = 0

    if yes_all and not dry_run:
        cur.execute("BEGIN")
        try:
            for r in rows:
                uid = r["uid"]
                changes: Dict[str, Any] = {}
                tp = trans_plans.get(uid) or {}
                mp = meta_plans.get(uid) or {}
                if tp.get("title_cn"):
                    changes["title_cn"] = tp["title_cn"]
                if tp.get("abstract_cn"):
                    changes["abstract_cn"] = tp["abstract_cn"]
                if uid in tag_plans:
                    changes["topic_tag"] = tag_plans[uid]
                for k in ("type","journal","pub_date","abstract_en"):
                    if k in mp:
                        changes[k] = mp[k]
                if changes:
                    cols = ", ".join([f"{k} = ?" for k in changes.keys()])
                    params = list(changes.values()) + [now_ts(), uid]
                    cur.execute(f"UPDATE articles SET {cols}, last_updated_at = ? WHERE uid = ?", params)
                    applied += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return applied, skipped

    # 逐条交互
    for r in rows:
        uid = r["uid"]
        tp = trans_plans.get(uid)
        tag = tag_plans.get(uid)
        mp = meta_plans.get(uid)
        if not (tp or tag or mp):
            continue

        print("\n" + "-"*100)
        print(f"UID: {uid}")
        print("拟更新：")
        if tp:
            if tp.get("title_cn"):
                print(f"  title_cn: {_fmt(r['title_cn'])} → {_fmt(tp['title_cn'])}")
            if tp.get("abstract_cn"):
                print(f"  abstract_cn: {_fmt(r['abstract_cn'])} → {_fmt(tp['abstract_cn'])}")
        if tag:
            print(f"  topic_tag: {_fmt(r['topic_tag'])} → {tag}")
        if mp:
            for k in ("type","journal","pub_date","abstract_en"):
                if k in mp:
                    print(f"  {k}: {_fmt(r[k])} → {_fmt(mp[k])}")

        if dry_run:
            print("  [dry-run] 不写入。")
            skipped += 1
            continue

        ans = input("是否写入该条更改？[y]是 / [n]否 / [a]剩余全部是 / [q]退出：").strip().lower()
        if ans == "a":
            yes_all = True
        if ans == "q":
            print("用户中止。")
            break

        do_apply = (ans == "y" or yes_all)
        if do_apply:
            changes: Dict[str, Any] = {}
            if tp and tp.get("title_cn"):
                changes["title_cn"] = tp["title_cn"]
            if tp and tp.get("abstract_cn"):
                changes["abstract_cn"] = tp["abstract_cn"]
            if tag:
                changes["topic_tag"] = tag
            if mp:
                for k in ("type","journal","pub_date","abstract_en"):
                    if k in mp:
                        changes[k] = mp[k]
            if changes:
                cols = ", ".join([f"{k} = ?" for k in changes.keys()])
                params = list(changes.values()) + [now_ts(), uid]
                try:
                    cur.execute(f"UPDATE articles SET {cols}, last_updated_at = ? WHERE uid = ?", params)
                    conn.commit()
                    applied += 1
                except Exception as e:
                    conn.rollback()
                    print(f"[ERR] 写入失败 uid={uid}: {e}", file=sys.stderr)
                    skipped += 1
            else:
                skipped += 1
        else:
            skipped += 1

    return applied, skipped

# -----------------------------
# 主流程
# -----------------------------

def _collect_dois_from_args(args: argparse.Namespace) -> List[str]:
    """从 --doi 和 --doi_file 收集 DOI 列表（去重、归一化前的原始收集）。"""
    dois: List[str] = []
    if args.doi:
        for item in args.doi:
            # 支持逗号分隔或多次传参
            parts = [p.strip() for p in item.split(",") if p.strip()]
            dois.extend(parts)
    if args.doi_file:
        try:
            with open(args.doi_file, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    dois.append(s)
        except Exception as e:
            print(f"[WARN] 读取 doi_file 失败：{e}", file=sys.stderr)
    # 去重，保留原形（匹配时会统一做 normalize）
    seen = set()
    uniq: List[str] = []
    for d in dois:
        key = _normalize_doi(d)
        if key and key not in seen:
            seen.add(key)
            uniq.append(d)
    return uniq

def main():
    args = parse_args()

    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}

    if (not args.dry_run) and (not _has_openai(cfg)):
        print("[WARN] 未检测到 openai.api_key；将无法进行中文翻译补齐。只执行打标与元数据复核。", file=sys.stderr)

    db_path = cfg.get("db", "./rss_state.db")
    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    started_at = now_ts()

    # >>> NEW: DOI 模式收集
    doi_inputs = _collect_dois_from_args(args)
    doi_mode = len(doi_inputs) > 0

    try:
        # 载入候选
        if doi_mode:
            rows = load_by_dois(cur, doi_inputs)
            total = len(rows)
            if total == 0:
                print("[OK] 未找到匹配 DOI 的记录。")
                log_run(conn, "backfill_verify_interactive", started_at, "ok", 0, "no_rows_doi")
                return
            print(f"[INFO] DOI 模式：匹配到 {total} 条记录，开始全量重建…")
        else:
            rows = load_candidates(cur, args.start, args.end, args.limit)
            total = len(rows)
            if total == 0:
                print("[OK] 指定范围内没有记录。")
                log_run(conn, "backfill_verify_interactive", started_at, "ok", 0, "no_rows")
                return
            print(f"[INFO] 载入 {total} 条记录，开始生成拟更新计划…")

        # 1) 元数据复核（先回源，拿到可能更新的 abstract_en）
        meta_plans, meta_no_improve = plan_metadata_updates(
            rows,
            http_timeout=args.http_timeout,
            per_domain_rps=args.per_domain_rps,
            max_retries=args.max_retries,
            backoff_base=args.backoff_base,
            backoff_factor=args.backoff_factor,
            sleep_between=args.sleep_between,
        )
        print(f"[INFO] 元数据复核发现 {len(meta_plans)} 条存在差异/可补齐；未改善/失败 {len(meta_no_improve)} 条。")

        # 2) 翻译计划（DOI 模式下强制重译 title_cn/abstract_cn 覆盖）
        force_all_translate = doi_mode  # >>> NEW: 只要是 DOI 模式，就强制重译
        trans_plans = plan_translations(
            rows, meta_plans, cfg, args.batch_size, args.max_retries, force_all_translate=force_all_translate
        )
        print(f"[INFO] 翻译拟更新条数：{len(trans_plans)}（按字段去重后可能小于条目数）")

        # 3) 打标计划（DOI 模式下强制 reclassify）
        reclassify_flag = args.reclassify or doi_mode  # >>> NEW
        tag_plans = plan_topic_tags(rows, meta_plans, cfg, reclassify_flag)
        print(f"[INFO] 主题打标拟更新条数：{len(tag_plans)}")

        # 汇总打印
        print_summary(rows, trans_plans, tag_plans, meta_plans)

        # 交互写入
        applied = skipped = 0
        if args.dry_run:
            print("[DRY-RUN] 仅展示拟更新，不进行任何写入。")
        else:
            applied, skipped = interactive_apply(conn, rows, trans_plans, tag_plans, meta_plans,
                                                 yes_all=args.yes_all, dry_run=False)

        status = "ok" if (applied >= 0) else "error"
        mode_note = "doi_mode" if doi_mode else "date_range"
        notes = (
            f"mode={mode_note}; rows={total}; applied={applied}; skipped={skipped}; "
            f"trans={len(trans_plans)} tag={len(tag_plans)} meta={len(meta_plans)}; "
            f"range=({args.start}..{args.end}) dry_run={args.dry_run} reclassify={reclassify_flag}"
        )
        log_run(conn, "backfill_verify_interactive", started_at, status, applied, notes)
        print(f"[DONE] 完成：载入 {total} 条；写入 {applied} 条；跳过 {skipped} 条。")

    except KeyboardInterrupt:
        log_run(conn, "backfill_verify_interactive", started_at, "error", 0, "KeyboardInterrupt")
        print("\n[INTERRUPTED] 用户中断。", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        log_run(conn, "backfill_verify_interactive", started_at, "error", 0, str(e))
        print(f"[ERR] 运行失败：{e}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
