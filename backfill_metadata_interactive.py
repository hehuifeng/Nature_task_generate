#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_metadata_interactive_batch.py

批量交互式“数据补齐”工具：
- 扫描 SQLite 中已入库的 articles
- 当以下字段不完整时，回源抓取 HTML 用 extract_article_fields 重新解析：
  * type 为空
  * journal 为空
  * pub_date 只有到“年-月”（缺少日）或明显不规范
- 先对所有候选记录逐条抓取并计算“拟更新值”，一次性打印汇总表
- 你仅需一次输入 yes/y 即可统一写入；否则不写入
- 对“解析后未产生更完整字段”的记录，会在最后集中打印其 UID/Title 以及仍缺失的字段
- 支持按域名限速与指数退避；写入 runs_log，并在 meta(key='last_backfill_at') 记录时间戳

可在 config.json 覆盖的字段：
{
  "db": "./rss_state.db",
  "http_timeout": 25,
  "per_domain_rps": 1.5,
  "max_retries": 3,
  "backoff_base": 0.6,
  "backoff_factor": 2.0,
  "sleep_between_fetches": 0.1,
  "max_backfill_rows": 1000
}
"""

from __future__ import annotations
import os, sys, re, time, json, sqlite3, textwrap
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# 复用你项目里的工具
from rss_common import (
    db_connect, now_ts, fetch, extract_article_fields, HEADERS, log_run
)

# -----------------------------
# 默认配置（可被 config.json 覆盖）
# -----------------------------
DEFAULT_DB_PATH = "./rss_state.db"
DEFAULT_HTTP_TIMEOUT = 25
DEFAULT_PER_DOMAIN_RPS = 1.5      # 每域最大请求频率（req/s）
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 0.6
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_SLEEP_BETWEEN = 0.1       # 每篇之间最小停顿
DEFAULT_MAX_ROWS = 1000           # 单次最多处理多少条（0 表示不限制）

TEXT_HTML_RE = re.compile(r'text/html', re.I)
DATE_YM_RE = re.compile(r'^\d{4}-\d{2}$')     # 形如 2024-07
DATE_YMD_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# -----------------------------
# 限速 / 重试工具
# -----------------------------
_last_request_at: Dict[str, float] = {}

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

def _is_incomplete_pub_date(v: Optional[str]) -> bool:
    """pub_date 只有到年-月，或格式明显不规范的，返回 True。"""
    if not v:
        return True
    v = v.strip()
    if DATE_YMD_RE.match(v):
        return False
    if DATE_YM_RE.match(v):
        return True
    # 其它不规则/过短的格式也尝试补齐
    return len(v) < 10

def _content_type_is_html(resp) -> bool:
    try:
        ctype = resp.headers.get("Content-Type", "")
    except Exception:
        ctype = ""
    return bool(TEXT_HTML_RE.search(ctype or ""))

def _title_for_display(row: sqlite3.Row) -> str:
    t_cn = row["title_cn"] if "title_cn" in row.keys() else None
    t_en = row["title_en"] if "title_en" in row.keys() else None
    return (t_cn or t_en or "").strip() or "(no title)"

def _fmt(v: Any) -> str:
    if v is None:
        return "(NULL)"
    s = str(v)
    return s if s.strip() else "(EMPTY)"

# def _short_uid(uid: str, width: int = 22) -> str:
#     return uid if len(uid) <= width else uid[:width-1] + "…"

# -----------------------------
# 核心：扫描并生成“拟更新计划”
# -----------------------------
def _collect_candidates(cur: sqlite3.Cursor, max_rows: int) -> List[sqlite3.Row]:
    where_sql = """
        (type IS NULL OR TRIM(type) = '')
        OR (journal IS NULL OR TRIM(journal) = '')
        OR (pub_date IS NULL OR LENGTH(TRIM(pub_date)) < 10)
    """
    limit_sql = f" LIMIT {max_rows} " if max_rows and max_rows > 0 else ""
    rows = cur.execute(f"""
        SELECT uid, article_url, type, journal, pub_date, title_en, title_cn
          FROM articles
         WHERE {where_sql}
         ORDER BY last_updated_at ASC
         {limit_sql}
    """).fetchall()
    return rows

def _probe_new_values(url: str,
                      http_timeout: int,
                      per_domain_rps: float,
                      max_retries: int,
                      backoff_base: float,
                      backoff_factor: float) -> Dict[str, Any] | None:
    """抓取 + 解析，返回 {'type','journal','date_published'}，失败返回 None。"""
    def _do_fetch():
        return fetch(url, HEADERS, timeout=http_timeout)
    r = _with_retries(_do_fetch, url_for_limit=url, per_domain_rps=per_domain_rps,
                      max_retries=max_retries, backoff_base=backoff_base, backoff_factor=backoff_factor)
    if not _content_type_is_html(r):
        return None
    fields = extract_article_fields(r.text, url)
    return {
        "type": fields.get("type"),
        "journal": fields.get("journal"),
        "date_published": fields.get("date_published")
    }

# -----------------------------
# 主：批量交互
# -----------------------------
def run_interactive_batch(config: Dict[str, Any]) -> Tuple[int, int, int]:
    """
    返回 (候选总数, 实际写入数, 失败数)
    """
    db_path = config.get("db", DEFAULT_DB_PATH)
    http_timeout = int(config.get("http_timeout", DEFAULT_HTTP_TIMEOUT))
    per_domain_rps = float(config.get("per_domain_rps", DEFAULT_PER_DOMAIN_RPS))
    max_retries = int(config.get("max_retries", DEFAULT_MAX_RETRIES))
    backoff_base = float(config.get("backoff_base", DEFAULT_BACKOFF_BASE))
    backoff_factor = float(config.get("backoff_factor", DEFAULT_BACKOFF_FACTOR))
    sleep_between = float(config.get("sleep_between_fetches", DEFAULT_SLEEP_BETWEEN))
    max_rows = int(config.get("max_backfill_rows", DEFAULT_MAX_ROWS))

    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 确保 meta 存在
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta(
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    conn.commit()

    rows = _collect_candidates(cur, max_rows)
    total_candidates = len(rows)
    if total_candidates == 0:
        print("没有需要补齐的记录。")
        return 0, 0, 0

    print(f"共找到 {total_candidates} 条可能需要补齐的记录。正在回源解析以生成更新计划…")

    # 准备两个清单
    plan_updates: List[Dict[str, Any]] = []   # 要更新的项（产生更完整字段）
    no_improve: List[Dict[str, Any]] = []     # 解析后仍不更完整/无法补齐的项

    # 扫描阶段：不写库，只生成计划
    for row in rows:
        uid = row["uid"]
        url = row["article_url"]
        old_type = row["type"]
        old_journal = row["journal"]
        old_pub_date = row["pub_date"]

        need_type = (not old_type or not str(old_type).strip())
        need_journal = (not old_journal or not str(old_journal).strip())
        need_pubdate = _is_incomplete_pub_date(old_pub_date)

        # 如果其实并不需要（可能被别的脚本补齐了），跳过
        if not (need_type or need_journal or need_pubdate):
            continue

        if not url:
            no_improve.append({
                "uid": uid,
                "title": _title_for_display(row),
                "url": "(missing)",
                "missing_fields": [x for x in [
                                "type" if need_type else None,
                                "journal" if need_journal else None,
                                "pub_date" if need_pubdate else None
                                ] if x]
            })
            continue

        try:
            newvals = _probe_new_values(
                url, http_timeout, per_domain_rps, max_retries, backoff_base, backoff_factor
            )
        except Exception as e:
            no_improve.append({
                "uid": uid,
                "title": _title_for_display(row),
                "url": url,
                "missing_fields": [x for x in (
                    "type" if need_type else None,
                    "journal" if need_journal else None,
                    "pub_date" if need_pubdate else None,
                ) if x],
                "reason": f"fetch/extract failed: {e}"
            })
            continue

        if not newvals:
            no_improve.append({
                "uid": uid,
                "title": _title_for_display(row),
                "url": url,
                "missing_fields": [x for x in (
                    "type" if need_type else None,
                    "journal" if need_journal else None,
                    "pub_date" if need_pubdate else None,
                ) if x],
                "reason": "non-HTML or parser returned None"
            })

            continue

        cand_type = newvals.get("type")
        cand_journal = newvals.get("journal")
        cand_pub_date = newvals.get("date_published")

        new_type = (cand_type if need_type and cand_type else old_type)
        new_journal = (cand_journal if need_journal and cand_journal else old_journal)
        new_pub_date = (cand_pub_date if need_pubdate and cand_pub_date else old_pub_date)

        improved = ((new_type != old_type) or (new_journal != old_journal) or (new_pub_date != old_pub_date))

        if improved:
            plan_updates.append({
                "uid": uid,
                "url": url,
                "title": _title_for_display(row),
                "old": {"type": old_type, "journal": old_journal, "pub_date": old_pub_date},
                "new": {"type": new_type, "journal": new_journal, "pub_date": new_pub_date},
            })
        else:
            # 记录仍缺失哪些字段
            still_missing = []
            if need_type and not cand_type:
                still_missing.append("type")
            if need_journal and not cand_journal:
                still_missing.append("journal")
            if need_pubdate and not cand_pub_date:
                still_missing.append("pub_date")

            no_improve.append({
                "uid": uid,
                "title": _title_for_display(row),
                "url": url,
                "missing_fields": still_missing if still_missing else ["(no better value)"]
            })

        time.sleep(sleep_between)

    # -------- 汇总展示：拟更新项 --------
    if plan_updates:
        print("\n" + "="*120)
        print("拟更新条目（将一次性确认后写入）：")
        print("-"*120)
        print(f"{'UID':<40} {'TITLE':<50}  CHANGES")
        print("-"*120)
        for it in plan_updates:
            uid = it["uid"]  # 不截断，完整 DOI/UID
            title = it["title"]
            title_line = (title[:47] + "…") if len(title) > 48 else title
            old = it["old"]; new = it["new"]
            changes = []
            for k in ("type","journal","pub_date"):
                if _fmt(old.get(k)) != _fmt(new.get(k)):
                    changes.append(f"{k}: {_fmt(old.get(k))} → {_fmt(new.get(k))}")
            changes_str = "; ".join(changes)
            print(f"{uid:<40} {title_line:<50}  {changes_str}")
        print("-"*120)
        print(f"合计拟更新：{len(plan_updates)} 条")
    else:
        print("\n没有任何条目解析出更完整的字段可写入。")

    # -------- 汇总展示：未改进项 --------
    if no_improve:
        print("\n" + "="*120)
        print("以下条目解析后仍未产生更完整的字段（或无法回源），请留意：")
        print("-"*120)
        print(f"{'UID':<40} {'TITLE':<50}  STILL MISSING / REASON")
        print("-"*120)
        for it in no_improve:
            uid = it["uid"]  # 不截断，完整 DOI/UID
            title = it["title"]
            title_line = (title[:47] + "…") if len(title) > 48 else title
            missing = [m for m in (it.get("missing_fields") or []) if m]  # 过滤 None
            reason = it.get("reason")
            tail = (", ".join(missing)) if missing else "(unknown)"
            if reason:
                tail = f"{tail} | {reason}"
            print(f"{uid:<40} {title_line:<50}  {tail}")
        print("-"*120)
        print(f"未改进：{len(no_improve)} 条")

    # -------- 一次性确认写入 --------
    if not plan_updates:
        # 即使没有可写入的，也记录 runs_log & meta
        started_at = now_ts()
        status_text = "ok"
        log_run(conn, "backfill_metadata_interactive_batch", started_at, status_text, 0,
                f"candidates={total_candidates} no_improve={len(no_improve)}")
        cur.execute("""INSERT INTO meta(key,value)
                            VALUES('last_backfill_at', ?)
                            ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                    (str(now_ts()),))
        conn.commit()
        conn.close()
        return total_candidates, 0, 0

    ans = input("\n确认将以上所有拟更新项一次性写入数据库吗？输入 yes/y 确认（其他任意键取消）：").strip().lower()
    if ans not in ("yes", "y"):
        print("已取消写入。")
        conn.close()
        return total_candidates, 0, 0

    # 统一写入（单事务）
    started_at = now_ts()
    written = 0
    failed = 0
    try:
        cur.execute("BEGIN")
        for it in plan_updates:
            try:
                cur.execute("""
                    UPDATE articles
                       SET type = ?,
                           journal = ?,
                           pub_date = ?,
                           last_updated_at = ?
                     WHERE uid = ?
                """, (it["new"]["type"], it["new"]["journal"], it["new"]["pub_date"], now_ts(), it["uid"]))
                written += 1
            except Exception as e:
                failed += 1
                print(f"[WARN] 写入失败 uid={it['uid']} -> {e}", file=sys.stderr)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 批量写入失败，已回滚：{e}", file=sys.stderr)
        failed = len(plan_updates)
        written = 0

    # 记录 runs_log & meta
    status_text = "ok" if failed == 0 else ("partial_ok" if written > 0 else "error")
    log_run(conn, "backfill_metadata_interactive_batch", started_at, status_text, written,
            f"candidates={total_candidates} applied={len(plan_updates)} no_improve={len(no_improve)} fail={failed}")
    cur.execute("""INSERT INTO meta(key,value)
                        VALUES('last_backfill_at', ?)
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (str(now_ts()),))
    conn.commit()
    conn.close()

    print(f"\n完成：候选 {total_candidates} 条 | 拟更新 {len(plan_updates)} 条"
          f" | 实际写入 {written} 条 | 未改进 {len(no_improve)} 条 | 写入失败 {failed} 条。")
    return total_candidates, written, failed

def main():
    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception:
        config = {}
    run_interactive_batch(config)

if __name__ == "__main__":
    main()
