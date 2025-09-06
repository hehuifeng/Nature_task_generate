#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
view_latest.py
- 快速把数据库中最新内容输出为 txt（便于人工检查“更新到什么地步了”）
- 可按主题过滤，可设条数，默认 200 条。可选输出到文件或 stdout。
- 记录 runs_log
"""

from __future__ import annotations
import os, sys, json, argparse
from datetime import datetime
from rss_common import db_connect, now_ts

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", nargs="*", default=[], help="主题过滤（留空=全部）")
    ap.add_argument("--limit", type=int, default=200, help="最多显示条数")
    ap.add_argument("--out", default="", help="输出到文件路径（留空则打印到 stdout）")
    args = ap.parse_args()

    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    conn = db_connect(cfg.get("db", "./rss_state.db"))
    cur = conn.cursor()
    started_at = now_ts()

    if args.topics:
        q = f"""SELECT uid, journal, title_en, pub_date, doi, article_url, topic_tag, fetched_at, abstract_en
                FROM articles
                WHERE topic_tag IN ({','.join(['?']*len(args.topics))})
                ORDER BY datetime(fetched_at) DESC
                LIMIT ?"""
        rows = list(cur.execute(q, [*args.topics, args.limit]))
    else:
        q = """SELECT uid, journal, title_en, pub_date, doi, article_url, topic_tag, fetched_at, abstract_en
               FROM articles
               ORDER BY datetime(fetched_at) DESC
               LIMIT ?"""
        rows = list(cur.execute(q, [args.limit]))

    lines = []
    lines.append(f"# 最新条目（{datetime.now().isoformat(timespec='seconds')}） 主题: {','.join(args.topics) or 'ALL'}")
    for i, r in enumerate(rows, 1):
        uid, journal, title, pub_date, doi, url, topic, fetched_at, abstract_en = r
        lines.append(f"{i}. [{topic or '其他'}] {journal or 'Unknown'} | {pub_date or ''} | {title or ''}")
        if doi: lines.append(f"   DOI: {doi}")
        if url: lines.append(f"   URL: {url}")
        if abstract_en: lines.append(f"   摘要: {abstract_en}")
        lines.append(f"   fetched_at: {fetched_at}")

    out_text = "\n".join(lines) + "\n"
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_text)
        print(f"[OK] wrote {len(rows)} rows to {args.out}")
    else:
        print(out_text)

    from rss_common import log_run
    log_run(conn, "view_latest", started_at, "ok", len(rows), f"topics={args.topics}")
    conn.close()

if __name__ == "__main__":
    main()
