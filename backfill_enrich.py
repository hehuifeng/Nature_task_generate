#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_enrich.py
- 为旧库中的文章增量回填：
  1) 中文翻译（title_cn / abstract_cn）
  2) 主题标签（topic_tag）
- 仅补缺，默认不覆盖已有内容；可通过 --reclassify 覆盖已有标签
- 支持批处理、时间窗口、limit、dry-run 等
- 依赖 rss_common 中的：db_connect / now_ts / translate_batch_en2zh / classify_topic / log_run / TOPICS
"""

from __future__ import annotations
import os, sys, json, time, argparse
from typing import List, Dict, Any, Optional, Tuple

from rss_common import (
    db_connect, now_ts, translate_batch_en2zh, classify_topic, log_run, TOPICS
)

def _has_openai(cfg: Dict[str, Any]) -> bool:
    ocfg = cfg.get("openai") or {}
    return bool(ocfg.get("api_key"))

def rows_to_dicts(rows):
    for r in rows:
        yield {
            "uid": r[0],
            "title_en": r[1],
            "abstract_en": r[2],
            "title_cn": r[3],
            "abstract_cn": r[4],
            "topic_tag": r[5],
            "fetched_at": r[6],
        }

def build_query(
    mode: str, since_days: Optional[int], limit: int, only_missing: str
) -> Tuple[str, list]:
    """
    mode: 'all' | 'translate' | 'tag'
    only_missing: 'any' | 'title' | 'abstract' —— 仅在 translate 模式下细化；'all' 时表示翻译的细化
    """
    wheres = []
    params: List[Any] = []

    # 缺失条件
    need_translate = "(title_cn IS NULL OR TRIM(title_cn)='') OR (abstract_cn IS NULL OR TRIM(abstract_cn)='')"
    need_title = "(title_cn IS NULL OR TRIM(title_cn)='')"
    need_abs   = "(abstract_cn IS NULL OR TRIM(abstract_cn)='')"
    need_tag   = "(topic_tag IS NULL OR TRIM(topic_tag)='')"

    if mode == "translate":
        if only_missing == "title":
            wheres.append(need_title)
        elif only_missing == "abstract":
            wheres.append(need_abs)
        else:
            wheres.append(need_translate)
    elif mode == "tag":
        wheres.append(need_tag)
    else:  # all
        # 任一需要就选进来
        wheres.append(f"(({need_translate}) OR ({need_tag}))")

    # 时间窗口（以 fetched_at 为准）
    if since_days is not None and since_days >= 0:
        from datetime import datetime, timedelta
        since_dt = (datetime.now() - timedelta(days=since_days)).isoformat(timespec="seconds")
        wheres.append("(fetched_at >= ?)")
        params.append(since_dt)

    q = f"""
      SELECT uid, title_en, abstract_en, title_cn, abstract_cn, topic_tag, fetched_at
      FROM articles
      WHERE {' AND '.join(wheres) if wheres else '1=1'}
      ORDER BY fetched_at ASC
      LIMIT ?
    """
    params.append(limit)
    return q, params

def chunked(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    ap = argparse.ArgumentParser(description="为旧库增量回填中文翻译与主题标签")
    ap.add_argument("--mode", choices=["all","translate","tag"], default="all",
                    help="回填模式：all=翻译+打标（默认），translate=仅翻译，tag=仅打标")
    ap.add_argument("--since_days", type=int, default=None, help="仅处理最近 N 天抓取的文章（fetched_at）")
    ap.add_argument("--limit", type=int, default=2000, help="最多加载多少条待处理记录（默认2000）")
    ap.add_argument("--batch_size", type=int, default=20, help="单批翻译条数（默认20；仅影响翻译）")
    ap.add_argument("--sleep", type=float, default=0.5, help="批间休眠秒数（默认0.5s）")
    ap.add_argument("--only_missing", choices=["any","title","abstract"], default="any",
                    help="仅翻译模式下的缺失选择：缺标题/缺摘要/任一缺失（默认 any）")
    ap.add_argument("--reclassify", action="store_true",
                    help="对已有 topic_tag 的记录也重新打标并覆盖（默认不覆盖）")
    ap.add_argument("--dry_run", action="store_true", help="只查看将处理哪些记录，不写库")
    ap.add_argument("--max_retries", type=int, default=2, help="OpenAI 调用失败时的最大重试次数（默认2）")
    args = ap.parse_args()

    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[ERR] 无法读取配置 {cfg_path}: {e}", file=sys.stderr)
        sys.exit(2)

    # 翻译需要 key；仅打标模式可不需要
    if args.mode in ("all","translate") and not _has_openai(cfg):
        print("[ERR] 需要回填翻译，但 openai.api_key 未配置。", file=sys.stderr)
        sys.exit(3)

    db_path = cfg.get("db", "./rss_state.db")
    conn = db_connect(db_path)
    cur = conn.cursor()
    started_at = now_ts()

    try:
        # 抓取候选记录
        q, params = build_query(args.mode, args.since_days, args.limit, args.only_missing)
        rows = list(cur.execute(q, params))
        items = list(rows_to_dicts(rows))
        total = len(items)
        if total == 0:
            print("[OK] 没有需要回填的记录。")
            log_run(conn, "backfill_enrich", started_at, "ok", 0, "no_missing")
            return

        print(f"[INFO] 共加载 {total} 条待回填记录（mode={args.mode}，batch_size={args.batch_size}）。")

        processed = 0
        updated_rows = 0

        # --- 1) 先做翻译（若开启） ---
        if args.mode in ("all","translate"):
            for batch in chunked(items, args.batch_size):
                # 构建翻译输入：仅对缺失的字段发起翻译
                titles_src, idx_title = [], []
                abstracts_src, idx_abs = [], []
                for i, m in enumerate(batch):
                    if (m.get("title_cn") is None or str(m.get("title_cn")).strip()=="") and (m.get("title_en") or ""):
                        titles_src.append(m["title_en"])
                        idx_title.append(i)
                    else:
                        titles_src.append("")
                    if (m.get("abstract_cn") is None or str(m.get("abstract_cn")).strip()=="") and (m.get("abstract_en") or ""):
                        abstracts_src.append(m["abstract_en"])
                        idx_abs.append(i)
                    else:
                        abstracts_src.append("")

                if not any(titles_src) and not any(abstracts_src):
                    processed += len(batch)
                    continue

                def _call_translate(arr: List[str]) -> List[Optional[str]]:
                    for attempt in range(args.max_retries + 1):
                        out = translate_batch_en2zh(arr, cfg)
                        if out and any(out):
                            return out
                        if attempt < args.max_retries:
                            time.sleep(1.5)
                    return [None for _ in arr]

                titles_zh = _call_translate(titles_src) if any(titles_src) else [None for _ in titles_src]
                time.sleep(0.1)
                abstracts_zh = _call_translate(abstracts_src) if any(abstracts_src) else [None for _ in abstracts_src]

                # 合并并写库（只补缺）
                if not args.dry_run:
                    for i, m in enumerate(batch):
                        upd_title = titles_src[i] and titles_zh[i]
                        upd_abs   = abstracts_src[i] and abstracts_zh[i]
                        if upd_title or upd_abs:
                            cur.execute(
                                """UPDATE articles
                                     SET title_cn = COALESCE(?, title_cn),
                                         abstract_cn = COALESCE(?, abstract_cn),
                                         last_updated_at = ?
                                   WHERE uid = ?""",
                                (titles_zh[i] if upd_title else None,
                                 abstracts_zh[i] if upd_abs else None,
                                 now_ts(), m["uid"])
                            )
                            updated_rows += cur.rowcount
                    conn.commit()

                processed += len(batch)
                done_pct = (processed / total) * 100.0
                print(f"[INFO] 翻译进度：{processed}/{total} ({done_pct:.1f}%) — 已更新 {updated_rows} 行。")
                time.sleep(args.sleep)

        # --- 2) 再做打标（若开启） ---
        if args.mode in ("all","tag"):
            processed2 = 0
            updated_tag_rows = 0
            for m in items:
                need_update = args.reclassify or (m.get("topic_tag") is None or str(m.get("topic_tag")).strip()=="")
                if not need_update:
                    processed2 += 1
                    continue

                # 用英文信息打标（优先），无英文再用中文兜底
                title_src = m.get("title_en") or m.get("title_cn") or ""
                abs_src   = m.get("abstract_en") or m.get("abstract_cn") or ""

                new_tag = classify_topic(title_src or "", abs_src or "", cfg={**cfg, "openai": cfg.get("openai") or {}})
                if not new_tag:  # 极端兜底
                    new_tag = "其他"

                if not args.dry_run:
                    if args.reclassify:
                        cur.execute(
                            "UPDATE articles SET topic_tag=?, last_updated_at=? WHERE uid=?",
                            (new_tag, now_ts(), m["uid"])
                        )
                    else:
                        cur.execute(
                            """UPDATE articles
                               SET topic_tag = COALESCE(topic_tag, ?),
                                   last_updated_at = ?
                             WHERE uid = ?""",
                            (new_tag, now_ts(), m["uid"])
                        )
                    updated_tag_rows += cur.rowcount
                processed2 += 1
                if processed2 % 200 == 0:
                    if not args.dry_run:
                        conn.commit()
                    print(f"[INFO] 打标进度：{processed2}/{total} — 累计更新 {updated_tag_rows} 行。")

            if not args.dry_run:
                conn.commit()
            print(f"[INFO] 打标完成：处理 {processed2} 条，更新 {updated_tag_rows} 行。")
            updated_rows += updated_tag_rows

        status = "ok" if not args.dry_run else "warn"
        notes = f"mode={args.mode}; updated_rows={updated_rows}; limit={args.limit}; reclassify={args.reclassify}; dry_run={args.dry_run}"
        log_run(conn, "backfill_enrich", started_at, status, updated_rows, notes)
        print(f"[DONE] 完成。实际更新 {updated_rows} 行（dry_run={args.dry_run}）。")

    except KeyboardInterrupt:
        log_run(conn, "backfill_enrich", started_at, "error", 0, "KeyboardInterrupt")
        print("\n[INTERRUPTED] 用户中断。", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        log_run(conn, "backfill_enrich", started_at, "error", 0, str(e))
        print(f"[ERR] 运行失败：{e}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
