#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_markdown.py
- 把数据库里“最新文章”（可传入时间窗/数量与主题过滤）汇总成一个 Markdown
- 生成导读（可用 OpenAI；否则本地兜底）
- 目录按「期刊」分组
- 记录 runs_log
- 注：翻译与打标已在抓取阶段完成，这里仅读取
"""

from __future__ import annotations
import os, sys, json, io, sqlite3, argparse
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import List, Dict, Any, Optional
from rss_common import db_connect, now_ts, ensure_dir, TOPICS, clean_text, _openai_chat

def build_digest(items: List[Dict[str,Any]], cfg: Dict[str,Any]) -> str:
    if not items:
        return "local导读：本次未检索到新文章。"
    # 用英文标题/摘要拼提示词（更利于模型理解学术细节），长度截断保护
    lines = [
        "请基于以下学术文章，先写一段<导读>（简要概括本次涉及的研究方向、值得关注的趋势），",
        "开头直接输出，不要加title。总分结构输出，分点要bullet。",
        "语言使用简体中文，避免冗长；导读不超过250字。",
        "",
        "以下是文章汇总：",
    ]
    for i, m in enumerate(items, 1):
        lines.append(f"{i}. 标题: {m.get('title_en') or ''}")
        if m.get("journal"):
            lines.append(f"   期刊: {m['journal']}")
        if m.get("type"):
            lines.append(f"   类型: {m['type']}")
        if m.get("pub_date"):
            lines.append(f"   日期: {m['pub_date']}")
        if m.get("doi"):
            lines.append(f"   DOI: {m['doi']}")
        if m.get("abstract_en"):
            abs_en = m['abstract_en']
            if len(abs_en) > 1500:
                abs_en = abs_en[:1500] + " ..."
            lines.append(f"   摘要(EN): {abs_en}")
    msg = [{"role":"system","content":"You are a concise Chinese science editor. Keep it under 250 Chinese characters for the overview. Do not add prefaces."},
           {"role":"user","content":"\n".join(lines)}]
    s = _openai_chat(msg, cfg)
    if s: return s
    # fallback
    by_journal = Counter([m.get("journal") for m in items if m.get("journal")])
    return f"local导读：共收录 {len(items)} 篇；期刊覆盖：{', '.join([f'{k}×{v}' for k,v in by_journal.most_common(5)])}。建议优先关注方法创新与可复现性。"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", nargs="*", default=[], help="主题过滤，如：生命科学 人工智能 3D打印和增材制造（留空=全部）")
    ap.add_argument("--since_days", type=int, default=3, help="取最近 N 天（默认3）")
    ap.add_argument("--limit", type=int, default=500, help="最多提取多少条（默认500）")
    ap.add_argument("--out_dir", default=None, help="输出目录，默认 config.json.out_dir 或 ./reports")
    args = ap.parse_args()

    cfg_path = os.getenv("PIPELINE_CONFIG", "config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    db_path = cfg.get("db", "./rss_state.db")
    out_dir = args.out_dir or cfg.get("out_dir", "./reports")
    ensure_dir(out_dir)

    conn = db_connect(db_path)
    cur = conn.cursor()
    started_at = now_ts()

    # 取数据
    since_dt = (datetime.now() - timedelta(days=args.since_days)).isoformat(timespec="seconds")
    topics = args.topics or TOPICS  # 默认全选
    q = f"""
      SELECT uid, feed_url, journal, title_en, title_cn, type, pub_date, doi, article_url, abstract_en, abstract_cn, topic_tag, fetched_at
      FROM articles
      WHERE (fetched_at >= ?) AND (topic_tag IN ({','.join(['?']*len(topics))}))
      ORDER BY fetched_at DESC
      LIMIT ?
    """
    rows = list(cur.execute(q, [since_dt, *topics, args.limit]))
    items = []
    for r in rows:
        items.append({
            "uid": r[0], "feed_url": r[1], "journal": r[2], "title_en": r[3], "title_cn": r[4],
            "type": r[5], "pub_date": r[6], "doi": r[7], "article_url": r[8],
            "abstract_en": r[9], "abstract_cn": r[10], "topic_tag": r[11], "fetched_at": r[12]
        })

    # 导读
    digest = build_digest(items, cfg)

    # 按期刊分组目录 + 内容
    by_journal: Dict[str, List[Dict[str,Any]]] = defaultdict(list)
    for m in items:
        j = m.get("journal") or "UnknownJournal"
        by_journal[j].append(m)

    # 写文件
    ts_compact = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic_str = "-".join(args.topics) if args.topics else "ALL"
    fname = f"Digest_{topic_str}_{ts_compact}.md"
    path = os.path.join(out_dir, fname)
    with io.open(path, "w", encoding="utf-8") as f:
        # 统计文章发表时间区间
        pub_dates = [m.get("pub_date") for m in items if m.get("pub_date")]
        if pub_dates:
            try:
                from dateutil.parser import parse as dtparse
                dates_parsed = [dtparse(d).date() for d in pub_dates if d]
                start_date = min(dates_parsed).isoformat()
                end_date = max(dates_parsed).isoformat()
                date_range = f"{start_date} ~ {end_date}"
            except Exception:
                date_range = f"{min(pub_dates)} ~ {max(pub_dates)}"
        else:
            date_range = "无数据"

        f.write(f"# 今日导读（主题：{topic_str}） — 涵盖时间：{date_range}\n\n")
        f.write(f"{digest}\n\n")
        f.write("## 目录（按期刊）\n\n")
        if not items:
            f.write("- （暂无）\n\n")
        else:
            for j in sorted(by_journal.keys()):
                f.write(f"- {j}\n")
                for idx, m in enumerate(by_journal[j], 1):
                    title_en = m.get('title_en') or ''
                    title_cn = m.get('title_cn') or ''
                    if title_cn:
                        f.write(f"  - {idx}. {title_en} {title_cn}\n")
                    else:
                        f.write(f"  - {idx}. {title_en}\n")
            f.write("\n---\n\n")

        f.write("## 内容\n\n")
        for j in sorted(by_journal.keys()):
            f.write(f"### {j}\n\n")
            for idx, m in enumerate(by_journal[j], 1):
                f.write(f"**{idx}. {m.get('title_en') or ''}**\n\n")
                if m.get("title_cn"): f.write(f"**标题（CN）：**{m['title_cn']}\n\n")
                if m.get("type"): f.write(f"**类型：**{m['type']}\n\n")
                if m.get("pub_date"): f.write(f"**发表日期：**{m['pub_date']}\n\n")
                if m.get("doi"): f.write(f"**DOI：**{m['doi']}\n\n")
                if m.get("article_url"): f.write(f"**链接：**{m['article_url']}\n\n")
                if m.get("abstract_en"): f.write(f"**Abstract(EN)：**\n\n{m['abstract_en']}\n\n")
                if m.get("abstract_cn"): f.write(f"**Abstract(CN)：**\n\n{m['abstract_cn']}\n\n")
                f.write("\n---\n\n")

    # 日志
    from rss_common import log_run
    log_run(conn, "generate_markdown", started_at, "ok", len(items), f"topics={topic_str}; file={path}")
    conn.close()
    print(f"[OK] Markdown written: {path}")

if __name__ == "__main__":
    main()
