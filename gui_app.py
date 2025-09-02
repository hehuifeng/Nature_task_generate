import os
import json
from tkinter import Tk, ttk, Listbox, Text, END, messagebox

import rss_fetch_store
from rss_common import db_connect

CONFIG_PATH = os.getenv("PIPELINE_CONFIG", "config.json")

articles = []


def fetch_latest():
    try:
        rss_fetch_store.main()
        messagebox.showinfo("完成", "已抓取最新 RSS 数据")
        refresh_articles()
    except Exception as e:
        messagebox.showerror("错误", str(e))


def get_latest(limit=50):
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    conn = db_connect(cfg.get("db", "./rss_state.db"))
    cur = conn.cursor()
    rows = list(
        cur.execute(
            "SELECT title_en, article_url FROM articles ORDER BY datetime(fetched_at) DESC LIMIT ?",
            (limit,),
        )
    )
    conn.close()
    return rows


def refresh_articles():
    global articles
    articles = get_latest()
    listbox.delete(0, END)
    for title, _ in articles:
        listbox.insert(END, title)


def on_select(event):
    if not listbox.curselection():
        return
    idx = listbox.curselection()[0]
    title, url = articles[idx]
    text.delete("1.0", END)
    text.insert(END, f"{title}\n{url}")


root = Tk()
root.title("Nature RSS GUI")

btn_fetch = ttk.Button(root, text="抓取最新", command=fetch_latest)
btn_fetch.pack(pady=5)

listbox = Listbox(root)
listbox.pack(fill="both", expand=True)
listbox.bind("<<ListboxSelect>>", on_select)

text = Text(root, height=5)
text.pack(fill="x")

refresh_articles()

root.mainloop()
