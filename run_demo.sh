# 建议用 cron/systemd/Windows 计划任务 每日跑一次
python3 rss_fetch_store.py

# 全部主题
python3 generate_markdown.py
# 仅生命科学 + AI，时间窗 5 天
python3 generate_markdown.py --topics 生命科学 人工智能 --since_days 5

# 打印最近 200 条
python3 view_latest.py
# 只看“3D打印和增材制造”最近 100 条并输出到文件
python3 view_latest.py --topics 3D打印和增材制造 --limit 100 --out latest_3dp.txt