> 📌 **数据呈现（在线预览）**：请跳转至 https://hehuifeng.github.io/rss_site/

# Nature Task Generate

自然学术文章 RSS 抽取与日报生成工具链。项目聚合多个脚本：从 RSS 订阅源抓取最新论文，提取元数据、主题打标、可选的中英翻译，并存入 SQLite 数据库；随后可生成 Markdown 日报或快速查看最新记录。

## 功能特性
- **RSS 抓取与入库**：`rss_fetch_store.py` 读取配置中的 feed 列表，抓取 RSS 与文章页面，去重后写入 SQLite，并可选使用 OpenAI 翻译标题与摘要。
- **统一数据库架构**：`rss_common.py` 定义 `feeds`、`seen`、`articles` 与 `runs_log` 等表结构，并启用 WAL 以提高并发读取能力。
- **翻译与主题分类**：内置批量英→中翻译和主题分类工具，可调用 OpenAI 完成翻译或智能分类，未配置时也有本地启发式规则。
- **交互式回填**：`backfill.py` 支持按日期范围或按 DOI 列表重抓文章、重打标签并强制重译，便于补齐历史数据。
- **Markdown 导出**：`generate_markdown.py` 根据时间窗与主题过滤汇总最新文章，可用 OpenAI 生成中文导读，并按期刊分组输出 Markdown 报告。
- **快速查看最新条目**：`view_latest.py` 从数据库中按主题或数量输出最新记录，便于人工检查数据更新情况。

## 目录结构
- `config_demo.json`：示例配置文件，包含 RSS 列表、数据库路径、输出目录以及可选的 OpenAI 参数。
- `rss_common.py`：公共模块，提供数据库操作、HTTP 请求、页面解析、翻译与主题分类等工具函数。
- `rss_fetch_store.py`：主抓取脚本，负责拉取 RSS、解析文章并写入数据库。
- `generate_markdown.py`：从数据库生成 Markdown 日报。
- `view_latest.py`：查看最近入库的条目。
- `backfill.py`：按时间或 DOI 回填/更新文章元数据和翻译。
- `run_demo.sh`：示例运行流程。
- `run_daily.sh`：示例 cron 脚本，用于每日自动执行并同步数据库。

## 配置
1. 复制 `config_demo.json` 为 `config.json` 并填写：
   - `feeds`：RSS 源列表。
   - `db`：SQLite 数据库路径。
   - `out_dir`：报告输出目录。
   - `openai`：如需翻译或智能分类，填写 OpenAI API Key、Base URL 与模型名；`classifier` 控制是否使用模型分类主题。
2. 通过环境变量 `PIPELINE_CONFIG` 指定其它配置文件路径（可选）。

## 使用示例
```bash
# 抓取并写入数据库
python rss_fetch_store.py

# 生成全部主题的 Markdown 日报
python generate_markdown.py

# 查看最新 200 条记录
python view_latest.py

# DOI 模式回填并重译
python backfill.py --doi 10.1038/example1 --doi 10.1038/example2
```

示例脚本 `run_demo.sh` 展示了常见组合操作；`run_daily.sh` 可作为定时任务模板，将数据库复制到静态站点并推送到 Git 仓库。

## 数据库结构
数据库为 SQLite，核心表如下：
- `feeds`：记录每个 RSS 的 etag、Last-Modified 等抓取状态。
- `seen`：已处理文章的去重信息。
- `articles`：文章元数据、原文与翻译、主题标签等。
- `runs_log`：每次脚本运行的时间、状态与处理条数。

## 依赖
- Python 3.9+
- `requests`
- `beautifulsoup4`
- `sqlite3`（标准库）
- 可选：OpenAI 接口需要网络访问权限以及有效 API Key。

## 许可
本项目以 [MIT 许可证](LICENSE) 开源，欢迎自由使用与分发。

