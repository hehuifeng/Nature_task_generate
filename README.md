# Nature RSS GUI

该项目原本提供命令行脚本以抓取 Nature 等期刊的 RSS 内容并存储到 SQLite。新增的 `gui_app.py` 提供了一个基于 Tkinter 的简单图形界面，方便在 macOS 上运行和查看最新文章。

## 使用方法
1. 安装依赖（Python 3 自带 Tkinter，无需额外安装）并确保配置好 `config.json` 以及数据库路径。
2. 运行 GUI：
   ```bash
   python gui_app.py
   ```
3. 点击 **“抓取最新”** 按钮可执行 RSS 抓取流程；左侧列表显示数据库中最新文章，选中可在下方查看标题与链接。

## 打包到 macOS 应用
可以使用 [PyInstaller](https://pyinstaller.org) 将界面打包为独立应用：
```bash
pip install pyinstaller
pyinstaller --windowed --onefile gui_app.py
```
生成的可执行文件位于 `dist/` 目录，可直接在 Mac 上运行。
