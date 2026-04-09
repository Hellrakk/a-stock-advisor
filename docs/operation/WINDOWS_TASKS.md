# Windows 计划任务（定时任务）

本项目在 Windows 上使用 **任务计划程序（Task Scheduler）** 代替 cron。

## 1. 安装（创建任务）

在项目根目录运行（PowerShell）：

```powershell
# 进入项目目录
cd <path-to-a-stock-advisor>

# 确保虚拟环境存在
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 创建/更新计划任务
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\windows\install_windows_tasks.ps1
```

安装脚本会创建这些任务（名称以 `A-Stock-Advisor ...` 开头），并将任务指向 `scripts\windows\run_task.ps1`。

## 2. 手动触发

你可以在任务计划程序里右键任务 → 运行；也可以用命令行：

```powershell
schtasks /Run /TN "A-Stock-Advisor Evening Master"
```

## 3. 查看执行结果

- 任务计划程序里查看 **Last Run Result**
- 详细日志在：`<project-root>\logs\task_*.log`

## 4. 常见问题

### 4.1 换了项目目录后任务失效

Windows 的计划任务命令（`/TR`）会记录 **安装时的绝对路径**。

如果你移动了项目目录，需要重新运行一次安装脚本：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\windows\install_windows_tasks.ps1
```

### 4.2 提示找不到 Python/虚拟环境

确保存在：`<project-root>\.venv\Scripts\python.exe`

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
