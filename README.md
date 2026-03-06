# wiztree-auto

使用 WizTree + Python + Windows 任务计划程序，按天采集 `C:` 盘空间数据，输出原始 CSV、历史汇总 CSV 和趋势图，用于跟踪磁盘空间占用变化。

## 功能

- 每日扫描 `C:` 盘并导出 WizTree 原始 CSV
- 将快照追加写入历史汇总表
- 生成 3 张趋势图
- 通过成功标记避免同一天重复采集
- 提供 PowerShell 脚本用于日常运行和注册计划任务

## 当前默认配置

配置文件位于 [config.json](/D:/hang/workspace/wiztree-auto/config.json)。当前默认值：

- 扫描目标：`C:`
- 调度时间：每天 `12:00`
- 错过任务后补跑：开启
- 任务权限：最高权限
- 热点路径模式：深层热点路径
- 热点路径数量：`20`
- 趋势窗口：最近 `30` 天
- 输出目录：`D:\hang\workspace\wiztree-auto\data`
- WizTree 路径：`C:\Program Files\WizTree\WizTree64.exe`

## 环境要求

- Windows
- Python 3.10+
- 已安装 WizTree 64 位
- 如需注册计划任务，建议使用管理员 PowerShell

## 安装

在项目根目录执行：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 手动运行

直接执行 Python 入口：

```powershell
.\.venv\Scripts\python.exe .\scripts\run_pipeline.py --config .\config.json
```

强制重跑当天快照：

```powershell
.\.venv\Scripts\python.exe .\scripts\run_pipeline.py --config .\config.json --force
```

通过 PowerShell 包装脚本运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1
```

强制重跑：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1 -Force
```

## 文件夹增长对比

新增脚本：`scripts/compare_growth.py`

用途：基于两份 WizTree 原始 CSV，对比全盘文件夹增长量，并生成树状 HTML 报告与平面 CSV 排行。

默认行为：

- 默认比较当前盘符下“最近两份”原始快照
- 使用 `--since YYYY-MM-DD` 时，基线快照取“该日期及之后的第一份快照”
- 最新快照默认取当前盘符下最新的一份原始快照
- 报告仅展示“增长量大于 0”或“新增”的文件夹
- 新增文件夹的增长量等于当前 `allocated_bytes`
- 树状视图会自动补上祖先目录，按最大可见增长排序，避免全量平铺

常用命令：

```powershell
.\.venv\Scripts\python.exe .\scripts\compare_growth.py --since 2026-03-01 --drive C:
```

显式指定两份原始 CSV：

```powershell
.\.venv\Scripts\python.exe .\scripts\compare_growth.py --baseline-csv .\data\raw\c\2026\03\wiztree_c_20260301_120000.csv --latest-csv .\data\raw\c\2026\03\wiztree_c_20260306_120000.csv
```

生成结果：

- `data/reports/c_growth_<baseline>_to_<latest>.html`
- `data/reports/c_growth_<baseline>_to_<latest>.csv`

HTML 报告内容：

- 全盘增长摘要
- 增长文件夹平面排行
- 新增文件夹排行
- 类似 WizTree 的可折叠树状视图

## 输出目录

所有输出都写入 `data/`：

- `data/raw/c/YYYY/MM/wiztree_c_YYYYMMDD_HHMMSS.csv`
  - WizTree 原始导出文件
- `data/drive_history.csv`
  - 列：`snapshot_ts, scan_date, drive, total_bytes, used_bytes, free_bytes, source_csv`
- `data/path_history.csv`
  - 列：`snapshot_ts, scan_date, drive, path, depth, size_bytes, allocated_bytes, files, folders, rank, source_csv`
- `data/c_used_free_30d.png`
  - 近 30 天已用/剩余空间趋势图
- `data/c_hot_paths_latest.png`
  - 最新一次快照的前 10 个热点路径柱状图
- `data/c_hot_paths_30d.png`
  - 最新一次快照前 10 个热点路径在近 30 天内的趋势图
- `data/state/c_YYYY-MM-DD.success.json`
  - 当天成功快照标记，用于避免重复采集
- `data/logs/run_YYYYMMDD_HHMMSS.log`
  - 包装脚本运行日志

## 项目结构

```text
wiztree-auto/
├─ config.json
├─ requirements.txt
├─ scripts/
│  ├─ run_pipeline.py
│  ├─ run_daily.ps1
│  └─ register_task.ps1
├─ tests/
│  ├─ test_pipeline.py
│  └─ fixtures/
└─ wiztree_auto/
   └─ pipeline.py
```

## 计划任务注册

注册脚本：`scripts/register_task.ps1`

执行方式：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\register_task.ps1
```

脚本会创建名为 `WizTree-Daily-C` 的任务，配置如下：

- 每天 `12:00` 触发
- 运行账号：当前用户
- 运行级别：`Highest`
- `StartWhenAvailable`
- `MultipleInstances=IgnoreNew`
- `ExecutionTimeLimit=2h`
- 动作为执行 `scripts/run_daily.ps1`

注意：当前机器上如果直接注册返回 `Access is denied`，请使用管理员 PowerShell 再执行一次。

## 测试

运行测试：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

测试覆盖：

- WizTree CSV 解析
- 历史 CSV 输出
- 同日幂等跳过
- 热点路径趋势缺失补零
- 图表文件生成`r`n- 原始快照增长对比与树状 HTML 报告生成

## 实现说明

- 图表依赖仅使用 `matplotlib`
- CSV 解析、汇总、历史追加均使用 Python 标准库
- 热点路径按 `allocated_bytes` 倒序取前 `20` 个
- `c_hot_paths_30d.png` 只绘制最新一次快照中的前 `10` 个热点路径
- 历史中缺失的路径按 `0` 填充

## 已验证

本项目已经在当前机器上完成以下验证：

- 单元测试通过
- 真实执行 `C:` 扫描成功
- 已生成原始 CSV、历史 CSV、趋势图和运行日志

## 常见问题

### 1. 注册计划任务时报 `Access is denied`

这是 Windows 权限问题，不是项目脚本逻辑错误。解决方式：

```powershell
Start-Process powershell -Verb RunAs
```

然后在管理员 PowerShell 中重新执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\register_task.ps1
```

### 2. WizTree 导出后 CSV 仍被占用

项目已在管道中处理“文件大小稳定且可读后再解析”，避免导出刚结束就读取导致的占用错误。

## 后续可扩展方向

- 增加 `D:` 或多盘扫描
- 增加邮件或通知
- 增加 HTML 报表
- 将历史数据导入 SQLite 或 DuckDB


