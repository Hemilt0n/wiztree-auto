from __future__ import annotations

import argparse
import csv
import html
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from .pipeline import (
    PipelineError,
    SnapshotData,
    bytes_to_gib,
    drive_token_from_drive,
    load_config,
    normalize_drive,
    parse_wiztree_csv,
)


TIMESTAMP_RE = re.compile(r"^wiztree_(?P<drive>[a-z])_(?P<stamp>\d{8}_\d{6})$", re.IGNORECASE)
FLAT_REPORT_FIELDS = [
    "path",
    "parent_path",
    "depth",
    "growth_allocated_bytes",
    "current_allocated_bytes",
    "baseline_allocated_bytes",
    "current_size_bytes",
    "baseline_size_bytes",
    "is_new",
    "files",
    "folders",
]


@dataclass(slots=True)
class RawSnapshotFile:
    path: Path
    timestamp: datetime

    @property
    def scan_date(self) -> date:
        return self.timestamp.date()

    @property
    def stamp(self) -> str:
        return self.timestamp.strftime("%Y%m%d_%H%M%S")


@dataclass(slots=True)
class FolderGrowth:
    path: str
    parent_path: str
    name: str
    depth: int
    growth_allocated: int
    current_allocated: int
    baseline_allocated: int
    current_size: int
    baseline_size: int
    files: int
    folders: int
    is_new: bool

    @property
    def has_direct_growth(self) -> bool:
        return self.growth_allocated > 0 or self.is_new


@dataclass(slots=True)
class GrowthTreeNode:
    path: str
    name: str
    depth: int
    growth_allocated: int
    current_allocated: int
    baseline_allocated: int
    current_size: int
    baseline_size: int
    files: int
    folders: int
    is_new: bool
    is_ancestor_only: bool = False
    children: list["GrowthTreeNode"] = field(default_factory=list)
    sort_growth: int = 0


@dataclass(slots=True)
class GrowthReport:
    drive: str
    baseline_used_bytes: int
    latest_used_bytes: int
    root_growth_allocated: int
    entries: list[FolderGrowth]
    direct_growth_entries: list[FolderGrowth]
    root_node: GrowthTreeNode


@dataclass(slots=True)
class ComparisonSelection:
    drive: str
    baseline: RawSnapshotFile
    latest: RawSnapshotFile
    requested_since: date | None
    requested_until: date | None


@dataclass(slots=True)
class GeneratedReport:
    html_path: Path
    csv_path: Path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two WizTree raw CSV snapshots and generate a growth report.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config.json",
        help="Path to config.json",
    )
    parser.add_argument("--drive", help="Drive to compare, defaults to the first configured scan target")
    parser.add_argument("--since", help="Baseline date in YYYY-MM-DD. Selects the first snapshot on or after this date.")
    parser.add_argument("--until", help="Latest date in YYYY-MM-DD. Selects the last snapshot on or before this date.")
    parser.add_argument("--baseline-csv", type=Path, help="Explicit baseline raw WizTree CSV path")
    parser.add_argument("--latest-csv", type=Path, help="Explicit latest raw WizTree CSV path")
    parser.add_argument("--output-html", type=Path, help="Explicit HTML output path")
    parser.add_argument("--output-csv", type=Path, help="Explicit flat CSV output path")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config(args.config)
    drive = normalize_drive(args.drive or config.scan_targets[0])
    requested_since = parse_optional_date(args.since)
    requested_until = parse_optional_date(args.until)

    selection = choose_snapshot_pair(
        output_root=config.output_root,
        drive=drive,
        requested_since=requested_since,
        requested_until=requested_until,
        baseline_csv=args.baseline_csv,
        latest_csv=args.latest_csv,
    )
    report = compare_snapshot_files(selection.baseline.path, selection.latest.path, selection.drive)
    generated = write_growth_reports(
        report=report,
        selection=selection,
        output_root=config.output_root,
        explicit_html=args.output_html,
        explicit_csv=args.output_csv,
    )

    print(f"Baseline: {selection.baseline.path}")
    print(f"Latest:   {selection.latest.path}")
    print(f"HTML:     {generated.html_path}")
    print(f"CSV:      {generated.csv_path}")
    return 0


def parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def choose_snapshot_pair(
    output_root: Path,
    drive: str,
    requested_since: date | None = None,
    requested_until: date | None = None,
    baseline_csv: Path | None = None,
    latest_csv: Path | None = None,
) -> ComparisonSelection:
    normalized_drive = normalize_drive(drive)
    discovered = discover_raw_snapshots(output_root, normalized_drive)

    latest = snapshot_from_path(latest_csv) if latest_csv else None
    if latest is None:
        latest_pool = discovered
        if requested_until is not None:
            latest_pool = [item for item in latest_pool if item.scan_date <= requested_until]
        if not latest_pool:
            raise PipelineError(f"No latest snapshot was found for {normalized_drive}")
        latest = latest_pool[-1]

    baseline = snapshot_from_path(baseline_csv) if baseline_csv else None
    if baseline is None:
        baseline_pool = [item for item in discovered if item.timestamp <= latest.timestamp]
        if requested_since is not None:
            baseline_pool = [item for item in baseline_pool if item.scan_date >= requested_since]
            if not baseline_pool:
                raise PipelineError(
                    f"No baseline snapshot was found for {normalized_drive} on or after {requested_since.isoformat()}"
                )
            baseline = baseline_pool[0]
        else:
            if len(baseline_pool) < 2:
                raise PipelineError(
                    f"At least two snapshots are required for {normalized_drive}; use --baseline-csv and --latest-csv to compare explicit files"
                )
            baseline = baseline_pool[-2]

    if baseline.path.resolve() == latest.path.resolve():
        raise PipelineError("Baseline and latest snapshots resolve to the same file")

    return ComparisonSelection(
        drive=normalized_drive,
        baseline=baseline,
        latest=latest,
        requested_since=requested_since,
        requested_until=requested_until,
    )


def discover_raw_snapshots(output_root: Path, drive: str) -> list[RawSnapshotFile]:
    token = drive_token_from_drive(drive)
    root = output_root / "raw" / token
    if not root.exists():
        return []

    snapshots: list[RawSnapshotFile] = []
    for path in root.rglob(f"wiztree_{token}_*.csv"):
        snapshots.append(snapshot_from_path(path))
    snapshots.sort(key=lambda item: item.timestamp)
    return snapshots


def snapshot_from_path(path: Path | None) -> RawSnapshotFile:
    if path is None:
        raise PipelineError("Snapshot path is required")
    if not path.exists():
        raise PipelineError(f"Snapshot file not found: {path}")

    timestamp = parse_snapshot_timestamp(path)
    return RawSnapshotFile(path=path.resolve(), timestamp=timestamp)


def parse_snapshot_timestamp(path: Path) -> datetime:
    match = TIMESTAMP_RE.match(path.stem)
    if match:
        return datetime.strptime(match.group("stamp"), "%Y%m%d_%H%M%S")
    return datetime.fromtimestamp(path.stat().st_mtime)


def compare_snapshot_files(baseline_csv: Path, latest_csv: Path, drive: str) -> GrowthReport:
    baseline_snapshot = parse_wiztree_csv(baseline_csv, expected_drive=drive)
    latest_snapshot = parse_wiztree_csv(latest_csv, expected_drive=drive)
    return build_growth_report(baseline_snapshot, latest_snapshot)


def build_growth_report(baseline_snapshot: SnapshotData, latest_snapshot: SnapshotData) -> GrowthReport:
    baseline_by_path = {item.path: item for item in baseline_snapshot.folders}
    entries: list[FolderGrowth] = []
    for latest_folder in latest_snapshot.folders:
        baseline_folder = baseline_by_path.get(latest_folder.path)
        baseline_allocated = baseline_folder.allocated_bytes if baseline_folder else 0
        baseline_size = baseline_folder.size_bytes if baseline_folder else 0
        growth_allocated = latest_folder.allocated_bytes - baseline_allocated
        is_new = baseline_folder is None
        if is_new:
            growth_allocated = latest_folder.allocated_bytes

        entries.append(
            FolderGrowth(
                path=latest_folder.path,
                parent_path=parent_folder_path(latest_folder.path),
                name=folder_name(latest_folder.path),
                depth=latest_folder.depth,
                growth_allocated=growth_allocated,
                current_allocated=latest_folder.allocated_bytes,
                baseline_allocated=baseline_allocated,
                current_size=latest_folder.size_bytes,
                baseline_size=baseline_size,
                files=latest_folder.files,
                folders=latest_folder.folders,
                is_new=is_new,
            )
        )

    direct_growth_entries = sorted(
        [entry for entry in entries if entry.has_direct_growth],
        key=lambda item: (-item.growth_allocated, -item.current_allocated, item.path.lower()),
    )
    root_node = build_growth_tree(
        drive=latest_snapshot.drive,
        root_growth=latest_snapshot.used_bytes - baseline_snapshot.used_bytes,
        root_current=latest_snapshot.used_bytes,
        root_baseline=baseline_snapshot.used_bytes,
        entries=entries,
    )
    return GrowthReport(
        drive=latest_snapshot.drive,
        baseline_used_bytes=baseline_snapshot.used_bytes,
        latest_used_bytes=latest_snapshot.used_bytes,
        root_growth_allocated=latest_snapshot.used_bytes - baseline_snapshot.used_bytes,
        entries=entries,
        direct_growth_entries=direct_growth_entries,
        root_node=root_node,
    )


def build_growth_tree(drive: str, root_growth: int, root_current: int, root_baseline: int, entries: list[FolderGrowth]) -> GrowthTreeNode:
    entry_map = {entry.path: entry for entry in entries}
    direct_visible_paths = {entry.path for entry in entries if entry.has_direct_growth}
    visible_paths: set[str] = set()
    for path in direct_visible_paths:
        current = path
        while current and not is_drive_root_path(current):
            visible_paths.add(current)
            current = parent_folder_path(current)

    root_path = f"{normalize_drive(drive)}\\"
    node_map: dict[str, GrowthTreeNode] = {}
    root_node = GrowthTreeNode(
        path=root_path,
        name=root_path,
        depth=0,
        growth_allocated=root_growth,
        current_allocated=root_current,
        baseline_allocated=root_baseline,
        current_size=root_current,
        baseline_size=root_baseline,
        files=0,
        folders=0,
        is_new=False,
    )

    for path in sorted(visible_paths, key=lambda item: (item.count("\\"), item.lower())):
        entry = entry_map[path]
        node_map[path] = GrowthTreeNode(
            path=entry.path,
            name=entry.name,
            depth=entry.depth,
            growth_allocated=entry.growth_allocated,
            current_allocated=entry.current_allocated,
            baseline_allocated=entry.baseline_allocated,
            current_size=entry.current_size,
            baseline_size=entry.baseline_size,
            files=entry.files,
            folders=entry.folders,
            is_new=entry.is_new,
            is_ancestor_only=path not in direct_visible_paths,
        )

    for path, node in node_map.items():
        parent_path = parent_folder_path(path)
        parent_node = node_map.get(parent_path) if parent_path and parent_path in node_map else root_node
        parent_node.children.append(node)

    finalize_tree(root_node)
    return root_node


def finalize_tree(node: GrowthTreeNode) -> None:
    for child in node.children:
        finalize_tree(child)
    child_growth = max((child.sort_growth for child in node.children), default=0)
    node.sort_growth = max(max(node.growth_allocated, 0), child_growth)
    node.children.sort(
        key=lambda item: (-item.sort_growth, -max(item.growth_allocated, 0), -item.current_allocated, item.path.lower())
    )


def write_growth_reports(
    report: GrowthReport,
    selection: ComparisonSelection,
    output_root: Path,
    explicit_html: Path | None = None,
    explicit_csv: Path | None = None,
) -> GeneratedReport:
    reports_dir = output_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    file_stem = f"{drive_token_from_drive(selection.drive)}_growth_{selection.baseline.stamp}_to_{selection.latest.stamp}"
    html_path = explicit_html or (reports_dir / f"{file_stem}.html")
    csv_path = explicit_csv or (reports_dir / f"{file_stem}.csv")
    html_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    write_flat_growth_csv(csv_path, report.direct_growth_entries)
    html_path.write_text(render_growth_html(report, selection), encoding="utf-8")
    return GeneratedReport(html_path=html_path, csv_path=csv_path)


def write_flat_growth_csv(path: Path, entries: list[FolderGrowth]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FLAT_REPORT_FIELDS)
        writer.writeheader()
        for entry in entries:
            writer.writerow(
                {
                    "path": entry.path,
                    "parent_path": entry.parent_path,
                    "depth": entry.depth,
                    "growth_allocated_bytes": entry.growth_allocated,
                    "current_allocated_bytes": entry.current_allocated,
                    "baseline_allocated_bytes": entry.baseline_allocated,
                    "current_size_bytes": entry.current_size,
                    "baseline_size_bytes": entry.baseline_size,
                    "is_new": str(entry.is_new).lower(),
                    "files": entry.files,
                    "folders": entry.folders,
                }
            )


def render_growth_html(report: GrowthReport, selection: ComparisonSelection) -> str:
    top_entries = report.direct_growth_entries[:30]
    new_entries = [entry for entry in report.direct_growth_entries if entry.is_new][:30]
    tree_html = "".join(render_tree_node(node) for node in report.root_node.children)
    top_rows_html = "".join(render_flat_table_row(entry) for entry in top_entries)
    new_rows_html = "".join(render_flat_table_row(entry) for entry in new_entries)

    return f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{html.escape(report.drive)} 文件夹增长报告</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255,255,255,0.88);
      --ink: #1e1d1a;
      --muted: #6b655c;
      --line: #d7cdbd;
      --rise: #a23b1e;
      --new: #1c6b48;
      --chip: #ece1d0;
      --shadow: 0 18px 50px rgba(68, 49, 18, 0.12);
      --mono: Consolas, 'Cascadia Mono', 'SFMono-Regular', monospace;
      --sans: 'Segoe UI', 'Microsoft YaHei', sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(216,175,95,0.28), transparent 28%),
        linear-gradient(180deg, #f7f1e8 0%, #efe8dc 100%);
    }}
    .page {{ max-width: 1440px; margin: 0 auto; padding: 32px 24px 48px; }}
    h1, h2 {{ margin: 0; font-weight: 700; }}
    h1 {{ font-size: 34px; letter-spacing: 0.02em; }}
    h2 {{ font-size: 20px; margin-bottom: 14px; }}
    p {{ margin: 0; color: var(--muted); }}
    .hero {{ display: grid; gap: 16px; margin-bottom: 24px; }}
    .meta {{ font-family: var(--mono); font-size: 13px; color: var(--muted); display: grid; gap: 4px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin: 22px 0 28px; }}
    .card, .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; box-shadow: var(--shadow); }}
    .card {{ padding: 18px 18px 16px; }}
    .card .label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    .card .value {{ display: block; font-size: 28px; margin-top: 8px; font-weight: 700; }}
    .card .sub {{ display: block; margin-top: 8px; font-family: var(--mono); font-size: 12px; color: var(--muted); }}
    .grid {{ display: grid; grid-template-columns: minmax(320px, 0.95fr) minmax(440px, 1.4fr); gap: 20px; align-items: start; }}
    .panel {{ padding: 18px; overflow: hidden; }}
    .table-wrap {{ overflow: auto; max-height: 420px; border-radius: 12px; border: 1px solid var(--line); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #f7f0e7; z-index: 1; font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }}
    td.path {{ font-family: var(--mono); min-width: 260px; }}
    .growth {{ font-weight: 700; color: var(--rise); }}
    .badge {{ display: inline-block; margin-left: 8px; padding: 2px 7px; border-radius: 999px; font-size: 11px; font-weight: 700; letter-spacing: 0.04em; background: var(--chip); color: var(--muted); }}
    .badge.new {{ background: rgba(28,107,72,0.12); color: var(--new); }}
    .badge.group {{ background: rgba(60,83,122,0.12); color: #314361; }}
    .tree-toolbar {{ display: flex; gap: 8px; justify-content: flex-end; margin-bottom: 12px; }}
    button {{ border: 1px solid var(--line); background: #fbf7f1; border-radius: 999px; padding: 8px 12px; cursor: pointer; font: inherit; }}
    .tree {{ display: grid; gap: 10px; }}
    details.node, .leaf {{ border: 1px solid var(--line); border-radius: 14px; background: rgba(255,255,255,0.72); }}
    details.node[open] {{ background: rgba(255,255,255,0.94); }}
    summary, .leaf-line {{ list-style: none; display: grid; grid-template-columns: minmax(260px, 1.3fr) repeat(3, minmax(110px, auto)) minmax(110px, auto); gap: 12px; align-items: center; padding: 12px 14px; cursor: pointer; }}
    summary::-webkit-details-marker {{ display: none; }}
    .node-name {{ font-weight: 700; }}
    .node-path {{ display: block; font-family: var(--mono); font-size: 12px; color: var(--muted); margin-top: 3px; word-break: break-all; }}
    .metric {{ font-family: var(--mono); font-size: 12px; color: var(--muted); }}
    .metric strong {{ display: block; color: var(--ink); font-size: 14px; }}
    .children {{ display: grid; gap: 10px; padding: 0 12px 12px 18px; }}
    .depth-1 {{ margin-left: 0; }}
    .depth-2 {{ margin-left: 18px; }}
    .depth-3 {{ margin-left: 36px; }}
    .depth-4 {{ margin-left: 54px; }}
    .depth-5 {{ margin-left: 72px; }}
    .depth-6 {{ margin-left: 90px; }}
    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
      summary, .leaf-line {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>
  <div class=\"page\">
    <section class=\"hero\">
      <h1>{html.escape(report.drive)} 全盘文件夹增长报告</h1>
      <p>基于 WizTree 原始 CSV 比较文件夹当前分配大小与基线快照的差值。新增文件夹按当前分配大小记为增长量。</p>
      <div class=\"meta\">
        <span>Baseline: {html.escape(selection.baseline.timestamp.isoformat(sep=' ', timespec='seconds'))} | {html.escape(str(selection.baseline.path))}</span>
        <span>Latest:   {html.escape(selection.latest.timestamp.isoformat(sep=' ', timespec='seconds'))} | {html.escape(str(selection.latest.path))}</span>
      </div>
    </section>
    <section class=\"cards\">
      <div class=\"card\"><span class=\"label\">Drive Growth</span><span class=\"value\">{format_bytes(report.root_growth_allocated, signed=True)}</span><span class=\"sub\">{format_gib(report.root_growth_allocated)} GiB</span></div>
      <div class=\"card\"><span class=\"label\">Current Used</span><span class=\"value\">{format_bytes(report.latest_used_bytes)}</span><span class=\"sub\">{format_gib(report.latest_used_bytes)} GiB</span></div>
      <div class=\"card\"><span class=\"label\">Growing Folders</span><span class=\"value\">{len(report.direct_growth_entries)}</span><span class=\"sub\">only folders with positive growth or newly created folders</span></div>
      <div class=\"card\"><span class=\"label\">New Folders</span><span class=\"value\">{sum(1 for entry in report.direct_growth_entries if entry.is_new)}</span><span class=\"sub\">marked with NEW in the tree below</span></div>
    </section>
    <section class=\"grid\">
      <div class=\"panel\">
        <h2>Flat Ranking</h2>
        <div class=\"table-wrap\">
          <table>
            <thead>
              <tr><th>Path</th><th>Growth</th><th>Current</th><th>Baseline</th></tr>
            </thead>
            <tbody>{top_rows_html}</tbody>
          </table>
        </div>
      </div>
      <div class=\"panel\">
        <h2>New Folders</h2>
        <div class=\"table-wrap\">
          <table>
            <thead>
              <tr><th>Path</th><th>Growth</th><th>Current</th><th>Baseline</th></tr>
            </thead>
            <tbody>{new_rows_html or '<tr><td colspan="4">No new folders in this range.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    </section>
    <section class=\"panel\" style=\"margin-top: 20px;\">
      <h2>Tree View</h2>
      <div class=\"tree-toolbar\">
        <button type=\"button\" onclick=\"setAllDetails(true)\">Expand All</button>
        <button type=\"button\" onclick=\"setAllDetails(false)\">Collapse All</button>
      </div>
      <div class=\"tree\">{tree_html}</div>
    </section>
  </div>
  <script>
    function setAllDetails(openState) {{
      document.querySelectorAll('details.node').forEach((element) => {{
        element.open = openState;
      }});
    }}
  </script>
</body>
</html>
"""


def render_flat_table_row(entry: FolderGrowth) -> str:
    badges = []
    if entry.is_new:
        badges.append('<span class="badge new">NEW</span>')
    return (
        "<tr>"
        f"<td class=\"path\">{html.escape(entry.path)}{''.join(badges)}</td>"
        f"<td class=\"growth\">{html.escape(format_bytes(entry.growth_allocated, signed=True))}</td>"
        f"<td>{html.escape(format_bytes(entry.current_allocated))}</td>"
        f"<td>{html.escape(format_bytes(entry.baseline_allocated))}</td>"
        "</tr>"
    )


def render_tree_node(node: GrowthTreeNode) -> str:
    badges = []
    if node.is_new:
        badges.append('<span class="badge new">NEW</span>')
    if node.is_ancestor_only:
        badges.append('<span class="badge group">GROUP</span>')

    line = (
        f"<div class=\"node-name\">{html.escape(node.name)}{''.join(badges)}"
        f"<span class=\"node-path\">{html.escape(node.path)}</span></div>"
        f"<div class=\"metric growth\"><strong>{html.escape(format_bytes(node.growth_allocated, signed=True))}</strong>Growth</div>"
        f"<div class=\"metric\"><strong>{html.escape(format_bytes(node.current_allocated))}</strong>Current</div>"
        f"<div class=\"metric\"><strong>{html.escape(format_bytes(node.baseline_allocated))}</strong>Baseline</div>"
        f"<div class=\"metric\"><strong>{node.files:,} files</strong>{node.folders:,} folders</div>"
    )

    css_depth = min(node.depth, 6)
    if node.children:
        children = "".join(render_tree_node(child) for child in node.children)
        open_attr = " open" if node.depth <= 2 else ""
        return (
            f"<details class=\"node depth-{css_depth}\"{open_attr}>"
            f"<summary>{line}</summary>"
            f"<div class=\"children\">{children}</div>"
            "</details>"
        )

    return f"<div class=\"leaf depth-{css_depth}\"><div class=\"leaf-line\">{line}</div></div>"


def format_bytes(value: int, signed: bool = False) -> str:
    negative = value < 0
    amount = float(abs(value))
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    unit = units[0]
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            break
        amount /= 1024
    text = f"{amount:,.2f} {unit}" if unit != "B" else f"{int(amount):,} {unit}"
    if negative:
        return f"-{text}"
    if signed and value > 0:
        return f"+{text}"
    return text

def format_gib(value: int) -> str:
    return f"{bytes_to_gib(value):,.2f}"


def parent_folder_path(path: str) -> str:
    if is_drive_root_path(path):
        return ""
    head, _sep, _tail = path.rpartition("\\")
    if len(head) == 2 and head.endswith(":"):
        return f"{head}\\"
    return head


def folder_name(path: str) -> str:
    if is_drive_root_path(path):
        return path
    return path.rsplit("\\", 1)[-1]


def is_drive_root_path(path: str) -> bool:
    return len(path) == 3 and path[1:] == ":\\"


