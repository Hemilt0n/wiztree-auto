from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Sequence


CSV_ENCODING = "utf-8-sig"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
DATE_FORMAT = "%Y-%m-%d"
DRIVE_HISTORY_FIELDS = [
    "snapshot_ts",
    "scan_date",
    "drive",
    "total_bytes",
    "used_bytes",
    "free_bytes",
    "source_csv",
]
PATH_HISTORY_FIELDS = [
    "snapshot_ts",
    "scan_date",
    "drive",
    "path",
    "depth",
    "size_bytes",
    "allocated_bytes",
    "files",
    "folders",
    "rank",
    "source_csv",
]


class PipelineError(RuntimeError):
    pass


ExportRunner = Callable[[Path, str, Path], None]


@dataclass(slots=True)
class AppConfig:
    scan_targets: list[str]
    schedule_time: str
    catch_up: bool
    run_level: str
    top_path_mode: str
    top_n: int
    chart_window_days: int
    output_root: Path
    task_name: str
    wiztree_path: Path


@dataclass(slots=True)
class FolderStat:
    drive: str
    path: str
    depth: int
    size_bytes: int
    allocated_bytes: int
    files: int
    folders: int


@dataclass(slots=True)
class SnapshotData:
    drive: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    folders: list[FolderStat]


@dataclass(slots=True)
class RunResult:
    status: str
    raw_csv_paths: list[Path]
    drive_rows_written: int
    path_rows_written: int
    charts: list[Path]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the WizTree daily pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config.json",
        help="Path to config.json",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the once-per-day success marker.",
    )
    return parser.parse_args(argv)


def load_config(config_path: Path) -> AppConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return AppConfig(
        scan_targets=[normalize_drive(value) for value in payload["scan_targets"]],
        schedule_time=payload["schedule_time"],
        catch_up=bool(payload["catch_up"]),
        run_level=payload["run_level"],
        top_path_mode=payload["top_path_mode"],
        top_n=int(payload["top_n"]),
        chart_window_days=int(payload["chart_window_days"]),
        output_root=Path(payload["output_root"]),
        task_name=payload.get("task_name", "WizTree-Daily-C"),
        wiztree_path=Path(payload.get("wiztree_path", r"C:\Program Files\WizTree\WizTree64.exe")),
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config(args.config)
    result = run_pipeline(config, force=args.force)
    print(
        "Completed status={status} raw_csvs={raw} drive_rows={drive_rows} path_rows={path_rows} charts={charts}".format(
            status=result.status,
            raw=len(result.raw_csv_paths),
            drive_rows=result.drive_rows_written,
            path_rows=result.path_rows_written,
            charts=len(result.charts),
        )
    )
    return 0


def run_pipeline(
    config: AppConfig,
    force: bool = False,
    now: datetime | None = None,
    export_runner: ExportRunner | None = None,
) -> RunResult:
    validate_config(config)
    export_runner = export_runner or run_wiztree_export
    now = now or datetime.now()
    snapshot_ts = now.strftime(TIMESTAMP_FORMAT)
    scan_date = now.strftime(DATE_FORMAT)
    ensure_output_root(config.output_root)

    raw_csv_paths: list[Path] = []
    charts: list[Path] = []
    drive_rows_written = 0
    path_rows_written = 0
    any_created = False

    for drive in config.scan_targets:
        drive_token = drive_token_from_drive(drive)
        success_marker = success_marker_path(config.output_root, drive_token, scan_date)
        if success_marker.exists() and not force:
            print(f"Skipping {drive}: success marker exists at {success_marker}")
            continue

        raw_csv = raw_csv_path(config.output_root, drive, now)
        print(f"Exporting {drive} to {raw_csv}")
        export_runner(config.wiztree_path, drive, raw_csv)

        snapshot = parse_wiztree_csv(raw_csv, expected_drive=drive)
        top_paths = select_top_paths(snapshot.folders, config.top_n)
        append_snapshot_to_history(
            output_root=config.output_root,
            snapshot_ts=snapshot_ts,
            scan_date=scan_date,
            drive_snapshot=snapshot,
            top_paths=top_paths,
            source_csv=raw_csv,
        )
        write_success_marker(success_marker, snapshot_ts, scan_date, drive, raw_csv)
        charts.extend(generate_charts(config.output_root, drive, config.chart_window_days))
        raw_csv_paths.append(raw_csv)
        drive_rows_written += 1
        path_rows_written += len(top_paths)
        any_created = True

    status = "created" if any_created else "skipped"
    return RunResult(
        status=status,
        raw_csv_paths=raw_csv_paths,
        drive_rows_written=drive_rows_written,
        path_rows_written=path_rows_written,
        charts=charts,
    )


def validate_config(config: AppConfig) -> None:
    if config.top_path_mode != "deep_hot_paths":
        raise PipelineError(f"Unsupported top_path_mode: {config.top_path_mode}")
    if config.top_n <= 0:
        raise PipelineError("top_n must be positive")
    if config.chart_window_days <= 0:
        raise PipelineError("chart_window_days must be positive")
    if not config.scan_targets:
        raise PipelineError("scan_targets cannot be empty")


def ensure_output_root(output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "raw").mkdir(parents=True, exist_ok=True)
    (output_root / "logs").mkdir(parents=True, exist_ok=True)
    (output_root / "state").mkdir(parents=True, exist_ok=True)


def run_wiztree_export(wiztree_path: Path, drive: str, output_csv: Path) -> None:
    if not wiztree_path.exists():
        raise PipelineError(f"WizTree executable was not found: {wiztree_path}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(wiztree_path),
        normalize_drive(drive),
        f"/export={output_csv}",
        "/admin=1",
        "/exportsplitfilename=1",
        "/exportdrivecapacity=1",
    ]

    print("Running WizTree:", " ".join(command))
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.stdout.strip():
        print(completed.stdout.strip())
    if completed.stderr.strip():
        print(completed.stderr.strip())
    if completed.returncode != 0:
        raise PipelineError(f"WizTree export failed with exit code {completed.returncode}")

    wait_for_nonempty_file(output_csv)


def wait_for_nonempty_file(path: Path, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_size = -1
    stable_polls = 0
    while time.monotonic() < deadline:
        if path.exists():
            size = path.stat().st_size
            if size > 0:
                if size == last_size:
                    stable_polls += 1
                else:
                    last_size = size
                    stable_polls = 0
                if stable_polls >= 2 and file_can_be_read(path):
                    return
        time.sleep(0.5)
    raise PipelineError(f"Expected WizTree CSV was not created or finalized: {path}")


def file_can_be_read(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except PermissionError:
        return False


def parse_wiztree_csv(csv_path: Path, expected_drive: str | None = None) -> SnapshotData:
    expected = normalize_drive(expected_drive) if expected_drive else None
    folders: list[FolderStat] = []
    drive = expected or ""
    total_bytes: int | None = None
    used_bytes: int | None = None

    with csv_path.open("r", encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        header = next(reader, None)
        if not header:
            raise PipelineError(f"CSV header was missing from {csv_path}")

        for row in reader:
            if not row:
                continue
            row = list(row)
            if len(row) < 11:
                continue
            while len(row) < 12:
                row.append("")

            full_name = row[0].strip()
            if not full_name:
                continue

            size_bytes = parse_int(row[1])
            allocated_bytes = parse_int(row[2])
            files = parse_int(row[5])
            folders_count = parse_int(row[6])
            root = normalize_drive(row[7]) if row[7] else ""
            is_folder = full_name.endswith("\\")
            normalized_path = normalize_export_path(full_name, is_folder)
            if not normalized_path:
                continue

            row_drive = root or normalize_drive(normalized_path[:2])
            if expected and row_drive != expected:
                continue

            if is_folder and is_drive_root(normalized_path):
                drive = row_drive
                total_bytes = parse_int(row[11]) or size_bytes or allocated_bytes
                used_bytes = allocated_bytes or size_bytes
                continue

            if is_folder:
                folders.append(
                    FolderStat(
                        drive=row_drive,
                        path=normalized_path,
                        depth=path_depth(normalized_path),
                        size_bytes=size_bytes,
                        allocated_bytes=allocated_bytes,
                        files=files,
                        folders=folders_count,
                    )
                )

    if total_bytes is None or used_bytes is None or not drive:
        raise PipelineError(f"Drive root row was not found in {csv_path}")

    free_bytes = max(total_bytes - used_bytes, 0)
    return SnapshotData(
        drive=drive,
        total_bytes=total_bytes,
        used_bytes=used_bytes,
        free_bytes=free_bytes,
        folders=folders,
    )


def select_top_paths(folders: list[FolderStat], top_n: int) -> list[FolderStat]:
    ranked = sorted(
        folders,
        key=lambda item: (-item.allocated_bytes, -item.size_bytes, item.path.lower()),
    )
    return ranked[:top_n]


def append_snapshot_to_history(
    output_root: Path,
    snapshot_ts: str,
    scan_date: str,
    drive_snapshot: SnapshotData,
    top_paths: list[FolderStat],
    source_csv: Path,
) -> None:
    drive_row = {
        "snapshot_ts": snapshot_ts,
        "scan_date": scan_date,
        "drive": drive_snapshot.drive,
        "total_bytes": str(drive_snapshot.total_bytes),
        "used_bytes": str(drive_snapshot.used_bytes),
        "free_bytes": str(drive_snapshot.free_bytes),
        "source_csv": str(source_csv),
    }
    append_csv_rows(output_root / "drive_history.csv", DRIVE_HISTORY_FIELDS, [drive_row])

    path_rows = []
    for rank, folder in enumerate(top_paths, start=1):
        path_rows.append(
            {
                "snapshot_ts": snapshot_ts,
                "scan_date": scan_date,
                "drive": folder.drive,
                "path": folder.path,
                "depth": str(folder.depth),
                "size_bytes": str(folder.size_bytes),
                "allocated_bytes": str(folder.allocated_bytes),
                "files": str(folder.files),
                "folders": str(folder.folders),
                "rank": str(rank),
                "source_csv": str(source_csv),
            }
        )
    append_csv_rows(output_root / "path_history.csv", PATH_HISTORY_FIELDS, path_rows)


def append_csv_rows(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    if not rows:
        return

    file_exists = csv_path.exists()
    with csv_path.open("a", encoding=CSV_ENCODING, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def generate_charts(output_root: Path, drive: str, window_days: int) -> list[Path]:
    drive_rows = [row for row in read_csv_rows(output_root / "drive_history.csv") if normalize_drive(row["drive"]) == normalize_drive(drive)]
    path_rows = [row for row in read_csv_rows(output_root / "path_history.csv") if normalize_drive(row["drive"]) == normalize_drive(drive)]
    if not drive_rows:
        return []

    drive_token = drive_token_from_drive(drive)
    daily_drive_rows = trim_rows_to_window(latest_rows_by_scan_date(drive_rows), window_days)
    scan_dates = [row["scan_date"] for row in daily_drive_rows]

    chart_paths = [
        plot_drive_usage_chart(output_root, drive_token, daily_drive_rows, window_days),
        plot_latest_hot_paths_chart(output_root, drive_token, path_rows),
        plot_hot_paths_trend_chart(output_root, drive_token, path_rows, scan_dates, window_days),
    ]
    return [path for path in chart_paths if path is not None]


def plot_drive_usage_chart(
    output_root: Path,
    drive_token: str,
    daily_drive_rows: list[dict[str, str]],
    window_days: int,
) -> Path | None:
    if not daily_drive_rows:
        return None

    plt = prepare_matplotlib()
    fig, ax = plt.subplots(figsize=(10, 5))
    dates = [datetime.strptime(row["scan_date"], DATE_FORMAT).date() for row in daily_drive_rows]
    used = [bytes_to_gib(int(row["used_bytes"])) for row in daily_drive_rows]
    free = [bytes_to_gib(int(row["free_bytes"])) for row in daily_drive_rows]

    ax.plot(dates, used, marker="o", linewidth=2, label="Used (GiB)")
    ax.plot(dates, free, marker="o", linewidth=2, label="Free (GiB)")
    ax.set_title(f"{drive_token.upper()}: Used vs Free ({window_days}d)")
    ax.set_ylabel("GiB")
    ax.set_xlabel("Date")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    output_path = output_root / f"{drive_token}_used_free_30d.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def plot_latest_hot_paths_chart(output_root: Path, drive_token: str, path_rows: list[dict[str, str]]) -> Path | None:
    latest_rows = latest_snapshot_rows(path_rows)
    if not latest_rows:
        return None

    top_rows = latest_rows[:10]
    labels = [short_path_label(row["path"]) for row in reversed(top_rows)]
    values = [bytes_to_gib(int(row["allocated_bytes"])) for row in reversed(top_rows)]

    plt = prepare_matplotlib()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, values, color="#28536b")
    ax.set_title(f"{drive_token.upper()}: Latest Hot Paths")
    ax.set_xlabel("Allocated GiB")
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()

    output_path = output_root / f"{drive_token}_hot_paths_latest.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def plot_hot_paths_trend_chart(
    output_root: Path,
    drive_token: str,
    path_rows: list[dict[str, str]],
    scan_dates: list[str],
    window_days: int,
) -> Path | None:
    latest_rows = latest_snapshot_rows(path_rows)
    if not latest_rows or not scan_dates:
        return None

    selected_paths = [row["path"] for row in latest_rows[:10]]
    series = build_hot_path_series(path_rows, scan_dates, selected_paths)
    plot_dates = [datetime.strptime(value, DATE_FORMAT).date() for value in scan_dates]

    plt = prepare_matplotlib()
    fig, ax = plt.subplots(figsize=(10, 6))
    for path in selected_paths:
        values = [bytes_to_gib(value) for value in series[path]]
        ax.plot(plot_dates, values, marker="o", linewidth=1.8, label=short_path_label(path))

    ax.set_title(f"{drive_token.upper()}: Top Hot Paths Trend ({window_days}d)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Allocated GiB")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8, loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()

    output_path = output_root / f"{drive_token}_hot_paths_30d.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def build_hot_path_series(
    path_rows: list[dict[str, str]],
    scan_dates: list[str],
    selected_paths: list[str],
) -> dict[str, list[int]]:
    daily_latest: dict[tuple[str, str], dict[str, str]] = {}
    for row in path_rows:
        key = (row["scan_date"], row["path"])
        current = daily_latest.get(key)
        if current is None or row["snapshot_ts"] > current["snapshot_ts"]:
            daily_latest[key] = row

    series: dict[str, list[int]] = {}
    for path in selected_paths:
        series[path] = [
            int(daily_latest[(scan_date, path)]["allocated_bytes"])
            if (scan_date, path) in daily_latest
            else 0
            for scan_date in scan_dates
        ]
    return series


def latest_snapshot_rows(path_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not path_rows:
        return []
    latest_snapshot = max(row["snapshot_ts"] for row in path_rows)
    rows = [row for row in path_rows if row["snapshot_ts"] == latest_snapshot]
    return sorted(rows, key=lambda row: int(row["rank"]))


def latest_rows_by_scan_date(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, dict[str, str]] = {}
    for row in rows:
        scan_date = row["scan_date"]
        current = grouped.get(scan_date)
        if current is None or row["snapshot_ts"] > current["snapshot_ts"]:
            grouped[scan_date] = row
    return [grouped[key] for key in sorted(grouped)]


def trim_rows_to_window(rows: list[dict[str, str]], window_days: int) -> list[dict[str, str]]:
    if not rows:
        return []
    latest_date = datetime.strptime(rows[-1]["scan_date"], DATE_FORMAT).date()
    window_start = latest_date - timedelta(days=window_days - 1)
    return [
        row
        for row in rows
        if datetime.strptime(row["scan_date"], DATE_FORMAT).date() >= window_start
    ]


def write_success_marker(
    success_marker: Path,
    snapshot_ts: str,
    scan_date: str,
    drive: str,
    source_csv: Path,
) -> None:
    payload = {
        "snapshot_ts": snapshot_ts,
        "scan_date": scan_date,
        "drive": normalize_drive(drive),
        "source_csv": str(source_csv),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }
    success_marker.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def success_marker_path(output_root: Path, drive_token: str, scan_date: str) -> Path:
    return output_root / "state" / f"{drive_token}_{scan_date}.success.json"


def raw_csv_path(output_root: Path, drive: str, timestamp: datetime) -> Path:
    token = drive_token_from_drive(drive)
    year_dir = timestamp.strftime("%Y")
    month_dir = timestamp.strftime("%m")
    file_name = f"wiztree_{token}_{timestamp.strftime(TIMESTAMP_FORMAT)}.csv"
    return output_root / "raw" / token / year_dir / month_dir / file_name


def drive_token_from_drive(drive: str) -> str:
    return normalize_drive(drive).rstrip(":").lower()


def normalize_drive(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().rstrip("\\")
    if len(text) >= 2 and text[1] == ":":
        return text[:2].upper()
    return text.upper()


def normalize_export_path(path: str, is_folder: bool) -> str:
    value = path.strip()
    if not value:
        return ""
    if is_folder and len(value) > 3 and value.endswith("\\"):
        return value[:-1]
    return value


def is_drive_root(path: str) -> bool:
    return len(path) == 3 and path[1:] == ':\\'


def path_depth(path: str) -> int:
    if is_drive_root(path):
        return 0
    relative = path[3:]
    if not relative:
        return 0
    return relative.count("\\") + 1


def parse_int(value: str | None) -> int:
    if value is None:
        return 0
    text = value.strip().rstrip(',')
    if not text:
        return 0
    return int(text)


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding=CSV_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def bytes_to_gib(value: int) -> float:
    return value / (1024 ** 3)


def short_path_label(path: str, max_length: int = 56) -> str:
    if len(path) <= max_length:
        return path
    return '...' + path[-(max_length - 3):]


def prepare_matplotlib():
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt

