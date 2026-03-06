import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from wiztree_auto.pipeline import (
    AppConfig,
    build_hot_path_series,
    parse_wiztree_csv,
    read_csv_rows,
    run_pipeline,
    select_top_paths,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def make_config(output_root: Path) -> AppConfig:
    return AppConfig(
        scan_targets=["C:"],
        schedule_time="12:00",
        catch_up=True,
        run_level="highest",
        top_path_mode="deep_hot_paths",
        top_n=20,
        chart_window_days=30,
        output_root=output_root,
        task_name="WizTree-Daily-C",
        wiztree_path=Path(r"C:\Program Files\WizTree\WizTree64.exe"),
    )


class PipelineTests(unittest.TestCase):
    def test_parse_wiztree_csv_extracts_drive_totals_and_folders(self) -> None:
        snapshot = parse_wiztree_csv(FIXTURES / "sample_wiztree_day2.csv", expected_drive="C:")

        self.assertEqual(snapshot.drive, "C:")
        self.assertEqual(snapshot.total_bytes, 2200)
        self.assertEqual(snapshot.used_bytes, 1500)
        self.assertEqual(snapshot.free_bytes, 700)

        top_paths = select_top_paths(snapshot.folders, top_n=4)
        self.assertEqual(
            [item.path for item in top_paths],
            [
                r"C:\Users\Alice\Downloads",
                r"C:\Users",
                r"C:\Program Files",
                r"C:\Users\Alice",
            ],
        )
        self.assertEqual(top_paths[0].depth, 3)
        self.assertEqual(top_paths[-1].allocated_bytes, 400)

    def test_run_pipeline_writes_history_and_charts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "data"
            config = make_config(output_root)

            def export_runner(_wiztree_path: Path, _drive: str, output_csv: Path) -> None:
                output_csv.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(FIXTURES / "sample_wiztree_day2.csv", output_csv)

            result = run_pipeline(
                config,
                force=False,
                now=datetime(2026, 3, 6, 12, 0, 0),
                export_runner=export_runner,
            )

            self.assertEqual(result.status, "created")
            self.assertEqual(result.drive_rows_written, 1)
            self.assertEqual(result.path_rows_written, 7)

            drive_rows = read_csv_rows(output_root / "drive_history.csv")
            self.assertEqual(drive_rows[0]["used_bytes"], "1500")
            self.assertEqual(drive_rows[0]["free_bytes"], "700")

            path_rows = read_csv_rows(output_root / "path_history.csv")
            self.assertEqual(path_rows[0]["path"], r"C:\Users\Alice\Downloads")
            self.assertEqual(path_rows[0]["rank"], "1")
            self.assertEqual(path_rows[-1]["path"], r"C:\Empty Folder")

            for chart_path in (
                output_root / "c_used_free_30d.png",
                output_root / "c_hot_paths_latest.png",
                output_root / "c_hot_paths_30d.png",
            ):
                self.assertTrue(chart_path.exists(), chart_path)
                self.assertGreater(chart_path.stat().st_size, 0, chart_path)

            success_marker = output_root / "state" / "c_2026-03-06.success.json"
            self.assertTrue(success_marker.exists())

    def test_second_run_same_day_skips_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "data"
            config = make_config(output_root)
            calls = {"count": 0}

            def export_runner(_wiztree_path: Path, _drive: str, output_csv: Path) -> None:
                calls["count"] += 1
                output_csv.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(FIXTURES / "sample_wiztree_day2.csv", output_csv)

            first_result = run_pipeline(
                config,
                force=False,
                now=datetime(2026, 3, 6, 12, 0, 0),
                export_runner=export_runner,
            )
            second_result = run_pipeline(
                config,
                force=False,
                now=datetime(2026, 3, 6, 13, 0, 0),
                export_runner=export_runner,
            )

            self.assertEqual(first_result.status, "created")
            self.assertEqual(second_result.status, "skipped")
            self.assertEqual(calls["count"], 1)
            self.assertEqual(len(read_csv_rows(output_root / "drive_history.csv")), 1)

    def test_hot_path_series_fills_missing_days_with_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "data"
            config = make_config(output_root)

            def export_day1(_wiztree_path: Path, _drive: str, output_csv: Path) -> None:
                output_csv.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(FIXTURES / "sample_wiztree_day1.csv", output_csv)

            def export_day2(_wiztree_path: Path, _drive: str, output_csv: Path) -> None:
                output_csv.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(FIXTURES / "sample_wiztree_day2.csv", output_csv)

            run_pipeline(
                config,
                force=False,
                now=datetime(2026, 3, 5, 12, 0, 0),
                export_runner=export_day1,
            )
            run_pipeline(
                config,
                force=False,
                now=datetime(2026, 3, 6, 12, 0, 0),
                export_runner=export_day2,
            )

            path_rows = read_csv_rows(output_root / "path_history.csv")
            drive_rows = read_csv_rows(output_root / "drive_history.csv")
            daily_dates = [row["scan_date"] for row in drive_rows]
            latest_paths = [row["path"] for row in path_rows if row["snapshot_ts"] == "20260306_120000"][:10]

            series = build_hot_path_series(path_rows, daily_dates, latest_paths)

            self.assertEqual(series[r"C:\Users\Alice\Downloads"], [0, 900])
            self.assertEqual(series[r"C:\Program Files"], [500, 520])


if __name__ == "__main__":
    unittest.main()
