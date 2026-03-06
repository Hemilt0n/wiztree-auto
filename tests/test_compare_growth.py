import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

from wiztree_auto.compare_growth import (
    build_growth_report,
    choose_snapshot_pair,
    compare_snapshot_files,
    snapshot_from_path,
    write_growth_reports,
)
from wiztree_auto.pipeline import parse_wiztree_csv


FIXTURES = Path(__file__).resolve().parent / "fixtures"


class CompareGrowthTests(unittest.TestCase):
    def test_build_growth_report_marks_new_folder_and_sorts_tree(self) -> None:
        baseline = parse_wiztree_csv(FIXTURES / "sample_wiztree_day1.csv", expected_drive="C:")
        latest = parse_wiztree_csv(FIXTURES / "sample_wiztree_day2.csv", expected_drive="C:")

        report = build_growth_report(baseline, latest)

        self.assertEqual(report.root_growth_allocated, 300)
        self.assertEqual(report.direct_growth_entries[0].path, r"C:\Users\Alice\Downloads")
        self.assertTrue(report.direct_growth_entries[0].is_new)
        self.assertEqual(report.direct_growth_entries[0].growth_allocated, 900)
        self.assertEqual(report.root_node.children[0].path, r"C:\Users")
        self.assertEqual(report.root_node.children[1].path, r"C:\Program Files")

    def test_choose_snapshot_pair_uses_since_date_and_latest_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "data"
            raw_dir = output_root / "raw" / "c" / "2026" / "03"
            raw_dir.mkdir(parents=True)
            baseline_csv = raw_dir / "wiztree_c_20260305_120000.csv"
            latest_csv = raw_dir / "wiztree_c_20260306_120000.csv"
            shutil.copyfile(FIXTURES / "sample_wiztree_day1.csv", baseline_csv)
            shutil.copyfile(FIXTURES / "sample_wiztree_day2.csv", latest_csv)

            selection = choose_snapshot_pair(
                output_root=output_root,
                drive="C:",
                requested_since=date(2026, 3, 5),
            )

            self.assertEqual(selection.baseline.path, baseline_csv.resolve())
            self.assertEqual(selection.latest.path, latest_csv.resolve())

    def test_write_growth_reports_outputs_html_and_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            baseline_csv = temp_path / "wiztree_c_20260305_120000.csv"
            latest_csv = temp_path / "wiztree_c_20260306_120000.csv"
            html_path = temp_path / "report.html"
            csv_path = temp_path / "report.csv"
            shutil.copyfile(FIXTURES / "sample_wiztree_day1.csv", baseline_csv)
            shutil.copyfile(FIXTURES / "sample_wiztree_day2.csv", latest_csv)

            report = compare_snapshot_files(baseline_csv, latest_csv, "C:")
            selection = type("Selection", (), {
                "drive": "C:",
                "baseline": snapshot_from_path(baseline_csv),
                "latest": snapshot_from_path(latest_csv),
                "requested_since": None,
                "requested_until": None,
            })()
            generated = write_growth_reports(
                report=report,
                selection=selection,
                output_root=temp_path,
                explicit_html=html_path,
                explicit_csv=csv_path,
            )

            self.assertEqual(generated.html_path, html_path)
            self.assertEqual(generated.csv_path, csv_path)
            self.assertTrue(html_path.exists())
            self.assertTrue(csv_path.exists())

            html_text = html_path.read_text(encoding="utf-8")
            csv_text = csv_path.read_text(encoding="utf-8-sig")

            self.assertIn("C:\\Users\\Alice\\Downloads", html_text)
            self.assertIn("NEW", html_text)
            self.assertLess(html_text.index("C:\\Users"), html_text.index("C:\\Program Files"))
            self.assertIn("C:\\Users\\Alice\\Downloads", csv_text)
            self.assertIn("growth_allocated_bytes", csv_text)


if __name__ == "__main__":
    unittest.main()
