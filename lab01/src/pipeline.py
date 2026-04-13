import argparse
import glob
import json
import logging
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path

from src.extract import read_csv
from src.transform import transform_orders

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    total_records: int = 0
    valid_records: int = 0
    error_records: int = 0
    error_counts: dict[str, int] = field(default_factory=dict)

    def add_error_record(self, issues: list[str]) -> None:
        self.error_records += 1
        for issue in issues:
            self.error_counts[issue] = self.error_counts.get(issue, 0) + 1

    @property
    def error_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.error_records / self.total_records * 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the order processing pipeline.")
    parser.add_argument(
        "--input",
        default="data/*.csv",
        help="Input file or glob pattern for CSV files (default: data/*.csv)",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory for JSON reports",
    )
    return parser.parse_args()


def resolve_input_paths(pattern: str) -> list[Path]:
    input_path = Path(pattern)
    if input_path.is_dir():
        pattern = str(input_path / "*.csv")
    paths = [Path(path) for path in glob.glob(pattern, recursive=True)]
    return sorted(paths)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    input_paths = resolve_input_paths(args.input)
    if not input_paths:
        logger.error("No input files found for pattern %s", args.input)
        raise SystemExit(1)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Processing %d input file(s)", len(input_paths))
    for path in input_paths:
        logger.info("  - %s", path)

    records = chain.from_iterable(read_csv(path) for path in input_paths)
    valid, errors = transform_orders(records)

    quality_report = QualityReport(
        total_records=len(valid) + len(errors),
        valid_records=len(valid),
    )
    for error in errors:
        quality_report.add_error_record(error.get("issues", []))

    with open(output_dir / "order_clean.json", "w", encoding="utf-8") as f:
        json.dump(valid, f, indent=2)

    with open(output_dir / "order_errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, indent=2, default=str)

    with open(output_dir / "quality_report.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "total_records": quality_report.total_records,
                "valid_records": quality_report.valid_records,
                "error_records": quality_report.error_records,
                "error_rate": round(quality_report.error_rate, 1),
                "error_counts": quality_report.error_counts,
            },
            f,
            indent=2,
        )

    logger.info("Pipeline complete")
    logger.info("Total records: %d", quality_report.total_records)
    logger.info("Valid records: %d", quality_report.valid_records)
    logger.info("Error records: %d", quality_report.error_records)
    logger.info("Error rate: %.1f%%", quality_report.error_rate)
    for error_type, count in quality_report.error_counts.items():
        logger.info("  %s: %d", error_type, count)


if __name__ == "__main__":
    main()
