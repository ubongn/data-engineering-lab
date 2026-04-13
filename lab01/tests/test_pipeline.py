from src.pipeline import QualityReport, resolve_input_paths


def test_quality_report_counts_errors():
    report = QualityReport(total_records=4, valid_records=2)
    report.add_error_record(["Missing order_id", "Invalid date"])
    report.add_error_record(["Missing order_id"])

    assert report.error_records == 2
    assert report.error_counts == {
        "Missing order_id": 2,
        "Invalid date": 1,
    }
    assert report.error_rate == 50.0


def test_resolve_input_paths_handles_directory(tmp_path):
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    first.write_text("order_id,customer_name,amount,order_date,status\n")
    second.write_text("order_id,customer_name,amount,order_date,status\n")

    paths = resolve_input_paths(str(tmp_path))
    assert paths == sorted([first, second])
