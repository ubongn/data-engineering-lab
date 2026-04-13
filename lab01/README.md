# Lab 01: Order Data Processing Pipeline

## Overview

This lab implements an order data ETL pipeline in `labs/lab01`.

The pipeline reads CSV order data, validates and enriches records, and writes:

- cleaned order data
- raw error records
- a JSON quality report

## What changed

The lab now includes:

- CLI arguments via `argparse`
- `--input` and `--output` options
- glob support for multiple CSV files
- directory input support
- structured `logging` instead of `print()`
- `QualityReport` dataclass for error tracking

## Project structure

- `src/`
  - `extract.py` — reads CSV files as dictionaries
  - `transform.py` — validates, cleans, and enriches orders
  - `pipeline.py` — CLI pipeline runner
  - `__init__.py` — package initializer
- `data/` — source CSV input files
- `tests/`
  - `test_transform.py`
  - `test_pipeline.py`
- `output/` — default pipeline outputs
- `README.md` — this file

## Running the pipeline

From `labs/lab01`:

```powershell
cd c:\Users\Ubong\Desktop\data\class-work\labs\lab01
py -3 -m src.pipeline --input data/*.csv --output output
```

If you want to process a whole directory:

```powershell
py -3 -m src.pipeline --input data --output output
```

## Output files

The pipeline generates:

- `order_clean.json` — cleaned valid orders
- `order_errors.json` — records rejected with issue details
- `quality_report.json` — aggregated counts and error breakdown

## Tests

Run tests from the lab root:

```powershell
py -3 -m pytest tests -v
```

## Notes

This version is designed for easy reuse and debugging, with configurable input, multiple-file support, and a summary report for data quality.
