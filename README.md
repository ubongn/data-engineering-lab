# Data Labs Workspace

This repository contains data engineering lab exercises, including an order data processing pipeline in `lab01`.

## Contents

- `lab01/` — order data ETL pipeline project
  - `data/` — source CSV files
  - `src/` — pipeline implementation modules
  - `tests/` — unit tests for the pipeline and transform logic
  - `output/` — example output files produced by the lab
  - `output_test/` — expected output files for lab validation
  - `README.md` — lab-specific documentation
- `lab02/` — API ingestion and parquet pipeline with Bronze/Silver/Gold layers

## Lab 01

The first lab builds a CSV-based order processing pipeline that:

- reads raw order records
- validates and cleans order data
- writes cleaned JSON output
- records invalid rows as errors
- generates a quality report

### Run Lab 01

From the `lab01` folder, use Python to run the pipeline:

```powershell
python -m src.pipeline --input data/*.csv --output output
```

Or process an entire input directory:

```powershell
python -m src.pipeline --input data --output output
```

### Run tests

From the `lab01` folder:

```powershell
python -m pytest tests -v
```

## Lab 02

The second lab builds an API ingestion pipeline that:

- extracts users, posts, and comments from a public API
- writes Bronze parquet files to `output/bronze`
- transforms the data into a Silver parquet file
- builds a Gold user summary parquet file
- supports `--skip-extract` to reuse existing Bronze files
- includes data quality checks and memory optimizations

### Run Lab 02

From the `lab02` folder:

```powershell
python -m src.pipeline.py
```

To rerun only transforms and gold generation with existing bronze data:

```powershell
python -m src.pipeline.py --skip-extract
```

### Run Lab 02 tests

From the `lab02` folder:

```powershell
python -m pytest tests -v
```

## Notes

This workspace is organized for data lab exercises. Use each lab's own `README.md` file for lab-specific details and expected outputs.
