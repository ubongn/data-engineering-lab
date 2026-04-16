# Lab 02 — API Ingestion Pipeline

This lab builds a small data engineering pipeline that:

- extracts user, post, and comment data from an API
- writes Bronze parquet files to `output/bronze`
- transforms bronze data into a Silver parquet file with post enrichment
- builds a Gold summary file with per-user aggregates
- supports a `--skip-extract` flag to reuse existing Bronze files
- includes data quality checks and pandas memory optimization

## Project structure

- `src/`
  - `pipeline.py` — main ETL runner and CLI
  - `extract.py` — fetches API data with retry handling
  - `load.py` — writes parquet files to disk
  - `transform.py` — builds Silver and Gold outputs
- `tests/`
  - `test_transform.py` — pytest coverage for transform logic
- `output/`
  - `bronze/` — raw parquet files
  - `silver/` — enriched parquet output
  - `gold/` — aggregated user summary parquet

## Requirements

Install Python dependencies before running the project.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install requests pandas pyarrow pytest
```

If you already have a Python environment, install the required packages directly:

```powershell
pip install requests pandas pyarrow pytest
```

## Run the pipeline

From `lab02`:

```powershell
python -m src.pipeline.py
```

or with the current environment activated:

```powershell

.\.venv\Scripts\Activate.ps1
python -m src.pipeline.py
```

## Skip extraction

If you already have existing Bronze parquet files in `output/bronze`, rerun transform and gold generation only:

```powershell
python -m src.pipeline.py --skip-extract
```

## Expected outputs

- `output/bronze/users.parquet`
- `output/bronze/posts.parquet`
- `output/bronze/comments.parquet`
- `output/silver/enriched_posts.parquet`
- `output/gold/user_summary.parquet`

## What the pipeline does

1. Extracts users, posts, and comments from `https://jsonplaceholder.typicode.com`
2. Writes the raw extracted records to Bronze parquet files
3. Joins posts with user details and comment counts
4. Computes word counts and per-user rank for each post
5. Applies data quality checks:
   - no null values in required fields
   - `comment_count >= 0`
6. Writes Silver output and then Gold user summary

## Run tests

From `lab02`:

```powershell
python -m pytest tests -v
```

## Notes

- The Gold layer aggregates by `user_name`.
- The Silver layer stores `user_name` and `company_name` as category types for memory efficiency.
- If the `--skip-extract` flag is used and Bronze files are missing, the pipeline will raise an error.
