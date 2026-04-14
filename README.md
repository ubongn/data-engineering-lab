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
- `lab02/` — additional lab work (not documented here yet)

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
py -3 -m src.pipeline --input data/*.csv --output output
```

Or process an entire input directory:

```powershell
py -3 -m src.pipeline --input data --output output
```

### Run tests

From the `lab01` folder:

```powershell
py -3 -m pytest tests -v
```

## Notes

This workspace is organized for data lab exercises. Use the `lab01/README.md` file for more specific details on the order pipeline implementation and file outputs.
