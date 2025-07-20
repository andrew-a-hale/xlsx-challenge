# XLSX Challenge: Payroll Data Generator

## Requirements

- Python 3.12+
- Dependencies:
  - duckdb
  - openpyxl
  - typer
  - uv

## Description

Given the `payroll_data.ndjson` seed it will generate time-series data that is similar to payroll.

## Usage

```bash
uv run generate.py [--source payroll_data.ndjson] [--out data]
```

Generates XLSX files with randomized payroll data and compresses them to `data.zip`.
