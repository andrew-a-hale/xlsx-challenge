# Python DuckDB Solution

## Requirements

- Python 3.12+
- Dependencies:
  - duckdb
  - openpyxl
  - typer
  - uv

## Usage

### Processing Payroll Data

```bash
# Convert XLSX to Parquet
uv run convert.py

# Run SQL processing
uv run process.py
```

## Files

- `convert.py`: Converts XLSX files to Parquet format
- `process.sql`: SQL queries for data processing
- `final.parquet`: Processed payroll data in Parquet format
