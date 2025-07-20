# Python DuckDB Solution

## Requirements

- DuckDB cli
- Python 3.12+
- uv
- duckdb (python library)
- openpyxl
- typer

## Usage

### Processing Payroll Data

```bash
# Convert XLSX to Parquet
uv run convert.py

# Run SQL processing
duckdb -c "$(cat process.sql)"
```

## Files

- `convert.py`: Converts XLSX files to Parquet format
- `process.sql`: SQL queries for data processing
- `final.parquet`: Processed payroll data in Parquet format
