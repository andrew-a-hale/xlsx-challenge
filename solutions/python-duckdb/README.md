# Python DuckDB Solution

## Requirements

- Python 3.12+
- DuckDB

## Usage

### Processing Payroll Data

```bash
# Convert XLSX to Parquet
uv run convert.py

# Run SQL processing
duckdb -c "$(cat process.sql)"
```

## Files

- `data/`: Source XLSX files
- `convert.py`: Converts XLSX files to Parquet format
- `process.sql`: SQL queries for data processing
- `final.parquet`: Processed payroll data in Parquet format
