# XLSX Challenge: Payroll Data Generation and Processing

## Overview

This project provides a set of Python scripts for generating and converting payroll data across multiple units and formats.

## Features

- Generate synthetic payroll data with randomized attributes
- Convert XLSX files to Parquet format
- Support for multiple organizational units (Brisbane, Sydney, Melbourne)
- Flexible data generation with randomized departments, employee types, and positions

## Requirements

- Python 3.12+
- Dependencies:
  - duckdb
  - openpyxl
  - typer
  - uv

## Usage

### Data Generation

```bash
uv run generate.py [--source payroll_data.ndjson] [--out xlsx]
```

Generates XLSX files with randomized payroll data in the specified output directory.

### XLSX to Parquet Conversion

```bash
uv run convert.py [--input-directory xlsx] [--verbose]
```

Converts XLSX files to a combined Parquet file.

### XLSX Processing

```bash
duckdb -c "$(cat process.sql)"
```

Converts XLSX files to a combined Parquet file.

## Project Structure

- `main.py`: Data generation script
- `convert.py`: XLSX to Parquet conversion script
- `payroll_data.ndjson`: Source data for generation
- `final.parquet`: Combined output after conversion

