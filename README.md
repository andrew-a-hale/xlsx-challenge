# XLSX Challenge: Payroll Data Processing

## Requirements

- Python 3.12+
- Dependencies:
  - duckdb
  - openpyxl
  - typer
  - uv

## Project Structure

- `generator/`: Contains scripts for generating payroll data
  - `generate.py`: Data generation script
  - `payroll_data.ndjson`: Source seed data for generation
- `solutions/`: Contains solution implementations for processing the generated data
  - Each solution is in its own subdirectory
- `data.zip`: Collection of XLSX files made by the generator for solutions to process

## Usage

### Data Generation

```bash
uv run generate.py [--source payroll_data.ndjson] [--out data]
```

Generates XLSX files with randomized payroll data and compresses them to `data.zip`.

### Solutions

Solutions for processing the generated payroll data are located in the
`solutions/` directory. Refer to individual solution README files for specific
usage instructions.
