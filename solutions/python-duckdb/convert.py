#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "duckdb>=1.3.0",
#   "openpyxl>=3.0.0",
#   "typer>=0.9.0",
# ]
# requires-python = ">=3.12"
# ///

import logging
import os
import shutil
import sys
import tempfile

import duckdb
import openpyxl
import typer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__file__)


def convert_xlsx_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    input_directory: str,
    output_directory: str,
) -> None:
    if not os.path.isdir(input_directory):
        logger.error(f"Error: {input_directory} is not a valid directory")
        sys.exit(1)

    raw = os.path.join(output_directory, "raw")
    os.makedirs(raw, exist_ok=True)

    for filename in os.listdir(input_directory):
        if not filename.endswith(".xlsx"):
            continue

        filepath = os.path.join(input_directory, filename)
        logger.info(f"Processing: {filepath}")

        try:
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            for sheet_name in wb.sheetnames:
                dim = wb[sheet_name].calculate_dimension()
                parquet_filename = (
                    f"{os.path.splitext(filename)[0]}_{sheet_name}.parquet"
                )
                parquet_path = os.path.join(output_directory, "raw", parquet_filename)

                conn.execute(
                    f"""\
COPY (
    SELECT '{filepath}' as filepath, '{sheet_name}' as sheet, row_number() over() as row, *
    FROM read_xlsx(
        '{filepath}',
        sheet = '{sheet_name}',
        header = false,
        stop_at_empty = false,
        range = '{dim}',
        all_varchar = true
    )
) TO '{parquet_path}' (FORMAT 'parquet')"""
                )

                logger.info(f"Converted sheet '{sheet_name}' to {parquet_filename}")

        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")


def write_combined_output(
    conn: duckdb.DuckDBPyConnection,
    output_directory: str,
) -> None:
    outfile = "final.parquet"
    if os.path.exists(outfile):
        os.remove(outfile)

    if output_directory.endswith("/"):
        output_directory = output_directory[:-1]

    conn.execute(
        f"""\
copy (
    select * from read_parquet('{output_directory}/**/*.parquet', union_by_name = true)
) to '{outfile}' (FORMAT 'parquet')"""
    )

    shutil.rmtree(output_directory)


def main(
    input_directory: str = typer.Option(
        "data", "-i", "--input-directory", help="Path to input XLSX files"
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Increase logging verbosity"
    ),
) -> None:
    logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)

    conn = duckdb.connect(":memory:")
    conn.install_extension("excel")

    tmpdir = tempfile.mkdtemp()
    convert_xlsx_to_parquet(conn, input_directory, tmpdir)
    write_combined_output(conn, tmpdir)


if __name__ == "__main__":
    typer.run(main)
