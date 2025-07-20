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
import zipfile

import duckdb
import openpyxl
import typer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__file__)

PATH = os.path.dirname(__file__)
ROOT = os.path.dirname(os.path.dirname(PATH))


def unzip_xlsx(zip_path: str, output_dir: str):
    try:
        if not os.path.exists(zip_path):
            logger.error(f"Zip file not found: {zip_path}")
            raise ValueError("Zip file not found")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            xlsx_files = [f for f in zip_ref.namelist() if f.endswith(".xlsx")]

            if not xlsx_files:
                logger.warning(f"No Excel files found in {zip_path}")
                return

            for xlsx_file in xlsx_files:
                zip_ref.extract(xlsx_file, output_dir)
                logger.info(f"Extracted: {xlsx_file}")

    except Exception as e:
        raise e


def convert_xlsx_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    input_directory: str,
    output_directory: str,
) -> None:
    if not os.path.isdir(input_directory):
        logger.error(f"Error: {input_directory} is not a valid directory")
        raise ValueError("Invalid input directory")

    for root, _, files in os.walk(input_directory):
        for filename in files:
            if not filename.endswith(".xlsx"):
                continue

            filepath = os.path.join(root, filename)
            logger.info(f"Processing: {filepath}")

            try:
                wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
                for sheet_name in wb.sheetnames:
                    dim = wb[sheet_name].calculate_dimension()
                    parquet_filename = (
                        f"{os.path.splitext(filename)[0]}_{sheet_name}.parquet"
                    )
                    parquet_path = os.path.join(output_directory, parquet_filename)

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
    outfile = os.path.join(PATH, "final.parquet")
    if os.path.exists(outfile):
        os.remove(outfile)

    if output_directory.endswith("/"):
        output_directory = output_directory[:-1]

    conn.execute(
        f"""\
copy (
    select * from read_parquet('{output_directory}/*.parquet', union_by_name = true)
) to '{outfile}' (FORMAT 'parquet')"""
    )


def main(
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Increase logging verbosity"
    ),
) -> None:
    logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)

    xlsx_tmpdir = tempfile.mkdtemp()
    unzip_xlsx(os.path.join(ROOT, "data.zip"), xlsx_tmpdir)

    out_tmpdir = tempfile.mkdtemp()
    conn = duckdb.connect(":memory:")
    conn.install_extension("excel")
    convert_xlsx_to_parquet(conn, xlsx_tmpdir, out_tmpdir)
    write_combined_output(conn, out_tmpdir)


if __name__ == "__main__":
    typer.run(main)
