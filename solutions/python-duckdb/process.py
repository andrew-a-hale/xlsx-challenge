#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "duckdb>=1.3.0",
# ]
# requires-python = ">=3.12"
# ///

import duckdb

conn = duckdb.connect(":memory:")
sql = open("process.sql").read()
print(len(conn.execute(sql).fetchall()))
