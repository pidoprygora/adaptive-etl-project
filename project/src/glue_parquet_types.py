"""Align local Parquet physical types with Glue/Athena DDL (Spark reads via Glue schema)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from generate_athena_ddl import TABLE_SCHEMAS

GLUE_TO_PANDAS_DTYPE: dict[str, str] = {
    "BIGINT": "int64",
    "INT": "int32",
    "INTEGER": "int32",
    "DOUBLE": "float64",
    "BOOLEAN": "bool",
    "STRING": "string",
}

GLUE_TO_ARROW_TYPE: dict[str, pa.DataType] = {
    "BIGINT": pa.int64(),
    "INT": pa.int32(),
    "INTEGER": pa.int32(),
    "DOUBLE": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
}


def coerce_dataframe_to_glue_schema(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Align DataFrame to Glue DDL: add missing columns, cast types, preserve column order."""
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        return df
    out = df.copy()
    for column, glue_type in schema:
        pandas_dtype = GLUE_TO_PANDAS_DTYPE.get(glue_type.upper())
        if column not in out.columns:
            if pandas_dtype == "string":
                out[column] = pd.Series([None] * len(out), dtype="string")
            elif pandas_dtype:
                out[column] = pd.Series([pd.NA] * len(out), dtype=pandas_dtype)
            else:
                out[column] = pd.NA
            continue
        if pandas_dtype:
            out[column] = out[column].astype(pandas_dtype)
    ordered = [column for column, _ in schema]
    return out[ordered]


def normalize_parquet_glue_types(table_name: str, parquet_path: Path) -> bool:
    """Rewrite Parquet ints/timestamps so Spark + Glue catalog agree on physical types."""
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        return False
    table = pq.read_table(parquet_path)
    glue_by_column = dict(schema)
    new_columns: list[pa.Array] = []
    new_fields: list[pa.Field] = []
    changed = False
    for field in table.schema:
        column = table.column(field.name)
        glue_type = glue_by_column.get(field.name)
        if glue_type:
            expected = GLUE_TO_ARROW_TYPE.get(glue_type.upper())
            if expected is not None and field.type != expected:
                changed = True
                column = pc.cast(column, expected)
                new_fields.append(pa.field(field.name, expected))
            else:
                new_fields.append(field)
        else:
            new_fields.append(field)
        new_columns.append(column)
    if not changed:
        return False
    pq.write_table(
        pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields)),
        parquet_path,
        compression="snappy",
    )
    return True
