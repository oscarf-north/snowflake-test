# sync_engine/source_adapter.py

import pandas as pd
from pathlib import Path

from .utils import normalize
from .config import DATA_FOLDER, ALLOWED_SCHEMAS


REQUIRED_COLUMNS = {"OBJECT_TYPE", "SCHEMA_NAME", "OBJECT_NAME", "DDL"}


def _load_from_csv_folder(data_folder: Path) -> pd.DataFrame:
    """
    Load all CSV files in `data_folder`, validate the schema,
    and return a single concatenated DataFrame.

    Each CSV must contain the columns:
        object_type, schema_name, object_name, ddl
    """
    csv_files = list(data_folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_folder}")

    dfs = []
    for file in csv_files:
        df = pd.read_csv(file)

        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"File {file} is missing required columns: {missing}")

        df["OBJECT_TYPE"] = df["OBJECT_TYPE"].map(normalize)
        df["SCHEMA_NAME"] = df["SCHEMA_NAME"].map(normalize)
        df["OBJECT_NAME"] = df["OBJECT_NAME"].map(normalize)

        dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)

    if ALLOWED_SCHEMAS:
        df_all = df_all[df_all["SCHEMA_NAME"].isin(ALLOWED_SCHEMAS)]

    return df_all


def load_source(source: str = "csv"):
    """
    Main entry point for the synchronization engine.

    Parameters
    ----------
    source : str
        - "csv"       → read from scripts/data/*.csv  (current dummy mode)
        - "snowflake" → (placeholder) will read from Snowflake tables

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns: object_type, schema_name, object_name, ddl
    """
    if source == "csv":
        return _load_from_csv_folder(DATA_FOLDER)

    if source == "snowflake":
        # Placeholder for future Snowflake implementation.
        # Idea:
        #   - Run queries against metadata tables in Snowflake
        #   - Build a DataFrame with the same columns and return it.
        raise NotImplementedError("Snowflake source is not implemented yet.")

    raise ValueError(f"Unknown source type: {source}")
