# sync_engine/source_adapter.py

import pandas as pd
from pathlib import Path

from .utils import normalize
from .config import DATA_PATH, ALLOWED_SCHEMAS_BY_DB


REQUIRED_COLUMNS = {"OBJECT_TYPE", "SCHEMA_NAME", "OBJECT_NAME", "DDL"}


def _load_from_csv_folder(data_folder: Path, db_name: str) -> pd.DataFrame:
    """
    Load all CSV files in `data_folder`, validate the schema,
    and return a single concatenated DataFrame.

    Each CSV must contain the columns:
        object_type, schema_name, object_name, ddl
    """
    if not data_folder.exists():
        raise FileNotFoundError(f"Source data folder not found: {data_folder}")

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

    # Get the list of allowed schemas for the given database.
    # Fallback to the default list if the db_name is not found.
    allowed_schemas = ALLOWED_SCHEMAS_BY_DB.get(db_name, ALLOWED_SCHEMAS_BY_DB.get("default", []))

    if allowed_schemas:
        print(f"Filtering for schemas: {allowed_schemas}")
        df_all = df_all[df_all["SCHEMA_NAME"].isin(allowed_schemas)]

    return df_all


def load_source(source: str, db_name: str):
    """
    Main entry point for the synchronization engine.

    Parameters
    ----------
    source : str
        - "csv"       → read from src/data/{db_name}/*.csv
        - "snowflake" → (placeholder) will read from Snowflake tables
    db_name : str
        The name of the database to sync.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns: object_type, schema_name, object_name, ddl
    """
    if source == "csv":
        db_data_path = DATA_PATH / db_name
        return _load_from_csv_folder(db_data_path, db_name=db_name)
    
    if source == "snowflake":
        # You'll do the same for the snowflake source when it's implemented
        # a_df = snowflake_adapter.get_ddls(db_name)
        # allowed_schemas = ...
        # return a_df[a_df["SCHEMA_NAME"].isin(allowed_schemas)]
        raise NotImplementedError(f"Snowflake source is not implemented yet for database '{db_name}'.")

    raise ValueError(f"Unknown source type: {source}")
