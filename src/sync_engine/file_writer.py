# sync_engine/file_writer.py

from pathlib import Path
from typing import Optional

import pandas as pd

from .config import DB_BASE_PATH, OBJECT_TYPE_MAP
from .utils import normalize


def get_file_path(row: pd.Series) -> Optional[Path]:
    """
    From a DataFrame row with (object_type, schema_name, object_name, ddl),
    compute the output file path:

        db/PRD_HOSPEND_REPORTING/<SCHEMA>/<type_folder>/<OBJECT_NAME>.sql

    Returns None if the object_type is not supported/mapped.
    """
    obj_type = normalize(row["OBJECT_TYPE"])
    schema = normalize(row["SCHEMA_NAME"])
    obj_name = normalize(row["OBJECT_NAME"])
 
    type_folder = OBJECT_TYPE_MAP.get(obj_type)
    if not type_folder:
        # Unknown / unsupported object type
        return None

    # Build the schema folder
    target_dir = DB_BASE_PATH / schema / type_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    return target_dir / f"{obj_name}.sql"


def write_file(file_path: Path, content: str) -> None:
    """
    Write the given DDL content to file_path.
    Overwrites the file if it already exists.
    """
    file_path.write_text(content, encoding="utf-8")
