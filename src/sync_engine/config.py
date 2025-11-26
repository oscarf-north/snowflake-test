# sync_engine/config.py

from pathlib import Path

# Root of the project (one level above this file's folder)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Name of the Snowflake DB (and corresponding folder under db/)
DB_NAME = "PRD_HOSPENG_REPORTING"

# Base path where generated .sql files will live
DB_BASE_PATH = ROOT_DIR / "db" / DB_NAME

# Folder where dummy CSV inputs live (tables.csv, views.csv, ...)
DATA_FOLDER = ROOT_DIR / "src" / "data"

# How object_type values map to subfolders in each schema
#   TABLE     → tables/
#   VIEW      → views/
#   PROCEDURE → procedures/
OBJECT_TYPE_MAP = {
    "TABLE": "tables",
    "VIEW": "views",
    "PROCEDURE": "procedures",
}

# Where git commands should run (repo root)
GIT_ROOT = ROOT_DIR

# Schemas to include in the sync
ALLOWED_SCHEMAS = ["AICORTEX", "DATAADMIN", "DATAWAREHOUSE", "GITHUB"]
