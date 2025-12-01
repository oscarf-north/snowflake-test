# sync_engine/config.py

from pathlib import Path

# Root of the project (one level above this file's folder)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Base path for the db folder where .sql files will be written
DB_PATH = ROOT_DIR / "db"

# Base folder where source CSV inputs live
DATA_PATH = ROOT_DIR / "src" / "data"

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

# Schemas to include in the sync, organized by database
ALLOWED_SCHEMAS_BY_DB = {
    "PRD_HOSPENG_REPORTING": ["AICORTEX", "DATAADMIN", "DATAWAREHOUSE", "GITHUB"],
    # Add other databases and their schemas here, for example:
    # "DEV_DB": ["DEV_SCHEMA_1", "DEV_SCHEMA_2"],
    
    # A default list for any database not explicitly listed.
    # An empty list means no schema filtering will be applied by default.
    "default": []
}
