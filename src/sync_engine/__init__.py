# sync_engine/__init__.py

"""
Sync engine package.

This package contains all the core logic for:
- Loading object metadata (DDL) from a source (CSV today, Snowflake tomorrow).
- Deciding if an object has changed (diff detection).
- Writing .sql files into the db/ folder following the agreed structure.
- Committing changes to Git.

The main script `scripts/sync.py` orchestrates these pieces.
"""
