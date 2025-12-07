# src/sync.py

"""
Main entry point for the Snowflake → Git sync (currently CSV → Git).

For now, it:
- Reads all CSVs in scripts/data/ as the source of truth.
- For each row (object), computes the target path in db/PRD_HOSPEND_REPORTING/.
- Only writes files whose DDL content actually changed.
- Optionally commits and pushes the changes to Git.
"""

import argparse
from datetime import datetime

from sync_engine.diff_engine import has_changed
from sync_engine.file_writer import get_file_path, write_file
from sync_engine.git_handler import (
    stage_all_changes,
    commit_changes,
    push_changes,
)
from sync_engine.source_adapter import load_source


def main():
    parser = argparse.ArgumentParser(description="Sync DDLs to Git repository.")
    parser.add_argument(
        "--source",
        required=True,
        choices=["csv", "snowflake"],
        help="Mandatory: Where to read DDLs from (e.g., 'csv' or 'snowflake').",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Mandatory: The name of the database to sync.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="If set, commit and push changes when DDL updates are detected.",
    )

    args = parser.parse_args()

    # 1. Load all objects (tables, views, procedures...) from the chosen source.
    df = load_source(source=args.source, db_name=args.database)

    # 2. Iterate over each object and sync it to the file system.
    changes = 0

    for _, row in df.iterrows():
        file_path = get_file_path(row, db_name=args.database)
        if file_path is None:
            print(f"Skipping unsupported object type: {row['object_type']}")
            continue

        ddl = row["DDL"]

        if has_changed(file_path, ddl):
            print(f"Updating: {file_path}")
            write_file(file_path, ddl)
            changes += 1
        else:
            print(f"No change: {file_path}")

    # 3. Report changes
    if changes > 0:
        print(f"\nTotal objects updated: {changes}")
    else:
        print("\nNo Changes detected by sync.")

    # 4. Optionally commit & push (only if there were changes)
    if not args.commit:
        print("Commit flag not set (--commit); no git commit/push performed.")
        return

    # At this point: --commit is set AND we have changes on disk.
    # Stage only the DDL output (handled inside stage_all_changes).
    stage_all_changes()

    # Timestamped commit message
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    message = f"[AUTO] Sync DDLs from {args.source} at {timestamp}."

    committed = commit_changes(message=message)

    if not committed:
        # This should be rare if we just staged, but keep it safe.
        print("No staged changes to commit after staging; skipping push.")
        return

    # Use GitHub App–based push (enforced in git_handler.push_changes)
    push_changes(branch="main")
    print("Git commit and push completed via GitHub App.")

if __name__ == "__main__":
    print("Starting...")
    main()
    print("END.")