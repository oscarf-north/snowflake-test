# scripts/sync.py

"""
Main entry point for the Snowflake → Git sync (currently CSV → Git).

For now, it:
- Reads all CSVs in scripts/data/ as the source of truth.
- For each row (object), computes the target path in db/PRD_HOSPEND_REPORTING/.
- Only writes files whose DDL content actually changed.
- Optionally commits and pushes the changes to Git.

Later, `--source snowflake` can be wired to a real Snowflake reader
without changing the rest of the code.
"""

import argparse

from datetime import datetime
from sync_engine.diff_engine import has_changed
from sync_engine.git_handler import commit_changes
from sync_engine.source_adapter import load_source
from sync_engine.file_writer import get_file_path, write_file

def main():
    parser = argparse.ArgumentParser(description="Sync DDLs to Git repository.")
    parser.add_argument(
        "--source",
        default="csv",
        choices=["csv", "snowflake"],
        help="Where to read DDLs from (csv for now, snowflake in the future).",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="If set, commit and push changes when DDL updates are detected.",
    )

    args = parser.parse_args()

    # 1. Load all objects (tables, views, procedures...) from the chosen source.
    df = load_source(source=args.source)

    # 2. Iterate over each object and sync it to the file system.
    changes = 0

    for _, row in df.iterrows():
        file_path = get_file_path(row)
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
        
    # 3. Optionally commit changes to Git.
    if changes > 0:
        print(f"\nTotal objects updated: {changes}")
    else:
        print("\nNo Changes detected by sync.")

    if args.commit:
        # Timestamped commit message
        Timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        commit_changes(message=f"[AUTO] Sync DDLs from {args.source} at {Timestamp}.")
    else:
        print("Commit flag not set (--commit); no git commit/push performed.")


if __name__ == "__main__":
    print("Starting...")
    main()
    print("END.")
