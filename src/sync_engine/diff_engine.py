# sync_engine/diff_engine.py

from pathlib import Path
from .utils import hash_text


def has_changed(file_path: Path, new_content: str) -> bool:
    """
    Return True if the file does not exist or its content differs
    from `new_content` (using MD5 hash comparison).
    """
    if not file_path.exists():
        # New file
        return True

    existing_content = file_path.read_text(encoding="utf-8")
    return hash_text(existing_content) != hash_text(new_content)
