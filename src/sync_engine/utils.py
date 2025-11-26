# sync_engine/utils.py

import hashlib

def normalize(text: str) -> str:
    """
    Normalize object names and schema names:
    - Convert to string
    - Strip whitespace
    - Uppercase

    This helps keep folder and file names consistent.
    """
    return str(text).strip().upper()


def hash_text(text: str) -> str:
    """
    Return an MD5 hash for the given text.
    Used to detect changes between old and new DDL.
    """
    return hashlib.md5(text.encode("utf-8")).hexdigest()
