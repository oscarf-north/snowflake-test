# sync_engine/git_handler.py

import os
import subprocess

from typing import Optional
from .config import GIT_ROOT


def _get_repository_from_env() -> Optional[str]:
    """
    Resolve the repository name in the form 'owner/repo'.

    Priority:
    1) GH_REPO          (explicitly set by us)
    2) GH_REPOSITORY    (standard in GitHub Actions)
    
    repo = os.getenv("GH_REPO")
    if repo:
        return repo
    """
    repo = os.getenv("GH_REPOSITORY")
    if repo:
        return repo

    return None


def _configure_remote_if_needed() -> None:
    """
    If GH_TOKEN and repository are available in env vars, configure
    the 'origin' remote to use an HTTPS URL with the token embedded.

    - In CI (GitHub Actions), we expect:
        GH_TOKEN      → automatically provided or passed from secrets
        GH_REPOSITORY → e.g. 'oscarjos/my-sf-sync'

    - Locally, you can source env_local.sh to define:
        GH_TOKEN
        GH_REPO

    If no token/repo is found, do nothing and rely on local git config.
    """
    token = os.getenv("GH_TOKEN")
    repo = _get_repository_from_env()

    if not token or not repo:
        # No explicit token/repo → rely on whatever git is already using
        print("No GH_TOKEN / GH_REPO found; using existing git remote configuration.")
        return

    url = f"https://x-access-token:{token}@github.com/{repo}.git"

    # Update origin remote URL
    print(f"Configuring git remote 'origin' to {url}")
    subprocess.run(
        ["git", "remote", "set-url", "origin", url],
        check=True,
        cwd=GIT_ROOT,
    )


def _has_staged_changes() -> bool:
    """
    Check if there are staged changes (`git diff --cached` not empty).
    Return True if there ARE staged changes.
    """
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=GIT_ROOT,
    )
    # 0 → no differences, 1 → differences present
    return result.returncode == 1


def commit_changes(message: str) -> None:
    """
    Stage `db/` folder, commit with the given message, and push.

    - Uses env-based GitHub remote if GH_TOKEN is set.
    - Otherwise, relies on the existing git configuration (SSH or HTTPS).

    This function is called from src/sync.py when `--commit` is passed.
    """
    # Stage the db folder (where generated SQL lives)
    subprocess.run(["git", "add", "db"], check=True, cwd=GIT_ROOT)

    if not _has_staged_changes():
        print("No staged changes to commit.")
        return

    # Configure remote based on env vars (local env or GitHub Actions)
    _configure_remote_if_needed()

    # Commit and push
    subprocess.run(["git", "commit", "-m", message], check=True, cwd=GIT_ROOT)
    subprocess.run(["git", "push"], check=True, cwd=GIT_ROOT)

    print("Changes committed and pushed.")