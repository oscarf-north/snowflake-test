import os
import subprocess

from .config import GIT_ROOT
from .github_app_auth import get_installation_access_token

def _run_git(args, check: bool = True) -> subprocess.CompletedProcess:
    """
    Run a git command in GIT_ROOT and return the completed process.
    """
    cmd = ["git"] + args
    result = subprocess.run(
        cmd,
        cwd=GIT_ROOT,
        check=check,
        capture_output=True,
        text=True,
    )
    return result


def _ensure_git_repo() -> None:
    """
    Ensure GIT_ROOT is already a git repository.
    We do NOT auto-init; if it's not a repo, we fail.
    """
    git_dir = os.path.join(GIT_ROOT, ".git")
    if not os.path.isdir(git_dir):
        raise RuntimeError(
            f"{GIT_ROOT} is not a git repository (.git directory missing). "
            "The sync process must run inside an existing cloned repo."
        )


def _ensure_git_identity() -> None:
    """
    Ensure git user.name and user.email are set in this repo.
    If missing, set a default bot identity.
    """
    _ensure_git_repo()

    def _get_config(key: str) -> str:
        result = subprocess.run(
            ["git", "config", key],
            cwd=GIT_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return ""
        return result.stdout.strip()

    name = _get_config("user.name")
    email = _get_config("user.email")

    # Set defaults only if missing
    if not name:
        _run_git(["config", "user.name", "Snowflake Sync Bot"])
    if not email:
        _run_git(["config", "user.email", "snowflake-sync-bot@example.com"])


def _get_repository_from_env() -> str:
    """
    Read the GitHub repository (owner/name) from environment variables.
    This is a hard requirement.
    """
    repo = os.getenv("GH_REPOSITORY") or os.getenv("GH_REPO")
    if not repo:
        raise RuntimeError(
            "GitHub repo is not configured. "
            "Set GH_REPOSITORY (or GH_REPO) to 'owner/repo'."
        )
    return repo


def _configure_remote_strict() -> None:
    """
    Configure the 'origin' remote to use a GitHub App installation token.

    NO FALLBACK:
      - If GitHub App configuration is missing/invalid, we raise.
      - This enforces that the only supported authentication method
        is GitHub App–based.
    """
    _ensure_git_repo()

    repo = _get_repository_from_env()

    # Get short-lived installation token from the GitHub App
    token = get_installation_access_token()
    if not token:
        raise RuntimeError("Failed to obtain GitHub App installation access token.")

    remote_url = f"https://x-access-token:{token}@github.com/{repo}.git"

    try:
        # Check if 'origin' exists
        result = _run_git(["remote", "get-url", "origin"], check=False)
        if result.returncode != 0:
            # origin does not exist, add it
            _run_git(["remote", "add", "origin", remote_url], check=True)
        else:
            # origin exists, set its URL
            _run_git(["remote", "set-url", "origin", remote_url], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to configure 'origin' remote with GitHub App token:\n"
            f"STDOUT:\n{e.stdout}\n\nSTDERR:\n{e.stderr}"
        ) from e


def stage_all_changes() -> None:
    """
    Stage all changes in the DDL output folder (e.g. db/).

    This ensures the automation only commits generated DDL files,
    not unrelated local changes (workflows, env files, etc.).
    """
    _ensure_git_repo()
    try:
        _run_git(["add", "db"], check=True) 
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to stage DDL changes.\nSTDOUT:\n{e.stdout}\n\nSTDERR:\n{e.stderr}"
        ) from e


def commit_changes(message: str) -> bool:
    """
    Commit staged changes with the given message.

    Returns:
      True  -> a commit was created
      False -> no staged changes (nothing to commit)
    """
    _ensure_git_repo()
    _ensure_git_identity()

    try:
        # Check if there are staged changes (index), not just any working tree changes
        staged = _run_git(["diff", "--cached", "--name-only"], check=True)
        if not staged.stdout.strip():
            # No staged changes: nothing to commit, just skip
            print("No staged changes to commit. Skipping git commit.")
            return False

        _run_git(["commit", "-m", message], check=True)
        return True
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to commit changes.\nSTDOUT:\n{e.stdout}\n\nSTDERR:\n{e.stderr}"
        ) from e


def push_changes(branch: str = "main") -> None:
    """
    Push the current HEAD to the given branch on 'origin'.

    ALWAYS:
      1. Configures 'origin' using a GitHub App installation token.
      2. Pushes to 'origin/<branch>'.

    If GitHub App configuration is incorrect, it raises and stops.
    """
    _configure_remote_strict()
    try:
        result = _run_git(["push", "origin", f"HEAD:{branch}"], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to push changes to origin/{branch}.\n"
            f"STDOUT:\n{e.stdout}\n\nSTDERR:\n{e.stderr}"
        ) from e


def commit_and_push(message: str, branch: str = "main") -> None:
    """
    High-level helper: stage, commit, and push using GitHub App auth only.
    """
    staged_anything = True
    stage_all_changes()
    committed = commit_changes(message)
    if not committed:
        # No changes to commit -> nothing to push
        return

    push_changes(branch=branch)