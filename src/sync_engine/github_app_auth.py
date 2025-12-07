
# src/sync_engine/github_app_auth.py

import os
import time
import jwt
import requests

GH_API_URL = "https://api.github.com"

def _get_app_config():
    app_id = os.environ["GH_APP_ID"]
    installation_id = os.environ["GH_INSTALLATION_ID"]

    private_key = os.getenv("GH_APP_PRIVATE_KEY")
    private_key_path = os.getenv("GH_APP_PRIVATE_KEY_PATH")

    if not private_key and private_key_path:
        with open(private_key_path, "r") as f:
            private_key = f.read()

    if not private_key:
        raise RuntimeError("GH_APP_PRIVATE_KEY or GH_APP_PRIVATE_KEY_PATH must be set")

    return app_id, installation_id, private_key


def generate_app_jwt() -> str:
    app_id, _, private_key = _get_app_config()
    now = int(time.time())
    payload = {
        "iat": now - 60,
        "exp": now + 600,   # max 10 minutes
        "iss": app_id,
    }
    encoded = jwt.encode(payload, private_key, algorithm="RS256")
    if isinstance(encoded, bytes):
        encoded = encoded.decode("utf-8")
    return encoded


def get_installation_access_token() -> str:
    app_id, installation_id, _ = _get_app_config()
    jwt_token = generate_app_jwt()

    url = f"{GH_API_URL}/app/installations/{installation_id}/access_tokens"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.post(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data["token"]