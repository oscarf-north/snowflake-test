import os
import requests

def test_github_connection():
    token = os.getenv("GITHUB_PAT")
    user = os.getenv("GITHUB_ORG")
    repo = os.getenv("GITHUB_REPO")

    if not token or not user or not repo:
        return "❌ Missing required environment variables: GITHUB_PAT, GITHUB_USER, GITHUB_REPO"

    url = f"https://api.github.com/repos/{user}/{repo}"
    headers = {"Authorization": f"token {token}"}
    print("URL = ", url)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return f"✅ Successfully connected to GitHub repo '{repo}' as '{user}'."
        elif response.status_code == 404:
            return "❌ Repository not found or no access. Check repo name and permissions."
        elif response.status_code == 401:
            return "❌ Unauthorized. Invalid or expired GitHub PAT."
        else:
            return f"⚠ Unexpected response: {response.status_code} – {response.text}"
    except Exception as e:
        return f"❌ Connection error: {e}"

if __name__ == "__main__":
    print(test_github_connection())