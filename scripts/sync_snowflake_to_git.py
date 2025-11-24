import os
import sys
import requests
import subprocess
import snowflake.connector

"""
Python script to extract DDLs from Snowflake and write them into .sql files.
Outputs files into the db/<database>/<schema>/<object_type>/ directories.
"""

def test_snowflake():
    print("=== Probando conexión a Snowflake ===")
    try:
        ctx = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        )
        cur = ctx.cursor()
        try:
            cur.execute(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
                "CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            row = cur.fetchone()
            print("Conexión OK.")
            print(f"  USER      : {row[0]}")
            print(f"  ROLE      : {row[1]}")
            print(f"  WAREHOUSE : {row[2]}")
            print(f"  DATABASE  : {row[3]}")
            print(f"  SCHEMA    : {row[4]}")
        finally:
            cur.close()
            ctx.close()
    except Exception as e:
        print("ERROR conectando a Snowflake:")
        print(e)
        return False
    return True


def test_github_api():
    print("\n=== Probando acceso a GitHub API con PAT ===")
    pat = os.environ.get("GITHUB_PAT")
    if not pat:
        print("GITHUB_PAT no está definido en variables de entorno.")
        return False

    headers = {"Authorization": f"token {pat}"}
    try:
        resp = requests.get("https://api.github.com/user", headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print("GitHub API OK.")
            print(f"  Login : {data.get('login')}")
            print(f"  Name  : {data.get('name')}")
            return True
        else:
            print(f"GitHub API devolvió status {resp.status_code}")
            print(resp.text)
            return False
    except Exception as e:
        print("ERROR llamando a GitHub API:")
        print(e)
        return False


def test_git_remote():
    print("\n=== Probando git ls-remote al repo ===")
    repo_url = os.environ.get("GITHUB_REPO_URL")
    if not repo_url:
        print("GITHUB_REPO_URL no está definido en variables de entorno.")
        return False

    try:
        result = subprocess.run(
            ["git", "ls-remote", repo_url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False, 
        )
        if result.returncode == 0:
            print("git ls-remote OK. Se pudo acceder al repo.")
            first_line = result.stdout.splitlines()[0] if result.stdout else ""
            if first_line:
                print(f"  Primera línea de refs: {first_line}")
            return True
        else:
            print("git ls-remote devolvió error:")
            print(result.stderr)
            return False
    except FileNotFoundError:
        print("El comando 'git' no está disponible en PATH.")
        return False
    except Exception as e:
        print("ERROR ejecutando git ls-remote:")
        print(e)
        return False


def main():
    ok_sf = test_snowflake()
    ok_gh_api = test_github_api()
    ok_git = test_git_remote()

    print("\n=== Resumen ===")
    print(f"Snowflake : {'OK' if ok_sf else 'FALLÓ'}")
    print(f"GitHub API: {'OK' if ok_gh_api else 'FALLÓ'}")
    print(f"git remote: {'OK' if ok_git else 'FALLÓ'}")

    if not (ok_sf and ok_gh_api and ok_git):
        sys.exit(1)


if __name__ == "__main__":
    main()