import os
import base64
import snowflake.connector

from cryptography.hazmat.primitives import serialization

def load_private_key():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    with open(key_path, "rb") as key_file:
        if passphrase:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode(),
            )
        else:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
            )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def test_snowflake_private_key_auth():
    required_vars = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_PRIVATE_KEY_PATH"
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        return f"❌ Missing environment variables: {', '.join(missing)}"

    try:
        private_key = load_private_key()

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=private_key,
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )

        cursor = conn.cursor()
        cursor.execute("""
            SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(),
                   CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION();
        """)
        result = cursor.fetchone()

        return (
            "🎯 Successfully authenticated to Snowflake using Private Key!\n"
            f"   • User: {result[0]}\n"
            f"   • Role: {result[1]}\n"
            f"   • Warehouse: {result[2]}\n"
            f"   • Database: {result[3]}\n"
            f"   • Schema: {result[4]}\n"
            f"   • Version: {result[5]}"
        )

    except Exception as e:
        return f"❌ Authentication Failed: {e}"
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    print(test_snowflake_private_key_auth())