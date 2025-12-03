CREATE OR REPLACE PROCEDURE "EXPORT_PROCEDURES_DDLS"("DB" VARCHAR, "TARGET_SCHEMA" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session

def _to_type_only_signature(argument_signature: str) -> str:
    """
    Convert ''(IN P_ID NUMBER, P_NAME VARCHAR)'' -> ''(NUMBER, VARCHAR)''
    and handle NULL/empty argument_signature as ''()''.
    """
    if not argument_signature or argument_signature.strip() == "":
        return "()"

    sig = argument_signature.strip()

    # Remove outer parentheses
    if sig.startswith("(") and sig.endswith(")"):
        sig = sig[1:-1]

    if sig.strip() == "":
        return "()"

    parts = sig.split(",")
    type_parts = []

    for part in parts:
        arg = part.strip()
        if not arg:
            continue

        # Split on whitespace: e.g. ["IN","P_ID","NUMBER"] or ["P_ID","NUMBER"]
        tokens = arg.split()

        # Remove IN/OUT/INOUT if present
        if tokens and tokens[0].upper() in ("IN", "OUT", "INOUT"):
            tokens = tokens[1:]

        # Remove parameter name (first token), keep rest as type
        if len(tokens) > 1:
            tokens = tokens[1:]

        if tokens:
            type_parts.append(" ".join(tokens))  # e.g. ''NUMBER'', ''TIMESTAMP_NTZ(9)''

    return "(" + ", ".join(type_parts) + ")" if type_parts else "()"


def main(session: Session, DB: str, TARGET_SCHEMA: str) -> str:
    if not TARGET_SCHEMA or TARGET_SCHEMA.strip() == "":
        return "Error: TARGET_SCHEMA parameter is required and cannot be empty."

    # 1) Ensure output table exists
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.export_ddls_sp (
            object_type STRING,
            schema_name STRING,
            object_name STRING,
            ddl         STRING
        )
    """
    session.sql(create_table_sql).collect()

    # 2) Truncate output table
    session.sql(f"TRUNCATE TABLE {TARGET_SCHEMA}.export_ddls_sp").collect()

    # 3) Get all schemas in DB (except INFORMATION_SCHEMA)
    schemas_df = session.sql(f"""
        SELECT schema_name
        FROM {DB}.information_schema.schemata
        WHERE schema_name <> ''INFORMATION_SCHEMA''
    """)
    schemas = [row[0] for row in schemas_df.collect()]

    for schema in schemas:
        # 4) Get procedures for this schema
        procs_df = session.sql(
            f"""
            SELECT procedure_name, argument_signature
            FROM {DB}.information_schema.procedures
            WHERE procedure_schema = ?
            """,
            params=[schema]
        )
        procs = procs_df.collect()

        for row in procs:
            name = row[0]
            argument_signature = row[1]

            # Build type-only signature for GET_DDL
            type_signature = _to_type_only_signature(argument_signature)

            # Fully qualified procedure name with types only
            # Example: "MYDB"."MYSCHEMA"."MY_PROC"(NUMBER, VARCHAR)
            full_name = f''"{DB}"."{schema}"."{name}"{type_signature}''
            object_name = f"{name}{type_signature}"

            try:
                # 5) Get DDL via GET_DDL(''PROCEDURE'', full_name)
                ddl_df = session.sql(
                    "SELECT GET_DDL(''PROCEDURE'', ?)",
                    params=[full_name]
                )
                ddl_rows = ddl_df.collect()
                ddl_text = ddl_rows[0][0] if ddl_rows else None

                # 6) Insert DDL into output table
                session.sql(
                    f"""
                    INSERT INTO {TARGET_SCHEMA}.export_ddls_sp
                        (object_type, schema_name, object_name, ddl)
                    VALUES (''PROCEDURE'', ?, ?, ?)
                    """,
                    params=[schema, object_name, ddl_text]
                ).collect()

            except Exception as e:
                # 7) Insert error into output table
                err_msg = "ERROR: " + str(e)
                session.sql(
                    f"""
                    INSERT INTO {TARGET_SCHEMA}.export_ddls_sp
                        (object_type, schema_name, object_name, ddl)
                    VALUES (''PROCEDURE'', ?, ?, ?)
                    """,
                    params=[schema, object_name, err_msg]
                ).collect()

    return "Stored Procedure DDL export completed."
';