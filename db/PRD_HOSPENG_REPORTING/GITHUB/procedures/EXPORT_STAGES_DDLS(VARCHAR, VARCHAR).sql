CREATE OR REPLACE PROCEDURE "EXPORT_STAGES_DDLS"("P_DB_NAME" VARCHAR, "P_OUTPUT_SCHEMA" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session

def run(session: Session, P_DB_NAME: str, P_OUTPUT_SCHEMA: str) -> str:
    # 1) Target table: "P_DB_NAME"."P_OUTPUT_SCHEMA"."EXPORT_DDLS_STAGES"
    target_table = f''"{P_DB_NAME}"."{P_OUTPUT_SCHEMA}"."EXPORT_DDLS_STAGES"''

    # 2) Create table if not exists
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            OBJECT_TYPE STRING,
            SCHEMA_NAME STRING,
            OBJECT_NAME STRING,
            DDL         STRING
        )
    """
    session.sql(create_sql).collect()

    # 3) Truncate table
    session.sql(f"TRUNCATE TABLE {target_table}").collect()

    # 4) Read all stages in the database
    stages_sql = f''''''
        SELECT
            STAGE_CATALOG,
            STAGE_SCHEMA,
            STAGE_NAME
        FROM "{P_DB_NAME}".INFORMATION_SCHEMA.STAGES
    ''''''
    stages_df = session.sql(stages_sql)

    rows_to_insert = []

    for row in stages_df.collect():
        stage_catalog = row["STAGE_CATALOG"]
        stage_schema  = row["STAGE_SCHEMA"]
        stage_name    = row["STAGE_NAME"]

        # FQN: "DB"."SCHEMA"."STAGE"
        stage_fqn = f''"{stage_catalog}"."{stage_schema}"."{stage_name}"''

        # Describe the stage
        desc_df = session.sql(f"DESCRIBE STAGE {stage_fqn}").collect()
        props = {r["property"]: r["property_value"] for r in desc_df}

        url                 = props.get("URL")
        TYPE = props.get("TYPE")
        COMPRESSION        = props.get("COMPRESSION")
        ENCODING          = props.get("ENCODING")
        LAST_REFRESHED_ON             = props.get("LAST_REFRESHED_ON")

        def nz(value: str) -> str:
            return value if value is not None else "<NULL>"

        ddl_prefix = f"CREATE OR REPLACE STAGE {stage_fqn}"
        ddl = (
            f"{ddl_prefix} "
            f"/* URL={nz(url)}"
            f" TYPE={nz(TYPE)}"
            f" COMPRESSION={nz(COMPRESSION)}"
            f" ENCODING={nz(ENCODING)}"
            f" LAST_REFRESHED_ON={nz(LAST_REFRESHED_ON)}"
            f" */"
        )

        rows_to_insert.append((
            "STAGE",        # OBJECT_TYPE
            stage_schema,   # SCHEMA_NAME
            stage_name,     # OBJECT_NAME
            ddl             # DDL
        ))

    if rows_to_insert:
        df_out = session.create_dataframe(
            rows_to_insert,
            schema=["OBJECT_TYPE", "SCHEMA_NAME", "OBJECT_NAME", "DDL"]
        )
        df_out.write.mode("append").save_as_table(target_table)

    return f"STAGE DDLs exported into {target_table} (rows={len(rows_to_insert)})"
';