CREATE OR REPLACE PROCEDURE PRD_HOSPENG_REPORTING.GITHUB.EXPORT_TABLES_DDLS("DB" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
var createTable = `
CREATE TABLE IF NOT EXISTS PUBLIC.export_ddls_tables (
    object_type STRING,
    schema_name STRING,
    object_name STRING,
    ddl STRING
)
`;
snowflake.createStatement({ sqlText: createTable }).execute();

snowflake.createStatement({ sqlText: `TRUNCATE TABLE PUBLIC.export_ddls_tables` }).execute();

var getSchemas = `
    SELECT schema_name
    FROM ${DB}.information_schema.schemata
    WHERE schema_name NOT IN (''INFORMATION_SCHEMA'')
`;
var schemaStmt = snowflake.createStatement({ sqlText: getSchemas });
var rsSchemas = schemaStmt.execute();

while (rsSchemas.next()) {
    var schema = rsSchemas.getColumnValue(1);

    var getTables = `
        SELECT table_name
        FROM ${DB}.information_schema.tables
        WHERE table_schema = ''${schema}''
          AND table_type = ''BASE TABLE''
    `;
    var tblStmt = snowflake.createStatement({ sqlText: getTables });
    var rsTables = tblStmt.execute();

    while (rsTables.next()) {
        var name = rsTables.getColumnValue(1);
        var fullName = `${DB}.${schema}.${name}`;

        try {
            var ddlStmt = snowflake.createStatement({
                sqlText: `SELECT GET_DDL(''TABLE'', ''${fullName}'')`
            });
            var ddlRs = ddlStmt.execute(); ddlRs.next();
            var ddlText = ddlRs.getColumnValue(1);

            var insert = `
                INSERT INTO PUBLIC.export_ddls_tables (object_type, schema_name, object_name, ddl)
                VALUES (''TABLE'', ''${schema}'', ''${name}'', :1)
            `;
            snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();

        } catch (err) {
            var insertErr = `
                INSERT INTO PUBLIC.export_ddls_tables (object_type, schema_name, object_name, ddl)
                VALUES (''TABLE'', ''${schema}'', ''${name}'', ''ERROR: '' || :1)
            `;
            snowflake.createStatement({ sqlText: insertErr, binds: [err.message] }).execute();
        }
    }
}

return ''Tables DDL export completed.'';
';