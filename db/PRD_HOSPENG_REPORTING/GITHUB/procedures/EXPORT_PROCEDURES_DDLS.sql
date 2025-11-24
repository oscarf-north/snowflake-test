CREATE OR REPLACE PROCEDURE PRD_HOSPENG_REPORTING.GITHUB.EXPORT_PROCEDURES_DDLS("DB" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
var createTable = `
CREATE TABLE IF NOT EXISTS PUBLIC.export_ddls_sp (
    object_type STRING,
    schema_name STRING,
    object_name STRING,
    ddl STRING
)
`;
snowflake.createStatement({ sqlText: createTable }).execute();

snowflake.createStatement({ sqlText: `TRUNCATE TABLE PUBLIC.export_ddls_sp` }).execute();

var getSchemas = `
    SELECT schema_name
    FROM ${DB}.information_schema.schemata
    WHERE schema_name NOT IN (''INFORMATION_SCHEMA'')
`;
var schemaStmt = snowflake.createStatement({ sqlText: getSchemas });
var rsSchemas = schemaStmt.execute();

while (rsSchemas.next()) {
    var schema = rsSchemas.getColumnValue(1);

    var getProcs = `
        SELECT procedure_name, argument_signature
        FROM ${DB}.information_schema.procedures
        WHERE procedure_schema = ''${schema}''
    `;
    var procStmt = snowflake.createStatement({ sqlText: getProcs });
    var rsProcs = procStmt.execute();

    while (rsProcs.next()) {
        var name = rsProcs.getColumnValue(1);
        var signature = rsProcs.getColumnValue(2);
        var fullName = `${DB}.${schema}.${name}${signature}`;
        var objectName = `${name}${signature}`;

        try {
            var ddlStmt = snowflake.createStatement({
                sqlText: `SELECT GET_DDL(''PROCEDURE'', ''${fullName}'')`
            });
            var ddlRs = ddlStmt.execute(); ddlRs.next();
            var ddlText = ddlRs.getColumnValue(1);

            var insert = `
                INSERT INTO PUBLIC.export_ddls_sp (object_type, schema_name, object_name, ddl)
                VALUES (''PROCEDURE'', ''${schema}'', ''${objectName}'', :1)
            `;
            snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();

        } catch (err) {
            var insertErr = `
                INSERT INTO PUBLIC.export_ddls_sp (object_type, schema_name, object_name, ddl)
                VALUES (''PROCEDURE'', ''${schema}'', ''${objectName}'', ''ERROR: '' || :1)
            `;
            snowflake.createStatement({ sqlText: insertErr, binds: [err.message] }).execute();
        }
    }
}

return ''Stored Procedure DDL export completed.'';
';