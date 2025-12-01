CREATE OR REPLACE PROCEDURE PRD_HOSPENG_REPORTING.GITHUB.EXPORT_VIEWS_DDLS("DB" VARCHAR, "TARGET_SCHEMA" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
if (!TARGET_SCHEMA || TARGET_SCHEMA.trim() === '''') {
    return "Error: TARGET_SCHEMA parameter is required and cannot be empty.";
}
var createTable = `
CREATE TABLE IF NOT EXISTS ${TARGET_SCHEMA}.export_ddls_views (
    object_type STRING,
    schema_name STRING,
    object_name STRING,
    ddl STRING
)
`;
snowflake.createStatement({ sqlText: createTable }).execute();

snowflake.createStatement({ sqlText: `TRUNCATE TABLE ${TARGET_SCHEMA}.export_ddls_views` }).execute();

var getSchemas = `
    SELECT schema_name
    FROM ${DB}.information_schema.schemata
    WHERE schema_name NOT IN (''INFORMATION_SCHEMA'')
`;
var schemaStmt = snowflake.createStatement({ sqlText: getSchemas });
var rsSchemas = schemaStmt.execute();

while (rsSchemas.next()) {
    var schema = rsSchemas.getColumnValue(1);

    var getViews = `
        SELECT table_name
        FROM ${DB}.information_schema.views
        WHERE table_schema = ''${schema}''
    `;
    var viewStmt = snowflake.createStatement({ sqlText: getViews });
    var rsViews = viewStmt.execute();

    while (rsViews.next()) {
        var name = rsViews.getColumnValue(1);
        var fullName = `${DB}.${schema}.${name}`;

        try {
            var ddlStmt = snowflake.createStatement({
                sqlText: `SELECT GET_DDL(''VIEW'', ''${fullName}'')`
            });
            var ddlRs = ddlStmt.execute(); ddlRs.next();
            var ddlText = ddlRs.getColumnValue(1);

            var insert = `
                INSERT INTO ${TARGET_SCHEMA}.export_ddls_views (object_type, schema_name, object_name, ddl)
                VALUES (''VIEW'', ''${schema}'', ''${name}'', :1)
            `;
            snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();

        } catch (err) {
            var insertErr = `
                INSERT INTO ${TARGET_SCHEMA}.export_ddls_views (object_type, schema_name, object_name, ddl)
                VALUES (''VIEW'', ''${schema}'', ''${name}'', ''ERROR: '' || :1)
            `;
            snowflake.createStatement({ sqlText: insertErr, binds: [err.message] }).execute();
        }
    }
}

return ''Views DDL export completed.'';
';