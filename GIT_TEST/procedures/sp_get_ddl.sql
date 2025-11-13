CREATE OR REPLACE PROCEDURE EXPORT_ALL_DDLS_TO_TABLE(DB STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
var results = [];

// Crear tabla destino si no existe
var createTable = `
CREATE TABLE IF NOT EXISTS git_test.export_ddls (
    object_type STRING,
    schema_name STRING,
    object_name STRING,
    ddl STRING
)
`;
snowflake.createStatement({ sqlText: createTable }).execute();

// Limpiar tabla antes de insertar nueva data
var truncate = `TRUNCATE TABLE git_test.export_ddls`;
snowflake.createStatement({ sqlText: truncate }).execute();


// 1. Obtener todas las schemas
var getSchemas = `
    SELECT schema_name
    FROM ${DB}.information_schema.schemata
    WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
`;
var stmtSchemas = snowflake.createStatement({sqlText: getSchemas});
var rsSchemas = stmtSchemas.execute();


while (rsSchemas.next()) {
    var schema = rsSchemas.getColumnValue(1);

    // ------------------------------------------------------
    // TABLES
    // ------------------------------------------------------
    var getTables = `
        SELECT table_name
        FROM ${DB}.information_schema.tables
        WHERE table_schema = '${schema}'
          AND table_type = 'BASE TABLE'
    `;
    var stmtTables = snowflake.createStatement({sqlText: getTables});
    var rsTables = stmtTables.execute();

    while (rsTables.next()) {
        var name = rsTables.getColumnValue(1);
        var ddlStmt = snowflake.createStatement({
            sqlText: `SELECT GET_DDL('TABLE', '${DB}.${schema}.${name}')`
        });
        var ddlRs = ddlStmt.execute(); ddlRs.next();
        var ddlText = ddlRs.getColumnValue(1);

        // Insertar en tabla git_test
        var insert = `
            INSERT INTO git_test.export_ddls (object_type, schema_name, object_name, ddl)
            VALUES ('TABLE', '${schema}', '${name}', :1)
        `;
        snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();
    }



    // ------------------------------------------------------
    // VIEWS
    // ------------------------------------------------------
    var getViews = `
        SELECT table_name
        FROM ${DB}.information_schema.views
        WHERE table_schema = '${schema}'
    `;
    var stmtViews = snowflake.createStatement({sqlText: getViews});
    var rsViews = stmtViews.execute();

    while (rsViews.next()) {
        var name = rsViews.getColumnValue(1);
        var ddlStmt = snowflake.createStatement({
            sqlText: `SELECT GET_DDL('VIEW', '${DB}.${schema}.${name}')`
        });
        var ddlRs = ddlStmt.execute(); ddlRs.next();
        var ddlText = ddlRs.getColumnValue(1);

        var insert = `
            INSERT INTO git_test.export_ddls (object_type, schema_name, object_name, ddl)
            VALUES ('VIEW', '${schema}', '${name}', :1)
        `;
        snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();
    }


    // ------------------------------------------------------
    // STORED PROCEDURES
    // ------------------------------------------------------
    var getProcs = `
        SELECT procedure_name, argument_signature
        FROM ${DB}.information_schema.procedures
        WHERE procedure_schema = '${schema}'
    `;
    var stmtProcs = snowflake.createStatement({sqlText: getProcs});
    var rsProcs = stmtProcs.execute();

    while (rsProcs.next()) {
        var name = rsProcs.getColumnValue(1);
        var signature = rsProcs.getColumnValue(2);
        var full = `${DB}.${schema}.${name}${signature}`;

        var ddlStmt = snowflake.createStatement({
            sqlText: `SELECT GET_DDL('PROCEDURE', '${full}')`
        });
        var ddlRs = ddlStmt.execute(); ddlRs.next();
        var ddlText = ddlRs.getColumnValue(1);

        var insert = `
            INSERT INTO git_test.export_ddls (object_type, schema_name, object_name, ddl)
            VALUES ('PROCEDURE', '${schema}', '${name}${signature}', :1)
        `;
        snowflake.createStatement({ sqlText: insert, binds: [ddlText] }).execute();
    }

}

return 'DDL export saved into git_test.export_ddls';

$$;