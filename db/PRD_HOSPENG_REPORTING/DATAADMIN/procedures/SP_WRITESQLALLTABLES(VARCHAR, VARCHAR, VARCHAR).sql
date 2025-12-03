CREATE OR REPLACE PROCEDURE "SP_WRITESQLALLTABLES"("DBNAME" VARCHAR(50), "SCHEMANAME" VARCHAR(50), "SQLNAME" VARCHAR(45))
RETURNS TABLE ("SQLTEXT" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  -- DBNAME VARCHAR(45)             := ''DEV_HOSPENG_REPORTING'';
  -- SCHEMANAME VARCHAR(45)         := ''DATASTAGE'';
  -- SQLNAME VARCHAR(45)            := ''CALL_SP_CreateDWTable'';
----------------------------------------------------------------------------------------------------------------------
  DBName_VAR VARCHAR(075)        := UPPER(DBNAME);
  SchemaName_VAR VARCHAR(075)    := UPPER(SCHEMANAME);
  SQLText VARCHAR(500)           := (SELECT SQLTEXTVALUE FROM DW_SQLTEXT WHERE SQLTEXTNAME = :SQLNAME);
  res_list                       resultset ;
  
----------------------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------------------
--Drop temp table if exists - used for dev as temp tables would be dropped when sproc session ends
DROP TABLE IF EXISTS all_tables_temp CASCADE;  

res_list := (
 SELECT listagg(REPLACE(REPLACE(REPLACE(:SQLText,''<TABLENAME>'',T.TABLE_NAME),''<DBNAME>'',:DBName_VAR),''<SCHEMANAME>'',:SchemaName_VAR)
   || '' \\n '')  AS SQLTEXT
   FROM information_schema.tables t
      WHERE t.table_type    = ''VIEW''
            and t.table_schema  = ''DATAADMIN''
    );
  
 RETURN TABLE(res_list);

END';