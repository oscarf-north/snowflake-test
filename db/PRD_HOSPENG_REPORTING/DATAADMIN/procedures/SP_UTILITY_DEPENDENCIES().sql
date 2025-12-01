CREATE OR REPLACE PROCEDURE "SP_UTILITY_DEPENDENCIES"()
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
--============================================================================================
--Procedure to pull the dependent tables of all the views in public ending with _DIM/_FACT/_XREF
declare
--Declaring variables
  select_statement varchar;
  res resultset;
begin
--Sql statement to pull the dependent tables of all the views in public ending with _DIM/_FACT/_XREF
select_statement := (SELECT listagg(stmt, '''') as statment_text FROM (SELECT
     ''SELECT * from table(get_object_references(database_name=>\\''DEV_HOSPENG_REPORTING\\'', schema_name=>\\''DATAADMIN\\'', object_name=> \\'''' || table_name || ''\\''))''
      || CASE WHEN row_num < max(row_num) over()
            THEN '' UNION ''
        ELSE '' ''
      END as stmt
      FROM (SELECT TABLE_NAME
            ,rank() over (ORDER BY table_name) as row_num
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ''DATAADMIN'' and
                  (TABLE_NAME like ''%_DIM'' or
                  TABLE_NAME like ''%_FACT'' or
                  TABLE_NAME like ''%_XREF''))));
--Executing the variable which is the sql statement
res := (execute immediate : select_statement);
--Returning the variable which is the result of sql statement
return table(res);
end;
';