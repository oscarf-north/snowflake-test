CREATE OR REPLACE PROCEDURE "SP_LOADDIMENSIONNONEROW_ALL"()
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
    SQLStmt resultset;
    ScriptIns varchar;
    SQLTextOut varchar;
    SQLIns varchar;
    SCRIPT resultset;
    BIGSCRIPTS resultset;

-----------------------------------------------------------------------------------------------------------
BEGIN
   --drop temp tables if they exist
   drop table if exists dwtable_lists; 
   drop table if exists dwtable_scripts; 

   create temp table dwtable_scripts (
     name varchar (100) 
     ,scripttext varchar
  );

   --create a table containing rows with the call statements to create the load stored procedures
   SELECT 
     REPLACE(''CALL DATAADMIN.SP_LOADDIMENSIONNONEROW(\\''<TABLENAME>\\'')''
       ,''<TABLENAME>'',TABLE_NAME) AS sqltext
     FROM information_schema.tables t
   WHERE TABLE_SCHEMA = ''DATAWAREHOUSE''
     AND TABLE_TYPE = ''BASE TABLE''
     --AND TABLE_NAME IN (''EMPLOYEE_DIM'',''ACTIVITY_FACT'')--WORKS ERROR MESSAGES ONLY
     --AND TABLE_NAME IN (''LOCATION_DIM'',''SHIFT_DIM'')  --WORKS LOAD STMTS ONLY
     --AND TABLE_NAME IN (''EMPLOYEE_DIM'',''LOCATION_DIM'')
     AND TABLE_NAME ILIKE ''%_DIM''
     AND TABLE_NAME NOT IN  (''DATASHARETEST_DIM'',''DATE_DIM'',''ORDER_SUMMARY'',''ERRORDWDATE_DIM'')
 ;
   
   CREATE temp TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  SQLStmt := (select * from dwtable_lists);
--CALL DATAADMIN.SP_LOADDIMENSIONNONEROW(''EMPLOYEE_DIM'')
  --loop through all rows that create the load stored proceedure 1 per table
      BEGIN
          FOR record IN SQLStmt DO
            SQLTextOut := record.sqltext;
            SCRIPT := (EXECUTE IMMEDIATE record.sqltext);           
            ScriptIns := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            INSERT INTO dwtable_scripts(scripttext)    
                VALUES(:ScriptIns);   --works       
          END FOR;
     END;

  BIGSCRIPTS := (SELECT listagg(scripttext) from dwtable_scripts);   
  RETURN TABLE(BIGSCRIPTS);
  
END';