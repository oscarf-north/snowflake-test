CREATE OR REPLACE PROCEDURE "SP_CHECKNEGONE_ALL"()
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

   create table dwtable_scripts (
     nameval varchar (100) 
     ,countval number
  );
  -- SELECT ''ADDRESS_DIM'' AS NAMEVAL,COUNT(*) AS COUNTVAL FROM  DATAWAREHOUSE.ADDRESS_DIM WHERE ADDRESS_DIM_pk = -1;
  --create a table containing rows with the call statements to create the load stored procedures
SELECT REPLACE(''SELECT \\''<TABLENAME>\\'' AS NAMEVAL,COUNT(*) AS COUNTVAL FROM  DATAWAREHOUSE.<TABLENAME> WHERE <TABLENAME>_pk = -1;''
       ,''<TABLENAME>'',TABLE_NAME) AS sqltext
  FROM information_schema.tables 
   WHERE TABLE_SCHEMA = ''DATAWAREHOUSE''
     AND TABLE_TYPE = ''BASE TABLE''
     AND TABLE_NAME ILIKE ''%_DIM''
     AND TABLE_NAME NOT IN  (''DATASHARETEST_DIM'',''DATE_DIM'',''ORDER_SUMMARY'',''ERRORDWDATE_DIM'');
   
   CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


  SQLStmt := (select * from dwtable_lists);

  --loop through all rows that create the load stored proceedure 1 per table
      BEGIN
          FOR record IN SQLStmt DO
            SQLTextOut := record.sqltext;
            SCRIPT := (EXECUTE IMMEDIATE record.sqltext);           
            INSERT INTO dwtable_scripts(nameval,countval) 
                    SELECT NAMEVAL,COUNTVAL FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
          END FOR;
     END;

  BIGSCRIPTS := (SELECT * from dwtable_scripts order by countval desc);   
  RETURN TABLE(BIGSCRIPTS);
  
END';