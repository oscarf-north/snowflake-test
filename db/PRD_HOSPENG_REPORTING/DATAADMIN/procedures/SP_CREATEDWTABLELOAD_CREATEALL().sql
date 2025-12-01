CREATE OR REPLACE PROCEDURE "SP_CREATEDWTABLELOAD_CREATEALL"()
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
     name varchar (100) 
     ,scripttext varchar
  );

   --create a table containing rows with the call statements to create the load stored procedures
   SELECT 
     REPLACE(''CALL DATAADMIN.SP_CreateDWTableLoad(\\''DEV_HOSPENG_REPORTING\\'',\\''DATASTAGE\\'',\\''<TABLENAME>\\'')''
       ,''<TABLENAME>'',TABLE_NAME) AS sqltext
     FROM information_schema.tables t
   WHERE TABLE_SCHEMA = ''DATAWAREHOUSE''
     AND TABLE_TYPE = ''BASE TABLE''
     AND TABLE_NAME not in  (''ORDER_SUMMARY'',''ITEM_SALES'',''ERRORDATE_DIM'',''DATE_DIM'')
     AND TABLE_NAME NOT ILIKE (''DW_%'')
     AND TABLE_NAME  ILIKE ANY (''%_FACT'',''%_DIM'',''%_XREF'',''%_SUMMARY'');
   
   CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  SQLStmt := (select * from dwtable_lists);

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