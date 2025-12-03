CREATE OR REPLACE PROCEDURE "SP_VALIDATEDWDATES_CREATEALL"("REPORTTYPE" VARCHAR(1), "ERRORREPORT" VARCHAR(5))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
    -- REPORTTYPE VARCHAR(1)          := ''P'';  --valid values {L,P,S}
    -- ERRORREPORT VARCHAR(5)         := ''COUNT''; --VALID VALUES {COUNT,ERROR,ALL}
    
    SQLStmt resultset;
    ScriptInsM varchar;
    ScriptInsC varchar;
    SQLTextOut varchar;
    SQLIns varchar;
    SCRIPT resultset;
    BIGSCRIPTS resultset;
  
    TableType_VAR  VARCHAR(075)  := CASE UPPER(REPORTTYPE)
                                        WHEN ''L''THEN ''VIEW'' 
                                        WHEN ''P'' THEN ''BASE TABLE''
                                        WHEN ''S'' THEN ''BASE TABLE''
                                        ELSE ''WRONG'' END; 
    SchemaName_VAR VARCHAR(075)  := CASE UPPER(REPORTTYPE) 
                                        WHEN ''L'' THEN ''DATAADMIN'' 
                                        WHEN ''P'' THEN ''DATAWAREHOUSE''
                                        WHEN ''S'' THEN ''DATAWAREHOUSE_TEMP''
                                        ELSE ''WRONG'' END;
                                        
    RunSQLText     VARCHAR(100)  := ''CALL DATAADMIN.SP_VALIDATEDWDATES('' 
                                      || ''\\'''' || REPORTTYPE || ''\\'',''
                                      || ''\\'''' || ''<TABLENAME>'' || ''\\'',''
                                      || ''\\'''' || ERRORREPORT || ''\\'''' || '');'';
                                        
-----------------------------------------------------------------------------------------------------------
BEGIN
   --drop temp tables if they exist
   drop table if exists dwtable_lists; 
   drop table if exists dwtable_scripts; 

   create table dwtable_scripts (
     MESSAGE varchar
     ,"COUNT"  int
  );

   --create a table containing rows with the call statements to create the load stored procedures
   SELECT 
     REPLACE(:RunSQLText,''<TABLENAME>'',TABLE_NAME) AS sqltext
     FROM information_schema.tables t
   WHERE  TABLE_SCHEMA = :SchemaName_VAR
     AND TABLE_TYPE    = :TableType_VAR
     AND TABLE_NAME not in ( ''ORDER_SUMMARY'',''DATASHARE_VIEW'',''DATASHARE_SECURE_VIEW'',''DATE_DIM'',''VW_REPORT_TENDER'',''ERRORDWDATE_DIM'',''ITEM_SALES'')
     AND TABLE_NAME NOT ILIKE ''DW_%''
     AND NOT TABLE_NAME ILIKE (''%REPORT%'')
     ;
   
   CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

   SQLStmt := (select * from dwtable_lists);

  --loop through all rows that create the load stored proceedure 1 per table
      BEGIN
          FOR record IN SQLStmt DO
            SQLTextOut := record.sqltext;
                BEGIN  
                   SCRIPT := (EXECUTE IMMEDIATE record.sqltext);         
                END;

                INSERT INTO dwtable_scripts(MESSAGE,COUNT) 
                    SELECT MESSAGE,COUNT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

          END FOR;
     END;

  BIGSCRIPTS := (SELECT * from dwtable_scripts);   
  RETURN TABLE(BIGSCRIPTS);
  
END';