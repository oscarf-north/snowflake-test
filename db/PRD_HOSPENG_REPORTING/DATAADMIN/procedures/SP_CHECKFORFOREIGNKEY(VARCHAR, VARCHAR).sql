CREATE OR REPLACE PROCEDURE "SP_CHECKFORFOREIGNKEY"("REPORTTYPE" VARCHAR(1), "TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- REPORTTYPE VARCHAR(1)  := ''L'';
        -- TABLENAME VARCHAR      := ''ADDRESS_DIM'';     --no foreign keys
        -- TABLENAME VARCHAR      := ''ACTIVITY_FACT'';  --HAS NULL VALUES

        SCHEMANAME VARCHAR(075):= CASE UPPER(REPORTTYPE) 
                                        WHEN ''L'' THEN ''DATAADMIN'' 
                                        WHEN ''P'' THEN ''DATAWAREHOUSE''
                                        WHEN ''S'' THEN ''DATAWAREHOUSE_TEMP''
                                        ELSE ''WRONG'' END;
        HAS_RESULTS int;
        FK_RESULTS resultset;
        FK_NORESULTS resultset;
        SQLStmt resultset;
        SQLTextOut varchar;
        THIS_COLUMN varchar;

BEGIN
   DROP TABLE IF EXISTS dwtable_lists; 
   DROP TABLE IF EXISTS REPORT_OUTPUT;  
    
   CREATE TABLE REPORT_OUTPUT (
     "TABLE" varchar
     ,"COLUMN" varchar
     ,"COUNT"  int
    );

    SELECT c.table_name                 as TABLE_NAME
        ,c.column_name                  as NATURAL_KEY
    FROM information_schema.columns c
         WHERE c.table_name   = c.table_name
            AND c.table_schema = :SCHEMANAME
            AND c.table_name   = :TABLENAME
            AND c.column_name  ilike ''%_FK''
        ;
        
    CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
     
    FK_NORESULTS := (
      SELECT :TABLENAME                   as "TABLE"
        ,''This table has NO Foreign Keys'' as "COLUMN"
        ,0                                as "COUNT"
      );
    HAS_RESULTS:=(SELECT COUNT(*) FROM dwtable_lists);

    SQLStmt := (select * from dwtable_lists);

  --loop through all rows that create the load stored proceedure 1 per table
      BEGIN
          FOR record IN SQLStmt DO
      
            THIS_COLUMN := record.NATURAL_KEY;
            
            SELECT :TABLENAME as "Table"
               ,:THIS_COLUMN  as "Column"
               ,SUM(case when identifier(:THIS_COLUMN) IS NULL THEN 1 ELSE 0 END) as "Count" 
            FROM identifier(:TABLENAME) ;

            INSERT INTO REPORT_OUTPUT("TABLE","COLUMN","COUNT") 
                SELECT "Table","Column","Count" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            
          END FOR;
            
     END;

FK_RESULTS := (SELECT * from REPORT_OUTPUT);   

IF (:HAS_RESULTS > 0)  --Has primary key
    THEN
      RETURN TABLE(FK_RESULTS); 
    ELSE
      RETURN TABLE(FK_NORESULTS);
  END IF;
                    
END';