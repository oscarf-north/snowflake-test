CREATE OR REPLACE PROCEDURE "SP_CHECKVARIANTDATATYPES_ALL"("REPORTTYPE" VARCHAR(1), "TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- REPORTTYPE VARCHAR(1)  := ''L'';

        SCHEMANAME VARCHAR(075)  := CASE UPPER(REPORTTYPE) 
                                        WHEN ''L'' THEN ''DATAADMIN'' 
                                        WHEN ''P'' THEN ''DATAWAREHOUSE''
                                        WHEN ''S'' THEN ''DATAWAREHOUSE_TEMP''
                                        ELSE ''WRONG'' END;

        TableType_VAR  VARCHAR(75) := CASE UPPER(REPORTTYPE) WHEN ''L''THEN ''VIEW'' ELSE ''BASE TABLE'' END;                                         
        HAS_RESULTS    int;
        VAR_RESULTS    resultset;
        VAR_NORESULTS  resultset;

BEGIN

   DROP TABLE IF EXISTS dwtable_lists; 
   DROP TABLE IF EXISTS REPORT_OUTPUT;  
    
   CREATE TABLE REPORT_OUTPUT (
     "TABLE" varchar
     ,"COLUMN" varchar
     ,"COUNT"  int
    );

    SELECT c.table_name                 as "Table"
        ,c.column_name                  as "Column"
        ,c.data_type                    as "Count"
    FROM information_schema.columns c
         WHERE c.table_name    = c.table_name
            AND c.table_schema = :SCHEMANAME
            AND c.data_type    = ''VARIANT''
            AND c.table_name   <> ''ERRORDWDATE_DIM''
        ;
        
    CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
     
    VAR_NORESULTS := (
      SELECT :SCHEMANAME                                            as "Table"
        ,''This Schema has NO Variant Columns.''                      as "Column"
        ,0                                                          as "Count"
      );
   
   HAS_RESULTS:=(SELECT COUNT(*) FROM dwtable_lists);        
   VAR_RESULTS := (SELECT * from dwtable_lists);   

IF (:HAS_RESULTS > 0)  --Has VARiant COLUMNS.
    THEN
      RETURN TABLE(VAR_RESULTS); 
    ELSE
      RETURN TABLE(VAR_NORESULTS);
  END IF;
                    
END';