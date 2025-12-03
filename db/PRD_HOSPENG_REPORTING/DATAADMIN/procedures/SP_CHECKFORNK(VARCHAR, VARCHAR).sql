CREATE OR REPLACE PROCEDURE "SP_CHECKFORNK"("REPORTTYPE" VARCHAR(1), "TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- TABLENAME VARCHAR       := ''ADDRESS_DIM'';
        -- --TABLENAME VARCHAR       := ''ORDER_SUMMARY'';        
        -- REPORTTYPE VARCHAR(1)   := ''L'';  --valid values {L,P,S}

        NK_COLUMN VARCHAR       := :TABLENAME || ''_NK'';
        SCHEMANAME VARCHAR(075) := CASE UPPER(REPORTTYPE) 
                                        WHEN ''L'' THEN ''DATAADMIN'' 
                                        WHEN ''P'' THEN ''DATAWAREHOUSE''
                                        WHEN ''S'' THEN ''DATAWAREHOUSE_TEMP''
                                        ELSE ''WRONG'' END;
        HAS_RESULTS int;
        NK_RESULTS resultset    ;
        NK_NORESULTS resultset    ;

BEGIN
   DROP TABLE IF EXISTS dwtable_lists;  

    SELECT c.table_name                 as TABLE_NAME
        ,c.column_name                  as NATURAL_KEY
        ,TRUE                           as HAS_PK
        ,''This table has a primary key'' as MESSAGE
    FROM information_schema.columns c
         WHERE c.table_name   = c.table_name
            AND c.table_schema = :SCHEMANAME
            AND c.table_name   = :TABLENAME
            AND c.column_name  = :NK_COLUMN
        ;
        
   CREATE TABLE dwtable_lists AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    NK_RESULTS   := (select * from dwtable_lists);
    NK_NORESULTS := (
      SELECT :TABLENAME                  as TABLE_NAME
        ,:NK_COLUMN                      as NATURAL_KEY
        ,FALSE                           as HAS_PK
        ,''This table has NO primary key'' as MESSAGE);
    HAS_RESULTS:=(SELECT COUNT(*) FROM dwtable_lists);

IF (:HAS_RESULTS > 0)  --Has primary key
    THEN
      RETURN TABLE(NK_RESULTS); 
    ELSE
      RETURN TABLE(NK_NORESULTS);
  END IF;
                    
END';