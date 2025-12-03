CREATE OR REPLACE PROCEDURE "SP_REPORTDATAGROOM"("REPORT" VARCHAR(500), "DAYS" NUMBER(38,0), "LOCATIONID" NUMBER(38,0))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
--=================================================================================================================
--This sproc interrogates report output.
--    creates perspective schemas
--    looks for cardinality issues with report data
--call DATAADMIN.SP_ReportDataGroom(''SP_REPORT_PMIX'',365,351)
--=================================================================================================================
DECLARE 
  dataout resultset; 
--------------------------------------------------------------------------------------------------------------------
 -- report VARCHAR(500):= ''SP_REPORT_DISCOUNT'';
 -- days number        := 365;  
 -- locationid number  := 351;
------------------------------------------------------------------------------------------------------------------
 sprocname VARCHAR(500);
 maxordinal number;
 enddate timestamp_tz   := (select current_timestamp()); 
 startdate timestamp_tz := dateadd(day,days * -1,enddate); 
 calltext VARCHAR(500)  := ''CALL DATAADMIN.'' || :report
                            || ''('''''' || :startdate  || '''''','''''' || :enddate || '''''','' || :locationid || '')''
                            ;

BEGIN
   drop table if exists report_data; 
   drop table if exists sproc_lists;
   drop table if exists error_lists;
   drop table if exists schema_lists;
   drop table if exists perspective_schema;
   drop table if exists column_list;
   drop table if exists aggregate_list;
   
  --get all the reporting sprocs
  SELECT * 
    FROM INFORMATION_SCHEMA.PROCEDURES
  WHERE PROCEDURE_NAME ILIKE ''%_REPORT_%''
    AND PROCEDURE_SCHEMA = ''DATAADMIN''
    AND PROCEDURE_NAME = :report
    ;

  CREATE TABLE sproc_lists AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  --load report data into temp table  
  sprocname:= (SELECT TOP 1 PROCEDURE_NAME FROM sproc_lists);

--return(''SPROCNAME:'' || :sprocname || ''\\n'' || ''calltext: '' || :calltext);
-- return(:calltext);
  
  EXECUTE IMMEDIATE :calltext;
  CREATE TABLE report_data AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 


--BEGIN:  look for cardinality issues
    BEGIN
      SELECT ''For the SQL: '' || :calltext || ''\\n'' || ''Error Count: '' || Count(*)  || ''\\n''|| LISTAGG("ID Count") AS "Error List"
            FROM (SELECT  COUNT(*) || '' rows for Support ID '' || "Support ID" || '' \\n '' AS "ID Count"
                  FROM report_data GROUP BY "Support ID" HAVING COUNT(*)  > 1 ORDER BY COUNT(*) DESC);
                  
    CREATE TABLE error_lists AS 
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 
    END;  

--BEGIN:  create perspective schema  
    BEGIN
     
        SELECT  
        ''"schema": {\\n''
          || LISTAGG (''  '' || ''"'' 
          || COLUMN_NAME   
          || ''" : '' 
          || CASE WHEN DATA_TYPE = ''NUMBER'' AND  NUMERIC_SCALE  > 0 THEN ''"float"'' 
               ELSE CASE WHEN DATA_TYPE = ''NUMBER'' AND  NUMERIC_SCALE  = 0 THEN ''"int"'' 
                 ELSE CASE WHEN DATA_TYPE =''TEXT'' THEN ''"str"'' 
                   ELSE CASE WHEN DATA_TYPE = ''TIMESTAMP_TZ'' THEN ''"datetime"''
                     ELSE CASE WHEN DATA_TYPE = ''TIMESTAMP_NTZ'' THEN ''"datetime"''
                       ELSE CASE WHEN DATA_TYPE = ''BOOLEAN'' THEN ''"bool"''
                         ELSE CASE WHEN DATA_TYPE = ''DATE'' THEN ''"date"''
                           ELSE DATA_TYPE
          
          END END END END END END END 
           || '','' 
           || ''\\n''
        )  WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        || ''}''
          AS DATA_TYPE 
     FROM information_schema.columns 
    WHERE table_name=''REPORT_DATA'';

  
    CREATE TABLE perspective_schema AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));      
  END; 


  BEGIN

  SELECT LISTAGG (''  '' || ''"'' 
          || COLUMN_NAME   
          || ''",'' 
          || ''\\n''
        )  WITHIN GROUP (ORDER BY ORDINAL_POSITION) 
          AS DATA_COLUMNS 
     FROM information_schema.columns 
    WHERE table_name=''REPORT_DATA'';

 CREATE TABLE column_list AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
  END;

  BEGIN

  SELECT LISTAGG (''  '' || ''"'' 
          || COLUMN_NAME  
          || ''" : '' 
          || CASE WHEN DATA_TYPE = ''NUMBER'' AND COLUMN_NAME <> ''Support ID'' AND COLUMN_NAME NOT ILIKE ''%percent%''
           AND COLUMN_NAME NOT ILIKE ''% ID'' AND COLUMN_NAME NOT ILIKE ''%year%''
          AND NUMERIC_SCALE  > 0 THEN ''"sum"'' 
               ELSE CASE WHEN DATA_TYPE = ''NUMBER'' AND  NUMERIC_SCALE  = 0 AND COLUMN_NAME <> ''Support ID'' 
               AND COLUMN_NAME NOT ILIKE ''%percent%'' AND COLUMN_NAME NOT ILIKE ''% ID'' AND COLUMN_NAME NOT ILIKE ''%year%''
                 THEN ''"sum"'' 
                 ELSE CASE WHEN DATA_TYPE =''TEXT'' THEN ''"unique"'' 
                   ELSE CASE WHEN DATA_TYPE = ''TIMESTAMP_TZ'' THEN ''"unique"'' 
                     ELSE CASE WHEN DATA_TYPE = ''TIMESTAMP_NTZ'' THEN ''"unique"'' 
                       ELSE CASE WHEN DATA_TYPE = ''BOOLEAN'' THEN ''"unique"'' 
                         ELSE CASE WHEN DATA_TYPE = ''DATE'' THEN ''"unique"'' 
                           ELSE ''"unique"'' 
                   END END END END END END END

          
          || '','' 
          || ''\\n''
        )  WITHIN GROUP (ORDER BY ORDINAL_POSITION) 
          AS DATA_COLUMNS 
     FROM information_schema.columns 
    WHERE table_name=''REPORT_DATA''
      AND column_name <> ''Support ID'';

 CREATE TABLE aggregate_list AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
  END;

  

  dataout := (SELECT ''Error List'' AS " "
                 ,IFNULL("Error List",''No Errors'') as "Report Info" 
                FROM error_lists
                   UNION
              SELECT ''Perspective Schema'',* FROM perspective_schema
                   UNION
              SELECT ''Column List'',left(DATA_COLUMNS,LENGTH(DATA_COLUMNS) - 2) FROM column_list   
                   UNION
              SELECT ''Aggregate List'',left(DATA_COLUMNS,LENGTH(DATA_COLUMNS) - 2) FROM aggregate_list
               );
               
  return table(dataout);
  
END;  --end of sproc
';