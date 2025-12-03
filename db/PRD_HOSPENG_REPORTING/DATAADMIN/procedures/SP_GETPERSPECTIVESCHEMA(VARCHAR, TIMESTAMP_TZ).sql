CREATE OR REPLACE PROCEDURE "SP_GETPERSPECTIVESCHEMA"("SPROCNAME" VARCHAR(500), "STARTDATE" TIMESTAMP_TZ(9))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
--=================================================================================================================
--This sproc creates the schema for a Perspective Report from the Source Data Stored Proceedure
-- call SP_GetPerspectiveSchema;
--=================================================================================================================
DECLARE 
--sprocName VARCHAR(500):= ''DATAADMIN.SP_REPORT_PMIX'';
 -- startdate timestamp_tz :=      ''2000-11-20T14:48:37.661Z'';  
  enddate timestamp_tz :=        ''2023-11-20T14:48:37.661Z''; 
  locationid number :=            351;
  dataout resultset;  

BEGIN
  drop table if exists schema_data; 
  CALL DATAADMIN.SP_REPORT_PMIX(''2000-11-20T14:48:37.661Z'',''2023-11-20T14:48:37.661Z'',351) ;
 
  CREATE TABLE schema_data AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  ALTER TABLE schema_data ADD COLUMN MAXROW NUMBER;

  dataout := (

  SELECT   ''"schema": {\\n''
    || LISTAGG (''  '' || ''"'' 
     || COLUMN_NAME 
     || ''" : '' 
     || CASE DATA_TYPE 
       WHEN ''TEXT'' THEN ''"str"'' 
       WHEN ''NUMBER'' THEN ''"float"''
       WHEN ''TIMESTAMP_TZ'' THEN ''"datetime"'' 
       WHEN ''BOOLEAN'' THEN ''"bool"''
      ELSE DATA_TYPE END 
      || '','' 
      || ''\\n''
      ) || ''}''
      AS DATA_TYPE 
    FROM information_schema.columns 
    WHERE table_name=''SCHEMA_DATA'' 
    ORDER BY ORDINAL_POSITION
    );
    

  return table(dataout);
END
';