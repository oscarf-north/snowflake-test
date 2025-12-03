CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_EXTRACTONEFILE"("FISCALDATE" VARCHAR(16777216), "LOCATIONID" VARCHAR(16777216), "FILE" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  -- locationid   string := ''[351]'';
  -- fiscaldate   string := ''2024-12-18'';
  -- file         string := ''CheckHeader'';
  reportSet    resultset;  
  sproc        string := ''DATAADMIN.SP_LOAD_AVERO_'' || :file; 
  fiscalform   string := to_char(to_date(:fiscaldate),''YYYYMMDD'');
  filelocation string := REPLACE(REPLACE(locationid,''['',''''),'']'');
  stagename    string := ''@DATAADMIN.STAGE-AVERO/AVEROFTP/Location'' || :filelocation || ''/'' || :fiscalform || ''/'' 
                            -- || :file || ''_Location'' || :filelocation || ''_'' || :fiscalform  || ''.csv'';
                            || :file ||  ''.csv'';
  sqlstatement string := ''COPY INTO <@stagename>   
                          FROM (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())))
                          FILE_FORMAT=(TYPE = CSV COMPRESSION=\\''NONE\\'')
                          HEADER     = TRUE
                          SINGLE     = TRUE
                          OVERWRITE  = TRUE'';
--=================================================================================================================================
BEGIN
    --Retreive the data from the Datawarehouse via stored procedure
    CALL IDENTIFIER(:sproc)(:fiscaldate,:fiscaldate,:locationid);
  
--=================================================================================================================================
reportSet := (  
  EXECUTE IMMEDIATE (REPLACE(:sqlstatement,''<@stagename>'',:stagename))

--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';