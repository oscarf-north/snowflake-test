CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_EXTRACTALLFILES"("FISCALDATE" VARCHAR(16777216), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet         resultset;
  -- locationid string := ''[709]'';
  -- fiscaldate string := ''2024-12-17'';
  
--=================================================================================================================================
BEGIN
 CALL dataadmin.SP_LOAD_AVERO_EXTRACTONEFILE(:fiscaldate,:locationid,''CheckHeader'');
 CALL dataadmin.SP_LOAD_AVERO_EXTRACTONEFILE(:fiscaldate,:locationid,''CheckDetail'');
 CALL dataadmin.SP_LOAD_AVERO_EXTRACTONEFILE(:fiscaldate,:locationid,''CheckTransfer'');
 CALL dataadmin.SP_LOAD_AVERO_EXTRACTONEFILE(:fiscaldate,:locationid,''MenuItemDetail'');
 CALL dataadmin.SP_LOAD_AVERO_EXTRACTONEFILE(:fiscaldate,:locationid,''Daypart'');

 -- ----------------------------------------------------------------------------------------------------------------------------------
 -- DROP TABLE if exists TEMP_FILES;
 --  LIST @DATAADMIN.STAGE-AVEROFTP;
  
 --  CREATE TEMP TABLE TEMP_FILES AS
 --     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

 --  SELECT "name",* FROM TEMP_FILES TEF WHERE "name" ILIKE (''s3://test-mqa-metl/AVEROFTP/Location351/20241218/%.csv'') order by TEF."name";
  
  
--=================================================================================================================================
reportSet := (  
  select ''Complete'' as STATUS
);
RETURN TABLE(reportSet); 
END';