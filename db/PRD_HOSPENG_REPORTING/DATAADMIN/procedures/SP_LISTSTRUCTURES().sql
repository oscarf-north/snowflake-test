CREATE OR REPLACE PROCEDURE "SP_LISTSTRUCTURES"()
RETURNS TABLE ("Table Name" VARCHAR(16777216), "Conforming View" VARCHAR(16777216), "Data Warehouse Table" VARCHAR(16777216), "Procedure" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE res_list resultset;

BEGIN
   --drop temp tables if they exist
   drop table if exists dataadmin_list ; 
   drop table if exists datawarehouse_list; 
   drop table if exists procedure_list;
   
--Get list of datawarehouse tables
SELECT  TABLE_NAME 
     FROM information_schema.tables t
   WHERE  TABLE_SCHEMA = ''DATAWAREHOUSE''
     AND TABLE_TYPE    = ''BASE TABLE''
     AND (TABLE_NAME ILIKE ''%_DIM''
       OR TABLE_NAME ILIKE ''%_FACT''
       OR TABLE_NAME ILIKE ''%_XREF'')
ORDER BY T.TABLE_NAME  
;

CREATE TABLE datawarehouse_list AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

--Get list of admin tables
SELECT  TABLE_NAME 
     FROM information_schema.tables t
   WHERE  TABLE_SCHEMA = ''DATAADMIN''
     AND TABLE_TYPE    = ''VIEW''
     AND (TABLE_NAME ILIKE ''%_DIM''
       OR TABLE_NAME ILIKE ''%_FACT''
       OR TABLE_NAME ILIKE ''%_XREF'')
ORDER BY T.TABLE_NAME  
;

CREATE TABLE dataadmin_list AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  

     -- select * from dataadmin_list order by TABLE_NAME;  --47
     -- select * from datawarehouse_list order by TABLE_NAME;  --13

SHOW PROCEDURES LIKE ''SP_STAGELOAD%'' IN SCHEMA DATAADMIN;

CREATE TABLE procedure_list AS
     SELECT "name" as "PROCEDURE", REPLACE("name",''SP_STAGELOAD'','''') AS TABLE_NAME  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  

--Compare the objects in daaadmin vs datawarehouse
 
res_list  := (
  SELECT TABLE_NAME     AS "Table Name"
    ,MAX(dataadmin)     AS "Conforming View"
    ,MAX(datawarehouse) AS "Data Warehouse Table"
    ,MAX(PROCEDURE)     AS "Procedure"
  -- ,CASE WHEN datawarehouse is NULL THEN ''MISSING'' ELSE ''FOUND'' END AS STATUSMESSAGE
  FROM (
        SELECT dal.TABLE_NAME, dal.TABLE_NAME AS dataadmin, NULL as datawarehouse, NULL as PROCEDURE
          FROM dataadmin_list            dal
        UNION
        SELECT dwl.TABLE_NAME, NULL AS dataadmin, dwl.TABLE_NAME as datawarehouse, NULL as PROCEDURE
         FROM datawarehouse_list   dwl
        UNION
        SELECT prl.TABLE_NAME, NULL AS dataadmin, NULL as datawarehouse, prl.PROCEDURE as PROCEDURE
         FROM procedure_list   prl         
        ) GROUP BY TABLE_NAME
); 

RETURN TABLE(res_list);
--========================================================================================================================
END';