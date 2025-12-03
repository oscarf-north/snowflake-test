CREATE OR REPLACE PROCEDURE "SP_LOADALLDWTABLES"("VALIDATE_DATE" VARCHAR(5))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
----------------------------------------------------------------------------------------------------------------------
  IsTableName      boolean;
  res_list         resultset;
  -- validate_date    varchar  := ''TRUE'';
  SQLText VARCHAR(75)   := ''CALL DATAADMIN.<SPROCNAME>(<VALIDATE_DATE>);''; 
----------------------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------------------
--put list of proceedures in a temp table
SHOW PROCEDURES 
  LIKE ''SP_STAGELOAD%''
    IN SCHEMA DATAADMIN; 

--read all of the load proceedures that eist into a result set
res_list := (SELECT REPLACE(REPLACE(:SQLText,''<SPROCNAME>'',"name"),''<VALIDATE_DATE>'',:validate_date) AS ExecStmt
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

DECLARE cur1 CURSOR FOR res_list;
  BEGIN
    FOR row_variable IN cur1 DO
      EXECUTE IMMEDIATE row_variable.ExecStmt;
    END FOR;
  END;

-- ---------------------------------------------------------------------------------------------------------------------- 
--RETURN condition message
RETURN (''INFORMATIONAL MESSAGE:  Load Complete'');

--========================================================================================================================
END';