CREATE OR REPLACE PROCEDURE "SP_LOADALLDWTABLES"()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
----------------------------------------------------------------------------------------------------------------------
  SQLText VARCHAR(75)            := ''CALL DATAADMIN.<SPROCNAME>();'';
  IsTableName                    BOOLEAN;
  res_list RESULTSET;

----------------------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------------------
--put list of proceedures in a temp table
SHOW PROCEDURES 
  LIKE ''SP_STAGELOAD%''
    IN SCHEMA DATAADMIN; 

--read all of the load proceedures that eist into a result set
res_list := (SELECT REPLACE(:SQLText,''<SPROCNAME>'',"name") AS ExecStmt
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

-- res_list := (SELECT * from proc_list_temp plt );
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