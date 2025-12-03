CREATE OR REPLACE PROCEDURE "SP_UPDATEDWTABLE"("SCHEMANAME" VARCHAR(50), "TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- DBNAME VARCHAR(45)    := ''DEV_HOSPENG_REPORTING'';--''CHEQUE_FACT'';--
        -- SCHEMANAME VARCHAR(45):= ''DATAWAREHOUSE'';--''CHEQUE_FACT'';--
        -- TABLENAME VARCHAR(50) := ''ITEM_FACT'';

        IsTableName            INT;
        col_list               RESULTSET;
        res                    RESULTSET;
       
        ErrorMessage resultset:= (SELECT ''Invalid Table name.'' AS MESSAGE);
        SuccessMessage resultset:= (SELECT ''Successful Table load.'' AS MESSAGE);        
        -- db_name varchar       := UPPER(DBNAME);
        schema_name varchar   := UPPER(SCHEMANAME);
        table_name varchar    := UPPER(TABLENAME);
        sqltext1 := 
  $$
  UPDATE DATAWAREHOUSE_TEMP.<TABLE NAME> tab
  SET tab.DW_ISCURRENTROW = FALSE
     ,tab.DW_ENDDATE = inlt.DW_ENDDATE_UPDATE
     ,tab.DW_UPDATETIME = CURRENT_TIMESTAMP()
  FROM(
    SELECT <TABLE NAME>_PK
      ,<TABLE NAME>_NK
      ,DW_ISCURRENTROW
      ,DW_STARTDATE
      ,SUM(CASE WHEN DW_ISCURRENTROW THEN 1 ELSE 0 END) 
          OVER (PARTITION BY <TABLE NAME>_NK)                                  AS DW_ISCURRENTROW_COUNT
      ,TIMESTAMPADD(NANOSECOND,-1,LEAD(DW_STARTDATE) OVER (PARTITION 
        BY  <TABLE NAME>_NK ORDER BY DW_STARTDATE))                            AS DW_ENDDATE_UPDATE
      ,RANK()OVER(PARTITION BY DW_ISCURRENTROW,<TABLE NAME>_NK 
         ORDER BY DW_STARTDATE desc)
                                                                               AS IS_UPDATEROW  
    FROM DATAWAREHOUSE_TEMP.<TABLE NAME>
  )  INLT
WHERE TAB.<TABLE NAME>_PK = INLT.<TABLE NAME>_PK
  AND INLT.IS_UPDATEROW > 1
  AND INLT.DW_ISCURRENTROW_COUNT > 1
  AND INLT.DW_ISCURRENTROW
  ;
$$;

----------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------
--If the table is not found in the catalouge - retern an error message
IsTableName := (SELECT count(*)  FROM information_schema.tables t 
                   WHERE t.table_type = ''BASE TABLE''
                     AND t.table_schema = ''DATAWAREHOUSE_TEMP''  AND t.table_name = :table_name); 
  
--========================================================================================================  
--ONLY RETURN RESULTS IF INPUT IS VALID TABLE NAME
IF (:IsTableName = 0) 
  THEN
      RETURN TABLE(ErrorMessage);
  ELSE 
     EXECUTE IMMEDIATE(REPLACE(sqltext1,''<TABLE NAME>'',:table_name));
     RETURN TABLE(SuccessMessage);
END IF;

----------------------------------------------------------------------------------------------------------
END';