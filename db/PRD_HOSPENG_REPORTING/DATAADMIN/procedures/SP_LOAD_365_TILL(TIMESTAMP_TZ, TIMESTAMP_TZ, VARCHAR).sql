CREATE OR REPLACE PROCEDURE "SP_LOAD_365_TILL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--==================================================================================================================================
BEGIN
--==================================================================================================================================
--ISSUE
-- business day is supposed to be fiscal day, but is formatted with hours, minutes, and seconds
-- we used to send open amount, but there is no field for that column
--==================================================================================================================================
reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT TO_CHAR(TO_TIMESTAMP(SHD.FISCAL_DAY),''MM/DD/YYYY HH24:MI:SS'') AS BusinessDate  --* DateTime(mm/dd/yyyy hh:mm:ss)
    ,CBD.CASHBANK_DIM_NK                                             AS DrawerNumber  --* string
    ,CBD.OPEN_AMOUNT::DECIMAL(20,2)                                 AS ExpectedAmount  --*decimal
    FROM DATAWAREHOUSE.CASHBANK_DIM            CBD
      INNER JOIN DATAWAREHOUSE.SHIFT_DIM       SHD
        ON CBD.SHIFT_DIM_FK = SHD.SHIFT_DIM_NK
          AND SHD.DW_ISCURRENTROW
          AND CBD.DW_ISCURRENTROW
          AND SHD.fiscal_day::date
            >= :startdate::date  
          AND  SHD.fiscal_day::date  
            <= :enddate::date    
          AND SHD.LOCATION_DIM_FK IN ( SELECT table1.value 
            FROM table(split_to_table(:locationidS, '',''))  table1)    
--==================================================================================================================================
);
RETURN TABLE(reportSet); 
END';