CREATE OR REPLACE PROCEDURE "SP_LOAD_365_SURCHARGES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');

--=================================================================================================================================
BEGIN
--=================================================================================================================================
reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT to_char(SUF.chequenumber) || ''.'' || to_char(SUF.cheque_fact_fk)          
                                   AS Surcharge_CheckNumber  --* string
    ,IFNULL(SUF.APPLIEDAMOUNT,0)   AS Surcharge_Amount       --* decimal
    ,SUD.SURCHARGE                 AS Surcharge_Name         --* string 
  FROM DATAWAREHOUSE.SURCHARGE_FACT          SUF
    INNER JOIN DATAWAREHOUSE.SURCHARGE_DIM   SUD
      ON SUF.SURCHARGE_DIM_NK = SUD.SURCHARGE_DIM_NK
        AND SUF.DW_ISCURRENTROW
        AND SUD.DW_ISCURRENTROW
      AND (SUF.FISCAL_DATE::date >= :startdate::date 
                AND SUF.FISCAL_DATE::date  <= :enddate::date)
      AND SUF.LOCATION_DIM_FK IN (
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)       
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';