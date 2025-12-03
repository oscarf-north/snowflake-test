CREATE OR REPLACE PROCEDURE "SP_LOAD_365_TENDERS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  -- GRANT usage ON procedure dataadmin.SP_LOAD_365_TENDERS(timestamp_tz,timestamp_tz,string) TO ROLE matillion;
--=================================================================================================================================
BEGIN
--=================================================================================================================================
reportSet := (  
-----------------------------------------------------------------------------------------------------------------------------------
SELECT PAY.PAYMENTMETHOD_DIM_NK                         AS  Number  --*sTRING 
  ,IFNULL(REPLACE(PAY.PAYMENTMETHODNAME,'','',''''),''None'') AS  Name    --*sTRING 
  ,IFNULL(REPLACE(PAY.PAYMENTMETHODTYPE,'','',''''),''None'') AS  Type    -- string
FROM DATAWAREHOUSE.PAYMENTMETHOD_DIM                        PAY
  INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM                 ORG
    ON PAY.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
      AND PAY.DW_ISCURRENTROW
      AND ORG.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                      LOC
    ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
      AND LOC.DW_ISCURRENTROW
      AND (LOC.LOCATION_DIM_NK in (SELECT table1.value 
        FROM table(split_to_table(:locationidS, '',''))  table1)    
          OR PAY.PAYMENTMETHOD_DIM_NK = -1
        )
GROUP BY PAY.PAYMENTMETHOD_DIM_NK
  ,Name
  ,Type

--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';