CREATE OR REPLACE PROCEDURE "SP_LOAD_365_TAXDEFINITIONS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');

--=================================================================================================================================
BEGIN
--=================================================================================================================================
reportSet := (  

------------------------------------------------------------------------------------------------------------------------------------
SELECT TRD.TAXRATE_DIM_NK                     AS Number --* string
    ,TRD.TAXRATE                              AS Name   --* string
  FROM DATAWAREHOUSE.TAXRATE_DIM              TRD
    INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM ORG
      ON TRD.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK 
        AND TRD.DW_ISCURRENTROW
        AND ORG.DW_ISCURRENTROW
    INNER JOIN DATAWAREHOUSE.LOCATION_DIM      LOC
      ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
      AND (LOC.LOCATION_DIM_NK IN (
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)   
            OR TRD.TAXRATE_DIM_NK = -1    )
    GROUP BY TRD.TAXRATE_DIM_NK,TRD.TAXRATE
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';