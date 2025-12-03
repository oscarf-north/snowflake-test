CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_DAYPART"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
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
----------------------------------------------------------------------------------------------------------------------------------
SELECT TO_CHAR(:STARTDATE,''YYYYMMDD'')               AS BUSDATE-- Business Date Date as YYYYMMD
    ,CHF.REVENUECENTER_DIM_FK                       AS RVCNUM -- Revenue Center Number Number--"Revenue center number" 
    ,RCD.REVENUECENTER                              AS RVCDESC --Revenue Center Description String 40 characters--"Revenue center 
    ,DAD.DAYPART                                    AS DPDESC --Day Part Description String 40 characters Day part description LUNC
    ,TO_CHAR(TIMESTAMPADD(SECONDS,DAS.START_TIME/1000,''2024-01-01''::TIMESTAMP),''HH:MM'')                   
                                                    AS DPSTARTTIME --Day Part Period Start Time Time as HH24:MM 23:59 Start of da                                                 
FROM DATAWAREHOUSE.CHEQUE_FACT                      CHF
  INNER JOIN DATAWAREHOUSE.DAYPART_DIM              DAD
    ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
      AND DAD.DW_ISCURRENTROW
      AND CHF.DW_ISCURRENTROW
      AND CHF.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      AND NOT CHF.IS_TRAINING
      AND CHF.STATUS in (''Closed'')
          AND (CHF.FISCAL_DATE::date >= :startdate::date 
                AND CHF.FISCAL_DATE::date  <= :enddate::date) 
  INNER JOIN DATAWAREHOUSE.DAYPARTSCHEDULE_DIM        DAS
    ON DAD.DAYPART_DIM_NK = DAS.DAYPART_DIM_FK
      AND DAD.DW_ISCURRENTROW
      AND DAS.DW_ISCURRENTROW 
  INNER JOIN DATAWAREHOUSE.REVENUECENTER_DIM           RCD
    ON RCD.REVENUECENTER_DIM_NK = CHF.REVENUECENTER_DIM_FK
      AND RCD.DW_ISCURRENTROW
    
-- SELECT TO_CHAR(:STARTDATE,''YYYYMMDD'') AS BUSDATE-- Business Date Date as YYYYMMD
--     ,NULL AS RVCNUM -- Revenue Center Number Number--"Revenue center number" 
--     ,NULL AS RVCDESC --Revenue Center Description String 40 characters--"Revenue center description"
--     ,DAD.DAYPART AS DPDESC --Day Part Description String 40 characters Day part description LUNCH"Day part description"
--     ,DAS.START_TIME AS DPSTARTTIME --Day Part Period Start Time Time as HH24:MM 23:59 Start of day part.
-- FROM DATAWAREHOUSE.DAYPART_DIM                 DAD
--   INNER JOIN DATAWAREHOUSE.DAYPARTSCHEDULE_DIM DAS
--     ON DAD.DAYPART_DIM_NK = DAS.DAYPART_DIM_FK
--       AND DAD.DW_ISCURRENTROW
--       AND DAS.DW_ISCURRENTROW
--       AND DAS.LOCATION_DIM_FK IN (--351,352
--              SELECT table1.value 
--                   FROM table(split_to_table(:locationidS, '',''))  table1)
--     INNER JOIN REVENUECENTER_DIM                RCD
--       ON RCD.LOCATION_DIM_FK = DAD.LOCATION_DIM_FK
--         AND RCD.DW_ISCURRENTROW


--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';