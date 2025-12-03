CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_GRATUITIES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  reportSet resultset;
--   startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';
--   enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z'';
--   locationid string           := ''[37,7,1,35,9,38,32,27,13,4,41,29,43,11,21,25,3,6,39]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today char(11)              := CURRENT_DATE()::date::VARCHAR(10);
-----------------------------------------------------------------------------------------------------------------------
BEGIN
-----------------------------------------------------------------------------------------------------------------------
     reportSet := (
         SELECT 
            ORG.ORGANIZATION                             AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK                     AS "Organization ID"
            ,LOC.LOCATIONNAME                            AS "Location Name"
            ,SF.LOCATION_DIM_FK                          AS "Location ID"
            ,CHK.FISCAL_DATE::DATE                       AS "Business Day"
            ,CHK.CHEQUENUMBER                             as "Check Number"
            ,SF.CHEQUE_FACT_FK                           as "Check ID"
            ,SF.IS_GRATUITY                              as "Is Gratuity Flag"
            ,SD.SURCHARGE                                as "Gratuity Name"
            ,SF.SURCHARGE_FACT_NK                        as "Gratuity ID"
            ,IFNULL(SF.APPLIEDAMOUNT,0)                  as "Gratuity Amount"
            ,IFNULL(emd.EMPLOYEE_NAME,''None'')            as "Employee for Gratuity"
            --for debugging:
            -- ,SF.MTLN_CDC_SEQUENCE_NUMBER                  as "MTLN_CDC_SEQUENCE_NUMBER"
            -- ,SUM(IFNULL(SF.APPLIEDAMOUNT,0)) OVER()      as "Total Applied Amount"
            -- ,SUM(IFNULL(SF.AMOUNT,0)) OVER()             as "Total Amount"
            -- ,COUNT(SF.SURCHARGE_FACT_NK) OVER()          as "Count Surcharge NKs"
            -- ,COUNT(DISTINCT SF.SURCHARGE_FACT_NK) OVER() as "Count DISTINCT Surcharge NKs"
         ---------------------------------------------------------------------------------------------------------------
         FROM DATAWAREHOUSE.SURCHARGE_FACT SF
         INNER JOIN DATAWAREHOUSE.CHEQUE_FACT CHK
            ON SF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
            AND SF.MTLN_CDC_SEQUENCE_NUMBER = CHK.MTLN_CDC_SEQUENCE_NUMBER
            -- AND SF.DW_ISCURRENTROW --instead of filtering on current row. we join con sequence number
            AND SF.IS_GRATUITY
            AND NOT SF.DW_ISDELETED 
            AND NOT SF.IS_TRAINING
            AND SF.STATUS in (''Enabled'')
            AND CHK.DW_ISCURRENTROW
            AND NOT CHK.IS_TRAINING
            AND NOT CHK.IS_VOID
            AND NOT CHK.DW_ISDELETED
            AND CHK.STATUS IN (''Closed'')
            AND SF.LOCATION_DIM_FK IN (--351,352
                SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1)
            AND (chk.FISCAL_DATE::date >= :startdate::date 
                  AND chk.FISCAL_DATE::date  <= :enddate::date)
         INNER JOIN DATAWAREHOUSE.SURCHARGE_DIM SD
            ON SF.SURCHARGE_DIM_NK = SD.SURCHARGE_DIM_NK
            AND SD.DW_ISCURRENTROW
         INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                     emd
            ON SF.EMPLOYEE_DIM_FK = emd.employee_dim_Nk
            AND emd.DW_ISCURRENTROW 
        INNER JOIN DATAWAREHOUSE.LOCATION_DIM LOC
            ON SF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
            AND LOC.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM ORG
            ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
         
         WHERE 1=1 
            AND SF.IS_GRATUITY
            -- AND SF.CHEQUE_FACT_FK = 166174
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';