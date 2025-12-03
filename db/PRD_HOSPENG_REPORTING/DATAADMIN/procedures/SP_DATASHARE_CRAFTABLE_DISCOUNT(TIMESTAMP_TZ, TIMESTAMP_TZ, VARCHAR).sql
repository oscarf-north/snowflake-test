CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_DISCOUNT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  reportSet resultset;
  -- startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';
  -- enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z'';
  -- locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today char(11)              := CURRENT_DATE()::date::VARCHAR(10);
-----------------------------------------------------------------------------------------------------------------------
BEGIN
-----------------------------------------------------------------------------------------------------------------------
    reportSet   := ( 
            SELECT 
             ORG.ORGANIZATION                           AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK                    AS "Organization ID"
            ,LOC.LOCATIONNAME                           AS "Location Name"
            ,CHF.LOCATION_DIM_FK                        AS "Location ID" 
            ,CHF.FISCAL_DATE::DATE                      AS "Business Day"
            ,CHF.DISCOUNTITEM_FACT_NK                    AS "Discount ID"
            ,CHF.DISCOUNTLEVEL                          AS "Discount Level"
            ,STD.STANDARDDISCOUNTNAME                   AS "Discount Name"
            ,CHF.CHEQUENUMBER                           AS "Check Number"  
            ,CHF.CHEQUE_FACT_FK                         AS "Check ID"
            ,ITF.NAME                                   AS "Item Name"
            ,ITF.ITEM_FACT_NK                           AS "Item ID"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'')     AS "Employee for Discount (approver)"
            ,CHF.APPLIED_AMOUNT  ::NUMBER(18,2)         AS "Discount Amount"
            ,CASE WHEN (CHF.DISCOUNTREASON = ''CASHDISCOUNT'') 
              THEN CHF.APPLIED_AMOUNT ELSE 0 END        AS "Cash Discount Amount"
            ,"Discount Amount" - "Cash Discount Amount" AS  "Disc Amount No Cash"
            
        FROM DATAWAREHOUSE.DISCOUNTITEM_FACT                      CHF
            INNER JOIN DATAWAREHOUSE.ITEM_FACT                    ITF
              ON CHF.ITEM_FACT_FK = ITF.ITEM_FACT_NK
                  AND ITF.ITEMSTATUS IN (''Added'',''Sent'')
                  AND NOT CHF.STATUS  = ''Disabled''
                  AND CHF.CHEQUESTATUS  IN (''Closed'')
                  -- AND CHF.DISCOUNTREASON <> ''CASHDISCOUNT''
                  AND CHF.FISCAL_DATE::date
                      >= :startdate::date
                  AND CHF.FISCAL_DATE::date   
                      <= :enddate ::date 
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.DW_ISCURRENTROW
                  AND ITF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED                
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 LOC  
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                 AND LOC.DW_ISCURRENTROW  
            INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM             ORG
                ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
                    AND ORG.DW_ISCURRENTROW
            INNER JOIN DATAWAREHOUSE.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW
            -- INNER JOIN DATAWAREHOUSE.DAYPART_DIM                  DAD
            --   ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
            --       AND DAD.DW_ISCURRENTROW
            -- LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_ADD
            --   ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
            --     AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW
    UNION ALL

        SELECT
             ORG.ORGANIZATION                           AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK                    AS "Organization ID"
            ,LOC.LOCATIONNAME                           AS "Location Name"
            ,CHF.LOCATION_DIM_FK                        AS "Location ID" 
            ,CHF.FISCAL_DATE::DATE                      AS "Business Day"
            ,CHF.DISCOUNTCHECK_FACT_NK                   AS "Discount ID"
            ,CHF.DISCOUNTLEVEL                          AS "Discount Level"
            ,STD.STANDARDDISCOUNTNAME                   AS "Discount Name"
            ,CHF.CHEQUENUMBER                           AS "Check Number"  
            ,CHF.CHEQUE_FACT_FK                         AS "Check ID"
            ,null                                       AS "Item Name"
            ,null                                       AS "Item ID"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'')     AS "Employee for Discount (approver)"
            ,CHF.APPLIED_AMOUNT  ::NUMBER(18,2)         AS "Discount Amount" 
            ,CASE WHEN CHF.DISCOUNTREASON = ''CASHDISCOUNT'' 
              THEN CHF.APPLIED_AMOUNT ELSE 0 END        AS "Cash Discount Amount"
            ,"Discount Amount" - "Cash Discount Amount" AS "Disc Amount No Cash"
        --------------------------------------------------------------------------------------------------------
        FROM DATAWAREHOUSE.DISCOUNTCHECK_FACT                     CHF
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 LOC
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                  AND CHF.FISCAL_DATE::date
                      >= :startdate::date
                  AND CHF.FISCAL_DATE::date   
                      <= :enddate ::date 
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.CHEQUESTATUS IN (''Closed'')
                  AND NOT CHF.STATUS = ''Disabled''
                  -- AND CHF.DISCOUNTREASON <> ''CASHDISCOUNT''
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  AND LOC.DW_ISCURRENTROW          
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM                                        ORG
                ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
                AND ORG.DW_ISCURRENTROW
            -- INNER JOIN DATAWAREHOUSE.DAYPART_DIM                 DAD
            --   ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
            --       AND DAD.DW_ISCURRENTROW                  
            LEFT JOIN DATAWAREHOUSE.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW
            -- LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_ADD
            --   ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
            --     AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW        
); 
--=====================================================================================================================
RETURN TABLE(reportSet);
END';