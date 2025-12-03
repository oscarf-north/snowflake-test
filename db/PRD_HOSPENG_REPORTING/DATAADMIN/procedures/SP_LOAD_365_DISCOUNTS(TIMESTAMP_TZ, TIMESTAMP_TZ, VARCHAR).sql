CREATE OR REPLACE PROCEDURE "SP_LOAD_365_DISCOUNTS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
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
    SELECT TO_CHAR(CHF.chequenumber) || ''.'' || TO_CHAR(CHF.cheque_fact_Fk)
                                                               AS CompPromo_CheckNumber--* String
      ,IFNULL(CASE WHEN length(STANDARDDISCOUNTNAME) > 0 THEN STD.STANDARDDISCOUNTNAME ELSE ''None'' END,''None'')        
                                                               AS Comp_CompName       --* String name of discount
      ,IFNULL(CHF.APPLIED_AMOUNT ::NUMBER(18,2),0.00)          AS Comp_Amount         --* Amount subtracted from the total due todiscount.
      ,IFNULL(CHF.PROMOCODE,''None'')                            AS Promo_PromoName     --* String name of promotion
      ,0.00 ::NUMBER(18,2)                                     AS Promo_Amount        --* Amount subtracted from the total due to discount.
   FROM DATAWAREHOUSE.DISCOUNTCHECK_FACT                       CHF
            INNER JOIN DATAWAREHOUSE.CHEQUE_FACT               CHK
              ON CHF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
                AND CHF.DW_ISCURRENTROW
                AND CHK.DW_ISCURRENTROW
                AND NOT CHK.IS_TRAINING
                AND CHK.STATUS = ''Closed''
                AND NOT CHF.STATUS  = ''Disabled''
                AND 
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM              LOC
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                  AND CHF.FISCAL_DATE::date
                     >= :startdate::date
                  AND CHF.FISCAL_DATE::date   
                     <= :enddate ::date 
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.CHEQUESTATUS IN (''Closed'')
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  AND LOC.DW_ISCURRENTROW          
                  AND NOT CHF.IS_TRAINING 
         LEFT JOIN DATAWAREHOUSE.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW                  
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';