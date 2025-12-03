CREATE OR REPLACE PROCEDURE "SP_REPORT_GIFTCARDAGING"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- locationid string      := ''[4,352,351,1,2,3,5,6,7]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--================================================================================================================================
BEGIN
--================================================================================================================================
--Find all of the locations for a merchant
DROP TABLE if exists TEMP_LOCS;

SELECT MOD.MERCHANT_DIM_NK
  ,MAX(MOD.MERCHANT) AS "Merchant"
  FROM DATAADMIN.MERCHANT_DIM             MOD
    INNER JOIN DATAWAREHOUSE.MERCHANT_ORGANIZATION_XREF MOX
      ON MOD.MERCHANT_DIM_NK = MOX.MERCHANT_DIM_FK
        AND MOD.DW_ISCURRENTROW
        AND MOX.DW_ISCURRENTROW
    INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM           ORG
      ON ORG.ORGANIZATION_DIM_NK = MOX.ORGANIZATION_DIM_FK
        AND ORG.DW_ISCURRENTROW
    INNER JOIN DATAWAREHOUSE.LOCATION_DIM               LOC
      ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
        AND LOC.DW_ISCURRENTROW
        AND LOCATION_DIM_NK in (SELECT table1.value 
        FROM table(split_to_table(:locationidS, '',''))  table1)
    GROUP BY MOD.MERCHANT_DIM_NK;

 CREATE TEMP TABLE TEMP_LOCS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
     
--================================================================================================================================
reportSet := (   
    SELECT gcd.giftcard_dim_nk                                                     AS "Support ID" 
       ,''GCA-'' ||row_number() over (order by gcd.giftcard_dim_nk)                  AS "Detail ID"  
--Status, category, level--------------------------------------------------------------------------------------------------------
--Geography-----------------------------------------------------------------------------------------------------------------------
--No location or revenue center since gift cards are at the merchant level
--Dates-------------------------------------------------------------------------------------------------------------------------
--Dates are utc since gift cards are at merchant level and a location time zone can''t be assigned
       ,IFNULL(TO_CHAR(gcd.CREATED_AT::DATE),''None'')                               AS "Last Redeemed At"                    
       ,IFNULL(TO_CHAR(gcd.ISSUED_AT::DATE),''None'')                                AS "Issued At"
       ,IFNULL(TO_CHAR(trn."Last Reloaded At"::DATE),''None'')                       AS "Last Reloaded At"  
       ,IFNULL(TO_CHAR(gcd.BALANCE_REQUESTED_AT::DATE),''None'')                     AS "Last Balance Check At" 
-- --Flags-----------------------------------------------------------------------------------------------------------------------  
        ,gcd.IS_LEGACY::boolean                                                    AS "Is Imported"   
        ,IFNULL(gcd.GIFTCARD,''None'')                                               AS "Gift Card Number" 
        ,IFNULL(mer."Merchant",''None'')                                             AS "Merchant"
-- --Facts-------------------------------------------------------------------------------------------------------------------------
        ,IFNULL(DATEDIFF(DAY,gcd.ISSUED_AT::date,CURRENT_DATE()::DATE),0)
        ::NUMBER(18,0)                                                             AS "Days Old" 
        ,IFNULL(GCD.BALANCE_REQUEST_COUNT,0)::NUMBER(18,0)                         AS "Balance Check Count" 
        ,IFNULL(gcd.START_BALANCE,0)::NUMBER(18,2)                                 AS "Issued Amount"
        ,IFNULL(trn."Reload Amount",0)::NUMBER(18,2)                               AS "Reload Amount"    
        ,IFNULL(trn."Redeemed Amount",0)::NUMBER(18,2)                             AS "Redeemed Amount"   
        ,IFNULL(gcd.BALANCE,0)::NUMBER(18,2)                                       AS "Balance Amount"                                     
---------------------------------------------------------------------------------------------------------------------------------
    FROM DATAWAREHOUSE.GIFTCARD_DIM                      gcd
      INNER JOIN TEMP_LOCS                               mer
        ON gcd.MERCHANT_DIM_FK = mer.MERCHANT_DIM_NK
          AND gcd.BALANCE > 0.000  
          AND gcd.IS_ISSUED
          AND gcd.DW_ISCURRENTROW
          AND NOT gcd.DW_ISDELETED

      LEFT JOIN (   --LOJ because not all cards will have an issuing transaction(legacy cards) or a relaod or redemption
      
              SELECT gcf.GIFTCARD_DIM_FK
                    ,SUM(CASE WHEN GCF.COMMAND IN (''Reload'',''VoidReload'') THEN 
                       CASE WHEN GCF.OPENING_BALANCE IS  NULL OR GCF.GIFTCARD_DIM_Fk = 1 THEN GCF.TRANSACTION_AMOUNT ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )  
                                                                              AS "Reload Amount"
                    ,MAX(CASE WHEN GCF.COMMAND IN (''Reload'',''VoidReload'') THEN CHF.FISCAL_DATE ELSE NULL END)          
                                                                              AS "Last Reloaded At"  
                    ,SUM(CASE WHEN GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'') THEN 
                          CASE WHEN GCF.OPENING_BALANCE IS NULL OR GCF.GIFTCARD_DIM_Fk = 1  
                            THEN GCF.TRANSACTION_AMOUNT * (-1) ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )  
                                                                              AS "Redeemed Amount"  
                    ,MAX(CASE WHEN GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'') THEN CHF.FISCAL_DATE ELSE NULL END)          
                                                                              AS "Last Redeemed At"                                                  
                  FROM DATAWAREHOUSE.GIFTCARD_DIM                             GCD
                    INNER JOIN TEMP_LOCS                                      LOC
                      ON LOC.MERCHANT_DIM_nK = GCD.MERCHANT_DIM_FK
                         AND GCD.DW_ISCURRENTROW
                         AND GCD.IS_ISSUED
                         AND GCD.BALANCE > 0.0000
                         AND NOT gcd.DW_ISDELETED
                    INNER JOIN DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT          GCF  
                      ON GCD.GIFTCARD_DIM_NK = GCF.GIFTCARD_DIM_FK
                         AND GCF.DW_ISCURRENTROW
                         AND NOT GCF.DW_ISDELETED
                    INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                       CHF
                      ON CHF.CHEQUE_FACT_NK = GCF.CHEQUE_FACT_FK
                          AND CHF.STATUS = ''Closed''
                          AND CHF.DW_ISCURRENTROW
                    GROUP BY gcf.GIFTCARD_DIM_FK
                                                                                        ) trn
                           ON trn.GIFTCARD_DIM_FK = gcd.GIFTCARD_DIM_NK
                               AND gcd.BALANCE > 0.000  
                               AND gcd.IS_ISSUED
                               AND gcd.DW_ISCURRENTROW
                               AND NOT gcd.DW_ISDELETED                    
); 

-----------------------------------------------------------------------------------------------------------------------------------
RETURN TABLE(reportSet);
--=================================================================================================================================
END';