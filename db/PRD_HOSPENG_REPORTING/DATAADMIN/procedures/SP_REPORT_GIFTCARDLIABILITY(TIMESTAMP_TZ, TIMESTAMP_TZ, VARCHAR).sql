CREATE OR REPLACE PROCEDURE "SP_REPORT_GIFTCARDLIABILITY"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-02-13 00:00:00.000 -0800'';  
  -- enddate timestamp_tz   := ''2029-02-13 00:00:00.000 -0800''; 
  -- locationid string      := ''[1,2,3,4,5,6,7,8,9,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  
---====================================================================================================================
BEGIN

--================================================================================================================================
--Find all of the locations for a merchant
DROP TABLE if exists TEMP_LOCS;
DROP TABLE if exists TEMP_TRANS;
DROP TABLE if exists TEMP_CARDS;
DROP TABLE if exists TEMP_ISSUELOC;
DROP TABLE if exists TEMP_RPT;

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
--Get all card transactions beween the start and end dates
    SELECT GCF.GIFTCARD_DIM_FK                                                AS GIFTCARD_DIM_FK
        ,GCF.GIFTCARDTRANSACTION_FACT_NK                                      AS GIFTCARDTRANSACTION_FACT_NK
        ,GCD.GIFTCARD                                                         AS "Gift Card Number"  
        ,CHF.CHEQUENUMBER                                                     AS "Check"
        ,GCD.IS_LEGACY                                                        AS "Is Imported"
        ,CHF.FISCAL_DATE                                                      AS "Fiscal Date"
        ,GCD.ISSUED_AT                                                        AS "Issued At"
        ,LOD.LOCATIONNAME                                                     AS "Transaction Location"
        ,(CASE WHEN GCF.COMMAND IN (''Reload'',''VoidReload'') THEN 
            CASE WHEN GCF.OPENING_BALANCE IS  NULL OR GCF.GIFTCARD_DIM_Fk = 1 
              THEN GCF.TRANSACTION_AMOUNT ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )  
                                                                              AS "Reload Amount"
          ,(CASE WHEN GCF.COMMAND IN (''Reload'',''VoidReload'') THEN CHF.FISCAL_DATE ELSE NULL END)          
                                                                              AS "Reloaded At"  
           ,(CASE WHEN GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'') THEN 
                          CASE WHEN GCF.OPENING_BALANCE IS NULL OR GCF.GIFTCARD_DIM_Fk = 1  
                            THEN GCF.TRANSACTION_AMOUNT * (-1) ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )  
                                                                              AS "Redeemed Amount"  
            ,(CASE WHEN GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'') THEN CHF.FISCAL_DATE ELSE NULL END)          
                                                                              AS "Redeemed At"  
            ,GCD.BALANCE                                                      AS "Balance" 
            ,GCF.OPENING_BALANCE                                              AS "Opening Balance"
            ,GCF.CLOSING_BALANCE                                              AS "Closing Balance"
        FROM DATAWAREHOUSE.GIFTCARD_DIM                             GCD
           INNER JOIN TEMP_LOCS                                     LOC
              ON LOC.MERCHANT_DIM_nK = GCD.MERCHANT_DIM_FK
                    AND GCD.DW_ISCURRENTROW
                    AND GCD.IS_ISSUED
                    AND NOT gcd.DW_ISDELETED
            INNER JOIN DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT        GCF  
               ON GCD.GIFTCARD_DIM_NK = GCF.GIFTCARD_DIM_FK
                    AND GCF.DW_ISCURRENTROW
                    AND NOT GCF.DW_ISDELETED
                    AND  GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'',''Reload'',''VoidReload'')
            INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                     CHF
                ON CHF.CHEQUE_FACT_NK = GCF.CHEQUE_FACT_FK
                    AND CHF.STATUS = ''Closed''
                    AND CHF.DW_ISCURRENTROW
                    AND (CHF.FISCAL_DATE::date >= :startdate::date 
                         AND CHF.FISCAL_DATE::date  <= :enddate::date)
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM                    LOD
                ON LOD.LOCATION_DIM_NK = CHF.LOCATION_DIM_FK
                   AND LOD.DW_ISCURRENTROW
         ;          
                           
   CREATE TEMP TABLE TEMP_TRANS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  

--================================================================================================================================
--Get a list of all Gift Cards with a redeem or relaod between the start and end dates.
SELECT GIFTCARD_DIM_FK, MAX("Is Imported") AS "Is Imported"
  FROM TEMP_TRANS
  GROUP BY GIFTCARD_DIM_FK;

  CREATE TEMP TABLE TEMP_CARDS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 

--================================================================================================================================

--Get the issuing location for all Gift Cards with a redeem or relaod between the start and end dates.    
SELECT GCF.GIFTCARD_DIM_FK
,TCD.GIFTCARD_DIM_FK
,TCD."Is Imported"     AS "Is Imported"
,LOC.LOCATIONNAME    AS "Issuing Location"
  FROM DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT GCF
    INNER JOIN TEMP_CARDS                    TCD
      ON TO_CHAR(GCF.GIFTCARD_DIM_FK) = TO_CHAR(TCD.GIFTCARD_DIM_FK)
        AND GCF.DW_ISCURRENTROW
        AND GCF.COMMAND = ''Issue''
     INNER JOIN CHEQUE_FACT                    CHF
       ON CHF.CHEQUE_FACT_NK = GCF.CHEQUE_FACT_FK
         AND CHF.DW_ISCURRENTROW
     INNER JOIN LOCATION_DIM                   LOC
       ON LOC.LOCATION_DIM_NK = CHF.LOCATION_DIM_FK
         AND LOC.DW_ISCURRENTROW
       ;

    CREATE TEMP TABLE TEMP_ISSUELOC AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 
     
--================================================================================================================================
--Assign a issuing location to all cards with transactions between start and end dates
SELECT TPT.GIFTCARDTRANSACTION_FACT_NK                                  AS  "Support ID" 
    ,''GCD-'' ||ROW_NUMBER() OVER(ORDER BY TPT.GIFTCARDTRANSACTION_FACT_NK) 
                                                                        AS "Detail ID" 
    ,IFNULL(TPT."Gift Card Number",''None'')                              AS "Gift Card Number" 
    ,TIL."Is Imported"                                                        AS "Is Imported"
    -- ,IFNULL(TPT."Check",''None'')                                      AS "Check"
    ,IFNULL(TO_CHAR(TPT."Issued At"::DATE),''Imported'')                  AS "Issued At"    
    ,TO_CHAR(TPT."Redeemed At"::DATE)                                   AS "Redeemed At"
    ,TO_CHAR(TPT."Reloaded At"::DATE)                                   AS "Reloaded At"
    ,IFNULL(TIL."Issuing Location",''None'')                              AS "Earned Location"    
    ,IFNULL(TIL."Issuing Location",''None'')                              AS "Issuing Location"
    ,IFNULL(TPT."Transaction Location",''None'')                          AS "Spending Locaton"
    ,IFNULL(TPT."Redeemed Amount",0.00)::NUMBER(18,2)                   AS "Redeemed Amount"
    ,IFNULL(TPT."Reload Amount",0.00)::NUMBER(18,2)                     AS "Reload Amount"    
    ,IFNULL(TPT."Opening Balance",0.00)::NUMBER(18,2)                   AS "Opening Balance"  
    ,IFNULL(TPT."Closing Balance",0.00)::NUMBER(18,2)                   AS "Closing Balance" 
    ,IFNULL(TPT."Balance",0.00)::NUMBER(18,2)                           AS "Current Balance"
  FROM TEMP_ISSUELOC                                                    TIL
      INNER JOIN TEMP_TRANS                                             TPT
        ON TIL.GIFTCARD_DIM_FK = TPT.GIFTCARD_DIM_FK
;

  CREATE TEMP TABLE TEMP_RPT AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 
     
--================================================================================================================================
reportSet := (  

SELECT * from TEMP_RPT
  ORDER BY 
                "Gift Card Number"
); 

-----------------------------------------------------------------------------------------------------------------------------------
RETURN TABLE(reportSet);
--=================================================================================================================================
END';