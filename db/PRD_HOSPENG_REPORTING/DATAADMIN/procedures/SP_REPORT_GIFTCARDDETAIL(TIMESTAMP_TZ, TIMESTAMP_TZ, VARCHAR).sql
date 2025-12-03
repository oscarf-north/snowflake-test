CREATE OR REPLACE PROCEDURE "SP_REPORT_GIFTCARDDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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
DROP TABLE if exists TEMP_LOCS;
DROP TABLE if exists TEMP_TRANS;
DROP TABLE if exists TEMP_REPORT;

------------------------------------------------------------------------------------------------------------------------
--Get all of the locations assocaited with a merchant - use to apply filters for legacy gift cards
SELECT MOD.MERCHANT_DIM_NK
  FROM MERCHANT_DIM                       MOD
    INNER JOIN MERCHANT_ORGANIZATION_XREF MOX
      ON MOD.MERCHANT_DIM_NK = MOX.MERCHANT_DIM_FK
        AND MOD.DW_ISCURRENTROW
        AND MOX.DW_ISCURRENTROW
    INNER JOIN ORGANIZATION_DIM           ORG
      ON ORG.ORGANIZATION_DIM_NK = MOX.ORGANIZATION_DIM_FK
        AND ORG.DW_ISCURRENTROW
    INNER JOIN LOCATION_DIM               LOC
      ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
        AND LOC.DW_ISCURRENTROW
        AND LOCATION_DIM_NK in (SELECT table1.value 
        FROM table(split_to_table(:locationidS, '',''))  table1)
    GROUP BY MOD.MERCHANT_DIM_NK;

 CREATE TEMP TABLE TEMP_LOCS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  

-----------------------------------------------------------------------------------------------
--get information on cards issued wilthin a certain date - NON LEGACY ONLY
SELECT GCD.GIFTCARD_DIM_NK  AS GIFTCARD_DIM_NK
    ,CHF.FISCAL_DATE        AS "Fiscal Date"
    ,CHF.CHEQUE_FACT_NK     AS "Check ID"
    ,CHF.LOCATION_DIM_FK    AS "Location ID"    
    ,CHF.CHEQUENUMBER       AS "Check"
    ,EMD.EMPLOYEE_NAME      AS "Employee"
    ,LOC.LOCATIONNAME       AS "Location"
    ,RCD.REVENUECENTER      AS "Revenue Center"
    ,DAD.DAYPART            AS "Daypart"
    ,GCD.ISSUED_AT          AS "Issued At"   
    ,GCD.IS_LEGACY          AS "Is Imported"
    ,GCD.START_BALANCE      AS "Issued Amount"
   FROM GIFTCARD_DIM                               GCD
    INNER JOIN TEMP_LOCS                           TLC
       ON GCD.MERCHANT_DIM_FK = TLC.MERCHANT_DIM_NK 
     INNER JOIN GIFTCARDTRANSACTION_FACT           GCF
       ON GCD.GIFTCARD_DIM_NK = GCF.GIFTCARD_DIM_FK
         AND GCF.DW_ISCURRENTROW
         AND GCD.DW_ISCURRENTROW
         AND GCD.IS_ISSUED
         AND NOT GCD.DW_ISDELETED
         AND GCD.DW_ISCURRENTROW
         AND GCF.COMMAND = ''Issue''
         AND GCD.ISSUED_AT::date >= :startdate::date
         AND GCD.ISSUED_AT::date <= :enddate::date   
      INNER JOIN CHEQUE_FACT                       CHF
        ON CHF.CHEQUE_FACT_NK = GCf.CHEQUE_FACT_FK
          AND CHF.DW_ISCURRENTROW
          AND CHF.STATUS = ''Closed''
      INNER JOIN EMPLOYEE_DIM                      EMD
        ON EMD.EMPLOYEE_DIM_NK = CHF.EMPLOYEE_DIM_FK
          AND EMD.DW_ISCURRENTROW
      INNER JOIN DAYPART_DIM                       DAD
        ON DAD.DAYPART_DIM_NK = CHF.DAYPART_DIM_FK
          AND DAD.DW_ISCURRENTROW
      INNER JOIN LOCATION_DIM                     LOC
        ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
          AND LOC.DW_ISCURRENTROW
      INNER JOIN REVENUECENTER_DIM                RCD
        ON CHF.REVENUECENTER_DIM_FK = RCD.REVENUECENTER_DIM_NK
          AND RCD.DW_ISCURRENTROW
     
  ORDER BY GCD.ISSUED_AT  
;

 CREATE TEMP TABLE TEMP_TRANS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID( )));  

----------------------------------------------------------------------------------------------------
--For final report - left join all transactions to all gift cards to pick up legacy cards.
--  an equivalent loj in temp_trans runs longer since Snowflake dosen''t apply filters for the
--  and clauses but instead waits for a where.
SELECT  TO_CHAR(GCD.GIFTCARD_DIM_NK)                                  AS "Support ID" 
    ,''GCD-'' ||row_number() over (order by GCD.GIFTCARD_DIM_NK) 
                                                                      AS "Detail ID" 
    ,IFNULL(gcd.GIFTCARD,''None'')                                      AS "Gift Card Number"    
    ,(MAX(TTRANS."Check ID"))                                         AS "Check ID"  
    ,(MAX(TTRANS."Fiscal Date"))                                      AS "Fiscal Date"
    ,(MAX(TTRANS."Location ID"))                                      AS "Location ID"
    ,IFNULL(MAX(TTRANS."Check"),''None'')                               AS "Check"
    ,IFNULL(MAX(TTRANS."Employee"),''None'')                            AS "Employee"
    ,IFNULL(MAX(TTRANS."Location"),''None'')                            AS "Location"
    ,IFNULL(MAX(TTRANS."Daypart"),''None'')                             AS "Daypart"
    ,IFNULL(MAX(TTRANS."Revenue Center"),''None'')                      AS "Revenue Center"
    ,IFNULL(MAX(COALESCE(GCD.ISSUED_AT,TTRANS."Issued At")) ::DATE ::STRING,''None'')          
                                                                      AS "Issued At"   
    ,MAX(CASE WHEN TTRANS."Is Imported" = ''false'' 
      then FALSE ELSE TRUE END)::BOOLEAN                               AS "Is Imported"
    ,MAX(IFNULL(COALESCE(GCD.START_BALANCE,TTRANS. "Issued Amount"),0))
    ::NUMBER(18,2)  
                                                                      AS "Issued Amount"
                                                                      
  FROM GIFTCARD_DIM                                                   GCD
    INNER JOIN TEMP_LOCS                                              TLC
       ON GCD.MERCHANT_DIM_FK = TLC.MERCHANT_DIM_NK   
          AND GCD.ISSUED_AT::date >= :startdate::date
          AND GCD.ISSUED_AT::date <= :enddate::date       
    LEFT OUTER JOIN TEMP_TRANS                                        TTRANS
      ON GCD.GIFTCARD_DIM_NK = TTRANS.GIFTCARD_DIM_NK
    GROUP BY "Gift Card Number" ,GCD.GIFTCARD_DIM_NK
;

 CREATE TEMP TABLE TEMP_REPORT AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
    
----------------------------------------------------------------------------------------------------
 reportSet   := (
   SELECT * FROM TEMP_REPORT 

--===================================================================================================
); 
 RETURN TABLE(reportSet); 
END';