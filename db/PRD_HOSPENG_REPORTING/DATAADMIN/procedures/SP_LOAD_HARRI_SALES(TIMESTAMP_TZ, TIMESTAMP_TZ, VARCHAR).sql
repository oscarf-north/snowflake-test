CREATE OR REPLACE PROCEDURE "SP_LOAD_HARRI_SALES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2025-06-03'';  
  -- enddate timestamp_tz   := ''2025-06-03''; 
  -- locationid string      := ''[1,351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=================================================================================================================================
BEGIN

DROP TABLE IF EXISTS temp_SALES;

-----------------------------------------------------------------------------------------------------------------------------------
--Sales Data
SELECT CHF.LOCATION_DIM_FK AS "storeId"
,CHF.FISCAL_DATE           AS "qtr_end_time"	
,SUM(CHF.NET)              AS "net_sales"	    --aggregate
,SUM(CHF.PARTY_COUNT)      AS "covers"	        --aggregate
,SUM(1)                    AS "checks"	        --aggregate
,ORD.ORDER_TYPE            AS "revenue_center"
FROM DATAWAREHOUSE.CHEQUE_FACT                              CHF
   INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                    ORD
      ON CHF.ORDERTYPE_DIM_FK = ORD.ORDERTYPE_DIM_NK
        AND CHF.DW_ISCURRENTROW
        AND ORD.DW_ISCURRENTROW
        AND CHF.STATUS in (''Closed'')
        AND NOT CHF.IS_TRAINING
        AND NOT CHF.DW_ISDELETED
        AND (CHF.UNPAID = 0 /* OR chk.STATUS = ''Voided''*/)
        AND CHF.LOCATION_DIM_FK IN (--351,352
            SELECT table1.value 
               FROM table(
              split_to_table(:locationidS, '',''))  table1)
        AND (CHF.FISCAL_DATE::date >= :startdate::date 
            AND CHF.FISCAL_DATE::date  <= :enddate::date)
  GROUP BY CHF.LOCATION_DIM_FK
     ,CHF.FISCAL_DATE
     ,ORD.ORDER_TYPE
;

  CREATE TEMP TABLE temp_SALES AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
     
--=================================================================================================================================
reportSet := (  
-----------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM temp_SALES  RES
--=================================================================================================================================
);

RETURN TABLE(reportSet); 
END';