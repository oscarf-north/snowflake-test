CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_CHECKTRANSFER"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
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

DROP TABLE IF EXISTS TEMP_TRANSFER;
DROP TABLE IF EXISTS TEMP_TRANSFER_DATA;

--get the checks that have changed revenue centers and related fks
SELECT inlt1.CHEQUE_FACT_NK
     ,inlt1.CHEQUE_FACT_PK
     ,inlt1.REVENUECENTER_DIM_FK
     ,inlt1.CHEQUE_FACT_PK_FROM
     ,inlt1.REVENUECENTER_DIM_NK_FROM
     ,inlt1.TOTAL_FROM     
     ,inlt1.CHEQUE_FACT_PK_TO
     ,inlt1.REVENUECENTER_DIM_NK_TO
     ,inlt1.TOTAL_TO
    FROM (
            SELECT CHF.DW_STARTDATE,CHF.CHEQUE_FACT_PK,CHF.CHEQUE_FACT_NK,CHF.REVENUECENTER_DIM_FK
            ,ROW_NUMBER() OVER (PARTITION BY CHF.CHEQUE_FACT_NK,CHF.REVENUECENTER_DIM_FK 
               ORDER BY DW_STARTDATE )      AS RANKIT
            ,ROW_NUMBER() OVER (PARTITION BY CHF.CHEQUE_FACT_NK
              ORDER BY DW_STARTDATE )       AS RANKIT2

            ,CHF.CHEQUE_FACT_PK                                  AS CHEQUE_FACT_PK_TO
            ,CHF.REVENUECENTER_DIM_FK                            AS REVENUECENTER_DIM_NK_TO
            ,CHF.TOTAL                                           AS TOTAL_TO            

            ,LAG (CHF.CHEQUE_FACT_PK) OVER (PARTITION BY CHF.CHEQUE_FACT_NK ORDER BY DW_STARTDATE) 
                                                                 AS CHEQUE_FACT_PK_FROM
            ,LAG (CHF.REVENUECENTER_DIM_FK) OVER (PARTITION BY CHF.CHEQUE_FACT_NK ORDER BY DW_STARTDATE) 
                                                                 AS REVENUECENTER_DIM_NK_FROM 
            ,LAG (CHF.TOTAL) OVER (PARTITION BY CHF.CHEQUE_FACT_NK ORDER BY DW_STARTDATE) 
                                                                 AS TOTAL_FROM                                                                   
           FROM  DATAWAREHOUSE.CHEQUE_FACT CHF
                         WHERE CHF.LOCATION_DIM_FK IN (--351,352
                SELECT table1.value 
              FROM table(split_to_table(:locationidS, '',''))  table1)  
                AND (CHF.FISCAL_DATE::date >= :startdate::date 
                AND CHF.FISCAL_DATE::date  <= :enddate::date)
                AND NOT CHF.IS_TRAINING
                AND NOT CHF.DW_ISDELETED
        ) inlt1
             WHERE inlt1.RANKIT = 1     --show only the times the revenuecenter changed 
                  AND inlt1.RANKIT2 > 1 --not the first row, it can never be a change
             ORDER BY inlt1.CHEQUE_FACT_NK,inlt1.DW_STARTDATE
             ;
-- SELECT * 
--   FROM (
--     SELECT inlt1.CHEQUE_FACT_NK  AS CHEQUE_FACT_NK
--      ,inlt1.CHEQUE_FACT_PK       AS CHEQUE_FACT_PK_TO
--      ,inlt1.REVENUECENTER_DIM_FK AS REVENUECENTER_DIM_FK_TO
--      ,LAG (inlt1.CHEQUE_FACT_PK) OVER (PARTITION BY inlt1.CHEQUE_FACT_NK ORDER BY inlt1.DW_STARTDATE) 
--                                  AS CHEQUE_FACT_PK_FROM
--      ,LAG (inlt1.REVENUECENTER_DIM_FK) OVER (PARTITION BY inlt1.CHEQUE_FACT_NK ORDER BY inlt1.DW_STARTDATE) 
--                                  AS REVENUECENTER_DIM_NK_FROM
--     FROM (
--             SELECT CHF.DW_STARTDATE,CHF.CHEQUE_FACT_PK,CHF.CHEQUE_FACT_NK,CHF.REVENUECENTER_DIM_FK
--                ,ROW_NUMBER() OVER (PARTITION BY CHF.CHEQUE_FACT_NK,CHF.REVENUECENTER_DIM_FK 
--                    ORDER BY CHF.REVENUECENTER_DIM_FK,CHF.DW_STARTDATE ) 
--                   AS RANKIT
--               FROM  DATAWAREHOUSE.CHEQUE_FACT CHF
--               WHERE CHF.LOCATION_DIM_FK IN (--351,352
--                 SELECT table1.value 
--               FROM table(split_to_table(:locationidS, '',''))  table1)  
--                 AND (CHF.FISCAL_DATE::date >= :startdate::date 
--                 AND CHF.FISCAL_DATE::date  <= :enddate::date)
--                 AND NOT CHF.IS_TRAINING
--                 AND NOT CHF.DW_ISDELETED
--              ) inlt1
--             WHERE inlt1.RANKIT = 1  --show when revenuecenter changed or first transaction
--             ORDER BY inlt1.CHEQUE_FACT_NK,inlt1.DW_STARTDATE
--     ) inlt2
--   WHERE inlt2.CHEQUE_FACT_PK_FROM IS NOT NULL
--   ORDER BY CHEQUE_FACT_NK;    

--write transfers to a temp table 
CREATE TEMP TABLE TEMP_TRANSFER AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
 -- select * From   TEMP_TRANSFER  
--=================================================================================================================================
reportSet := (  
----------------------------------------------------------------------------------------------------------------------------------
SELECT CHF.CHEQUE_FACT_NK              AS ROWNUM        --Row number
  ,TO_CHAR(CHF.FISCAL_DATE,''YYYYMMDD'') AS BUSDATE       --Business day 
  ,CHF.REVENUECENTER_DIM_FK            AS RVCNUM        --Revenue center number
  ,CHF.REVENUECENTERNAME               AS RVCDESC       --Revenue center description
  ,CHF.CHEQUE_FACT_NK                  AS CHKSEQ        --Unique check sequence identifier 
  ,CHF.CHEQUENUMBER                    AS CHKNUM        --Check description
  ,NULL                                AS TRANSTORVCNUM --Transfer check revenue center number
  ,NULL                                AS TRANSTORVCDESC--Transfer check revenue center description
  ,CHF.CHEQUE_FACT_NK                  AS TRANSTOCHKSEQ --Transfer unique check sequence identifier 
  ,CHF.CHEQUENUMBER                    AS TRANSTOCHKNUM --Transfer check number
  ,CHF.TOTAL                           AS TRANSAMT      --Transfer amount
FROM DATAWAREHOUSE.CHEQUE_FACT                         CHF
  INNER JOIN TEMP_TRANSFER                             TMP
    ON CHF.CHEQUE_FACT_NK = TMP.CHEQUE_FACT_NK
  INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM EMD
    ON EMD.EMPLOYEE_DIM_NK = CHF.EMPLOYEE_DIM_FK
      AND CHF.DW_ISCURRENTROW
      AND EMD.DW_ISCURRENTROW
      AND CHF.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      AND NOT CHF.IS_TRAINING
      AND CHF.DW_ISCURRENTROW
      AND CHF.STATUS in (''Closed'')
      AND (CHF.FISCAL_DATE::date >= :startdate::date 
                AND CHF.FISCAL_DATE::date  <= :enddate::date)
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 LOC
    ON LOC.LOCATION_DIM_NK = CHF.LOCATION_DIM_FK
      AND CHF.DW_ISCURRENTROW
      AND LOC.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.SHIFT_DIM                    SHD
    ON SHD.SHIFT_DIM_NK = CHF.SHIFT_DIM_FK
      AND SHD.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.JOBPOSITION_DIM              JPD
    ON JPD.JOBPOSITION_DIM_NK = SHD.JOBPOSITION_DIM_FK
      AND JPD.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.REVENUECENTER_DIM            RCD
    ON RCD.REVENUECENTER_DIM_NK = CHF.REVENUECENTER_DIM_FK
      AND RCD.DW_ISCURRENTROW
--=================================================================================================================================

);
RETURN TABLE(reportSet); 
END';