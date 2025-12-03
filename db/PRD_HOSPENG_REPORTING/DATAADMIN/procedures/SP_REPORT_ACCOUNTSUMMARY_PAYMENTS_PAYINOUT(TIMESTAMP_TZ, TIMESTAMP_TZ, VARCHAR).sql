CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_PAYINOUT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,2,3,4]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN

 reportSet:= (
   SELECT TO_CHAR(ROW_NUMBER() OVER (ORDER BY "Location ID"))  AS  "Support ID" 
     ,INLT_1."Location ID"  as "Location ID"
     ,INLT_1."Type"         as "Type"
     ,SUM(INLT_1."Count")   as "Count"
     ,SUM(INLT_1."Cash")    as "Cash"
     ,SUM(INLT_1."Credit")  as "Credit"
     ,SUM(INLT_1."Total")   as "Total"
     
   FROM (
      SELECT SHD.LOCATION_DIM_FK::decimal(36,0)                                   as "Location ID"  
        ,CASE WHEN PAR.IS_PAY_IN THEN ''Pay In'' ELSE ''Pay Out''END                  as "Type"  
        ,SUM(1::NUMBER(18,0))                                                     as "Count"
       ,(CASE WHEN PMD.PAYMENTMETHODTYPE = ''Cash'' THEN 
         SUM( PIO.AMOUNT * CASE WHEN PAR.IS_PAY_IN THEN 1 ELSE -1 END
              ) ELSE 0 END )::DECIMAL(36,2)                                       as "Cash" 
    
        ,(CASE WHEN PMD.PAYMENTMETHODTYPE <> ''Cash'' THEN 
          SUM( PIO.AMOUNT * CASE WHEN PAR.IS_PAY_IN THEN 1 ELSE -1 END
             ) ELSE 0 END ) ::DECIMAL(36,2)                                       as "Credit"           
        ,SUM( PIO.AMOUNT * CASE WHEN PAR.IS_PAY_IN THEN 1 ELSE -1 END
              ) ::DECIMAL(36,2)                                                   as "Total" 
           FROM DATAWAREHOUSE.PAYINOUT_FACT                                       PIO  
                INNER JOIN DATAWAREHOUSE.SHIFT_DIM                                SHD
                  ON PIO.SHIFT_DIM_FK = SHD.SHIFT_DIM_NK
                      AND SHD.DW_ISCURRENTROW
                      AND PIO.DW_ISCURRENTROW
                      AND NOT PIO.DW_ISDELETED
                      AND NOT PIO.IS_VOID
                      AND PIO.STATUS = ''Success''
                      AND SHD.FISCAL_DAY::date >= :startdate::date 
                      AND SHD.FISCAL_DAY::date <= :enddate::date  
                      AND SHD.LOCATION_DIM_FK in (
                        SELECT table1.value 
                          FROM table(split_to_table(:locationidS, '',''))  table1) 
                INNER JOIN DATAWAREHOUSE.PAYMENTMETHOD_DIM                         PMD
                  ON PMD.PAYMENTMETHOD_DIM_NK = PIO.PAYMENTMETHOD_DIM_FK
                      AND PMD.DW_ISCURRENTROW
                INNER JOIN DATAWAREHOUSE.PAYINPAYOUTREASON_DIM                     PAR
                  ON PAR.PAYINPAYOUTREASON_DIM_NK = PIO.PAYINPAYOUTREASON_DIM_FK
                      AND PAR.DW_ISCURRENTROW
                GROUP BY "Location ID"  
                    ,"Type"
                    ,PMD.PAYMENTMETHODTYPE
                    
  )  INLT_1
     GROUP BY  "Location ID"
     ,"Type"    
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';