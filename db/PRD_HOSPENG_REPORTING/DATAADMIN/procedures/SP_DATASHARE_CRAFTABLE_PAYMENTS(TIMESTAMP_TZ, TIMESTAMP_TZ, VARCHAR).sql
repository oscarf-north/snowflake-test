CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_PAYMENTS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
   -- GRANT usage ON procedure dataadmin.SP_REPORT_MODIFIER(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
-----------------------------------------------------------------------------------------------------------------------
  BEGIN
    --drop temp tables
    DROP TABLE IF EXISTS REPORT_DATA;

-----------------------------------------------------------------------------------------------------------------------
  CREATE  TEMPORARY TABLE REPORT_DATA
    AS
  SELECT 
           org.ORGANIZATION                                                                                 AS "Organization Name"
          ,org.ORGANIZATION_DIM_NK                                                                          AS "Organization ID"
          ,loc.LOCATIONNAME                                                                                 AS "Location Name"
          ,pay.LOCATION_DIM_FK                                                                              AS "Location ID"
          ,pay.FISCALDATE::DATE                                                                             AS "Business Day"
          ,pay.CHEQUE_FACT_FK                                                                               AS "Check ID"

-- FROM WILL Payment Type: Credit Card, Payment Method: Visa
         ,PAYMENTTYPE                                                                                       AS "Tender Type"

         
        ,IFNULL(case pay.cardbrand when '''' 
             THEN ''Not a Credit Card'' 
            else pay.cardbrand end,''None'')                                                                  AS "Tender Name"



        
        ,CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2) 
                  else pay.TOTAL::DECIMAL(18,2) end                                                         AS "Tender Amount"


          ,IFNULL(pay.cardholderName,''None'')                                                                AS "Cardholder Name"
          ,IFNULL(pay.LASTFOURCCNUMBER,''None'')                                                              AS "Cender Number (last 4)"                  

          FROM DATAWAREHOUSE.payments_FACT                                                                  pay
          INNER JOIN DATAWAREHOUSE.location_DIM                                                             loc
            ON pay.location_DIM_FK = loc.location_DIM_NK
              AND pay.FISCALDATE::date >= :startdate::date 
              AND pay.FISCALDATE::date <= :enddate::date  
              AND pay.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
              AND pay.dw_iscurrentrow
              AND loc.dw_iscurrentrow
              AND NOT pay.IS_TRAINING
              AND NOT pay.dw_isdeleted
              AND pay.PAYMENTSTATUS = ''Success''
          INNER JOIN DATAWAREHOUSE.PaymentMethod_DIM                       ptd      
            ON pay.PaymentMethod_DIM_FK = ptd.PaymentMethod_DIM_NK
              AND ptd.dw_iscurrentrow
              AND NOT ptd.dw_isdeleted
              
          INNER JOIN DATAWAREHOUSE.organization_dim                     org
            ON loc.organization_DIM_FK = org.organization_DIM_NK
              AND org.dw_iscurrentrow
              AND NOT org.dw_isdeleted
          -- INNER JOIN DATAWAREHOUSE.daypart_dim                           dpd
          --   ON pay.daypart_dim_fk = dpd.daypart_dim_nk
          --     AND dpd.dw_iscurrentrow
          --     AND NOT dpd.dw_isdeleted
          -- INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                         otd
          --   ON otd.Ordertype_DIM_nK = pay.Ordertype_DIM_FK
          --     AND otd.dw_iscurrentrow
          -- LEFT JOIN DATAWAREHOUSE.employee_DIM                           emd
          --   ON emd.employee_DIM_NK = pay.EMPLOYEE_DIM_FK_AS_PAYEE
          --     AND emd.dw_iscurrentrow 
          -- LEFT JOIN DATAWAREHOUSE.CCTransaction_FACT                      cct   
          --   ON cct.cctransaction_fact_nk = pay.TRANSACTION_FACT_FK                     
          --     AND cct.dw_iscurrentrow                                                                
          --     AND cct.TRANSACTION_NUMBER = 1                             
    ORDER BY loc.locationname
    
    ;
    
-----------------------------------------------------------------------------------------------------------------------    
    --return values from the sproc with validated columns only  
     reportSet := (
         SELECT * FROM REPORT_DATA
     );

--=====================================================================================================================
RETURN TABLE(reportSet); 
END';