CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_FEES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2020-07-10'';  
  -- enddate string      := ''2029-07-10''; 
  -- locationid string   := ''[351]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_TABLE1;

------------------------------------------------------------------------------------------------  
 CREATE TEMP TABLE TEMP_TABLE1 AS
 SELECT suf.LOCATION_DIM_FK                                             as "Location ID" 
    ,IfNULL(replace(replace(sud.type,''Fixed'',''Fixed ''),''Open'',''Open '')
      || case when sud.type ilike(''%Amount'') then  '' $'' else '' ''  end
      || to_char((sud.fixed_value/1000000)::decimal(15,2)),''None'') 
      || CASE WHEN sud.type ilike(''%Percent'') then ''%'' else '' '' end
                                                                        as "Rate"
    ,sud.SURCHARGE                                                      as "Surcharge"
    ,SUM(1)::NUMBER(18,0)                                               as "Count"
    ,SUM(SUF.APPLIEDAMOUNT)::NUMBER(18,2)                               as "Total"  
FROM DATAWAREHOUSE.CHEQUE_FACT                                          CHK
      INNER JOIN DATAWAREHOUSE.SURCHARGE_FACT                           SUF
          ON SUF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
             AND CHK.DW_ISCURRENTROW
             AND SUF.DW_ISCURRENTROW
             AND NOT CHK.DW_ISDELETED
             AND NOT CHK.IS_TRAINING
             AND CHK.STATUS = ''Closed''
             AND NOT SUF.STATUS = ''Disabled''
             -- AND NOT SUF.IS_GRATUITY
             AND CHK.FISCAL_DATE::date
                >= :startdate::date 
            AND CHK.FISCAL_DATE::date  
                <= :enddate::date 
            AND CHK.LOCATION_DIM_FK IN ( 
               SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.SURCHARGE_DIM           SUD
        ON SUF.SURCHARGE_DIM_NK = SUD.SURCHARGE_DIM_NK
          AND SUD.DW_ISCURRENTROW
GROUP BY  
  "Rate"
  ,"Surcharge"
  ,"Location ID" 
;

--=========================================================================================
 reportSet:= (
 SELECT  ROW_NUMBER() OVER (ORDER BY "Location ID")  AS  "Support ID" 
   ,*   
 FROM TEMP_TABLE1
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';