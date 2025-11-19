create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.CLOSEOUTSUMMARY_FACT(
	CLOSEOUTSUMMARY_FACT_PK,
	CLOSEOUTSUMMARY_FACT_NK,
	NAME,
	DW_STARTDATE,
	DW_ENDDATE,
	DW_ISDELETED,
	DW_ISCURRENTROW,
	MTLN_CDC_LAST_CHANGE_TYPE,
	MTLN_CDC_LAST_COMMIT_TIMESTAMP,
	MTLN_CDC_SEQUENCE_NUMBER,
	MTLN_CDC_LOAD_BATCH_ID,
	MTLN_CDC_LOAD_TIMESTAMP,
	MTLN_CDC_PROCESSED_DATE_HOUR,
	MTLN_CDC_SRC_VERSION,
	MTLN_CDC_FILENAME,
	MTLN_CDC_FILEPATH,
	MTLN_CDC_SRC_DATABASE,
	MTLN_CDC_SRC_SCHEMA,
	MTLN_CDC_SRC_TABLE,
	LOCATION_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	FISCAL_DATE,
	TAX,
	TIPS,
	VOIDS,
	DISCOUNTS,
	FEES,
	GRATUITIES,
	GROSS_RECEIPTS,
	GROSS_SALES
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  to_char(tab.fiscal_day_int) || '.' || tab.location_id       AS CLOSEOUTSUMMARY_FACT_PK  
--natural keys------------------------------------------------------------------------------
  ,to_char(tab.fiscal_day_int) || '.' || tab.location_id      AS CLOSEOUTSUMMARY_FACT_NK  
--name-------------------------------------------------------------------------------------------
  ,tab.fiscal_day_int || ':' || tab.created_at                AS NAME
--data warehouse REQUIRED rows-------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY to_char(tab.fiscal_day_int) || '.' || tab.location_id
      ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
      ,tab.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY to_char(tab.fiscal_day_int) || '.' || tab.location_id
    ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION,tab.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (
    PARTITION BY to_char(tab.fiscal_day_int) || '.' || tab.location_id 
    ORDER BY 
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
   ,tab.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          
  
  ,CASE WHEN tab.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED          
  ,CASE WHEN RANK() OVER(
  PARTITION BY to_char(tab.fiscal_day_int) || '.' || tab.location_id
  ORDER BY  
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,tab.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,tab.MTLN_CDC_SRC_VERSION DESC
   ,tab.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     
--CDC Meta data REQUIRED rows-------------------------------------------------------------------------------
  ,tab.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                   
  ,tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP              
  ,tab.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                    
  ,tab.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                      
  ,tab.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                     
  ,tab.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR                
  ,tab.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                        
  ,tab.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                           
  ,tab.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                           
  ,tab.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                       
  ,tab.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                         
  ,tab.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          
--foreign keys-------------------------------------------------------------------------------
  ,tab.LOCATION_id                     AS LOCATION_DIM_FK 
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
--Alphabetize between the lines
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
--Alphebetize between the lines
  ,to_timestamp_tz(tab.CREATED_AT)                  AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)                  AS UPDATED_AT
  ,TO_DATE('20' || tab.FISCAL_DAY_INT,'YYYYMMDD')   AS FISCAL_DATE 
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
--Alphebetize between the lines
  ,TO_NUMBER(tab.TAX,38,4)/1000000 ::number(38,4)                AS TAX
  ,TO_NUMBER(tab.TIPS,38,4)/1000000 ::number(38,4)               AS TIPS
  ,TO_NUMBER(tab.VOIDS,38,4)/1000000  ::number(38,4)             AS VOIDS
  ,TO_NUMBER(tab.DISCOUNTS,38,4)/1000000::number(38,4)           AS DISCOUNTS  
  ,TO_NUMBER(tab.FEES,38,4)/1000000  ::number(38,4)              AS FEES
  ,TO_NUMBER(tab.GRATUITIES,38,4)/1000000::number(38,4)          AS GRATUITIES
  ,TO_NUMBER(tab.GROSS_RECEIPTS,38,4)/1000000 ::number(38,4)     AS GROSS_RECEIPTS
  ,TO_NUMBER(tab.GROSS_SALES,38,4)/1000000  ::number(38,4)       AS GROSS_SALES
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CLOSEOUT_SUMMARY     tab
;
