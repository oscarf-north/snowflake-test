create or replace view GIFTCARDTRANSACTION_FACT(
	GIFTCARDTRANSACTION_FACT_PK,
	GIFTCARDTRANSACTION_FACT_NK,
	TRANSACTION,
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
	GIFTCARD_DIM_FK,
	CHEQUE_FACT_FK,
	LOCATION_DIM_FK,
	ADJUSTED_BY_ID,
	ADJUSTS_ID,
	ADJUSTER,
	IS_VOIDED,
	IS_ADJUSTED,
	CREATED_AT,
	UPDATED_AT,
	AUTH_GUID,
	COMMAND,
	CARD_ENTRY_METHOD,
	CURRENCY_CODE,
	AMOUNT,
	ADJUSTED_AMOUNT,
	TRANSACTION_AMOUNT,
	TIP,
	TOTAL,
	BALANCE,
	OPENING_BALANCE,
	CLOSING_BALANCE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------ 
   GCD.ID                                                            AS GIFTCARDTRANSACTION_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,GCD.ID                                                            AS GIFTCARDTRANSACTION_FACT_NK
--name---------------------------------------------------------------------------------------
  ,GCD.TRAN_NBR                                                      AS TRANSACTION
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
        GCD.ID 
        ORDER BY GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,GCD.MTLN_CDC_SEQUENCE_NUMBER,GCD.MTLN_CDC_SRC_VERSION
      ,GCD.MTLN_CDC_FILENAME)),6))
                                                                      AS DW_STARTDATE        
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    GCD.ID
    ORDER BY GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,GCD.MTLN_CDC_SEQUENCE_NUMBER,GCD.MTLN_CDC_SRC_VERSION,GCD.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY GCD.ID  ORDER BY 
    GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP,GCD.MTLN_CDC_SEQUENCE_NUMBER,GCD.MTLN_CDC_SRC_VERSION
   ,GCD.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                       AS DW_ENDDATE         
  
  ,CASE WHEN GCD.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                           AS DW_ISDELETED          
  ,CASE WHEN RANK() OVER(PARTITION BY  GCD.ID 
    ORDER BY  
    GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,GCD.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,GCD.MTLN_CDC_SRC_VERSION DESC
   ,GCD.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                        AS DW_ISCURRENTROW    
--CDC Meta data-------------------------------------------------------------------------------
  ,GCD.MTLN_CDC_LAST_CHANGE_TYPE                                        AS MTLN_CDC_LAST_CHANGE_TYPE
  ,GCD.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                   AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,GCD.MTLN_CDC_SEQUENCE_NUMBER                                         AS MTLN_CDC_SEQUENCE_NUMBER
  ,GCD.MTLN_CDC_LOAD_BATCH_ID                                           AS MTLN_CDC_LOAD_BATCH_ID
  ,GCD.MTLN_CDC_LOAD_TIMESTAMP                                          AS MTLN_CDC_LOAD_TIMESTAMP
  ,GCD.MTLN_CDC_PROCESSED_DATE_HOUR                                     AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,GCD.MTLN_CDC_SRC_VERSION                                             AS MTLN_CDC_SRC_VERSION
  ,GCD.MTLN_CDC_FILENAME                                                AS MTLN_CDC_FILENAME
  ,GCD.MTLN_CDC_FILEPATH                                                AS MTLN_CDC_FILEPATH
  ,GCD.MTLN_CDC_SRC_DATABASE                                            AS MTLN_CDC_SRC_DATABASE
  ,GCD.MTLN_CDC_SRC_SCHEMA                                              AS MTLN_CDC_SRC_SCHEMA
  ,GCD.MTLN_CDC_SRC_TABLE                                               AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(REPLACE(GCD.GIFTCARD_ID,'"',''),'-1')                         AS GIFTCARD_DIM_FK
  ,IFNULL(TRY_TO_NUMBER(gcd.POS_CHECK_ID),-1)                           AS CHEQUE_FACT_FK
  ,IFNULL(GCD.LOCATION_ID,-1)                                           AS LOCATION_DIM_FK    
  ,GCD.ADJUSTED_BY_ID                                                   AS ADJUSTED_BY_ID
  ,GCD.ADJUSTS_ID                                                       AS ADJUSTS_ID 
  ,CASE WHEN ADJUSTED_BY_ID IS NOT NULL 
      THEN GCD.ID -1 ELSE ADJUSTS_ID END                                AS ADJUSTER
--flags---------------------------------------------------------------------------------------
  ,REPLACE(GCD.IS_VOIDED,'"','')  ::BOOLEAN                             AS IS_VOIDED
  ,REPLACE(GCD.IS_ADJUSTED,'"','')::BOOLEAN                             AS IS_ADJUSTED  
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(GCD.CREATED_AT)                                      AS CREATED_AT   
  ,to_timestamp_tz(GCD.UPDATED_AT)                                      AS UPDATED_AT
 -- ,to_timestamp_tz(replace(value:ccData:expirationDate,'"',''))       AS EXPIRATIONDATE   

--names, options, etc-------------------------------------------------------------------------
  ,gcd.AUTH_GUID                                                        AS AUTH_GUID
  ,gcd.COMMAND                                                          AS COMMAND
  ,gcd.CARD_ENTRY_METHOD                                                AS CARD_ENTRY_METHOD
  ,gcd.CURRENCY_CODE                                                    AS CURRENCY_CODE
--Counts and Amounts--------------------------------------------------------------------------
   ,GCD.AMOUNT::NUMBER(38,4)                                            AS AMOUNT
   -- ,SUM(CASE WHEN gcd.ADJUSTED_BY_ID IS NOT NULL 
   --    THEN GCD.AMOUNT * CASE WHEN GCD.COMMAND IN ('NoNSFSale','Adjust','VoidReload','VoidIssue') 
   --       THEN -1 ELSE 1 END
   --  ELSE 0 END) OVER (PARTITION by GCD.ID,GCD.ADJUSTED_BY_ID) 
   --                                                                      AS ADJUSTEMENT_AMOUNT
   ,IFNULL(LAG(GCD.AMOUNT * CASE WHEN GCD.COMMAND IN ('NoNSFSale','Adjust','VoidReload','VoidIssue')
     THEN -1 ELSE 1 END)
      OVER (PARTITION BY GCD.ID ORDER BY CASE WHEN ADJUSTED_BY_ID IS NOT NULL 
      THEN GCD.ID -1 ELSE ADJUSTS_ID END),0)  
                                                                        AS ADJUSTED_AMOUNT  

   ,GCD.AMOUNT - IFNULL(LAG(GCD.AMOUNT * CASE WHEN GCD.COMMAND IN ('NoNSFSale','Adjust','VoidReload','VoidIssue')
     THEN -1 ELSE 1 END)
      OVER (PARTITION BY GCD.ID ORDER BY CASE WHEN ADJUSTED_BY_ID IS NOT NULL 
      THEN GCD.ID -1 ELSE ADJUSTS_ID END),0)  
   
                                                                        AS TRANSACTION_AMOUNT                                                                        


                                                                        
   ,GCD.TIP::NUMBER(38,4)                                               AS TIP
   ,GCD.TOTAL::NUMBER(38,4)                                             AS TOTAL
   ,GCD.BALANCE::NUMBER(38,4)                                           AS BALANCE
   ,LAG(GCD.BALANCE) OVER (PARTITION BY GCD.GIFTCARD_ID ORDER BY GCD.CREATED_AT) ::NUMBER(18,2) 
                                                                        AS OPENING_BALANCE   
   ,GCD.BALANCE::NUMBER(38,4)                                           AS CLOSING_BALANCE
----------------------------------------------------------------------------------------------
FROM DATALANDING.GIFTCARD_PUBLIC_GIFTCARD_TX                            GCD

;