CREATE OR REPLACE PROCEDURE "SP_STAGELOADEMPLOYEE_DIM"("VALIDATE_DATE" BOOLEAN)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE res resultset;
      --validate_date boolean := TRUE;
      errorCount_res resultset;
      highwater int := IFNULL((SELECT MAX(MTLN_CDC_SEQUENCE_NUMBER) FROM DATAWAREHOUSE_TEMP.EMPLOYEE_DIM),0);
  BEGIN
    res := (
       INSERT INTO DATAWAREHOUSE_TEMP.EMPLOYEE_DIM(   
          EMPLOYEE_DIM_NK, 
          EMPLOYEE_NAME, 
          DW_STARTDATE, 
          DW_ENDDATE, 
          DW_ISDELETED, 
          DW_RANGESTART, 
          DW_RANGEEND, 
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
          ORGANIZATION_DIM_FK, 
          IS_DISABLED, 
          IS_TRAINING, 
          TERMINATE_DATE, 
          BIRTH_DATE, 
          CREATED_AT, 
          UPDATED_AT, 
          FIRST_NAME, 
          LAST_NAME, 
          IMPORT_GUID, 
          SYSTEM_ID, 
          PAYROLL_ID, 
          PIN, 
          EMAIL, 
          INITIALS, 
          NICKNAME, 
          UI_COLOR, 
          CARD_CODE, 
          AVATAR, 
          GENDER, 
          FLSA_STATUS 
) 
 SELECT   EMPLOYEE_DIM_NK  as   EMPLOYEE_DIM_NK,  
   EMPLOYEE_NAME  as   EMPLOYEE_NAME,  
   DW_STARTDATE  as   DW_STARTDATE,  
   DW_ENDDATE  as   DW_ENDDATE,  
   DW_ISDELETED  as   DW_ISDELETED,  
   :highwater  as   DW_RANGESTART,  
   MAX(MTLN_CDC_SEQUENCE_NUMBER) OVER(PARTITION BY 1)  as   DW_RANGEEND,  
   DW_ISCURRENTROW  as   DW_ISCURRENTROW,  
   MTLN_CDC_LAST_CHANGE_TYPE  as   MTLN_CDC_LAST_CHANGE_TYPE,  
   MTLN_CDC_LAST_COMMIT_TIMESTAMP  as   MTLN_CDC_LAST_COMMIT_TIMESTAMP,  
   MTLN_CDC_SEQUENCE_NUMBER  as   MTLN_CDC_SEQUENCE_NUMBER,  
   MTLN_CDC_LOAD_BATCH_ID  as   MTLN_CDC_LOAD_BATCH_ID,  
   MTLN_CDC_LOAD_TIMESTAMP  as   MTLN_CDC_LOAD_TIMESTAMP,  
   MTLN_CDC_PROCESSED_DATE_HOUR  as   MTLN_CDC_PROCESSED_DATE_HOUR,  
   MTLN_CDC_SRC_VERSION  as   MTLN_CDC_SRC_VERSION,  
   MTLN_CDC_FILENAME  as   MTLN_CDC_FILENAME,  
   MTLN_CDC_FILEPATH  as   MTLN_CDC_FILEPATH,  
   MTLN_CDC_SRC_DATABASE  as   MTLN_CDC_SRC_DATABASE,  
   MTLN_CDC_SRC_SCHEMA  as   MTLN_CDC_SRC_SCHEMA,  
   MTLN_CDC_SRC_TABLE  as   MTLN_CDC_SRC_TABLE,  
   ORGANIZATION_DIM_FK  as   ORGANIZATION_DIM_FK,  
   IS_DISABLED  as   IS_DISABLED,  
   IS_TRAINING  as   IS_TRAINING,  
   TERMINATE_DATE  as   TERMINATE_DATE,  
   BIRTH_DATE  as   BIRTH_DATE,  
   CREATED_AT  as   CREATED_AT,  
   UPDATED_AT  as   UPDATED_AT,  
   FIRST_NAME  as   FIRST_NAME,  
   LAST_NAME  as   LAST_NAME,  
   IMPORT_GUID  as   IMPORT_GUID,  
   SYSTEM_ID  as   SYSTEM_ID,  
   PAYROLL_ID  as   PAYROLL_ID,  
   PIN  as   PIN,  
   EMAIL  as   EMAIL,  
   INITIALS  as   INITIALS,  
   NICKNAME  as   NICKNAME,  
   UI_COLOR  as   UI_COLOR,  
   CARD_CODE  as   CARD_CODE,  
   AVATAR  as   AVATAR,  
   GENDER  as   GENDER,  
   FLSA_STATUS  as   FLSA_STATUS 
  FROM DATAADMIN.EMPLOYEE_DIM
     WHERE MTLN_CDC_SEQUENCE_NUMBER > :highwater);

 
 --======================================================================================================================== 

 
--========================================================================================================================
CALL SP_UpdateDWTable( ''DATASTAGE'', ''EMPLOYEE_DIM'');
IF (validate_date = TRUE)  THEN  errorCount_res := (CALL DATAADMIN.SP_ValidateDWDates(''p'',''EMPLOYEE_DIM'',''COUNT''));
END IF;
return table(errorCount_res); END';