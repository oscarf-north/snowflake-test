CREATE OR REPLACE PROCEDURE "SP_CREATEDWTABLE"("DBNAME" VARCHAR(50), "SCHEMANAME" VARCHAR(50), "TABLENAME" VARCHAR(50))
RETURNS TABLE ("SQLTEXT" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  -- DBNAME VARCHAR(45)             := ''DEV_HOSPENG_REPORTING'';
  -- SCHEMANAME VARCHAR(45)         := ''DATASTAGE'';
  -- TABLENAME VARCHAR(45)          := ''DAYPART_DIM'';--''CHEQUE_FACT'';--ERRORDWDATE_DIM''LOCATIONGROUP_DIM'';ACTIVITIY_FACT

----------------------------------------------------------------------------------------------------------------------
  DBName_VAR VARCHAR(075)        := UPPER(DBNAME);
  SchemaName_VAR VARCHAR(075)    := UPPER(SCHEMANAME);
  TableName_VAR VARCHAR(050)     := UPPER(TABLENAME);
  FullTableName_VAR VARCHAR(500) := :DBName_VAR || ''.'' || :SchemaName_VAR ||  ''.'' || :TableName_VAR;
  TableType_VAR  VARCHAR(10)     := ''VIEW'' ;
  res_list                       resultset:= (SELECT ''Invalid Table or View Name'' AS "Error Message");
  dateError_list                 resultset:= (SELECT :TableName_VAR as MESSAGE , 0 as COUNT);
  variantError_list              resultset:= (SELECT :TableName_VAR as MESSAGE , 0 as COUNT);
  IsTableName                    BOOLEAN;
  count_requiredErr_list  INT    := 0;
  count_variantErr_list  INT     := 0;  

  ERROR_NOT_NUMERIC_text         := ''--WARNING: <COLUMN_NAME> is type <DATA_TYPE>, but based on naming conventions, number or decimal may be required.'';
  ERROR_NOT_BOOLEAN_text         := ''--WARNING: <COLUMN_NAME> is type <DATA_TYPE>. but based on naming conventions,boolean may be required.'';
  ERROR_NOT_DATE_text            := ''--ALERT: <COLUMN_NAME> is type <DATA_TYPE>. but based on naming conventions, a datatype like date or timestamp is required.  Try  to_timestamp_tz(<COLUMN_NAME>) '';
  ERROR_VARIANT_text             := ''--ALERT: <COLUMN_NAME> is <DATA_TYPE>. but the Datawarehouse should not store variant data.'';
  ERROR_UPDATED_COLUMN_NAME_text := ''--ALERT: <COLUMN_NAME> should be renamed as to UPDATED_AT for consistency.'';

----------------------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------------------
--Drop temp table if exists - used for dev as temp tables would be dropped when sproc session ends
DROP TABLE IF EXISTS column_list_temp CASCADE;  
DROP TABLE IF EXISTS all_columns_temp CASCADE; 
DROP TABLE IF EXISTS dwDateError_list CASCADE;
DROP TABLE IF EXISTS dwVariantError_list CASCADE;
DROP TABLE IF EXISTS requiredCols_list CASCADE; 
DROP TABLE IF EXISTS requiredErr_list CASCADE; 
DROP TABLE IF EXISTS resultsTable_list CASCADE;

----------------------------------------------------------------------------------------------------------------------
--get table and columns to be used for both audit and sql text creation.  If something that is NOT a table nane
--  is passed in, we will stop the proc.
 CREATE TEMP TABLE all_columns_temp AS
   SELECT t.TABLE_NAME
     ,c.ORDINAL_POSITION 
     ,c.COLUMN_NAME 
     ,c.DATA_TYPE 
     ,c.DATETIME_PRECISION
     ,c.CHARACTER_OCTET_LENGTH 
     ,c.NUMERIC_PRECISION 
     ,c.CHARACTER_MAXIMUM_LENGTH
     ,c.NUMERIC_SCALE
     ,FALSE as ERROR_NOT_NUMERIC
     ,FALSE as ERROR_NOT_DATE
     ,FALSE as ERROR_NOT_BOOLEAN
     ,FALSE as ERROR_VARIANT 
     ,FALSE AS ERROR_UPDATED_COLUMN_NAME --source column names vary - but data warehouse only uses updated_at
   from information_schema.tables t
      inner join information_schema.columns c
          on t.table_schema     = c.table_schema
            and t.table_name    = c.table_name
            and t.table_type    = ''VIEW''
            and t.table_schema  = ''DATAADMIN''
            and t.table_name    = :TableName_VAR
;

-- ----------------------------------------------------------------------------------------------------------------------
--If the table is not found in the catalouge - retern an error message
IsTableName := (CASE WHEN (SELECT count(*)  FROM all_columns_temp t) = 0 THEN FALSE ELSE TRUE END) ;  

-- ----------------------------------------------------------------------------------------------------------------------
IF (:IsTableName)  --dont run any additional sql if input is not a real table name - prevent sql injection
  THEN
 CREATE TEMP TABLE column_list_temp AS
    SELECT LISTAGG(INLT2.SQLTextValue  || '' \\n '') WITHIN GROUP (ORDER BY INLT2.SQLTextOrdinal) AS sqlText
      FROM(
      --additional columns added to the view for the dw table
      SELECT INLT1.SQLTextOrdinal
          ,REPLACE(INLT1.SQLTextValue, ''<TableName>'', MAX(INLT1.TableName)OVER(PARTITION BY 1)) as SQLTextValue
        FROM (
           SELECT SQT.SQLTextOrdinal     AS SQLTextOrdinal
             ,NULL                       AS TableName
             ,SQT.SQLTextValue           AS SQLTextValue
             FROM DataAdmin.DW_SQLText  SQT
            WHERE SQT.SQLTextGroup = ''DWAdditionalColumns''
              AND SQT.IsCurrentVersion 
              AND NOT SQT.ISDELETED
        
           UNION
         
--grab all of the columns from the conforming view to include in the dw table
        SELECT
             c.Ordinal_position, c.table_name
             ,'' '' || c.column_name || '''' || '' '' 
                  --|| CASE WHEN  c.column_name ILIKE (''%_PK'') THEN c.DATA_TYPE ELSE '' '' END
                  || CASE WHEN c.column_name ilike (''%_PK'') THEN ''INT IDENTITY(1,1)''
                      ELSE
                   DATA_TYPE 
                  || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'',''DATE'') AND  c.column_name NOT ilike (''%_PK'')
                      THEN '' (''  || COALESCE(CAST(c.CHARACTER_MAXIMUM_LENGTH as STRING)
                               ,CAST(c.DATETIME_PRECISION as STRING)
                               ,CAST(c.CHARACTER_OCTET_LENGTH as STRING)
                               ,CAST(c.NUMERIC_PRECISION as STRING),'' '')
                   ELSE '' ''
                     END 
                  END
               || CASE WHEN DATA_TYPE  = ''NUMBER'' AND c.column_name NOT ilike (''%_PK'') THEN '','' || CAST(NUMERIC_SCALE AS STRING)  ELSE '' '' END
               || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'',''DATE'') AND  c.column_name NOT ilike (''%_PK'') THEN '')'' ELSE '' '' END
              

               || CASE WHEN c.ordinal_position = MAX(c.ordinal_position) OVER (PARTITION BY 1) THEN '');'' ELSE '', '' END
                        AS sqlText
      FROM all_columns_temp c
      ) INLT1
     
        ) INLT2
          ;

-- ---------------------------------------------------------------------------------------------------------------------- 
--Warning for any conforming view that dosen''t set up the dw start and end dates OR create a table from non existant view
dateError_list := ( CALL SP_VALIDATEDWDATES(''l'',:TableName_VAR,''COUNT'') );  --get any dw date warnnings

CREATE TEMP TABLE dwDateError_list 
     AS
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

--Get a suggested data type for any columns that are of type variant-no Data Warehouse cols should be variant
variantError_list := (CALL SP_SUGGESTVARIANTDATATYPES(:DBName_VAR, ''DATAADMIN'', :TableName_VAR));

CREATE TEMP TABLE dwVariantError_list 
     AS
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

count_variantErr_list := (CASE WHEN (SELECT TOP 1 MESSAGE FROM dwVariantError_list) ILIKE ''review%'' THEN 0 ELSE (SELECT COUNT(*) FROM dwVariantError_list ) END);

-- ---------------------------------------------------------------------------------------------------------------------- 
--Warning to ensure all required columns exist in the table
CREATE TEMP TABLE requiredCols_list
  as
SELECT :TableName_var || ''_PK'' as ColName_Required,''Required_Column'' as ColTypes_Required
    UNION
SELECT :TableName_var || ''_NK'' as ColName_Required,''Required_Column'' as ColTypes_Required
    UNION    
SELECT ''DW_STARTDATE'' as ColName_Required,''Required_Column'' as ColTypes_Required
    UNION
SELECT ''DW_ENDDATE'',''Required Column''
  UNION
SELECT ''DW_ISDELETED'',''Required Column''
  UNION
SELECT ''DW_ISCURRENTROW'',''Required Column''
  UNION
SELECT ''MTLN_CDC_LAST_CHANGE_TYPE'',''Required Column''
  UNION
SELECT ''MTLN_CDC_LAST_COMMIT_TIMESTAMP'',''Required Column''
  UNION
SELECT ''MTLN_CDC_SEQUENCE_NUMBER'',''Required Column''
  UNION
SELECT ''MTLN_CDC_LOAD_BATCH_ID'',''Required Column''
  UNION
SELECT ''MTLN_CDC_LOAD_TIMESTAMP'',''Required Column''
  UNION
SELECT ''MTLN_CDC_PROCESSED_DATE_HOUR'',''Required Column''
  UNION
SELECT ''MTLN_CDC_SRC_VERSION'',''Required Column''
  UNION
SELECT ''MTLN_CDC_FILENAME'',''Required Column''
  UNION
SELECT ''MTLN_CDC_FILEPATH'',''Required Column''
  UNION
SELECT ''MTLN_CDC_SRC_DATABASE'',''Required Column''
  UNION
SELECT ''MTLN_CDC_SRC_SCHEMA'',''Required Column''
  UNION
SELECT ''MTLN_CDC_SRC_TABLE'',''Required Column''
  UNION
SELECT ''CREATED_AT'',''Required Column''
  UNION
SELECT ''UPDATED_AT'',''Required Column''
;

--find required columns that are not in the dw table being created
CREATE TEMP TABLE requiredErr_list
  AS
SELECT req.ColName_Required ,tab.COLUMN_NAME ,ColTypes_Required
  FROM requiredCols_list           req
    LEFT JOIN all_columns_temp     tab
      ON UPPER(req.ColName_Required) = UPPER(tab.COLUMN_NAME)
 WHERE tab.COLUMN_NAME is null
;

count_requiredErr_list := (SELECT COUNT(*) FROM requiredErr_list AS COUNTROW) ;

-- ---------------------------------------------------------------------------------------------------------------------- 
--Mark all columns whose data type may need to be changed
 UPDATE all_columns_temp
   SET ERROR_NOT_NUMERIC = CASE 
     WHEN upper(COLUMN_NAME) LIKE ANY (''%^_FK'',''%^_ID'',''%^_PK'',''%^_COUNT'',''%^_AMOUNT'',''%NET%'',''%GROSS%'',''TIP'',''%ID'') ESCAPE ''^''
           AND NOT (UPPER(COLUMN_NAME) LIKE ANY(''%UUID%'',''IS_%'',''%^_GUID'')  ESCAPE ''^'') 
           AND NOT (UPPER(DATA_TYPE) LIKE ANY(''%NUMBER%'',''%DECIMAL%'')  ESCAPE ''^'') 
           THEN TRUE ELSE FALSE END
       ,ERROR_NOT_BOOLEAN = CASE WHEN upper(COLUMN_NAME) 
         LIKE ANY (''%^_IS^_%'',''%^_HAS^_%'',''IS^_%'',''HAS^_%'',''%ENABLED%'',''%DELETED%'')  ESCAPE ''^''
           AND NOT (UPPER(COLUMN_NAME) LIKE ANY (''%_AT'')ESCAPE ''^'') 
           AND NOT (UPPER(DATA_TYPE) LIKE ANY (''%BOOLEAN%'')ESCAPE ''^'')  
             THEN TRUE ELSE FALSE END
       ,ERROR_NOT_DATE = CASE WHEN upper(COLUMN_NAME) 
         LIKE ANY (''%^_AT'',''%^_DATE'')  ESCAPE ''^''
           AND NOT DATA_TYPE  LIKE ANY (''%DATE%'',''%TIME%'')   ESCAPE ''^''
           THEN TRUE ELSE FALSE END
       ,ERROR_UPDATED_COLUMN_NAME = CASE WHEN UPPER(COLUMN_NAME) = ''MODIFIED_AT'' THEN TRUE ELSE FALSE END
   ;   

ALTER TABLE all_columns_temp ADD COLUMN ERROR_MESSAGE string;

UPDATE all_columns_temp 
SET ERROR_MESSAGE =
  CASE WHEN ERROR_NOT_NUMERIC THEN :ERROR_NOT_NUMERIC_text
  WHEN ERROR_NOT_DATE THEN :ERROR_NOT_DATE_text
  WHEN ERROR_NOT_BOOLEAN THEN :ERROR_NOT_BOOLEAN_text
  WHEN ERROR_VARIANT THEN :ERROR_VARIANT_text
  WHEN ERROR_UPDATED_COLUMN_NAME THEN :ERROR_UPDATED_COLUMN_NAME_text
END;

--Put all results together into single temp table
CREATE TEMP TABLE resultsTable_list
  AS
  SELECT LISTAGG( Statement)  AS "SQL Text"
     FROM (
    
      SELECT ''\\n--=========================================================================================== \\n'' 
        AS Statement, 1 AS ORDINAL_T
         
      UNION
        
      SELECT ''\\n--Review any messages shown below and modify the Conforming view as needed. \\n'' 
        , 2 AS ORDINAL_T
        
         UNION
         
        SELECT LISTAGG(CASE WHEN COUNT = 0 THEN ''--INFORMATIONAL MESSAGE:  '' ELSE 
           CASE WHEN MESSAGE ILIKE ''No Variant%'' 
           THEN ''--INFORMATIONAL MESSAGE :'' ELSE ''--CRITICAL ALERT :'' END
           END 
             || MESSAGE || '' has '' || COUNT || '' date errors.  '' || CASE WHEN COUNT = 0 THEN '' \\n'' ELSE ''  Execute '' 
             ||  '' CALL SP_VALIDATEDWDATES(\\''l\\'',\\'''' || :TableName_VAR || ''\\'',\\''ERROR\\'')   ''  ||  ''for Details.  \\n'' END) AS MESSAGE
               , 2 fROM dwDateError_list           

         UNION
         
        SELECT CASE WHEN :count_variantErr_list > 0
          THEN  LISTAGG( ''--'' ||
            CASE WHEN MESSAGE ILIKE ''No Variant%'' 
           THEN ''INFORMATIONAL MESSAGE :  '' ELSE ''CRITICAL ALERT :'' END
             || MESSAGE ||  ''\\n'') 
          ELSE NULL END
            , 3 
        FROM dwVariantError_list
               
        UNION

        SELECT 
          CASE WHEN :count_requiredErr_list > 0 
           THEN
                ''--CRITICAL ALERT:  The following require columns are missing from the conforming view: \\n--'' || LISTAGG(''      '' 
               || ColName_Required || '', '')
               || ''\\n--      Cut and paste the columns and definitions from the template to correct. \\n'' 
           ELSE  NULL END
             ,4
         FROM  requiredErr_list 

       
      UNION
       
      SELECT LISTAGG( REPLACE(REPLACE(t.ERROR_MESSAGE,''<DATA_TYPE>'',t.DATA_TYPE),''<COLUMN_NAME>'',t.COLUMN_NAME) || ''\\n'')
        , 5 AS ORDINAL_T 
        FROM all_columns_temp t 
          WHERE ERROR_NOT_NUMERIC OR ERROR_NOT_BOOLEAN OR ERROR_NOT_DATE OR ERROR_VARIANT OR ERROR_UPDATED_COLUMN_NAME
      
      UNION
      
      SELECT *, 6 AS ORDINAL_T FROM column_list_temp C
    ORDER BY ORDINAL_T
);

 
res_list  := (SELECT * FROM resultsTable_list);
 
-- ---------------------------------------------------------------------------------------------------------------------- 
END IF;

--========================================================================================================================
RETURN TABLE(res_list);

--========================================================================================================================
END';