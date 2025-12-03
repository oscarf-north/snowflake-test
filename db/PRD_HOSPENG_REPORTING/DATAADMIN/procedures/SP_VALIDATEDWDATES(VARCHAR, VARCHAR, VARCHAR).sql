CREATE OR REPLACE PROCEDURE "SP_VALIDATEDWDATES"("REPORTTYPE" VARCHAR(1), "TABLENAME" VARCHAR(45), "ERRORREPORT" VARCHAR(5))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
--CALL SP_VALIDATEDWDATES(''l'',''CHEQUE_FACT'',''ALL'');
--ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = ''YYYY-MM-DD HH24:MI:SS.FF'';
-- SchemaName_VAR
-- REPORTTYPE VARCHAR(1)          := ''L'';  --valid values {L,P,S}
-- TABLENAME VARCHAR(45)          := ''CHEQUE_FACT'';--''WRONG_FACT'';--''ERRORDWDATE_DIM''--''CHEQUE_FACT''--''WRONG_FACT''
-- ERRORREPORT VARCHAR(5)         := ''COUNT'';--ERROR, ALL, COUNT
----------------------------------------------------------------------------------------------------------------------
  DBName_VAR VARCHAR(075)        := ''PRD_HOSPENG_REPORTING'';
  SchemaName_VAR VARCHAR(075)    := CASE UPPER(REPORTTYPE) 
                                        WHEN ''L'' THEN ''DATAADMIN'' 
                                        WHEN ''P'' THEN ''DATAWAREHOUSE''
                                        WHEN ''S'' THEN ''DATAWAREHOUSE_TEMP''
                                        ELSE ''DATAWAREHOUSE'' END;
  TableName_VAR VARCHAR(050)     := UPPER(TABLENAME);
  ErrorReport_VAR VARCHAR(500)   := UPPER(ERRORREPORT);
  FullTableName_VAR VARCHAR(500) := DBName_VAR || ''.'' || SchemaName_VAR ||  ''.'' || TableName_VAR;
  TableType_VAR  VARCHAR(75)     := CASE UPPER(REPORTTYPE) WHEN ''L''THEN ''VIEW'' ELSE ''BASE TABLE'' END; 

  NaturalKey_VAR varchar(250);
  ErrorCount int;
  ErrorSet resultset;
  CountError resultset   := (SELECT ''No Errors'' as "MESSAGE", 0 as "COUNT");
  NK_ErrorMessage resultset := (SELECT :TableName_VAR || '' ERROR:  Missing Natural Key.'' AS "MESSAGE", 1 as "COUNT");
  TB_ErrorMessage resultset := (SELECT :TableName_VAR || '' ERROR:  Invalid view or table name.'' AS "MESSAGE", 1 as "COUNT");
----------------------------------------------------------------------------------------------------------------------
BEGIN
----------------------------------------------------------------------------------------------------------------------
--Get the Primary Key for the dw table or landing view.  Validate that the value passed in is acutally a conforming vies
--  by looking for the table or view passed in.  
NaturalKey_VAR := 
  ( 
   SELECT c.column_name
    FROM information_schema.tables t
      INNER JOIN information_schema.columns c
          ON t.table_schema     = c.table_schema
            AND t.table_name    = c.table_name
            AND t.table_schema  = :SchemaName_VAR
            AND t.table_type    = :TableType_VAR
            AND t.table_name    = :TableName_VAR
            AND :TableName_VAR  = left(c.column_name,length(:TableName_VAR))
            AND c.column_name ilike ''%_NK''
    );

TableName_VAR := 
  ( 
   SELECT t.table_name
    FROM information_schema.tables t
      WHERE t.table_schema  = :SchemaName_VAR
            AND t.table_type    = :TableType_VAR
            AND t.table_name    = :TableName_VAR

    );

----------------------------------------------------------------------------------------------------------------------
--Drop temp table if exists - used for dev as temp tables would be dropped when sproc session ends
DROP TABLE IF EXISTS ErrorSet CASCADE;  

----------------------------------------------------------------------------------------------------------------------  
--Query Errors and return the result set either the errors or all data for review.
IF (:NaturalKey_VAR IS NOT NULL)
  THEN
   CREATE TEMP TABLE ErrorSet AS
    SELECT *
     FROM(
 
    SELECT INLT1.PK                                                   AS PK
        ,INLT1.MTLN_CDC_SEQUENCE_NUMBER                               AS MTLN_CDC_SEQUENCE_NUMBER
        ,INLT1.MTLN_CDC_LAST_COMMIT_TIMESTAMP                         AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
        ,INLT1.RANKORDER                                              AS RANKORDER
        ,INLT1.DW_ISCURRENTROW                                        AS DW_ISCURRENTROW
        ,INLT1.DW_STARTDATE                                           AS DW_STARTDATE
        ,TO_TIMESTAMP(INLT1.DW_ENDDATE)                               AS DW_ENDDATE
        ,TO_TIMESTAMP(INLT1.DW_STARTDATE_PREVROW)                     AS DW_STARTDATE_PREVROW
        ,INLT1.LAST_COLUMN                                            AS lAST_COLUMN

        ,COUNT(DISTINCT INLT1.DW_ENDDATE) 
           OVER (PARTITION BY INLT1.PK)                               AS COUNT_ENDDATE

        ,CASE WHEN (TO_TIMESTAMP(INLT1.DW_STARTDATE_PREVROW) = TO_TIMESTAMP(INLT1.DW_ENDDATE) 
              AND INLT1.DW_ENDDATE <> ''9999-09-09 09:09:08.999999999'')
            OR (INLT1.DW_ISCURRENTROW )
            THEN FALSE ELSE TRUE END                                  AS ERROR_DW_DATES_NONCONTIGUOUS
        ,CASE WHEN (INLT1.DW_ISCURRENTROW 
            AND ABS(DATEDIFF(MILLISECOND,INLT1.DW_ENDDATE,''9999-09-09 09:09:08.999999999'' )) = 0) 
               OR (NOT INLT1.DW_ISCURRENTROW
                    AND  ABS(DATEDIFF(MILLISECOND,INLT1.DW_ENDDATE,''9999-09-09 09:09:08.999999999'' )) > 0)
            THEN FALSE ELSE TRUE END                                  AS ERROR_CURRENTROW_DATES
        ,CASE WHEN DW_CURRENTROW_COUNT > 1 THEN TRUE ELSE FALSE END   AS ERROR_MULTIPLE_CURENT_ROWS
        ,CASE WHEN INLT1.LAST_COLUMN = ''9999-09-09 09:09:08.999999999''
            THEN FALSE ELSE TRUE END                                  AS ERROR_NO_CURRENT_ROW 

        ,
        CASE WHEN COUNT_HIGHENDDATE > 1 THEN TRUE ELSE FALSE END

                                                                      AS ERROR_MULTIPLE_MAX_DATE    
        ,CASE WHEN (INLT1.HIGH_STARTDATE = INLT1.DW_STARTDATE
            AND INLT1.DW_ISCURRENTROW = TRUE)
            OR (INLT1.HIGH_STARTDATE <> INLT1.DW_STARTDATE
                AND INLT1.DW_ISCURRENTROW = FALSE)
            THEN FALSE ELSE TRUE END                                   AS ERROR_LOW_STARTDATE
       ,CASE WHEN NOT HIGH_CURRENTROW 
            THEN TRUE ELSE FALSE END                                   AS ERROR_NO_TRUE_CURENTROW
            --------------------------------------------------------------------------------------
        ,CASE WHEN MAX( INLT1.MTLN_CDC_SEQUENCE_NUMBER)  OVER (PARTITION BY INLT1.PK) 
          =
          MIN( INLT1.MTLN_CDC_SEQUENCE_NUMBER) OVER (PARTITION BY INLT1.PK)
          AND
          MAX( INLT1.MTLN_CDC_FILENAME)  OVER (PARTITION BY INLT1.PK) 
          =
          MIN( INLT1.MTLN_CDC_FILENAME) OVER (PARTITION BY INLT1.PK)
          AND
          MAX( INLT1.MTLN_CDC_SRC_VERSION)  OVER (PARTITION BY INLT1.PK) 
          =
          MIN( INLT1.MTLN_CDC_SRC_VERSION) OVER (PARTITION BY INLT1.PK)
           THEN TRUE ELSE FALSE END                                    AS ERROR_DUPLICATE_CDCSQUENCE
           --------------------------------------------------------------------------------------
        FROM (
            SELECT IDENTIFIER(:NaturalKey_VAR)                         AS PK
                ,row_number() OVER(PARTITION BY IDENTIFIER(:NaturalKey_VAR)
                    ORDER BY PF.DW_ENDDATE)                            AS RANKORDER
                ,PF.DW_STARTDATE                                       AS DW_STARTDATE
                ,PF.DW_ENDDATE                                         AS DW_ENDDATE
                ,PF.DW_ISCURRENTROW                                    AS DW_ISCURRENTROW
                ,PF.MTLN_CDC_SEQUENCE_NUMBER                           AS MTLN_CDC_SEQUENCE_NUMBER
                ,PF.MTLN_CDC_LAST_COMMIT_TIMESTAMP                     AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
                ,PF.MTLN_CDC_FILENAME                                  AS MTLN_CDC_FILENAME
                ,PF.MTLN_CDC_SRC_VERSION                               AS MTLN_CDC_SRC_VERSION
                ,SUM(CASE WHEN PF.DW_ISCURRENTROW = TRUE THEN 1 
                  ELSE 0 END) 
                    OVER (PARTITION BY IDENTIFIER(:NaturalKey_VAR))    AS DW_CURRENTROW_COUNT

                ,DATEADD(NS,-1,lEAD(PF.DW_STARTDATE) 
                    OVER (PARTITION BY IDENTIFIER(:NaturalKey_VAR) 
                        ORDER BY PF.DW_STARTDATE))         
                                                                      AS DW_STARTDATE_PREVROW
                ,LAST_VALUE(PF.DW_ENDDATE) OVER (PARTITION BY 
                      IDENTIFIER(:NaturalKey_VAR) 
                    ORDER BY PF.DW_ENDDATE NULLS LAST)                 AS LAST_COLUMN
                ,MAX(PF.DW_STARTDATE) OVER (PARTITION 
                    BY IDENTIFIER(:NaturalKey_VAR))                    AS HIGH_STARTDATE

                ,MAX(PF.DW_ISCURRENTROW) OVER (PARTITION 
                    BY IDENTIFIER(:NaturalKey_VAR))                    AS HIGH_CURRENTROW
                
                ,SUM(CASE WHEN PF.DW_ENDDATE
                  = ''9999-09-09 09:09:08.999999999'' 
                    THEN 1 ELSE 0 END)                                 
                  OVER (PARTITION  BY IDENTIFIER(:NaturalKey_VAR))     AS COUNT_HIGHENDDATE
            FROM TABLE(:FullTableName_VAR) PF
            ORDER BY IDENTIFIER(:NaturalKey_VAR),PF.DW_ENDDATE
                                                                                         ) INLT1
                                                                                             ) INLT2
    WHERE (INLT2.ERROR_DW_DATES_NONCONTIGUOUS  --RETURN ALL DATA OR ERRORS ONLY
        OR INLT2.ERROR_CURRENTROW_DATES
        OR INLT2.ERROR_MULTIPLE_CURENT_ROWS 
        OR INLT2.ERROR_LOW_STARTDATE
        or INLT2.ERROR_NO_TRUE_CURENTROW)
        OR INLT2.ERROR_MULTIPLE_MAX_DATE
         OR (:ErrorReport_VAR = ''ALL'')

    ORDER BY INLT2.PK  
        ,INLT2.MTLN_CDC_SEQUENCE_NUMBER   
        ,INLT2.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                                                                   
    ;

   ErrorSet   := (SELECT * from ErrorSet);  
   ErrorCount := (SELECT COUNT(*) from ErrorSet);
   CountError := (SELECT :TableName_VAR as MESSAGE,:ErrorCount as "COUNT" );
   
END IF;

--------------------------------------------------------------------------------------------------------
--RETURN RESULTS 
 IF (:TableName_VAR IS NULL)  --ONLY RETURN RESULTS IF INPUT IS VALID TABLE NAME
    THEN
      RETURN TABLE(TB_ErrorMessage);
  ELSEIF (:NaturalKey_VAR IS NULL)  --ONLY RETURN RESULTS IF INPUT IS VALID TABLE NAME
    THEN
      RETURN TABLE(NK_ErrorMessage);
 ELSEIF (:ERRORREPORT = ''COUNT'')
      THEN
       RETURN TABLE(CountError);
    ELSE
      RETURN TABLE(ErrorSet);
  END IF;
 
--------------------------------------------------------------------------------------------------------  
END';