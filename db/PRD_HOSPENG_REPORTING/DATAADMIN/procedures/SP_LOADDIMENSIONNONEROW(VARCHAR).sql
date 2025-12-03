CREATE OR REPLACE PROCEDURE "SP_LOADDIMENSIONNONEROW"("TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- TABLENAME VARCHAR(50) := ''ITEMMODIFIER_DIM'';
        -- TABLENAME VARCHAR(50) := ''EMPLOYEE_DIM'';
        --TABLENAME VARCHAR(50) := ''ACTIVITY_FACT'';
        -- TABLENAME VARCHAR(50) := ''LOCATION_DIM'';

        
        NK_NAME VARCHAR := :TABLENAME || ''_NK'';

        TABLENAME_SCHEMA VARCHAR(100) := ''DATAWAREHOUSE.'' || :TABLENAME;
        SQLOUT RESULTSET;
        ROWTEXT VARCHAR:= ''------------------------------------------------------------------------------------------------------------------\\n'';
        HEADERTEXT VARCHAR := ''--Load Placeholder Row for '' || :TABLENAME || ''\\n'';
        -- COUNTSQL := ''SELECT COUNT(*) AS ROWCOUNTNUM FROM '' || TABLENAME_SCHEMA || '' WHERE to_char('' || NK_NAME || '') = -1'';
        COUNTROW_NUM NUMBER := (SELECT COUNT(*) AS ROWCOUNTNUM FROM IDENTIFIER(:TABLENAME) WHERE to_char(IDENTIFIER(:NK_NAME)) = to_char(-1));
        ERRORMESSAGE_1 resultset:= (SELECT ''--'' || :TABLENAME || '' is not a dimension table.  Only Dimension tables require this row.\\n'' as MESSAGE);
        ERRORMESSAGE_2 resultset:= (SELECT ''--The table '' || :TABLENAME || '' already has a missing row with a count of '' 
             || :COUNTROW_NUM || '' .\\n''  as MESSAGE);
BEGIN
 DROP TABLE IF EXISTS all_cols_temp; 
 -- select * from all_cols_temp;
 
 IF (:COUNTROW_NUM = 0  AND :TABLENAME_SCHEMA ilike ''%_DIM'') 
   THEN
         CREATE TEMP TABLE all_cols_temp AS
              select   ''  '' || case when c.column_name ilike ''%PK'' then ''-1''
                          when c.column_name ilike ''%NK'' then ''-1'' 
                          when c.column_name ilike ''%FK'' and c.data_type = ''NUMBER'' then ''-1'' 
                          when c.column_name ilike ''%FK'' and c.data_type <> ''NUMBER'' then ''''''-1''''''
                          -- when c.ordinal_position = 3 then ''''''None''''''
                          when c.column_name = ''DW_STARTDATE'' THEN ''''''1900-01-01 01:01:11.111''''''
                          when c.column_name = ''DW_ENDDATE'' THEN   ''''''9999-09-09 09:09:08.999''''''
                          when c.column_name = ''DW_ISDELETED'' then ''FALSE''
                          when c.column_name = ''DW_INSERTDATETIME'' then '''''''' || TO_CHAR(GETDATE()::timestamp_ntz)|| ''''''''
                          when c.column_name = ''DW_UPDATETIME'' then ''NULL'' 
                          when c.column_name = ''DW_ISCURRENTROW'' then ''TRUE''
                          when c.column_name = ''MTLN_CDC_LAST_CHANGE_TYPE'' then ''''''n''''''
                          when c.column_name = ''MTLN_CDC_LAST_COMMIT_TIMESTAMP'' then ''-1''
                          when c.column_name = ''MTLN_CDC_SEQUENCE_NUMBER'' then ''-1''
                          when c.column_name = ''MTLN_CDC_LOAD_BATCH_ID'' then ''-1''
                          when c.column_name = ''MTLN_CDC_PROCESSED_DATE_HOUR'' then ''NULL''
                          when c.column_name = ''MTLN_CDC_LOAD_TIMESTAMP'' then ''''''1900-01-01 01:01:11.111''''''
                          when c.column_name = ''MTLN_CDC_SRC_VERSION'' then ''-1''
                          when c.column_name = ''MTLN_CDC_FILENAME'' then ''''''n''''''
                          when c.column_name = ''MTLN_CDC_FILEPATH'' then ''''''None''''''
                          when c.column_name = ''MTLN_CDC_SRC_DATABASE'' then ''''''None''''''
                          when c.column_name = ''MTLN_CDC_SRC_SCHEMA'' then ''''''None''''''
                          when c.column_name = ''MTLN_CDC_SRC_TABLE'' then ''''''None''''''
                          when c.column_name = ''CREATED_AT'' then '''''''' || TO_CHAR(GETDATE()::timestamp_ntz)|| ''''''''
                          when c.column_name = ''UPDATED_AT'' then ''NULL''
                          when c.data_type IN (''TEXT'') then ''''''None''''''
                          when c.data_type IN (''NUMBER'') then ''-1''
                          when c.data_type IN (''VARIANT'',''BOOLEAN'',''TIMESTAMP_TZ'',''TIMESTAMP_NTZ'',''DATE'') 
                            then ''NULL''  
                          else ''NULL''
                         end
                      || ''/*'' || c.column_name || '':'' || c.data_type || ''*/''
                      || case when c.ordinal_position = max(ordinal_position) over (partition by 1)
                             then '' ''
                           else '', ''
                           end as column_name
                      ,ordinal_position
              from information_schema.tables t
                 inner join information_schema.columns c
                    on t.table_schema      = c.table_schema
                        and t.table_name   = c.table_name
                        and t.table_type   = ''BASE TABLE''
                        and t.table_schema = ''DATAWAREHOUSE''
                        and t.table_name   = :TABLENAME
                    order by c.ordinal_position;
        
            SQLOUT := (
            SELECT LISTAGG(SQLTEXT)  WITHIN GROUP (ORDER BY ORDINAL)
                    FROM (
                       SELECT :ROWTEXT AS SQLTEXT, 1 AS ORDINAL
                           UNION
                       SELECT :HEADERTEXT AS SQLTEXT, 2 AS ORDINAL
                           UNION  
                       SELECT :ROWTEXT AS SQLTEXT, 3 AS ORDINAL
                           UNION                   
                       SELECT ''INSERT INTO DATAWAREHOUSE.'' || :TABLENAME || '' VALUES ( '' AS SQLTEXT , 4 AS ORDINAL
                           UNION
                       SELECT LISTAGG(COLUMN_NAME) WITHIN GROUP (ORDER BY ordinal_position) || ''\\n'' AS SQLTEXT , 5 AS ORDINAL 
                         FROM ALL_COLS_TEMP
                           UNION
                       SELECT '');\\n'' AS SQLTEXT, 6 AS ORDINAL
                            UNION  
                       SELECT :ROWTEXT AS SQLTEXT, 7 AS ORDINAL
         
                     )  
                ORDER BY ORDINAL
             
            );

END IF;--END IF THERE IS NOT ALREADY A -1 ROW
  -----------------------------------------------------------------------------------------------------------
 IF (:TABLENAME NOT ilike ''%_DIM'') 
    THEN
      RETURN TABLE(ERRORMESSAGE_1);
    ELSEIF (:COUNTROW_NUM > 0)  
    THEN
      RETURN TABLE(ERRORMESSAGE_2);      
    ELSE
       RETURN TABLE(SQLOUT);
  END IF;
  
  -----------------------------------------------------------------------------------------------------------
END';