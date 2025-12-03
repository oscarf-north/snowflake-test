CREATE OR REPLACE PROCEDURE "CREATE_TABLE_DW"("TABLE_NAME_INPUT" VARCHAR(16777216))
RETURNS TABLE ("SQLTEXT" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE res_list            resultset;
        --table_name_input    varchar := ''PAYMENTS_FACT''; --uncomment for dev
        table_name          varchar := table_name_input; 
-----------------------------------------------------------------------------------------
BEGIN
  res_list := (
    SELECT LISTAGG(INLT2.SQLTextValue  || '' \\n '') WITHIN GROUP (ORDER BY INLT2.SQLTextOrdinal) AS sqlText
      FROM(
      --additional columns added to the view for the dw table
      SELECT INLT1.SQLTextOrdinal
          ,REPLACE(INLT1.SQLTextValue,''<TableName>'', MAX(INLT1.TableName)OVER(PARTITION BY 1)) as SQLTextValue
        FROM (
           SELECT SQT.SQLTextOrdinal     AS SQLTextOrdinal
             ,NULL                       AS TableName
             ,SQT.SQLTextValue           AS SQLTextValue
             FROM dev_hospeng_reporting.datawarehouse.DW_SQLText  SQT
            WHERE SQT.SQLTextGroup = ''DWAdditionalColumns''
              AND SQT.IsCurrentVersion 
              AND NOT SQT.ISDELETED
        
           UNION
          
--grab all of the columns from the conforming view to include in the dw table
        SELECT
             c.Ordinal_position, t.table_name
             ,'' '' || c.column_name || '''' || '' '' 
                || CASE WHEN c.column_name LIKE ''%_PK''
                    THEN ''int identity(1,1),''
                  ELSE 
                     CASE WHEN c.column_name LIKE ''%_FK''
                        THEN '' int,''
                      ELSE 
                  c.DATA_TYPE || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'') 
                    THEN '' (''  || COALESCE(CAST(c.CHARACTER_MAXIMUM_LENGTH as STRING)
                               ,CAST(c.DATETIME_PRECISION as STRING)
                               ,CAST(c.CHARACTER_OCTET_LENGTH as STRING)
                               ,CAST(c.NUMERIC_PRECISION as STRING),'' '')
                 ELSE '' ''
                     END
              
               || CASE WHEN DATA_TYPE  = ''NUMBER'' THEN '','' || CAST(NUMERIC_SCALE AS STRING)  ELSE '' '' END
               || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'') THEN '')'' ELSE '' '' END
              

               || CASE WHEN c.ordinal_position = MAX(c.ordinal_position) OVER (PARTITION BY 1) THEN '');'' ELSE '', '' END
              END 
            END
                        AS sqlText
      from information_schema.tables t
      inner join information_schema.columns c
          on t.table_schema     = c.table_schema
            and t.table_name    = c.table_name
            and t.table_type    = ''VIEW''
            and t.table_schema  = ''POSTGRES_PUBLIC''
            and t.table_name    = :table_name
            
        UNION
     --add natural key for all foreign keys      
     select c.Ordinal_position + 0.5                  as ordinal_position 
      , :table_name                                   as table_name
      --,replace(c.column_name,''FK'',''NK'') || '',''        as column_name
            ,'' '' || replace(c.column_name,''FK'',''NK'') || '''' || '' '' || c.DATA_TYPE
                || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'')
                  THEN '' (''  || COALESCE(CAST(c.CHARACTER_MAXIMUM_LENGTH as STRING)
                               ,CAST(c.DATETIME_PRECISION as STRING)
                               ,CAST(c.CHARACTER_OCTET_LENGTH as STRING)
                               ,CAST(c.NUMERIC_PRECISION as STRING),'' '')
                              
               ELSE '' '' END
       
               || CASE WHEN DATA_TYPE  = ''NUMBER'' THEN '','' || CAST(NUMERIC_SCALE AS STRING)  ELSE '' '' END
               || CASE WHEN DATA_TYPE NOT IN (''BOOLEAN'',''VARIANT'') THEN ''),'' ELSE '','' END
    
              
                        AS sqlText
     from information_schema.tables t
      inner join information_schema.columns c
          on t.table_schema     = c.table_schema
            and t.table_name    = c.table_name
            and t.table_type    = ''VIEW''
            and t.table_schema  = ''POSTGRES_PUBLIC''
            and t.table_name    = :table_name
            and c.column_name   like ''%_FK''
                                                    ) INLT1
      --order by SQLTextOrdinal
                                                            ) INLT2
                                                                    )
    ;
--======================================================================================    
return table(res_list);
--======================================================================================
END;
';