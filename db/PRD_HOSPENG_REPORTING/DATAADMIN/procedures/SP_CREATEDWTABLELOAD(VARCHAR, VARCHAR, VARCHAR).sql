CREATE OR REPLACE PROCEDURE "SP_CREATEDWTABLELOAD"("DBNAME" VARCHAR(50), "SCHEMANAME" VARCHAR(50), "TABLENAME" VARCHAR(50))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
        -- DBNAME VARCHAR(45)    := ''DEV_HOSPENG_REPORTING'';--''CHEQUE_FACT'';--
        -- SCHEMANAME VARCHAR(45):= ''DATASTAGE'';--''CHEQUE_FACT'';--
        -- TABLENAME VARCHAR(50) := ''SHIFT_DIM'';--''ITEM_FACT'';

        IsTableName            VARCHAR(50);
        col_list               RESULTSET;
        res                    RESULTSET;
        select_statement       VARCHAR;
        all_sqltext            VARCHAR;
        ErrorMessage resultset:= (SELECT ''Invalid Table or View Name'' AS "Error Message");
        db_name varchar       := UPPER(DBNAME);
        schema_name varchar   := UPPER(SCHEMANAME);
        table_name varchar    := UPPER(TABLENAME);
        sqlText1    varchar   := (SELECT LISTAGG( REPLACE(REPLACE(DST.SQLTEXTVALUE || ''\\n'',''<TABLENAME>''
                                  ,:table_name),''<DBNAME>'',:db_name))WITHIN GROUP (ORDER BY DST.SQLTEXTORDINAL)
                                  FROM DW_SQLText DST
                                    WHERE DST.SQLTEXTGROUP = ''SP_CREATEDATAWAREHOUSETABLELOAD''
                                      AND DST.SQLTEXTNAME = ''SQLTEXT1''
                                      AND DST.ISCURRENTVERSION
                                      AND NOT DST.ISDELETED
                                  ORDER BY DST.SQLTEXTORDINAL);
                                  
        sqlTextB    varchar  := (SELECT LISTAGG(REPLACE(REPLACE(DST.SQLTEXTVALUE || ''\\n'',''<TABLENAME>''
                                  ,:table_name),''<DBNAME>'',:db_name)) WITHIN GROUP (ORDER BY DST.SQLTEXTORDINAL)
                                  FROM DW_SQLText DST
                                    WHERE DST.SQLTEXTGROUP = ''SP_CREATEDATAWAREHOUSETABLELOAD''
                                      AND DST.SQLTEXTNAME = ''SQLTEXTB''
                                      AND DST.ISCURRENTVERSION
                                      AND NOT DST.ISDELETED
                                  ORDER BY DST.SQLTEXTORDINAL);

            
        sqlTextC    varchar  := (SELECT LISTAGG(REPLACE(REPLACE(REPLACE(DST.SQLTEXTVALUE || ''\\n'',''<TABLE_NAME>''
                                  ,:table_name),''<SCHEMA_NAME>'',:schema_name),''<DB_NAME>'',:db_name)) WITHIN GROUP (ORDER BY DST.SQLTEXTORDINAL)
                                  FROM DW_SQLText DST
                                    WHERE DST.SQLTEXTGROUP = ''SP_CREATEDATAWAREHOUSETABLELOAD''
                                      AND DST.SQLTEXTNAME = ''SQLTEXTC''
                                      AND DST.ISCURRENTVERSION
                                      AND NOT DST.ISDELETED
                                  ORDER BY DST.SQLTEXTORDINAL);

        
-----------------------------------------------------------------------------------------
begin
  drop table if exists all_cols_temp; 
  drop table if exists all_sql_temp;
  
  create temp table all_sql_temp (
    ordinal int
    ,sqlText varchar
  );

------------------------------------------------------------------------------------
  create temp table all_cols_temp as
      select  ''  ''
              || c.column_name 
              || case when c.ordinal_position = max(ordinal_position) over (partition by 1)
                     then '' ''
                   else '', ''
                   end as column_name
              ,c.ordinal_position
              ,min(c.ordinal_position) over(partition by 1) as minOrdinal
              ,max(c.ordinal_position) over(partition by 1) as maxOrdinal
      from information_schema.tables t
         inner join information_schema.columns c
            on t.table_schema      = c.table_schema
                and t.table_name   = c.table_name
                and t.table_type   = ''BASE TABLE''
                and t.table_schema = ''DATAWAREHOUSE''
                and t.table_name   = :table_name
                and c.column_name not in (''DW_UPDATETIME'',''DW_INSERTDATETIME'')  
                and NOT c.column_name ilike(''%_PK'')
                
            order by c.ordinal_position;
            
------------------------------------------------------------------------------------
--If the table is not found in the catalouge - retern an error message
IsTableName := (CASE WHEN (SELECT count(*)  FROM all_cols_temp t) = 0 THEN FALSE ELSE TRUE END) ;  
            
-----------------------------------------------------------------------------------------           
  insert into all_sql_temp (ordinal,sqlText)
     select  1 as ordinal
       ,:sqlText1 || listagg(''        '' || column_name || ''\\n'') within group (order by ordinal_position) 
         || '')'' as sqlText
        from all_cols_temp;

------------------------------------------------------------------------------------
col_list := (
  SELECT LISTAGG(sqlText) WITHIN GROUP (ORDER BY inlt1.ORDINAL)
    FROM (
      SELECT sqlText || '' \\n '' as sqlText, 1 AS ORDINAL 
      FROM all_sql_temp   
        UNION
      SELECT LISTAGG(case when ordinal_position = minordinal 
         then ''SELECT ''
         else '' '' 
         end
        || case  column_name  
           when ''  DW_RANGESTART, '' then ''  :highwater ''
           when ''  DW_RANGEEND, ''   then ''  MAX(MTLN_CDC_SEQUENCE_NUMBER) OVER(PARTITION BY 1) ''           
           else replace(column_name,'','') 
           end
        || '' as '' 
        || case when ordinal_position = maxordinal 
             then replace(column_name,'','') || :sqlTextB || :sqlTextC
            else column_name
            end  || '' \\n'') WITHIN GROUP (ORDER BY ordinal_position) 
        as output_text
      , 2 AS ORDINAL 
      from all_cols_temp 
    ) inlt1
);
  
--======================================================================================  
 IF (:IsTableName IS NULL)  --ONLY RETURN RESULTS IF INPUT IS VALID TABLE NAME
    THEN
      RETURN TABLE(ErrorMessage);
    ELSE
      RETURN TABLE(col_list);
  END IF;

END';