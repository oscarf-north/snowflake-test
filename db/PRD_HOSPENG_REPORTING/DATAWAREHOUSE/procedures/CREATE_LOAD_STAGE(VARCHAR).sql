CREATE OR REPLACE PROCEDURE "CREATE_LOAD_STAGE"("TABLE_NAME_INPUT" VARCHAR(16777216))
RETURNS TABLE ("SQLTEXT" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
declare select_statement varchar;
        col_list    resultset;
        --select_list resultset;
        res         resultset;
        sqlText2    varchar := '')'';
        sqlTextA    varchar := ''SELECT '';
        --table_name_input varchar := ''ADDRESS_DIM'';
        table_name  varchar := table_name_input; 
        sqlText1    varchar := ''INSERT OVERWRITE INTO DEV_HOSPENG_REPORTING.DATASTAGE.'' 
                    || :table_name ||'' (  \\n''; 
        sqlTextB    varchar := '' from DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.'' || :table_name || '';''; 
        all_sqltext varchar;
-----------------------------------------------------------------------------------------
begin
  drop table if exists DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_cols; 
  drop table if exists DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_sql;
  
  create temp table DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_sql (
    ordinal int
    ,sqlText varchar
  );

------------------------------------------------------------------------------------
  create temp table DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_cols as
      select  c.column_name 
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
                and t.table_schema = ''DATASTAGE''
                and t.table_name   = :table_name
                --and c.column_name Not like ''DW_%               
            order by c.ordinal_position;
            
-----------------------------------------------------------------------------------------           
  insert into DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_sql (ordinal,sqlText)
     select  1 as ordinal
       ,:sqlText1 || listagg(column_name) within group (order by ordinal_position) 
         || :sqlText2 as sqlText
        from DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_cols;

------------------------------------------------------------------------------------
col_list := (
  SELECT LISTAGG(sqlText) WITHIN GROUP (ORDER BY inlt1.ORDINAL)
    FROM (
      SELECT sqlText || '' \\n '' as sqlText, 1 AS ORDINAL 
      FROM DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_sql   
        UNION
      SELECT LISTAGG(case when ordinal_position = minordinal 
         then :sqlTextA
         else '' '' 
         end
        || replace(column_name,'','') 
        --|| case when column_name like ''%_FK%'' then '' || ''''.'''' ||  DW_BATCHID '' else '' '' end
        || '' as '' 
        || case when ordinal_position = maxordinal 
             then replace(column_name,'','') || :sqlTextB
            else column_name
            end  || '' \\n'') WITHIN GROUP (ORDER BY ordinal_position) 
        as output_text
      , 2 AS ORDINAL 
        --,ordinal_position + 100 as ordinal
      from DEV_HOSPENG_REPORTING.POSTGRES_PUBLIC.all_cols 
    ) inlt1
);

res := (
     select ''skssksk sksk'' as output_text
   );
   
  
--======================================================================================         
return table(col_list);

end;
';