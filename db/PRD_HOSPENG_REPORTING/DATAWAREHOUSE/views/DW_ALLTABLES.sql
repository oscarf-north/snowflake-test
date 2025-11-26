create or replace view DW_ALLTABLES(
	TABLE_NAME,
	HASDATAWAREHOUSETABLE,
	VIEW_COLUMN_COUNT,
	DW_COLUMN_COUNT,
	FK_COUNT
) as
--============================================================================================
--call DEV_HOSPENG_REPORTING.PUBLIC.UTILITY_DEPENDENCIES()
select t.table_name                                                                                  as table_name
,MAX(CASE WHEN t.table_type = 'BASE TABLE' and t.table_schema = 'DATAWAREHOUSE' THEN TRUE ELSE FALSE END)
                                                                                                     as HasDatawarehouseTable
,SUM(case when t.table_schema = 'POSTGRES_PUBLIC' and t.table_type = 'VIEW' then 1 else 0 end)       as view_column_count
,SUM(case when t.table_schema = 'DATAWAREHOUSE' and t.table_type = 'BASE TABLE' then 1 else 0 end)   as dw_column_count
,sum(case when t.table_schema = 'POSTGRES_PUBLIC' and t.table_type = 'VIEW' AND c.column_name like '%_FK%' 
                                   THEN 1 ELSE 0 END)                                                                                                                                                                                            as fk_count
      from information_schema.tables t
      inner join information_schema.columns c
          on t.table_schema     = c.table_schema
            and t.table_name    = c.table_name
            and ((t.table_type = 'VIEW' and  t.table_schema = 'POSTGRES_PUBLIC')
                   or (t.table_type = 'BASE TABLE' and  t.table_schema = 'DATAWAREHOUSE'))
            and t.table_type    IN ('VIEW','BASE TABLE')
            and t.table_name NOT LIKE 'DW_%'
 group by t.table_name
 order by t.table_name
 ;