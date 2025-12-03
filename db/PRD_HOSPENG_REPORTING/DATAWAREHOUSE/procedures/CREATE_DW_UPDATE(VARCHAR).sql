CREATE OR REPLACE PROCEDURE "CREATE_DW_UPDATE"("TABLE_NAME_INPUT" VARCHAR(16777216))
RETURNS TABLE ("SQLTEXT" VARCHAR(16777216))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE res_list    resultset;
        --table_name_input varchar := ''PAYMENT_FACT''; --uncomment for development
        table_name  varchar := table_name_input; 
        
-----------------------------------------------------------------------------------------
BEGIN
  res_list := (        
   SELECT LISTAGG(REPLACE(txt.SQLTEXTVALUE,''<Table Name>'',:table_name) || '' \\n '') 
        WITHIN GROUP(ORDER BY txt.SQLTEXTORDINAL)
    FROM DEV_HOSPENG_REPORTING.DATAWAREHOUSE.DW_SQLTEXT  txt
    WHERE txt.SQLTEXTGROUP = ''DWUpdateText''
      AND NOT txt.ISDELETED
      AND txt.ISCURRENTVERSION        
   )  --end of select stmt for sql_text variable
   
   ;
--======================================================================================    
    return table(res_list);
--======================================================================================
END;
--======================================================================================
';