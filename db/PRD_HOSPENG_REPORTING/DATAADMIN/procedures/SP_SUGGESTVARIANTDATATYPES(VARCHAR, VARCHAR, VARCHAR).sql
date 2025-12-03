CREATE OR REPLACE PROCEDURE "SP_SUGGESTVARIANTDATATYPES"("DBNAME" VARCHAR(50), "SCHEMANAME" VARCHAR(50), "TABLENAME" VARCHAR(50))
RETURNS TABLE ("COLUMN_NAME" VARCHAR(75), "MESSAGE" VARCHAR(999))
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  -- DBNAME VARCHAR(45)             := ''DEV_HOSPENG_REPORTING'';
  -- SCHEMANAME VARCHAR(45)         := ''DATAADMIN'';
  -- TABLENAME VARCHAR(45)          := ''ERRORDWDATE_DIM'';  --''ACTIVITY_FACT''--''WRONG_FACT'';--''CHEQUE_FACT'';--ERRORDWDATE_DIM''LOCATIONGROUP_DIM'';

----------------------------------------------------------------------------------------------------------------------
  DBName_VAR VARCHAR(075)        := UPPER(DBNAME);
  SchemaName_VAR VARCHAR(075)    := UPPER(SCHEMANAME);
  TableName_VAR VARCHAR(050)     := UPPER(TABLENAME);
  TableType_VAR  VARCHAR(75)     := ''VIEW''; 
  ColumnCount INT                ;
  TABLENAME_COUNT int            := 0;
  ResultList RESULTSET           := (SELECT ''Non-existant view or table.'' AS COLUMN_NAME,''Reiew input view or table.'' AS MESSAGE);
  dtypSQL    VARCHAR ;
  sqltext1 := 
  $$
  UNION

  SELECT ''<COLUMN_NAME>'' as COLUMN_NAME
    , ''<COLUMN_NAME>'' || '' is a VARIANT data type, but should be '' 
      || CASE WHEN MIN(typeof(<COLUMN_NAME>)) =  MAX(typeof(<COLUMN_NAME>))  THEN MIN(typeof(<COLUMN_NAME>))
        ELSE MIN(typeof(<COLUMN_NAME>)) || '' or '' ||  MAX(typeof(<COLUMN_NAME>)) END
      || ''.'' as Message
    FROM <TABLE_NAME>
$$;

----------------------------------------------------------------------------------------------------------------
BEGIN

DROP TABLE IF EXISTS Column_List CASCADE; 

----------------------------------------------------------------------------------------------------------------
--MAKEe sure this is a valid table or view name
TABLENAME_COUNT:= (
  SELECT count(t.table_name) FROM information_schema.tables t
   WHERE t.table_schema  = :SchemaName_VAR
     AND t.table_type    = :TableType_VAR
     AND t.table_name    = :TableName_VAR
);

----------------------------------------------------------------------------------------------------------------
--If there is a valid table
----------------------------------------------------------------------------------------------------------------
IF (TABLENAME_COUNT > 0 )  --if it''s a valid table name, look for any columns with type variant
THEN
        CREATE TEMP TABLE Column_List AS
              SELECT t.table_name,c.column_name,c.ordinal_position,c.data_type
              FROM information_schema.tables t
                INNER JOIN information_schema.columns c
                  ON t.table_schema     = c.table_schema
                    AND t.table_name    = c.table_name
                    AND t.table_schema  = :SchemaName_VAR
                    AND t.table_type    = ''VIEW''
                    AND t.table_name    = :TableName_VAR;

----------------------------------------------------------------------------------------------------------------
  ColumnCount:=(SELECT COUNT(*) FROM Column_List c where upper(c.data_type) = ''VARIANT'');
  ResultList:= (SELECT ''No Variant Columns.'' AS COLUMN_NAME,''No variant data type columns exist for thist table or view.'' AS MESSAGE);           
----------------------------------------------------------------------------------------------------------------        
END IF;  --end if there is a valid table

----------------------------------------------------------------------------------------------------------------
--Only execute this block if there are columns of a type variant as that is the only data type used by vtype
----------------------------------------------------------------------------------------------------------------
IF ( :ColumnCount > 0 )  --only continue if an actual table has been found
  THEN
   dtypSQL:=(
      SELECT SUBSTRING(LISTAGG(REPLACE(REPLACE(:SQLTEXT1,''<COLUMN_NAME>'',COLUMN_NAME),''<TABLE_NAME>'',:TableName_VAR)),9,99999999)
        AS SQLEXECTEXT
        FROM  Column_List C  where upper(c.data_type) = ''VARIANT'' 
        );
      
   ResultList := (EXECUTE IMMEDIATE :dtypSQL );
END IF;
    
 RETURN TABLE(ResultList);
END';