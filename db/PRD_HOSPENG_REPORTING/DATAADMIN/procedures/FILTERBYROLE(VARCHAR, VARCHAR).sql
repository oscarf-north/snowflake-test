CREATE OR REPLACE PROCEDURE "FILTERBYROLE"("TABLENAME" VARCHAR(16777216), "ROLE" VARCHAR(16777216))
RETURNS TABLE ("TESTTEXT" VARCHAR(16777216))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'filter_by_role'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
   df = session.table(table_name)
   #return df.filter(col("role") == role)
   return df
';