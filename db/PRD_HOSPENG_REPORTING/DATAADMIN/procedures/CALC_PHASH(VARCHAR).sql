CREATE OR REPLACE PROCEDURE "CALC_PHASH"("FILE_PATH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','imagehash','pillow')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from PIL import Image
import imagehash
from snowflake.snowpark.files import SnowflakeFile

def run(ignored_session, file_path):
    with SnowflakeFile.open(file_path, ''rb'') as f:
        return imagehash.average_hash(Image.open(f))
';