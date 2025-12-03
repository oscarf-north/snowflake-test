CREATE OR REPLACE PROCEDURE "SP_GET_ENVVARS"("SECRET_NAMES" ARRAY)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
/*
How to use in python:
```
    # 1. Define the environment variables you need for this procedure
    required_vars = [''S3_STAGE_NAME'', ''SFTP_BASE_DIR'']
    
    fetched_vars : dict = json.loads(session.call(''DATAADMIN.SP_GET_ENVVARS'', required_vars))

    # 2. Unpack the dictionary values into variables in a specific order.
    s3_stage, sftp_base_dir = [fetched_vars.get(key) for key in required_vars]
```

How to use in sql procedure:
```
DECLARE
    config_obj VARIANT;
    s3_stage VARCHAR;
    sftp_base_dir VARCHAR;
BEGIN
    CALL DATAADMIN.SP_GET_ENVVARS(ARRAY_CONSTRUCT(''S3_STAGE_NAME'', ''SFTP_BASE_DIR''));

    SELECT "SP_GET_ENVVARS" INTO :config_obj FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    s3_stage := :config_obj:S3_STAGE_NAME::STRING;
    sftp_base_dir := :config_obj:SFTP_BASE_DIR::STRING;
```
 */
DECLARE
    secrets_obj VARIANT;
BEGIN
    -- Aggregate the key-value pairs into a single JSON object
    -- CORRECTED: Use PARAM_NAME and PARAM_VALUE to match the table columns
    SELECT OBJECT_AGG(PARAM_NAME, PARAM_VALUE::VARIANT)
    INTO :secrets_obj
    FROM DATAADMIN.ENV_VARS
    -- Filter the table to only include the names passed in the input array
    WHERE PARAM_NAME IN (SELECT VALUE::STRING FROM TABLE(FLATTEN(INPUT => :secret_names)));

    RETURN secrets_obj;
END;
';