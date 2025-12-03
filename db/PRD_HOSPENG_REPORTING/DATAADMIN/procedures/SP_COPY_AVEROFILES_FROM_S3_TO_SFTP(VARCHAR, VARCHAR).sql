CREATE OR REPLACE PROCEDURE "SP_COPY_AVEROFILES_FROM_S3_TO_SFTP"("DATE_NAME" VARCHAR, "LOCATION_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pysftp')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (SFTP_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('cred'=DATAADMIN.SFTP_CREDENTIALS)
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
import pysftp
import os
import _snowflake
import json

def run(session: snowpark.Session, date_name: str, location_id: str) -> str:
    """
    Copies files from a specified path in a Snowflake S3 stage to a corresponding
    path on an SFTP server.

    Args:
        session: The Snowflake Snowpark session object.
        date_name: The date for which to process files, in ''YYYY-MM-DD'' format.
        location_id: The location identifier.

    Returns:
        A string containing logs of the operation.
    
    Raises:
        RuntimeError: If there are critical errors, e.g., connecting to SFTP,
                      listing S3 files, or retrieving credentials.
        IOError: If creating the remote directory on the SFTP server fails.
    """
    # --- CONFIGURATION ---
    #retrieve env vars
    required_vars = [''S3_STAGE_NAME'', ''SFTP_BASE_DIR'']    
    fetched_vars : dict = json.loads(session.call(''DATAADMIN.SP_GET_ENVVARS'', required_vars))
    S3_STAGE_NAME, SFTP_BASE_DIR = [fetched_vars.get(key) for key in required_vars]

    # Hardcoded variables that don''t change accross dev and prod account.
    S3_BASE_PATH = ''AVEROFTP''
    SFTP_HOST = ''sftp.averoinc.com''
    SFTP_SECRET_NAME = ''cred'' # Alias for the Snowflake secret
    TEMP_DIR = ''/tmp'' # Snowflake''s temporary directory for staging files

    logs = []
    files_copied_count = 0
    
    try:
        # --- 1. Path Construction ---
        # Convert ''YYYY-MM-DD'' to ''YYYYMMDD''
        date_text = date_name.replace(''-'', '''')
        logs.append(f"Processing for Location: {location_id}, Date: {date_name} ({date_text})")

        # Construct source and destination paths
        s3_source_path = f"{S3_BASE_PATH}/Location{location_id}/{date_text}"
        sftp_target_dir = f"{SFTP_BASE_DIR}/Location{location_id}/{date_text}"
        
        logs.append(f"Source S3 path: @{S3_STAGE_NAME}/{s3_source_path}")
        logs.append(f"Target SFTP path: {sftp_target_dir}")

        # --- 2. List files in S3 Stage ---
        try:
            s3_files_to_copy = session.sql(f"LIST @{S3_STAGE_NAME}/{s3_source_path}").collect()
            if not s3_files_to_copy:
                message = f"Warning: No files found in S3 path ''@{S3_STAGE_NAME}/{s3_source_path}''. Procedure finished successfully with no files to copy."
                logs.append(message)
                return "\\n".join(logs)
            logs.append(f"Found {len(s3_files_to_copy)} files in S3 to copy.")
        except Exception as e:
            # This could be a legitimate error if the path doesn''t exist.
            raise RuntimeError(f"Error listing files from S3 stage path ''@{S3_STAGE_NAME}/{s3_source_path}'': {e}")

        # --- 3. Get SFTP Credentials ---
        try:
            creds = _snowflake.get_username_password(SFTP_SECRET_NAME)
            SFTP_USER = creds.username
            SFTP_PASS = creds.password
            logs.append("Successfully retrieved SFTP credentials.")
        except Exception as e:
            raise RuntimeError(f"Error retrieving SFTP credentials: {e}")

        # --- 4. Connect to SFTP and Transfer Files ---
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # In production, it''s more secure to use known host keys.

        with pysftp.Connection(host=SFTP_HOST, username=SFTP_USER, password=SFTP_PASS, cnopts=cnopts) as sftp:
            logs.append(f"✅ Successfully connected to SFTP server: {SFTP_HOST}")

            # --- 4a. Create remote directory if it doesn''t exist ---
            if not sftp.exists(sftp_target_dir):
                logs.append(f"Target directory ''{sftp_target_dir}'' does not exist. Creating it.")
                try:
                    sftp.makedirs(sftp_target_dir)
                    logs.append(f"✅ Successfully created directory: {sftp_target_dir}")
                except Exception as e:
                    raise IOError(f"Failed to create remote directory ''{sftp_target_dir}'': {e}")
            else:
                logs.append(f"Target directory ''{sftp_target_dir}'' already exists.")

            # --- 4b. Loop through files and transfer ---
            for file_row in s3_files_to_copy:
                full_s3_path = file_row[''name'']
                file_name = os.path.basename(full_s3_path)
                
                # Correctly construct the relative path for the GET command.
                # It should be relative to the stage''s root directory, using the same
                # path prefix that was used to list the files.
                stage_file_path = f"@{S3_STAGE_NAME}/{s3_source_path}/{file_name}"
                local_temp_path = os.path.join(TEMP_DIR, file_name)
                sftp_remote_path = f"{sftp_target_dir}/{file_name}" # pysftp uses forward slashes

                try:
                    # Download from S3 stage to local /tmp
                    logs.append(f"  - Downloading {stage_file_path} to {TEMP_DIR}...")
                    session.file.get(stage_file_path, TEMP_DIR)
                    
                    # Upload from local /tmp to SFTP
                    logs.append(f"  - Uploading {local_temp_path} to {sftp_remote_path}...")
                    sftp.put(local_temp_path, sftp_remote_path)
                    logs.append(f"  - ✅ Success: Uploaded {file_name}.")
                    files_copied_count += 1
                    
                except Exception as e:
                    # Log file-specific error and re-raise to fail the entire procedure
                    logs.append(f"  - ❌ Error copying file {file_name}: {e}")
                    raise # Re-raise the exception to be caught by the outer block
                finally:
                    # Clean up the temp file
                    if os.path.exists(local_temp_path):
                        os.remove(local_temp_path)

        summary = f"Process finished. Copied {files_copied_count}/{len(s3_files_to_copy)} files."
        logs.append(summary)
        return "\\n".join(logs)

    except Exception as e:
        logs.append(f"❌ An unexpected error occurred: {e}")
        # Re-raise the exception to make the SP fail, so the calling DAG can log it.
        raise e
';