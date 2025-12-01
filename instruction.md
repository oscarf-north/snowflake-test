# Project Setup Guide (CSV Files)

This guide provides step-by-step instructions to set up the project and run the synchronization process using CSV data files.

## 1. Local Environment Setup

First, you need to install the required Python libraries.

1.  **Install Dependencies:**
    Open your terminal and run the following command from the project root directory:
    ```bash
    pip install -r requirements.txt
    ```

2. **Configure GitHub Credentials**  
    The project connects to GitHub using environment variables. You'll need to create a file to store your credentials.

3.  **Create the Environment File:** 
    Navigate to the `config/` directory and create a file named `env.sh`.

4.  **Add Your Credentials:**
    Copy the content from `config/env_template.sh` into your new `config/env.sh` and fill in your specific GitHub connection details, **ignore Snowflake variables**. It should look like this:

        export GH_TOKEN=""
        export GH_REPOSITORY="nabancard/hospeng-snowflake"

5.  **Load the Environment Variables:**
    Before running any scripts, you must load these variables into your terminal session. **You need to do this for every new terminal session.**
    ```bash
    source config/private/env.sh
    ```

## 2. Prepare the DDL Data Files

This workflow relies on CSV files that contain the exported Data Definition Language (DDL) from Snowflake.

1.  **Obtain CSV Files:**
    You must have the following three CSV files:
    *   `export_ddls_tables.csv`
    *   `export_ddls_views.csv`
    *   `export_ddls_procedures.csv`

    For this you can run the following stored procedures in snowflake: 
        - hospeng-snowflake/db/PRD_HOSPENG_REPORTING/GITHUB/procedures/EXPORT_PROCEDURES_DDLS.sql
        - hospeng-snowflake/db/PRD_HOSPENG_REPORTING/GITHUB/procedures/EXPORT_TABLES_DDLS.sql
        - hospeng-snowflake/db/PRD_HOSPENG_REPORTING/GITHUB/procedures/EXPORT_VIEWS_DDLS.sql

    The output of the procedures are tables, that you have to donwload as CSV files. 

2.  **Place Files in the Correct Directory:**
    Create a directory structure `src/data/<DATABASE_NAME>/`. For example, if your database is `PRD_HOSPENG_REPORTING`, the path would be `src/data/PRD_HOSPENG_REPORTING/`. Place the three CSV files inside this directory.

    The final structure should look like this:
    ```
    src/
    └── data/
        └── <DATABASE_NAME>/
            ├── export_ddls_tables.csv
            ├── export_ddls_views.csv
            └── export_ddls_procedures.csv
    ```

    **Note:** Each CSV file should contain four columns, without a header row: `object_type`, `schema_name`, `object_name`, and `ddl`.

## 3. Run the Sync Script

With the CSV files in place, you can now run the main Python script to process the DDLs and write them to the `db/` directory structure.

1.  **Execute `sync.py`:**
    Run the following command from the project root. Make sure to use the same database name that you used for the folder in the previous step.

    ```bash
    python src/sync.py --source csv --database <DATABASE_NAME> --commit
    ```

After the script finishes, you should see the `db/` directory updated with `.sql` files corresponding to the data in your CSVs. You can then review and commit these changes to Git.
