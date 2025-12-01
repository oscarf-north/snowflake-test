CREATE OR REPLACE PROCEDURE "SP_SEND_HARD_FAILS_OVER_SLACK"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_hard_fail_alert'
EXTERNAL_ACCESS_INTEGRATIONS = (SLACK_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret'=DATAADMIN.SLACK_WEBHOOK_PROD_DATAWAREHOUSE_ALERTS)
EXECUTE AS CALLER
AS '
"""
-- Description:
--   Monitors for and reports "hard fails" of Snowflake Tasks. A hard fail is a critical
--   error that prevents a task from completing, such as a SQL compilation error, permission
--   issue, or warehouse problem, resulting in a ''FAILED'' state.
--
--   The procedure operates statefully:
--   1. It determines the last time it ran by checking for the most recent hard fail logged in
--      the `dataadmin.error_logs` table, creating an efficient, overlapping time window.
--   2. It queries `information_schema.task_history` to find any new task failures.
--   3. It cross-references these failures against its own log to ensure alerts are not duplicated.
--   4. New failures are formatted into a detailed critical alert and sent to a Slack channel.
--   5. Only AFTER a successful Slack notification, it records the failures in the
--      `dataadmin.error_logs` table to prevent re-alerting in the future.
--
--   This procedure is designed to be run on a simple schedule (e.g., every 15 minutes) by a
--   master monitoring task to provide near real-time alerts on pipeline infrastructure failures.
--
-- Parameters:
--   This procedure does not accept any parameters.
--
-- Returns:
--   VARCHAR: A status message indicating the outcome of the operation.
--     - ''No new hard fails to report.'' -> No new task failures were found.
--     - ''Successfully reported and logged {failure_count} hard fail(s).'' -> On success.
--     - A descriptive error message if the API call to Slack fails. The procedure will halt
--       without logging the errors to ensure they are picked up on the next run.
"""
import _snowflake
import json
import urllib.request
from snowflake.snowpark.functions import col, lit, max as sp_max, coalesce, dateadd, current_timestamp, concat, call_function
from collections import defaultdict

def trim_message(message, max_len=800, front_len=200, back_len=300):
    message = message.replace(''`'', "''")
    if len(message) > max_len:
        return message[:front_len] + "... (trimmed) ..." + message[-back_len:]
    return message

def send_hard_fail_alert(session):
    # 1. Determine the start time for the search.
    # This query finds the last hard fail''s timestamp, subtracts 1 hour as a safety margin,
    # and defaults to a 24-hour lookback if no previous hard fails have been logged.
    ts_query = """
        SELECT DATEADD(''hour'', -1, COALESCE(
            (SELECT MAX(event_timestamp) FROM dataadmin.error_logs WHERE error_type_id = 1),
            DATEADD(''hour'', -24, CURRENT_TIMESTAMP())
        ))
    """
    last_check_ts = session.sql(ts_query).collect()[0][0]

    # 2. Query information_schema.task_history for new failed tasks.
    task_history_df = session.sql(f"""
        SELECT
            GRAPH_RUN_GROUP_ID,
            ATTEMPT_NUMBER,
            NAME,
            STATE,
            ERROR_CODE,
            ERROR_MESSAGE,
            QUERY_ID,
            COMPLETED_TIME
        FROM TABLE(information_schema.task_history(
            SCHEDULED_TIME_RANGE_START => TO_TIMESTAMP_LTZ(''{last_check_ts}''),
            ERROR_ONLY => TRUE
        ))
    """)

    # 3. Exclude records that have already been reported in error_logs.
    # We create a unique run key to identify distinct task runs.
    existing_logs_df = session.table(''dataadmin.error_logs'').filter(col(''ERROR_TYPE_ID'') == 1).select(
        concat(col(''TASK_RUN_GROUP_ID''), lit(''|''), col(''ATTEMPT_NUMBER'')).alias(''RUN_KEY'')
    )

    new_failures_df = task_history_df.withColumn(
        ''RUN_KEY'', concat(col(''GRAPH_RUN_GROUP_ID''), lit(''|''), col(''ATTEMPT_NUMBER''))
    ).join(
        existing_logs_df,
        [''RUN_KEY''],
        ''left_anti''
    )

    # Cache the result to materialize the list of new failures.
    new_failures_df.cache_result()
    
    failures_to_report = new_failures_df.collect()
    failure_count = len(failures_to_report)
    if failure_count == 0:
        return ''No new hard fails to report.''
    
    # This ID will be used to mark these errors as part of this notification batch.
    get_graph_run_id_query = """
    BEGIN
        -- Attempt to get the real ID
        RETURN SYSTEM$TASK_RUNTIME_INFO(''CURRENT_TASK_GRAPH_RUN_GROUP_ID'');
    EXCEPTION
        -- If any error occurs (like not being in a task), run this block instead
        WHEN OTHER THEN
            RETURN UUID_STRING();
    END;
    """
    notification_query_id = session.sql(get_graph_run_id_query).collect()[0][0]

    # 4. Build and send the Slack message.
    grouped_failures = defaultdict(list)
    for row in failures_to_report:
        grouped_failures[row[''NAME'']].append(row)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":x: Critical Alert: {failure_count} Snowflake Task Failure(s) Detected"
            }
        },
        {"type": "divider"}
    ]

    for task_name, failures in grouped_failures.items():
        task_error_count = len(failures)
        
        # Add a header for the task group
        task_header_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{task_name}:* {task_error_count} failure(s)"
            }
        }
        blocks.append(task_header_block)

        # Show up to 5 errors to avoid huge messages
        for failure in failures[:5]:
            error_message = trim_message(failure[''ERROR_MESSAGE''])
            query_id = failure[''QUERY_ID'']
            completed_time = failure[''COMPLETED_TIME''].strftime(''%Y-%m-%d %H:%M:%S %Z'')

            # Each failure gets its own block to avoid the 3000 char limit
            failure_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"  • *Failed At:* {completed_time}\\n"
                            f"    *Error:* `{error_message}`\\n"
                            f"    *Query ID:* `{query_id}`"
                }
            }
            blocks.append(failure_block)
        
        if len(failures) > 5:
            more_failures_block = {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"...and {len(failures) - 5} more."
                    }
                ]
            }
            blocks.append(more_failures_block)

        blocks.append({"type": "divider"})

    slack_payload = {"blocks": blocks}
    webhook_secret = _snowflake.get_generic_secret_string(''webhook_secret'')
    webhook_url = f"https://hooks.slack.com/services/{webhook_secret}"
    payload_json = json.dumps(slack_payload).encode(''utf-8'')
    
    req = urllib.request.Request(
        webhook_url,
        data=payload_json,
        headers={''Content-Type'': ''application/json''}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status != 200:
                # Abort if Slack notification fails to avoid incorrectly logging the errors.
                return f"Failed to send Slack message. Status: {response.status}, Body: {response.read().decode()}"
    except Exception as e:
        return f"Error sending Slack message: {e}"

    # 5. After sending the alert, write the failure information to `dataadmin.error_logs`.
    df_to_insert = new_failures_df.select(
        call_function("uuid_string").alias("ID"),
        col(''COMPLETED_TIME'').alias(''EVENT_TIMESTAMP''),
        col(''QUERY_ID'').alias(''PARENT_QUERY_ID''),
        col(''GRAPH_RUN_GROUP_ID'').alias(''TASK_RUN_GROUP_ID''),
        col(''ATTEMPT_NUMBER''),
        lit(None).astype(''string'').alias(''SESSION_ID''), # Not available in task_history
        col(''NAME'').alias(''TASK_NAME''),
        col(''QUERY_ID'').alias(''FAILED_QUERY_ID''),
        lit(1).alias(''ERROR_TYPE_ID''), # 1 = hard fail
        lit(''CRITICAL'').alias(''SEVERITY''),
        col(''ERROR_CODE'').alias(''SQL_ERROR_CODE''),
        col(''ERROR_MESSAGE'').alias(''SQL_ERROR_MESSAGE''),
        col(''STATE'').alias(''SQL_STATE''),
        lit(None).astype(''variant'').alias(''DETAILS''),
        lit(notification_query_id).alias(''ERROR_NOTIFICATION_ID''),
        current_timestamp().alias(''ERROR_NOTIFICATION_TS'')
    )

    df_to_insert.write.mode("append").save_as_table("dataadmin.error_logs")

    return f"Successfully reported and logged {failure_count} hard fail(s)."
';