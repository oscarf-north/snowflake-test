CREATE OR REPLACE PROCEDURE "SP_SEND_ERROR_TYPES_OVER_SLACK"("CHANNEL" VARCHAR, "ERROR_TYPES" ARRAY)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_alert'
EXTERNAL_ACCESS_INTEGRATIONS = (SLACK_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret'=DATAADMIN.SLACK_WEBHOOK_PROD_DATAWAREHOUSE_ALERTS)
EXECUTE AS OWNER
AS '
"""
-- Description:
--   Queries the `dataadmin.error_logs` table for unnotified errors that match a specific list
--   of error types. It then aggregates these errors, formats them into a structured and readable
--   message using the Slack Block Kit API, and sends the alert to a pre-configured Slack channel
--   via a webhook.
--
--   After successfully sending the notification, it updates the processed error log entries
--   with a notification timestamp and a unique batch ID. This prevents the same error from
--   being sent in subsequent runs. The procedure is designed to be idempotent and is ideal
--   for automated execution within a Snowflake Task for near real-time error monitoring.
--
-- Parameters:
--   channel (VARCHAR): The name of the target Slack channel (e.g., ''#data-alerts'').
--     Note: This parameter is for logging and logical separation. The actual destination is
--     determined by the webhook URL stored in the `webhook_secret`.
--
--   error_types (ARRAY): An array of strings representing the `ERROR_TYPE_ID`s to query for.
--     The procedure will only look for errors whose type is present in this array.
--
-- Returns:
--   VARCHAR: A status message indicating the outcome.
--     - ''No new errors to report.'' -> No matching, unnotified errors were found.
--     - ''Alert sent for {error_count} error(s).'' -> The notification was sent successfully.
--     - A descriptive error message if the API call to Slack fails.
--
-- Dependencies:
--   - Table: `dataadmin.error_logs` must exist and contain the error records.
--   - External Access Integration: `slack_external_access_integration` must be configured to allow
--     network egress to `hooks.slack.com`.
--   - Secret: `dataadmin.SLACK_WEBHOOK_PROD_DATAWAREHOUSE_ALERTS` must be created and contain the
--     unique path/token of the Slack incoming webhook URL (the part after `https://hooks.slack.com/services/`).
--
-- Usage Example:
--   -- Call the procedure to send alerts for specific data quality and timeout errors.
--   CALL dataadmin.sp_send_error_types_over_slack(
--     ''datawarehouse-alerts'',
--     [2,3]
--   );
"""
import _snowflake
import json
import urllib.request
from snowflake.snowpark.functions import col, lit, array_contains, count, listagg, when, current_timestamp

def trim_message(message, max_len=800, front_len=200, back_len=300):
    message = message.replace(''`'', "''")
    if len(message) > max_len:
        return message[:front_len] + "... (trimmed) ..." + message[-back_len:]
    return message

def send_alert(session, channel, error_types):
    # 1. Query for unnotified errors using Snowpark DataFrames
    error_logs_df = session.table(''dataadmin.error_logs'')

    unnotified_errors_df = error_logs_df.filter(
        (col(''ERROR_NOTIFICATION_TS'').is_null()) &
        (array_contains(col(''ERROR_TYPE_ID'').cast(''variant''), lit(error_types)))
    )

    # Cache the result to avoid re-querying
    unnotified_errors_df.cache_result()

    error_count = unnotified_errors_df.count()
    if error_count == 0:
        return ''No new errors to report.''

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
    notification_id = session.sql(get_graph_run_id_query).collect()[0][0]

    # Collect the IDs for the final UPDATE statement
    error_ids_to_update = [row[''ID''] for row in unnotified_errors_df.select(''ID'').collect()]

    # 2. Aggregate and format the error data in Python
    # Group by task name to structure the alert
    grouped_errors = unnotified_errors_df.group_by(''TASK_NAME'').agg(
        listagg(''SQL_ERROR_MESSAGE'', ''\\n'').alias(''MESSAGES''),
        count(''*'').alias(''ERROR_COUNT'')
    ).collect()

    # 3. Build the Slack Block Kit Payload in Python
    # This gives us much more control over formatting.
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":alert: *{error_count} new error(s) found in Snowflake!*"
            }
        },
        {"type": "divider"}
    ]

    for row in grouped_errors:
        task_name = row[''TASK_NAME''] or ''MANUAL_RUN''
        task_error_count = row[''ERROR_COUNT'']
        
        task_header_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{task_name}:* {task_error_count} error(s)"
            }
        }
        blocks.append(task_header_block)

        messages = row[''MESSAGES''].split(''\\n'')
        
        # Show up to 5 errors to avoid huge messages
        for msg in messages[:5]:
            error_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"  • `{trim_message(msg)}`"
                }
            }
            blocks.append(error_block)

        if len(messages) > 5:
            more_errors_block = {
                 "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"...and {len(messages) - 5} more."
                    }
                ]
            }
            blocks.append(more_errors_block)

        blocks.append({"type": "divider"})


    slack_payload = {"blocks": blocks}

    # 4. Send the message to Slack
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
                return f"Failed to send message. Status: {response.status}, Body: {response.read().decode()}"
    except Exception as e:
        return f"Error sending Slack message: {e}"

     # 5. Update the log table to mark errors as notified
    session.table(''dataadmin.error_logs'').update(
        {
            "ERROR_NOTIFICATION_TS": current_timestamp(),
            "ERROR_NOTIFICATION_ID": lit(notification_id)
        },
        col(''ID'').in_(error_ids_to_update)
    )

    return f"Alert sent for {error_count} error(s)."
';