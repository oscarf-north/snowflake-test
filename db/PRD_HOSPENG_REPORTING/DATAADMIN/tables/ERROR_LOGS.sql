create or replace TABLE PRD_HOSPENG_REPORTING.DATAADMIN.ERROR_LOGS (
	ID VARCHAR(16777216) NOT NULL DEFAULT UUID_STRING() COMMENT 'Unique identifier for each error log entry.',
	EVENT_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when the error event occurred.',
	PARENT_QUERY_ID VARCHAR(16777216) COMMENT 'The query_id associated with the task run in the Snowflake UI.',
	TASK_RUN_GROUP_ID VARCHAR(16777216) COMMENT 'This ID plus the attempt number uniquely identifies the run. Will be ''MANUAL_RUN'' if not run by a task.',
	ATTEMPT_NUMBER NUMBER(38,0) COMMENT 'The attempt number for the task run, can be 2 or more if the task has retries.',
	SESSION_ID VARCHAR(16777216) COMMENT 'The session ID for the task execution. Useful as a \"run id\" to identify all commands executed in that session.',
	TASK_NAME VARCHAR(16777216) COMMENT 'The name of the task, obtained at runtime.',
	FAILED_QUERY_ID VARCHAR(16777216) COMMENT 'The query_id of the specific statement that failed, obtained at runtime.',
	ERROR_TYPE_ID NUMBER(38,0) COMMENT 'Identifier for the error type. For example, 1 for a hard fail and 2 for a soft fail.',
	SEVERITY VARCHAR(16777216) COMMENT 'The severity level of the error, such as WARN, ERROR, or CRITICAL.',
	SQL_ERROR_CODE VARCHAR(16777216) COMMENT 'The specific SQL error code returned by Snowflake.',
	SQL_ERROR_MESSAGE VARCHAR(16777216) COMMENT 'The descriptive error message returned by Snowflake.',
	SQL_STATE VARCHAR(16777216) COMMENT 'The SQLSTATE five-character error code returned by Snowflake.',
	DETAILS VARIANT COMMENT 'A generic JSON object to store any valuable metadata for later inspection or understanding of the error.',
	ERROR_NOTIFICATION_ID VARCHAR(16777216) COMMENT 'The graph run ID of the alerting task that reads this table to notify on errors.',
	ERROR_NOTIFICATION_TS TIMESTAMP_NTZ(9) COMMENT 'Timestamp indicating when the error in this row was reported.',
	primary key (ID)
);
