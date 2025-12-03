create or replace task TASK_SEND_ERROR_TYPES_OVER_SLACK
	warehouse=HOSPENG_XSMALL_WH
	schedule='5 MINUTE'
	USER_TASK_TIMEOUT_MS=3600000
	as CALL dataadmin.sp_send_error_types_over_slack('dev_reporting_errors', [2,3]);