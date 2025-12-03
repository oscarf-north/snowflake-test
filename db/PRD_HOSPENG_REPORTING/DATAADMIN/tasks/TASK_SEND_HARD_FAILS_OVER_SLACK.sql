create or replace task TASK_SEND_HARD_FAILS_OVER_SLACK
	warehouse=HOSPENG_XSMALL_WH
	schedule='5 MINUTE'
	USER_TASK_TIMEOUT_MS=3600000
	as CALL dataadmin.sp_send_hard_fails_over_slack();