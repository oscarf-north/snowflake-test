create or replace task TASK_CHECKNEGONE_ALL
	warehouse=HOSPENG_SMALL_WH
	schedule='USING CRON 0 8 * * * America/Chicago'
	USER_TASK_TIMEOUT_MS=3600000
	as CALL dataadmin.sp_dag_CHECKNEGONE_ALL();