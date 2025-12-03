create or replace task TASK_DAG_SFTPJOBS
	warehouse=HOSPENG_SMALL_WH
	schedule='USING CRON 0 5 * * * America/Chicago'
	SUSPEND_TASK_AFTER_NUM_FAILURES=3
	USER_TASK_TIMEOUT_MS=3600000
	as CALL dataadmin.sp_dag_sftpjobs();