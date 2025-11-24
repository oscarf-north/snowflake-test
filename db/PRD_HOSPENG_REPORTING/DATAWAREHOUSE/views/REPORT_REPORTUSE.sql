create or replace view PRD_HOSPENG_REPORTING.DATAWAREHOUSE.REPORT_REPORTUSE(
	"Report Name",
	"Start Date",
	"Start Time",
	"End Time",
	"Query Tag",
	"Bytes Scanned",
	"Rows Produced",
	"Query Text",
	"Execution Status",
	"Error Message",
	"Compilation Time",
	"Execution Time",
	"Total Elapsed Time"
) as
--======================================================================
SELECT REPLACE(ARRAY_TO_STRING(ARRAY_SLICE(STRTOK_TO_ARRAY(QRY.QUERY_TAG, '/')
  , 1, 2), '.'),'report_','')						as "Report Name"
,TO_DATE(QRY.START_TIME)							as "Start Date" 
,QRY.START_TIME										as "Start Time"
,QRY.END_TIME								        as "End Time"
,QRY.QUERY_TAG										as "Query Tag"
,QRY.BYTES_SCANNED									as "Bytes Scanned"
,QRY.ROWS_PRODUCED									as "Rows Produced"
,QRY.QUERY_TEXT										as "Query Text"
,QRY.EXECUTION_STATUS								as "Execution Status"
--,COALESCE(QRY.ERROR_CODE,0.00,'99.9')				as "Error Code"
,COALESCE(QRY.ERROR_MESSAGE,'None')					as "Error Message"
,QRY.COMPILATION_TIME								as "Compilation Time"
,QRY.EXECUTION_TIME									as "Execution Time"
,QRY.TOTAL_ELAPSED_TIME								as "Total Elapsed Time"
	FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
		USER_NAME => 'APP_AIRBYTE_HOSPENG'
        --NOTE:  we have default time of 7 days to keep this history
        --,END_TIME_RANGE_START=>to_timestamp_ltz('2022-02-23 12:00:00.000 -0000')
        --,END_TIME_RANGE_END=>to_timestamp_ltz('2023-12-12 12:00:00.000 -0000')
    ))                                                             QRY
WHERE QRY.QUERY_TYPE = 'SELECT'
  AND QRY.QUERY_TAG LIKE 'REPORT%'
ORDER BY QRY.START_TIME;