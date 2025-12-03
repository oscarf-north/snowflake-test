CREATE OR REPLACE PROCEDURE "SP_REPORT_SUMMARY"("DAY" DATE)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- -- =====================================================================================
--Example Call Statement
-- CALL DATAADMIN.SP_REPORT_SUMMARY(''2024-06-13'');
-- GRANT usage ON procedure dataadmin.SP_REPORT_PMIX(timestamp_tz,timestamp_tz,string) TO ROLE HOSPENG_ADMIN;
-- --=========================================================================================
DECLARE 
  reportSet resultset;
  -- day date := ''2024-06-13'';

--=========================================================================================
BEGIN
 reportSet:= (

SELECT DATE(CHK.CLOSED_AT)                                       AS "Date"
   ,CHK.NET::DECIMAL(18,2)                                       AS "Net Sales"
   ,CHK.SURCHARGE::DECIMAL(18,2) - CHK.GRATUITIES::DECIMAL(18,2) AS "Service Charge"
   ,CHK.TAX::DECIMAL(18,2)                                       AS "Tax Collected"
   --------------------------------
   ,CHK.DISCOUNTITEM                                             AS "Item Discounts"
   ,CHK.DISCOUNTCHECK                                            AS "Check Discounts"
   ,CHK.DISCOUNT                                                 AS "Total Discounts"

  FROM DATAADMIN.CHEQUE_FACT  CHK
    WHERE location_dim_fk = 2
      AND CHK.STATUS = ''Closed''
      AND DATE(CHK.CLOSED_AT) = :day--DATE(''2024-06-13'')


);
RETURN TABLE(reportSet); 
END;
-- ';