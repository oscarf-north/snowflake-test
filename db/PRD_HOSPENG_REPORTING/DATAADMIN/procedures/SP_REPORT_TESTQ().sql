CREATE OR REPLACE PROCEDURE "SP_REPORT_TESTQ"()
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- -- ====================================================================================================================
--Example Call Statement
--CALL DATAADMIN.SP_REPORT_PMIX(''2000-11-20T14:48:37.661Z'',''2023-11-20T14:48:37.661Z'',''[351,352]'');
-- ====================================================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2023-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
-- -- ====================================================================================================================
-- --NOTE:  Convert to local timezone.
-- --NOTE:  Split checks - report quantity - what about an item that was split, but one split check was 
-- --       removed?  In that case, we would have 3/4 of an item see sql.
-- --==========================================================================================
BEGIN

reportSet := (
 SELECT 1 as count
    
--------------------------------------------------------------------------------------------   
FROM DATAADMIN.ITEM_FACT                                    itf
     
--==========================================================================================
);
RETURN TABLE(reportSet); 
END;
';