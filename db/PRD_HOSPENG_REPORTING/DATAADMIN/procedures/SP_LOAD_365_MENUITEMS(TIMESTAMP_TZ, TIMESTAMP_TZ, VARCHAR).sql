CREATE OR REPLACE PROCEDURE "SP_LOAD_365_MENUITEMS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--==================================================================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_item; 
  DROP TABLE IF EXISTS TEMP_header; 

----------------------------------------------------------------------------------------------------------------------------------  
  SELECT ''ItemNumber''
  ,''ItemName''     
  ,''CategoryName''  
;  

 CREATE TEMP TABLE TEMP_header AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 
     
------------------------------------------------------------------------------------------------------------------------------------
SELECT  IFNULL(REPLACE(MEN.MENUITEMNAME_DIM_NK,'','',''''),''None'')  AS ItemNumber     --*  Menu Item ID
  ,IFNULL(REPLACE(MEN.MENUITEMNAME,'','',''''),''None'')              AS ItemName       --* string Name of the Menu Item.
  ,IFNULL(REPLACE(CCD.COGSCATEGORY,'','',''''),''None'')              AS CategoryName   --* string Cogs category
 FROM DATAWAREHOUSE.MENUITEM_DIM                                MED
   INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                    MEN
     ON MED.MENUITEMNAME_DIM_FK = MEN.MENUITEMNAME_DIM_NK
       AND MED.DW_ISCURRENTROW
       AND MEN.DW_ISCURRENTROW
   INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM                    ORG
     ON MED.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
       AND ORG.DW_ISCURRENTROW
   INNER JOIN DATAWAREHOUSE.LOCATION_DIM                        LOC
     ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
       AND LOC.DW_ISCURRENTROW
       AND LOC.LOCATION_DIM_NK in (SELECT table1.value 
        FROM table(split_to_table(:locationidS, '',''))  table1) 
    INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM                 MEG
        ON MEN.REPORTCATEGORY_DIM_FK = MEG.REPORTCATEGORY_DIM_NK
          AND MEG.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                 CCD
        ON CCD.COGSCATEGORY_DIM_NK = MEG.COGSCATEGORY_DIM_FK    
          AND CCD.DW_ISCURRENTROW        
GROUP BY IFNULL(REPLACE(MEN.MENUITEMNAME_DIM_NK,'','',''''),''None'') 
  ,IFNULL(REPLACE(MEN.MENUITEMNAME,'','',''''),''None'')       
  ,IFNULL(REPLACE(CCD.COGSCATEGORY,'','',''''),''None'') 
ORDER BY  IFNULL(REPLACE(MEN.MENUITEMNAME_DIM_NK,'','',''''),''None'') 
  ,IFNULL(REPLACE(MEN.MENUITEMNAME,'','',''''),''None'')       
  ,IFNULL(REPLACE(CCD.COGSCATEGORY,'','',''''),''None'') 
  ;

  CREATE TEMP TABLE TEMP_item AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 
  
--==================================================================================================================================
 
reportSet := ( 
  SELECT * FROM TEMP_header
    UNION ALL
  SELECT * FROM TEMP_item
--==================================================================================================================================
);
RETURN TABLE(reportSet); 
END';