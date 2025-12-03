CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_MENUITEMDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');

--=================================================================================================================================
BEGIN
--=================================================================================================================================
reportSet := (  
----------------------------------------------------------------------------------------------------------------------------------
SELECT TO_CHAR(:STARTDATE,''YYYYMMDD'') AS BUSDATE-- Business Date Date as YYYYMMD
,MEN.MENUITEMNAME_DIM_NK AS MINUM        --Item number  --Numeric identifierfor the menu item
,MEN.MENUITEMNAME AS MIDESC              --Item description -- ie ''hamburger''
,CCD.COGSCATEGORY_DIM_NK AS CATNUM       --Category number
,CCD.COGSCATEGORY AS CATDESC             --Category description
,MEG.REPORTCATEGORY_DIM_NK AS MINCATNUM  --Minor category number
,MEG.REPORTCATEGORY AS MINCATDESC        --Minor category description
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
 GROUP BY MEN.MENUITEMNAME_DIM_NK 
    ,MEN.MENUITEMNAME 
    ,CCD.COGSCATEGORY_DIM_NK
    ,CCD.COGSCATEGORY 
    ,MEG.REPORTCATEGORY_DIM_NK
    ,MEG.REPORTCATEGORY          
 ORDER BY MEN.MENUITEMNAME_DIM_NK,MEG.REPORTCATEGORY_DIM_NK,CCD.COGSCATEGORY_DIM_NK
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';