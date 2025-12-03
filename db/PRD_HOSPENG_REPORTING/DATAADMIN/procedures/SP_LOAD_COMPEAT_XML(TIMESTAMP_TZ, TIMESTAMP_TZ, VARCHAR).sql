CREATE OR REPLACE PROCEDURE "SP_LOAD_COMPEAT_XML"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz      := ''2025-06-05'';
  -- enddate timestamp_tz        := ''2025-06-05'';
  -- locationid string           := ''[26]'';
  startdatelabor timestamp_tz := DATEADD(DAY,-30, to_char(:startdate::DATE));
  locationidS string          := REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  pollDate string             := to_varchar(:startdate,''MM/DD/YYYY'');

---------------------------------------------------------------------------------------------------------------------------
  tagOpen1 string        := ''<?xml version="1.0"?><PollData CompanyID="2159" LocationID="'' || ''17'' || ''" PollDate="'' || :pollDate ||''"><Configuration>'';
  tagConfigClose string  := ''</Configuration>'';
  tagClose1 string       := ''</PollData>'';  
-- tagCheckOpen   string := ''<Check Number="%Checkno%"><CloseTime>%CloseTime%</CloseTime><DaypartName>%DayPart%</DaypartName> <EmployeeNumber>%EmpNo%</EmployeeNumber><GuestCount>%GuestCount%</GuestCount><OpenTime>%OpenTime%</OpenTime><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName>'';  
  tagItem string         := ''<Item Number="%ItemNumber%" Name="%ItemName%"></Item>'';
  tagCategoryOpen string := ''<Category Name="%CategoryName%"><Items>'';
  tagEmployee string     := ''<Employee Number="%EmpID%" FirstName="%FirstName%" LastName="%LastName%">'';
  tagTender string       := ''<Tender Number="%TenderId%" Name="%Name%" Type="%NonCash%" />'';  
  tagTax string          := ''<Tax Number="%TaxNumber%" Name="%TaxName%"><Rate>%TaxRate%</Rate></Tax>'';
  tagCheckOpen    string := ''<Check Number="%Checkno%"><CloseTime>%CloseTime%</CloseTime><DaypartName>%DayPart%</DaypartName> <EmployeeNumber>%EmpNo%</EmployeeNumber><GuestCount>%GuestCount%</GuestCount><OpenTime>%OpenTime%</OpenTime><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName>'';
  tagCheckClose   string := ''<Check>'';
   -- tagItemsale string := ''<ItemSale Quantity="%Quantity%" GrossAmount="%GrossAmount%"><CloseTime>%CloseTime%</CloseTime><DaypartName>%DayPart%</DaypartName> <EmployeeNumber>%EmpNo%</EmployeeNumber><GuestCount>%GuestCount%</GuestCount><OpenTime>%OpenTime%</OpenTime><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName><ItemNumber>%ItemNumber%</ItemNumber>%Promo%</ItemSale>'';
  tagItemsale       string := ''<ItemSale Quantity="%Quantity%" GrossAmount="%GrossAmount%"><DaypartName>%DayPart%</DaypartName><Time>%Time%</Time><EmployeeNumber>%EmpNo%</EmployeeNumber><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName><ItemNumber>%ItemNumber%</ItemNumber>%Promo%</ItemSale>''; 
  tagPayment        string := ''<Payment BaseAmount="%BaseAmount%"><TenderNumber>%TenderNumber%</TenderNumber></Payment>'';

  tagGiftCard       string := ''<GCSale GrossAmount="%Amount%"></GCSale>'';
  tagSurcharges     string := ''<Surcharge Amount="%SurchargeAmount%" ><SurchargeName>%SurchargeName%</SurchargeName><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName><DaypartName>%DayPart%</DaypartName><Time>%Time%</Time></Surcharge>'';  


  
  tagExclusiveTaxes string := ''<ExclusiveTax Amount="%Amount%"><TaxNumber>%TaxNumber%</TaxNumber><Time>%Time%</Time><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName><DaypartName>%DayPart%</DaypartName></ExclusiveTax>'';
  -- daypart, order mode, and revenue center
  tagCheckPromo     string := ''<Promo Amount="%Amount%"><PromoName>%PromoName%</PromoName><DaypartName>%DayPart%</DaypartName><OrderModeName>%OrderType%</OrderModeName><RevenueCenterName>%RevCenter%</RevenueCenterName><Time>%Time%</Time></Promo>'';
  tagRefunds        string :=''<Refund Amount="%Amount%" Quantity="%Quantity%"></Refund>'';
  tagVoids          string :=''<Void Amount="%Amount%" Quantity="%Quantity%"><ItemNumber>%ItemNumber%</ItemNumber><VoidName>%VoidName%</VoidName><Time>%Time%</Time></Void>'';
  -- tagShift       string :=''<Shift ShiftNumber="%ShiftNumber%" ClockInDate="%ClockInDate%" ClockInTime="%ClockInTime%"> <EmployeeNumber>%EmployeeNumber%</EmployeeNumber><ClockOutTime>%ClockOutTime%</ClockOutTime><JobName>%JobName%</JobName> <PayRate>%PayRate%</PayRate><RegularHours>%RegularHours%</RegularHours> <OvertimeHours>%OvertimeHours%</OvertimeHours><OvertimePayRate>%OvertimePayRate%</OvertimePayRate></Shift>'';
  tagPaidInOut      string:=''<PaidInOut Amount="%PaidInOut%" AffectsCash="True"><PaidInOutName>%PaidInOutName%</PaidInOutName> </PaidInOut>'';
tagDeposits         string:=''<Deposit DateOfBusiness="%FiscalDay%" Amount="%Amount%"> </Deposit>'';
 
--==========================================================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_salesdetail; 
  DROP TABLE IF EXISTS TEMP_salesdetail_all;
                                             --<Configuration>
  DROP TABLE IF EXISTS TEMP_catitem;         --  <Category><Item>
  DROP TABLE IF EXISTS TEMP_employees;       --  <Employee>
  DROP TABLE IF EXISTS TEMP_tender;          --  <Tender>
  DROP TABLE IF EXISTS TEMP_taxdetail;       --  <Tax>    --are we using this???check on it
                                             --</Configuration>

  DROP TABLE IF EXISTS TEMP_checksummary;    --Summary Check Data
  DROP TABLE IF EXISTS TEMP_discountSummary; --Summary Discount Data  
  DROP TABLE IF EXISTS TEMP_paidInOutSummary;--Summary Payin Pay out
  
  DROP TABLE IF EXISTS TEMP_check;           --<Check>
  DROP TABLE IF EXISTS TEMP_itemsale;        --  <ItemSales>
  DROP TABLE IF EXISTS TEMP_itemPromo;       --    <Promo>
  DROP TABLE IF EXISTS TEMP_payments;        --  <Payments>
  DROP TABLE IF EXISTS TEMP_refunds;         --  <Refunds>
  DROP TABLE IF EXISTS TEMP_surcharges;      --  <Surcharges>  
  DROP TABLE IF EXISTS TEMP_voids;           --  <Voids>
  DROP TABLE IF EXISTS TEMP_exclusiveTaxes;  --  <ExclusiveTaxes>
  DROP TABLE IF EXISTS TEMP_checkPromo;      --  <Promo>
  DROP TABLE IF EXISTS TEMP_checkitem;
  DROP TABLE IF EXISTS TEMP_gcsales;         --  <GiftCardSales>
                                             --</Check>
                                             
  DROP TABLE IF EXISTS TEMP_PaidInOuts;      --<PaidInOuts>
  DROP TABLE IF EXISTS TEMP_Deposits;        --<Deposits>  
  DROP TABLE IF EXISTS TEMP_shifts;          --<Shifts>

  DROP TABLE IF EXISTS TEMP_allShifts;
  DROP TABLE IF EXISTS TEMP_item;
  DROP TABLE IF EXISTS TEMP_items;
  DROP TABLE IF EXISTS TEMP_voids_all;
  DROP TABLE IF EXISTS TEMP_xml;  
  
---------------------------------------------------------------------------------------------------------------------------
--Menu and Category
CALL dataadmin.SP_LOAD_COMPEAT_MENUITEMS(:startdate,:enddate,:locationid);
-- CALL dataadmin.SP_LOAD_365_MENUITEMS(''2020-04-27'',''2029-04-27'',''[26]'');
SELECT DISTINCT REPLACE(:tagCategoryOpen,''%CategoryName%'',IFNULL(REPLACE("''CATEGORYNAME''",'','',''''),''None'') )
  || LISTAGG(REPLACE(REPLACE(:tagItem,''%ItemNumber%'',IFNULL(REPLACE("''ITEMNUMBER''",'','',''''),''None''))
    ,''%ItemName%'',IFNULL(REPLACE(REPLACE(REPLACE(
    regexp_replace("''ITEMNAME''", ''[^a-zA-Z,_,0-9, ]+'', '''') 
    ,'','',''''),''"'',''''),'' '',''''),''None''))) OVER (PARTITION BY "''CATEGORYNAME''")
  || ''</Items></Category>''  AS XMLTEXT
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "''ITEMNUMBER''" <> ''ItemNumber''  --REMOVE THE HEADER
  ;

CREATE TEMP TABLE TEMP_catitem AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 

-- The item numbers (323, 537, & 630) are not defined in the configuration section.
--     SELECT * FROM TEMP_catitem
---------------------------------------------------------------------------------------------------------------------------
--Employees
CALL dataadmin.SP_LOAD_365_EMPLOYEES(:startdatelabor,:enddate,:locationid);
--tagEmployee string     := ''<Employee Number="%EmpID%" FirstName="%FirstName%" LastName="%LastName%"/>'';
CREATE TEMP TABLE TEMP_employees AS
SELECT ''<Employees>''
    || LISTAGG(
       REPLACE( 
          REPLACE(
           REPLACE(:tagEmployee,''%EmpID%'',EMPLOYEEID),
             ''%FirstName%'',FIRSTNAME
             ),''%LastName%'',LASTNAME
             )
            || ''</Employee>''
        )
     || ''</Employees>''
         AS XMLTEXT
       FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
       
---------------------------------------------------------------------------------------------------------------------------
--Tender
CALL dataadmin.SP_LOAD_365_TENDERS(:startdate,:enddate,:locationid);

-- --tagTender string     := ''<Tender Number="%TenderId%" Name="%Name%" Type="%NonCash%" />'';
CREATE TEMP TABLE TEMP_tender AS
SELECT ''<Tenders>''
    || LISTAGG(
       REPLACE( 
          REPLACE(
           REPLACE(:tagTender,''%TenderId%'',NUMBER),
             ''%Name%'',NAME
             ),''%NonCash%'',TYPE
             )
        )
     || ''</Tenders>''
         AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

---------------------------------------------------------------------------------------------------------------------------
--Tax
-- tagTax string := ''<Tax Number="%TaxNumber%" Name="%TaxName%"><Rate>%TaxRate%</Rate></Tax>'';
--select * From TEMP_taxdetail
CREATE TEMP TABLE TEMP_taxdetail AS
SELECT  LISTAGG(
       REPLACE(REPLACE(REPLACE(
          :tagTax
             ,''%TaxNumber%'',TRD.TAXRATE_DIM_NK),
             ''%TaxName%'',TRD.TAXRATE),
             ''%TaxRate%'',TRD.PERCENT::DECIMAL(36,2))  
        )
         AS XMLTEXT
  FROM DATAWAREHOUSE.TAXRATE_DIM              TRD
    INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM ORG
      ON TRD.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK 
        AND TRD.DW_ISCURRENTROW
        AND ORG.DW_ISCURRENTROW
    INNER JOIN DATAWAREHOUSE.LOCATION_DIM      LOC
      ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
      AND LOC.DW_ISCURRENTROW
      AND (LOC.LOCATION_DIM_NK IN (
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)   
            OR TRD.TAXRATE_DIM_NK = -1    )
    GROUP BY TRD.TAXRATE_DIM_NK,TRD.TAXRATE
; 

---------------------------------------------------------------------------------------------------------------------------
--Check Level Information
CREATE TEMP TABLE TEMP_checksummary AS
SELECT CHK.cheque_fact_nk                                               AS Checkno
    ,chk.STATUS                                                         AS CheckStatus
    ,TO_CHAR(TO_TIMESTAMP(CHK.CLOSED_AT),''MM/DD/YYYY HH24:MI:SS'')       AS CloseTime
    ,DPD.DAYPART                                                        AS DayPart
    ,CHK.EMPLOYEE_DIM_FK                                                AS EmpNo
    ,CHK.PARTY_COUNT                                                    AS GuestCount
    ,TO_CHAR(TO_TIMESTAMP(CHK.OPENED_AT),''MM/DD/YYYY HH24:MI:SS'')       AS OpenTime
    ,OTD.ORDER_TYPE                                                     AS OrderType
    ,CHK.REVENUECENTERNAME                                              AS RevCenter 
    ,CHK.TAX                                                            AS ExclusiveTax
FROM DATAWAREHOUSE.CHEQUE_FACT                                          chk
  INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                                otd
     ON chk.ORDERTYPE_DIM_FK = otd.ORDERTYPE_DIM_NK
       AND otd.DW_ISCURRENTROW
       AND chk.DW_ISCURRENTROW       
       AND chk.STATUS in (''Closed''/*,''Voided''*/)
       AND (chk.UNPAID = 0 /* OR chk.STATUS = ''Voided''*/)
       AND NOT chk.IS_TRAINING
       AND NOT chk.DW_ISDELETED
       AND chk.CLOSED_AT is not null
       AND (chk.FISCAL_DATE::date   >= :startdate::date 
          AND chk.FISCAL_DATE::date <= :enddate::date)
       AND chk.LOCATION_DIM_FK IN (
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)          
    INNER JOIN DATAWAREHOUSE.DAYPART_DIM                                 dpd
      ON chk.DAYPART_DIM_FK = dpd.DAYPART_DIM_NK
        AND dpd.DW_ISCURRENTROW
;

CREATE TEMP TABLE TEMP_check AS
SELECT Checkno
       ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( REPLACE(:tagCheckOpen,''%Checkno%'',Checkno),
             ''%CloseTime%'',CloseTime),
             ''%DayPart%'',DayPart),
             ''%EmpNo%'',EmpNo),
             ''%GuestCount%'',GuestCount::DECIMAL(36,0)),
             ''%OpenTime%'',OpenTime),
             ''%OrderType%'',OrderType),
             ''%RevCenter%'',RevCenter),
             ''%ExclusiveTax%'',ExclusiveTax::DECIMAL(36,2))
         AS XMLTEXT
FROM TEMP_checksummary
; 

----------------------------------------------------------------------------------------------------------------------------
--Discount Summary
--TEMP_discountSummary
CALL DATAADMIN.SP_REPORT_DISCOUNT(:startdate,:enddate,:locationid);
-- CALL dataadmin.SP_REPORT_DISCOUNT(''2020-12-17'',''2029-12-17'',''[351]'');
CREATE TEMP TABLE TEMP_discountSummary AS
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
; 

----------------------------------------------------------------------------------------------------------------------------
--Item level Info  
CREATE TEMP TABLE TEMP_salesdetail_all 
  AS
SELECT to_char(CHK.cheque_fact_nk)                                 AS CheckNumber --* string
    ,itf.ITEMSTATUS                                                AS ITEMSTATUS
    ,itf.MENUITEMNAME_DIM_FK                                       AS "Menu Item ID"
    ,TO_CHAR(TO_TIMESTAMP(CHK.CLOSED_AT),''MM/DD/YYYY HH24:MI:SS'')  AS CloseTime  --* DateTime(mm/dd/yyyy hh:mm:ss)
    ,TO_CHAR(row_number() OVER (ORDER BY itf.ITEM_FACT_NK)::NUMBER(18,0))                   
                                                                   AS ItemSale_TicketItemNumber  --* integer  
    ,itf.ITEM_FACT_NK                                              AS Item_Fact_NK                              
    ,TO_CHAR(itf.MENUITEMNAME_DIM_FK)                              AS ItemSale_ItemNumber  --* string 
    ,TO_CHAR((itf.GROSS -  IFNULL(itf.INCLUSIVETAX,0)) ::DECIMAL(18,2))     
                                                                   AS ItemSale_GrossAmount --* decimal
    ,TO_CHAR(1)                                                    AS ItemSale_Modifiers_ItemNumber --**string this is an fk 
    ,TO_CHAR(0)                                                    AS ItemSale_Modifiers_Quantity --int
    ,TO_CHAR(0)                                                    AS ItemSale_Modfiers_GrossAmount --** int
    ,''None''                                                        AS ItemSale_Comp_CompName --* string name of discount  
    ,TO_CHAR(0.00)                                                 AS ItemSale_Comp_Amount  --*decimal amount subtracted from tota
    ,''None''                                                        AS ItemSale_Promo_PromoName --*string  promo name
    ,TO_CHAR(itf.DiscountItem::decimal(18,2))                      AS ItemSale_Promo_Amount --*amount subtracted from total 
    ,IFNULL(REPLACE(itf.REVENUECENTERNAME,'','',''''),''None'')          AS ItemSale_RevenueCenterName  --string
    ,TO_CHAR(IFNULL(itf.QUANTITY,0)::number(18,0))                 AS ItemSale_Quantity           --int

    FROM DATAWAREHOUSE.ITEM_FACT                                   itf
     INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                     med
       ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
          AND itf.ITEMSTATUS IN (''Added'',''Sent'',''Voided'')
          AND itf.CHECKSTATUS IN (''Closed'')
          AND itf.DW_ISCURRENTROW  
          AND med.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                          chk
        ON chk.CHEQUE_FACT_NK = itf.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND chk.STATUS in (''Closed'')
          AND itf.ITEMSTATUS IN (''Added'',''Sent'',''Voided'')
          AND chk.UNPAID = 0
          AND chk.CLOSED_AT is not null
          AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM                    meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                      ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW         
     ORDER BY CHK.CLOSED_AT DESC;  
  
CREATE TEMP TABLE TEMP_itemsale  AS
SELECT DISTINCT ITM.CHECKNUMBER  AS Checkno
  ,ITM.ItemSale_TicketItemNumber AS ItemNO
  , LISTAGG(
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        :tagItemsale,''%Quantity%'',TO_CHAR(IFNULL(ITM.ITEMSALE_QUANTITY,0)::number(18,0))),
             ''%GrossAmount%'',ITM.ITEMSALE_GROSSAMOUNT ::DECIMAL(18,2)),
             ''%ItemNumber%'',ITM.ITEMSALE_ITEMNUMBER),
             ''%CloseTime%'',ITM.CloseTime),
             ''%DayPart%'',CHK.DayPart),
             ''%EmpNo%'',CHK.EmpNo),
             ''%GuestCount%'',CHK.GuestCount::DECIMAL(36,0)),
             ''%OpenTime%'',CHK.OpenTime),
             ''%OrderType%'',CHK.OrderType),
             ''%Time%'',CHK.OpenTime),
             ''%RevCenter%'',CHK.RevCenter),
             ''%ExclusiveTax%'',ExclusiveTax::DECIMAL(36,2))
             ) OVER (PARTITION BY ITM.CHECKNUMBER,ITM.ItemSale_TicketItemNumber)
         AS XMLTEXT   
FROM TEMP_SALESDETAIL_ALL                             ITM
  INNER JOIN TEMP_CHECKSUMMARY                        CHK
    ON TO_CHAR(ITM.CHECKNUMBER) = TO_CHAR(CHK.CHECKNO)
      AND ITM.ITEMSTATUS <> ''Voided''
; 

---------------------------------------------------------------------------------------------------------------------------          
--GiftCardSales

-- select * from TEMP_gcsales  where  "Check ID" = 266298;
CREATE TEMP TABLE TEMP_gcsales                                       AS
SELECT   DISTINCT CHF.CHEQUE_FACT_NK                                 AS Checkno        
 ,CHF.CHEQUE_FACT_NK                                                 AS "Check ID"
 ,''<GiftCardSales>''  ||
  LISTAGG( replace(:tagGiftCard,''%Amount%'',GCD.START_BALANCE::decimal(18,2) )
   ) OVER (PARTITION BY CHF.CHEQUE_FACT_NK)       
   || ''</GiftCardSales>''
                                                                     AS XMLTEXT
   FROM GIFTCARD_DIM                                                 GCD
     INNER JOIN DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT               GCF
       ON GCD.GIFTCARD_DIM_NK = GCF.GIFTCARD_DIM_FK
          AND GCF.DW_ISCURRENTROW
          AND GCD.DW_ISCURRENTROW
          AND GCD.IS_ISSUED
          AND NOT GCD.DW_ISDELETED
          AND GCD.DW_ISCURRENTROW
          AND GCF.COMMAND = ''Issue'' 
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                       CHF
        ON CHF.CHEQUE_FACT_NK = GCf.CHEQUE_FACT_FK
          AND CHF.DW_ISCURRENTROW
          AND CHF.STATUS = ''Closed''
          AND CHF.FISCAL_DATE::date >= :startdate::date
          AND CHF.FISCAL_DATE::date <= :enddate::date  
          AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)    
          ;
          
---------------------------------------------------------------------------------------------------------------------------
--Payments
CALL dataadmin.SP_LOAD_365_PAYMENTS(:startdate,:enddate,:locationid);
-- -- CALL dataadmin.SP_LOAD_365_PAYMENTS(''2020-12-17'',''2029-12-17'',''[351]'');
-- -- --tagPayment string:=''<Payment BaseAmount="%BaseAmount%"><TenderNumber>%TenderNumber%</TenderNumber></Payment>'';
CREATE TEMP TABLE TEMP_payments AS
SELECT DISTINCT SPLIT_PART("''PAYMENT_CHECKNUMBER''",''.'',2) AS Checkno
   ,''<Payments>''
    || LISTAGG(
          REPLACE(REPLACE(
             :tagPayment,''%BaseAmount%'',"''PAYMENT_BASEAMOUNT''"),
             ''%TenderNumber%'',"''PAYMENT_TENDERNUMBER''")
        ) OVER (PARTITION BY "''PAYMENT_CHECKNUMBER''")
     || ''</Payments>''
         AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "''PAYMENT_CHECKNUMBER''" <> ''Payment_CheckNumber'';

----------------------------------------------------------------------------------------------------------------------------
--Promo Check Level (aka Discount)
--select * From TEMP_discountSummary
CREATE TEMP TABLE TEMP_checkPromo AS
SELECT DISTINCT "Check ID" AS Checkno
   ,''<Promos>''
    || LISTAGG(
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
             :tagCheckPromo
               ,''%Amount%'',"Discount Amount"),
               ''%PromoName%'',"Discount Name"),

               ''%DayPart%'',"Daypart"),
               ''%OrderType%'',ORDERTYPE),
               ''%Time%'',CLOSETIME),               
               ''%RevCenter%'',"Revenue Center")

        ) OVER (PARTITION BY "Check ID" )
     || ''</Promos>''
         AS XMLTEXT
FROM TEMP_discountSummary                 DIS
  LEFT JOIN TEMP_checkSummary             CHK
    ON DIS."Check ID" = CHK.CHECKNO
  WHERE "Discount Level" = ''Check'';

----------------------------------------------------------------------------------------------------------------------------
--Promo Item Level (aka Discount)
CREATE TEMP TABLE TEMP_itemPromo AS
SELECT DISTINCT tis.ItemSale_TicketItemNumber AS Itemno
   ,''<Promos>''
    || LISTAGG(
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
          :tagCheckPromo
            ,''%Amount%'',"Discount Amount"),
            ''%PromoName%'',"Discount Name"),
            ''%Time%'',CHK.CLOSETIME),
            ''%DayPart%'',"Daypart"),
            ''%OrderType%'',ORDERTYPE),
            ''%RevCenter%'',"Revenue Center")

        ) OVER (PARTITION BY  "Check ID" || ''.''|| SPLIT_PART("Support ID",''.'',2)  ) 
     || ''</Promos>''
         AS XMLTEXT
FROM TEMP_discountSummary                DIS
  INNER JOIN TEMP_salesdetail_all        TIS
     ON dis."Check ID" || ''.'' || SPLIT_PART(dis."Support ID",''.'',2) = tis.Item_Fact_NK 
       AND "Discount Level" = ''Item''
  LEFT JOIN TEMP_checkSummary             CHK
    ON DIS."Check ID" = CHK.CHECKNO
       ;

----------------------------------------------------------------------------------------------------------------------------
--surcharges
CALL dataadmin.SP_LOAD_365_SURCHARGES(:startdate,:enddate,:locationid);
-- CALL dataadmin.SP_LOAD_365_SURCHARGES(''2020-12-17'',''2029-12-17'',''[26]'');
CREATE TEMP TABLE TEMP_surcharges AS
SELECT DISTINCT SPLIT_PART(SURCHARGE_CHECKNUMBER,''.'',2) AS Checkno
   ,''<Surcharges>''
    || LISTAGG(
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
             :tagSurcharges,
              ''%SurchargeAmount%'',SURCHARGE_AMOUNT),
              ''%SurchargeName%'',SURCHARGE_NAME),
              ''%DayPart%'',DAYPART),
              ''%OrderType%'',ORDERTYPE),
              ''%RevCenter%'',REVCENTER),
              ''%Time%'',CLOSETIME)    
              
        ) OVER (PARTITION BY SURCHARGE_CHECKNUMBER)
     || ''</Surcharges>''
         AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))  SUR
  INNER JOIN TEMP_checksummary            CHK
    ON TO_CHAR(CHK.CHECKNO) = SPLIT_PART(SUR.SURCHARGE_CHECKNUMBER,''.'',2)
;

----------------------------------------------------------------------------------------------------------------------------
--refunds
CALL dataadmin.SP_REPORT_REFUNDS(:startdate,:enddate,:locationid);
-- CALL dataadmin.SP_REPORT_REFUNDS(''2020-12-17'',''2029-12-17'',''[351]'');
-- tagRefunds string:=''<Refund Amount="%Amount%" Quantity="%Quantity%"></Refund>'';
CREATE TEMP TABLE TEMP_refunds AS
SELECT DISTINCT  "Check ID" AS Checkno
   ,''<Refunds>''
    || LISTAGG(
          REPLACE( REPLACE(
            :tagRefunds,''%Amount%'',"Refund Amount"),
            ''%Quantity%'',1)
        ) OVER (PARTITION BY "Check ID")
     || ''</Refunds>''
         AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

----------------------------------------------------------------------------------------------------------------------------
--voids
-- select "Item ID","Amount",* From TEMP_voids_all where "Item ID" = 199434.72209527-74ad-4a90-a5c6-250249db5ab5
CALL DATAADMIN.SP_REPORT_VOID_0001(:startdate,:enddate,:locationid);
CREATE TEMP TABLE TEMP_voids_all AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

CREATE TEMP TABLE TEMP_voids AS
SELECT DISTINCT VOD."Check ID"                  AS Checkno
   ,''<Voids>''
    || LISTAGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
          :tagVoids,''%Amount%'',VOD."Amount"),
          ''%Quantity%'',VOD."Count"),
          ''%VoidName%'',VOD."Reason"),
          ''%Time%'',TSD.CLOSETIME),
          ''%ItemNumber%'',IFNULL(TO_CHAR(TSD."Menu Item ID"),'' ''))
        ) OVER (PARTITION BY "Check ID")
     || ''</Voids>''                               AS XMLTEXT
FROM TEMP_voids_all                              VOD
   LEFT JOIN TEMP_salesdetail_all                TSD
     ON (VOD."Item ID" = TSD.ITEM_FACT_NK
       AND VOD."Level" = ''Item'')
;

-- select * from TEMP_salesdetail_all;
----------------------------------------------------------------------------------------------------------------------------
--PaidInOut Summary
--TEMP_PaidInOutSummary
CALL DATAADMIN.SP_REPORT_PAYINOUT(:startdate,:enddate,:locationid);
-- CALL DATAADMIN.SP_REPORT_PAYINOUT(''2024-08-02T14:48:37.661Z'',''2025-08-02T14:48:37.661Z'',''[351]'');

CREATE TEMP TABLE TEMP_paidInOutSummary AS
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
; 

----------------------------------------------------------------------------------------------------------------------------
--PaidInOut
--tagPaidInOut string:=''<PaidInOut Amount="%PaidInOut%" AffectsCash="True"><PaidInOutName>%PaidInOutName%</PaidInOutName> </PaidInOut>'';
CREATE TEMP TABLE TEMP_PaidInOuts AS
SELECT ''<PaidInOuts>''
    || LISTAGG(
          REPLACE(REPLACE(
             :tagPaidInOut,''%PaidInOut%'',"Amount" * CASE WHEN "Pay Type" = ''Pay Out'' THEN -1 ELSE 1 END ),
             ''%PaidInOutName%'',"Reason")
        ) 
     || ''</PaidInOuts>''
         AS XMLTEXT
FROM TEMP_paidInOutSummary 
;

----------------------------------------------------------------------------------------------------------------------------
--Deposits
CALL DATAADMIN.SP_REPORT_CASH(:startdate,:enddate,:locationid);
-- CALL DATAADMIN.SP_REPORT_CASH(''2020-12-17'',''2029-12-17'',''[351]'');
--tagDeposits string:=''<Deposit DateOfBusiness=”%FiscalDay%” Amount=”%Amount%”> </Deposit>'';

CREATE TEMP TABLE TEMP_Deposits AS
SELECT ''<Deposits>''
    || LISTAGG(
          REPLACE(REPLACE(
             :tagDeposits,''%FiscalDay%'',TO_VARCHAR(TO_DATE("Fiscal Day"),''MM/DD/YYYY'')),
             ''%Amount%'',"Deposit")
        ) 
     || ''</Deposits>''
         AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
  WHERE "Event Type"=''Deposit''
;

----------------------------------------------------------------------------------------------------------------------------
--labor
CALL DATAADMIN.SP_REPORT_LABOR(:startdatelabor,:enddate,:locationid);
-- CALL DATAADMIN.SP_REPORT_LABOR(''2020-03-20'',''2025-04-26'',''[351]'');
CREATE TEMP TABLE TEMP_allShifts AS
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

CREATE TEMP TABLE TEMP_shifts AS
SELECT    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        -- :tagShift,
        ''<Shift ShiftNumber="%ShiftNumber%" ClockInDate="%ClockInDate%" ClockInTime="%ClockInTime%"> <EmployeeNumber>%EmployeeNumber%</EmployeeNumber><ClockOutTime>%ClockOutTime%</ClockOutTime><JobName>%JobName%</JobName> <PayRate>%PayRate%</PayRate><RegularHours>%RegularHours%</RegularHours> <OvertimeHours>%OvertimeHours%</OvertimeHours><OvertimePayRate>%OvertimePayRate%</OvertimePayRate></Shift>'',
          ''%ShiftNumber%'',ROW_NUMBER()OVER(PARTITION BY "Fiscal Day","Employee ID" ORDER BY "Shift ID" )
          ),   
          ''%ClockInDate%'',TO_VARCHAR(TO_DATE("Clocked In At"),''MM/DD/YYYY'')),   
          ''%ClockInTime%'',TO_VARCHAR(TO_TIMESTAMP("Clocked In At"),''MM/DD/YYYY hh:MI:SS'')),
          ''%EmployeeNumber%'',"Employee ID"),
          ''%ClockOutTime%'',TO_VARCHAR(TO_TIMESTAMP("Clocked Out At"),''MM/DD/YYYY hh:MI:SS'')),
          ''%JobName%'',"Job Position"),
          ''%PayRate%'',"Regular Rate"),
          ''%RegularHours%'',("Regular Seconds"/3600)::DECIMAL(18,2)),
          ''%OvertimeHours%'',("Overtime Seconds"/3600)::DECIMAL(18,2)),  
          ''%OvertimePayRate%'',"Overtime Rate")                                                                              
         AS XMLTEXT
FROM TEMP_allShifts
;

----------------------------------------------------------------------------------------------------------------------------
-- --exclusive Taxes
CALL dataadmin.SP_LOAD_365_TAXDETAILS(:startdate,:enddate,:locationid);
-- -- call dataadmin.SP_LOAD_365_TAXDETAILS(''2020-12-17'',''2029-12-17'',''[26]'');
-- -- tagExclusiveTaxes string:=''<ExclusiveTax Amount="%Amount%"><TaxNumber>%TaxNumber%</TaxNumber>'';
CREATE TEMP TABLE TEMP_exclusiveTaxes AS
SELECT DISTINCT SPLIT_PART("CheckNumber",''.'',2) AS Checkno
   ,''<ExclusiveTaxes>''
    || LISTAGG(
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            :tagExclusiveTaxes,
              ''%Amount%'',"Tax_Amount"),
              ''%TaxNumber%'',"Tax_TaxNumber"),
              ''%DayPart%'',CHK.DAYPART),
              ''%OrderType%'',CHK.ORDERTYPE),
              ''%RevCenter%'',CHK.REVCENTER),
              ''%Time%'',CHK.OPENTIME)
        ) OVER (PARTITION BY SPLIT_PART("CheckNumber",''.'',2))
     || ''</ExclusiveTaxes>''                      AS XMLTEXT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))         TAX
  INNER JOIN TEMP_checksummary                   CHK
    ON TO_CHAR(CHK.CHECKNO) = TO_CHAR(SPLIT_PART(TAX."CheckNumber",''.'',2))
      AND "CheckNumber" <> ''CheckNumber'';

--------------------------------------------------------------------------------------------------------------------------- 
CREATE TEMP TABLE TEMP_items
AS
SELECT  DISTINCT CheckNO                                                           AS CheckNO
  ,''<ItemSales>'' || LISTAGG(XMLTEXT) OVER (PARTITION BY CheckNO) || ''</ItemSales>'' AS XMLTEXT
FROM (  
    SELECT TIS.CheckNo                                                          AS CheckNo
          ,REPLACE(IFNULL(TIS.XMLTEXT,'' ''),''%Promo%'',IFNULL(TIP.XMLTEXT,'' ''))   AS XMLTEXT
    FROM TEMP_itemsale              TIS        
      LEFT JOIN TEMP_itemPromo      TIP
        ON TIS.itemNO = TIP.itemNO
)  ORDER BY CheckNO
;
 
----------------------------------------------------------------------------------------------------------------------------
--select * From TEMP_checkitem where xmltext ilike (''%266298%'');
CREATE TEMP TABLE TEMP_checkitem 
AS
SELECT TIS.XMLTEXT                --CHECK
  || IFNULL(TCP.XMLTEXT,'' '')      --CHECK PROMO
  || IFNULL(STX.XMLTEXT,'' '')      --TAXES
  || IFNULL(TCS.XMLTEXT,'' '')      --ITEMS
  || IFNULL(TGC.XMLTEXT,'' '')      --GIFTCARD
  || IFNULL(TPS.XMLTEXT,'' '')      --PAYMENTS
  || IFNULL(TVS.XMLTEXT,'' '')      --VOIDS 
  || IFNULL(RTS.XMLTEXT,'' '')      --REFUNDS
  || IFNULL(SPS.XMLTEXT,'' '')      --SURCHARGES
  || ''</Check>''                   AS XMLTEXT
  FROM TEMP_check                 TIS
    LEFT JOIN TEMP_checkPromo     TCP
      ON TIS.Checkno = TCP.Checkno   
    LEFT JOIN TEMP_exclusiveTaxes STX
      ON TIS.Checkno  = STX.Checkno  
    LEFT JOIN TEMP_items          TCS
      ON TIS.Checkno  = TCS.Checkno
    LEFT JOIN TEMP_payments       TPS   
      ON TIS.Checkno  = TPS.Checkno
    LEFT JOIN TEMP_surcharges     SPS
      ON TIS.Checkno  = SPS.Checkno
    LEFT JOIN TEMP_refunds         RTS
      ON TIS.Checkno  = RTS.Checkno 
    LEFT JOIN TEMP_voids           TVS
       ON TIS.Checkno  = TVS.Checkno 
    LEFT JOIN TEMP_gcSales         TGC
       ON TIS.Checkno  = TGC.Checkno 
;

--==========================================================================================================================
SELECT listagg(XMLTEXT) WITHIN GROUP(ORDER BY RID )
                                        AS XMLTEXT
  FROM (
        SELECT :tagOpen1 as XMLTEXT      ,0005 as RID
          UNION
        SELECT ''<Categories>'' as XMLTEXT ,0010 as RID
          UNION                                           
        SELECT XMLTEXT                   ,0015 as RID
        FROM TEMP_catitem
          UNION
        SELECT ''</Categories>''as XMLTEXT  ,0020 as RID
          UNION
        SELECT XMLTEXT                    ,0025 as RID
        FROM TEMP_employees
          UNION
        SELECT XMLTEXT                    ,0030 as RID
        FROM TEMP_tender
          UNION
        SELECT ''<Taxes>'' as XMLTEXT       ,0035 as RID
          UNION
        SELECT XMLTEXT                    ,0040 as RID
        FROM TEMP_taxdetail 
          UNION
        SELECT ''</Taxes>'' as XMLTEXT      ,0045 as RID
          UNION
        SELECT :tagConfigClose as XMLTEXT ,0050 as RID
          UNION
        SELECT ''<SalesDetail>'' as XMLTEXT ,0060 as RID
          UNION
        SELECT ''<Checks>'' as XMLTEXT      ,0070 as RID
          UNION
        SELECT LISTAGG(XMLTEXT)           ,0080 as RID
        FROM TEMP_checkitem
          UNION
        SELECT ''</Checks>''      AS XMLTEXT,0090 as RID
          UNION
        SELECT XMLTEXT          AS XMLTEXT,0095 as RID
          FROM TEMP_PaidInOuts
          UNION
        SELECT XMLTEXT          AS XMLTEXT,0099 as RID
          FROM TEMP_Deposits
          UNION
        SELECT ''</SalesDetail>'' AS XMLTEXT,0100 as RID
          UNION
        SELECT ''<LaborDetail>''  AS XMLTEXT,0110 as RID
          UNION
        SELECT ''<Shifts>''||LISTAGG(XMLTEXT)||''</Shifts>'' AS XMLTEXT,0120 as RID
        FROM TEMP_shifts
          UNION
        SELECT ''</LaborDetail>'' AS XMLTEXT,0130 as RID
          UNION
        SELECT :tagClose1       AS XMLTEXT,1000 as RID
        );
--select * From TEMP_xml
CREATE TEMP TABLE TEMP_xml AS
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
  
--=========================================================================================================================
reportSet := (  
 select parse_xml(XMLTEXT) AS XMLTEXT From TEMP_xml
);

RETURN TABLE(reportSet); 
END';