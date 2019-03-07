/*
Conversion of SNZ address notification tables to constructing households (chh) schema

Simon Anastasiadis
2018-11-12

We have developed code that determines the validity of address notification tables, at both
a household and individual level. In order to determine the magnitude of improvement that
our work accomplishes over SNZ's default approach, it is necessary to run the existing address
notification table through our validation code.

To this end, this script converts the notification table to the structure required by our
validation code. We produce three variants:
1. if census is the source then a notification is ineligable for validation
2. if census is the source and no other record corroborates it, then notification is ineligable for validaiton
3. all records are eligable for validation

Note, [validation] = 'YES' means that the record comes from a source used to construct the validation
table and hence should be excluded from the validation analysis.

Note, when validating you can only subset on area levels. Selecting people randomly (e.g. only those 
snz_uid that end in 00) misreports accuracy as it becomes rare to observe multiple individuals at the
same address.
*/

IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2016-15].[chh_snz_address_notif_table1]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2016-15].[chh_snz_address_notif_table1];

--DROP TABLE [IDI_Sandpit].[DL-MAA2016-15].[chh_snz_address_notif_table2]
--DROP TABLE [IDI_Sandpit].[DL-MAA2016-15].[chh_snz_address_notif_table3]

SELECT [snz_uid]
      ,[ant_notification_date] AS notification_date
      ,[ant_replacement_date]
      ,[snz_idi_address_register_uid] AS address_uid
      ,[ant_address_source_code] AS [source]
	  ,CASE WHEN [ant_address_source_code] = 'CEN' THEN 'YES' ELSE 'NO' END AS [validation]
	  --,CASE WHEN [ant_address_source_code] = 'CEN' AND ant_supporting_address_source_codes = '' THEN 'YES' ELSE 'NO' END AS [validation]
	  --,'NO' AS [validation]
INTO [IDI_Sandpit].[DL-MAA2016-15].[chh_snz_address_notif_table1]
FROM [IDI_Clean_20181020].[data].[address_notification]


USE IDI_UserCode
GO

IF OBJECT_ID('[DL-MAA2016-15].[chh_snz_address_notif_full_table]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_snz_address_notif_full_table];
GO

CREATE VIEW [DL-MAA2016-15].[chh_snz_address_notif_full_table] AS
SELECT snz_uid
,ant_notification_date AS notification_date
,[ant_replacement_date]
,snz_idi_address_register_uid AS address_uid
,ant_address_source_code AS [source]
,CASE WHEN [ant_address_source_code] = 'CEN' THEN 'YES' ELSE 'NO' END AS [validation]
FROM [IDI_Clean_20181020].[data].address_notification_full
WHERE ant_address_source_code <> 'NZTD'
AND ant_address_source_code <> 'NZTM'
GO