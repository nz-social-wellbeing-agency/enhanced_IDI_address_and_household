/**************************************************************************************
Description: Establish views and tables as input to constructing enhanced household address information

Input:
[IDI_Sandpit].[DL-MAA2016-15].[chh_household_refined]
[IDI_Sandpit].[DL-MAA2016-15].[chh_household_validation]
[IDI_Clean_20190420].[data].[address_notification]

Output:
One table having the same format and structure as [IDI_Clean_REFRESHDATE].[data].[address_notification]
But generated according to our CHH methodology.
 
Author: Simon Anastasiadis

Dependencies: constructing households project
 
Notes:
1) Output table name contains refresh code to reduce confusion working across refreshes
 
Issues:
 
History (reverse order):
2019-10-18 SA v1
**************************************************************************************/

/* DELETE TABLE IF EXISTS */
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2016-15].[chh_address_notification_20190420]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2016-15].[chh_address_notification_20190420];
GO

/* WRITE TABLE */
SELECT hhld.[snz_uid]
      ,hhld.[notification_date] AS [ant_notification_date]
      ,DATEADD(DAY, -1, hhld.next_date) AS [ant_replacement_date]
      ,hhld.[address_uid] AS [snz_idi_address_register_uid]
      ,location.[ant_post_code]
      ,location.[ant_region_code]
      ,location.[ant_ta_code]
      ,location.[ant_meshblock_code]
      ,NULL AS [ant_supporting_address_source_codes]
      ,hhld.[source] AS [ant_address_source_code]
INTO [IDI_Sandpit].[DL-MAA2016-15].[chh_address_notification_20190420]
FROM (

SELECT [snz_uid]
      ,[notification_date]
      ,[address_uid]
      ,[source]
      ,LEAD([notification_date], 1, '9999-01-01') OVER( PARTITION BY [snz_uid] ORDER BY [notification_date]) AS next_date
FROM (
	SELECT [snz_uid], [notification_date], [address_uid], [source]
	FROM [IDI_Sandpit].[DL-MAA2016-15].[chh_household_refined]

	UNION ALL

	SELECT [snz_uid], [notification_date], [address_uid], [source]
	FROM [IDI_Sandpit].[DL-MAA2016-15].[chh_household_validation]
	WHERE [source] NOT IN ('cen2013_111', 'cen2013_131', 'cen2013_151', 'cen2013_444', 'cen2013_511','cen2013_611')
) unioned

) hhld
LEFT JOIN (
	SELECT [snz_idi_address_register_uid]
	      ,MAX([ant_post_code]) AS [ant_post_code]
	      ,MAX([ant_region_code]) AS [ant_region_code]
	      ,MAX([ant_ta_code]) AS [ant_ta_code]
	      ,MAX([ant_meshblock_code]) AS [ant_meshblock_code]
	FROM [IDI_Clean_20190420].[data].[address_notification]
	WHERE [snz_idi_address_register_uid] IS NOT NULL
	GROUP BY [snz_idi_address_register_uid]
) location
ON hhld.address_uid = location.[snz_idi_address_register_uid]

/* ADD INDEX */
CREATE CLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2016-15].[chh_address_notification_20190420] ( [snz_uid] )

/* COMPACT TABLE */
ALTER TABLE [IDI_Sandpit].[DL-MAA2016-15].[chh_address_notification_20190420] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
