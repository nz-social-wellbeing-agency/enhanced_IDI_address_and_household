/******************************************************************************************************************
Description: Establish views and tables as input to validating enhanced household address information

Input:

Output:
 
Author: Simon Anastasiadis
 
Dependencies:
 
Notes:
1) Uses history refreshes for some versioning
2) Currently locked to 2019-04-20 refersh
3) Required columns: snz_uid, notification_date, address_uid, household_uid, source
 
Issues:
 
History (reverse order):
2019-08-13 SA updated to 2019-04-02 refresh
2019-03-05 SA views are now saved in IDI_UserCode, several tables moved to IDI_Adhoc
2018-11-07 SA v0
******************************************************************************************************************/

/* Establish database for writing views */
USE IDI_UserCode
GO

/* SNZ 2013 census usual residence */
IF OBJECT_ID('[DL-MAA2016-15].[chh_census_validation]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_census_validation];
GO

CREATE VIEW [DL-MAA2016-15].chh_census_validation AS
SELECT DISTINCT a.[snz_uid]
      ,'2013-03-05' AS notification_date
	  ,a.[snz_idi_address_register_uid] AS address_uid
      ,b.[snz_cen_hhld_uid] AS household_uid
	  ,'cen2013' AS [source]
FROM [IDI_Clean_20190420].[cen_clean].[census_address] a
LEFT JOIN [IDI_Clean_20190420].[cen_clean].[census_individual] b
ON a.snz_uid = b.snz_uid

WHERE address_type_code = 'UR'
AND snz_idi_address_register_uid IS NOT NULL
AND cen_ind_fam_grp_code <> '00';
GO

/* SNZ HES */
IF OBJECT_ID('[DL-MAA2016-15].[chh_hes_validation]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_hes_validation];
GO

CREATE VIEW [DL-MAA2016-15].chh_hes_validation AS
SELECT DISTINCT a.snz_uid
,DATEFROMPARTS(b.hes_hhd_year_nbr, b.hes_hhd_month_nbr, 15) AS notification_date
,a.snz_idi_address_register_uid as address_uid
,a.snz_hes_hhld_uid AS household_uid
,CONCAT('HES_',a.hes_add_hes_year_code) AS [source]

FROM [IDI_Clean_20190420].[hes_clean].[hes_address] AS a
LEFT JOIN (

	SELECT hes_hhd_hes_year_code
	,hes_hhd_year_nbr
	,hes_hhd_month_nbr
	,snz_hes_hhld_uid
	FROM [IDI_Clean_20190420].[hes_clean].[hes_household] 

	UNION ALL
 
	SELECT cast(hes_year as varchar) AS hes_hhd_hes_year_code
	,dvyear as hes_hhd_year_nbr
	,dvmonth as hes_hhd_month_nbr
	,snz_hes_hhld_uid
	FROM [IDI_Adhoc].[clean_read_HES].[hes_household_1516]

	UNION ALL

	SELECT cast(hes_year as varchar)
	,dvyear as hes_hhd_year_nbr
	,dvmonth as hes_hhd_month_nbr
	,snz_hes_hhld_uid
	FROM [IDI_Adhoc].[clean_read_HES].[hes_household_1617]

) AS b
ON a.hes_add_hes_year_code = b.hes_hhd_hes_year_code
AND a.snz_hes_hhld_uid = b.snz_hes_hhld_uid

WHERE snz_idi_address_register_uid IS NOT NULL
AND hes_hhd_year_nbr IS NOT NULL
GO

/* SNZ GSS */
IF OBJECT_ID('[DL-MAA2016-15].[chh_gss_validation]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_gss_validation];
GO

CREATE VIEW [DL-MAA2016-15].chh_gss_validation AS
SELECT DISTINCT j.snz_uid
	,CAST(k.notification_date AS DATE) AS notification_date
	,j.[snz_idi_address_register_uid] AS address_uid
	,k.household_uid
	,k.[source]
FROM (

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20190420].[gss_clean].[gss_household_2008]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20190420].[gss_clean].[gss_household_2010]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20190420].[gss_clean].[gss_household_2012]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20190420].[gss_clean].[gss_household]

) k
INNER JOIN [IDI_Clean_20190420].[gss_clean].[gss_identity] j
ON k.snz_uid = j.snz_uid
AND k.[source] = j.[gss_id_collection_code]
AND k.household_uid = j.snz_gss_hhld_uid

WHERE household_uid IS NOT NULL
AND j.[snz_idi_address_register_uid] IS NOT NULL
AND k.notification_date IS NOT NULL;
GO

/* SNZ HLFS */
IF OBJECT_ID('[DL-MAA2016-15].[chh_hlfs_validation]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_hlfs_validation];
GO

CREATE VIEW [DL-MAA2016-15].chh_hlfs_validation AS
SELECT DISTINCT a.snz_uid
,b.[hlfs_urd_interview_date] as notification_date
,a.[snz_idi_address_register_uid] as address_uid
,b.snz_hlfs_hhld_uid AS household_uid
,'HLFS'+cast(year(cast(a.hlfs_adr_quarter_date as datetime)) as varchar)  as [source]

FROM [IDI_Clean_20190420].[hlfs_clean].[household_address] as a
INNER JOIN [IDI_Clean_20190420].[hlfs_clean].[data] as b
ON a.[snz_hlfs_uid] = b.[snz_hlfs_uid] 
AND a.[snz_hlfs_hhld_uid] = b.[snz_hlfs_hhld_uid]
AND a.[hlfs_adr_quarter_date] = b.[hlfs_urd_quarter_date] 

WHERE a.[snz_idi_address_register_uid] IS NOT NULL
AND b.[hlfs_urd_interview_date] IS NOT NULL
GO

/*
View for current address table

Convert current address table into CHH format
so validation-suite can access its accuracy.
*/
IF OBJECT_ID('[DL-MAA2016-15].[chh_current_address_table]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_current_address_table];
GO

CREATE VIEW [DL-MAA2016-15].chh_current_address_table AS
SELECT DISTINCT snz_uid
	,ant_notification_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'current' AS [source]
	,'NO' AS [validation]
FROM [IDI_Clean_20190420].[data].[address_notification];
GO

/* SNZ 2013 census usual residence by household composition */
IF OBJECT_ID('[DL-MAA2016-15].[chh_census_composition]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_census_composition];
GO

CREATE VIEW [DL-MAA2016-15].chh_census_composition AS
SELECT DISTINCT a.[snz_uid]
      ,'2013-03-05' AS notification_date
	  ,a.[snz_idi_address_register_uid] AS address_uid
      ,b.[snz_cen_hhld_uid] AS household_uid
	  ,CONCAT('cen2013_',IIF(c.[cen_hhd_hhld_comp_code] IN (111, 131, 151, 511, 611), c.[cen_hhd_hhld_comp_code], 444)) AS [source]
FROM [IDI_Clean_20190420].[cen_clean].[census_address] a
LEFT JOIN [IDI_Clean_20190420].[cen_clean].[census_individual] b
ON a.snz_uid = b.snz_uid
INNER JOIN [IDI_Clean_20190420].[cen_clean].[census_household] c
ON b.[snz_cen_hhld_uid] = c.[snz_cen_hhld_uid]

WHERE address_type_code = 'UR'
AND snz_idi_address_register_uid IS NOT NULL
AND cen_ind_fam_grp_code <> '00';
GO


