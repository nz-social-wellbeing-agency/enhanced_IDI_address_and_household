/******************************************************************************************************************
Description: Establish views and tables as input to constructing enhanced household address information

Input:

Output:
 
Author: Simon Anastasiadis

Dependencies:
 
Notes:
1) Uses history refreshes for some versioning
2) Currently locked to 2019-10-20 refersh
3) Required columns: snz_uid, notification_date, address_uid, source, validation
 
Issues:
 
History (reverse order):
2020-02-24 SA updated to 2019-10-20 refresh
2019-08-13 SA updated to 2019-04-20 refresh
2019-06-21 SA limit to single view, add high quality flag
2019-03-05 SA views are now saved in IDI_UserCode, several tables have been moved to IDI_Adhoc
2018-11-07 SA v0
******************************************************************************************************************/

/* Establish database for writing views */
USE IDI_UserCode
GO

/* ACC */
IF OBJECT_ID('[DL-MAA2016-15].[chh_acc_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_acc_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_acc_notifications AS
SELECT DISTINCT snz_uid
	,acc_cla_accident_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'acc' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.acc_clean.claims
WHERE snz_idi_address_register_uid IS NOT NULL
AND acc_cla_accident_date IS NOT NULL
GO

/* IRD applied date */
IF OBJECT_ID('[DL-MAA2016-15].[chh_ird_applied_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_ird_applied_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_ird_applied_notifications AS
SELECT DISTINCT snz_uid
	,ir_apc_applied_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'ird_applied' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.ir_clean.ird_addresses
WHERE snz_idi_address_register_uid IS NOT NULL
AND ir_apc_applied_date IS NOT NULL
AND ir_apc_address_status_code = 'V' /*V = valid */
AND ir_apc_main_address_ind = 'Y'
AND ir_apc_applied_date <> '2008-12-13'
AND ir_apc_applied_date <> '2008-12-14'
AND ir_apc_applied_date <> '2009-01-17'
AND ir_apc_applied_date <> '2009-03-08'
GO

/* IRD timestamped applications */
IF OBJECT_ID('[DL-MAA2016-15].[chh_ird_timestamp_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_ird_timestamp_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_ird_timestamp_notifications AS
SELECT DISTINCT snz_uid
	,ir_apc_ird_timestamp_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'ird_timestamp' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.ir_clean.ird_addresses a
WHERE snz_idi_address_register_uid IS NOT NULL
AND ir_apc_ird_timestamp_date IS NOT NULL
AND ir_apc_address_status_code = 'V' /*V = valid */
AND ir_apc_main_address_ind = 'Y'
AND ir_apc_ird_timestamp_date <> '2008-12-13'
AND ir_apc_ird_timestamp_date <> '2008-12-14'
AND ir_apc_ird_timestamp_date <> '2009-01-17'
AND ir_apc_ird_timestamp_date <> '2009-03-08'
AND NOT EXISTS (
	SELECT 1
	FROM IDI_Clean_20191020.ir_clean.ird_addresses b
	WHERE a.snz_uid = b.snz_uid
	AND a.ir_apc_ird_timestamp_date = b.ir_apc_applied_date
)
GO

/* MOE */
IF OBJECT_ID('[DL-MAA2016-15].[chh_moe_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_moe_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_moe_notifications AS
SELECT DISTINCT snz_uid
	,moe_spi_mod_address_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'moe' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.moe_clean.student_per
WHERE snz_idi_address_register_uid IS NOT NULL
AND moe_spi_mod_address_date IS NOT NULL
GO

/* MSD residential */
IF OBJECT_ID('[DL-MAA2016-15].[chh_msd_residential_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_msd_residential_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_msd_residential_notifications AS
SELECT DISTINCT snz_uid
	,msd_rsd_start_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'msd_residential' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM IDI_Clean_20191020.msd_clean.msd_residential_location
WHERE snz_idi_address_register_uid IS NOT NULL
AND msd_rsd_start_date IS NOT NULL
AND msd_rsd_start_date <> '1991-11-11'
GO

/* MSD residential - partner */
IF OBJECT_ID('[DL-MAA2016-15].[chh_msd_partner_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_msd_partner_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_msd_partner_notifications AS
SELECT DISTINCT p.[partner_snz_uid] AS snz_uid
	,m.msd_rsd_start_date AS notification_date
	,m.snz_idi_address_register_uid AS address_uid
	,'msd_partner' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.msd_clean.msd_residential_location m
INNER JOIN [IDI_Clean_20191020].[msd_clean].[msd_partner] p
ON m.snz_uid = p.snz_uid
AND p.[msd_ptnr_ptnr_from_date] <= m.msd_rsd_start_date
AND (m.msd_rsd_start_date <= p.[msd_ptnr_ptnr_to_date] OR p.[msd_ptnr_ptnr_to_date] IS NULL)
WHERE m.snz_idi_address_register_uid IS NOT NULL
AND m.msd_rsd_start_date IS NOT NULL
GO

/* MSD residential - child */
IF OBJECT_ID('[DL-MAA2016-15].[chh_msd_child_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_msd_child_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_msd_child_notifications AS
SELECT DISTINCT c.[child_snz_uid] AS snz_uid
	,m.msd_rsd_start_date AS notification_date
	,m.snz_idi_address_register_uid AS address_uid
	,'msd_child' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.msd_clean.msd_residential_location m
INNER JOIN [IDI_Clean_20191020].[msd_clean].[msd_child] c
ON m.snz_uid = c.snz_uid
AND c.[msd_chld_child_from_date] <= m.msd_rsd_start_date
AND (m.msd_rsd_start_date <= c.[msd_chld_child_to_date] OR c.[msd_chld_child_to_date] IS NULL)
WHERE m.snz_idi_address_register_uid IS NOT NULL
AND m.msd_rsd_start_date IS NOT NULL
GO

/* MSD postal */
IF OBJECT_ID('[DL-MAA2016-15].[chh_msd_postal_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_msd_postal_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_msd_postal_notifications AS
SELECT DISTINCT snz_uid
	,msd_pst_start_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'msd_postal' AS [source]
	,'NO' AS [validation]
	,0 AS [high_quality]
FROM IDI_Clean_20191020.msd_clean.msd_postal_location
WHERE snz_idi_address_register_uid IS NOT NULL
AND msd_pst_start_date IS NOT NULL
GO

/* MOH NHI */
IF OBJECT_ID('[DL-MAA2016-15].[chh_moh_nhi_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_moh_nhi_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_moh_nhi_notifications AS
SELECT DISTINCT snz_uid
	,moh_nhi_effective_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'moh_nhi' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM IDI_Clean_20191020.moh_clean.pop_cohort_nhi_address
WHERE snz_idi_address_register_uid IS NOT NULL
AND moh_nhi_effective_date IS NOT NULL
GO

/* MOH PHO */
IF OBJECT_ID('[DL-MAA2016-15].[chh_moh_pho_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_moh_pho_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_moh_pho_notifications AS
SELECT DISTINCT snz_uid
	,moh_adr_consultation_date AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'moh_pho' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM IDI_Clean_20191020.moh_clean.pop_cohort_pho_address
WHERE snz_idi_address_register_uid IS NOT NULL
AND moh_adr_consultation_date IS NOT NULL
GO

/* SNZ 2013 census usual residence */
IF OBJECT_ID('[DL-MAA2016-15].[chh_census_ur_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_census_ur_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_census_ur_notifications AS
SELECT DISTINCT snz_uid
	,'2013-03-05' AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'census_UR' AS [source]
	,'YES' AS [validation]
	,1 AS [high_quality]
FROM IDI_Clean_20191020.cen_clean.census_address
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
GO

/* SNZ 2013 census usual residence 5 years ago */
IF OBJECT_ID('[DL-MAA2016-15].[chh_census_ur5_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_census_ur5_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_census_ur5_notifications AS
SELECT DISTINCT snz_uid
	,'2008-03-05' AS notification_date
	,snz_idi_address_register_uid AS address_uid
	,'census_UR5' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM IDI_Clean_20191020.cen_clean.census_address
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR5'
GO

/* SNZ 2013 census years at current address */
IF OBJECT_ID('[DL-MAA2016-15].[chh_census_prev_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_census_prev_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_census_prev_notifications AS
SELECT a.[snz_uid]
	,DATEADD(year, -1, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -2, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -3, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -4, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -5, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -6, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -7, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -8, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -9, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -10, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -11, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -12, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -13, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -14, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013')

UNION ALL

SELECT a.[snz_uid]
	,DATEADD(year, -15, '2013-03-05') AS notification_date
	,b.[snz_idi_address_register_uid] AS address_uid
	,'census_prev' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[cen_clean].[census_individual] a
INNER JOIN [IDI_Clean_20191020].[cen_clean].[census_address] b
ON a.snz_uid = b.snz_uid
WHERE snz_idi_address_register_uid IS NOT NULL
AND address_type_code = 'UR'
AND [cen_ind_yrs_at_addr_code] NOT IN ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014')
GO

/* SNZ HES */
IF OBJECT_ID('[DL-MAA2016-15].[chh_hes_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_hes_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_hes_notifications AS
SELECT DISTINCT a.snz_uid
,DATEFROMPARTS(b.hes_hhd_year_nbr, b.hes_hhd_month_nbr, 15) AS notification_date
,a.snz_idi_address_register_uid as address_uid
,CONCAT('HES_',a.hes_add_hes_year_code) AS [source]
,'YES' as [validation]
,1 AS [high_quality]

FROM [IDI_Clean_20191020].[hes_clean].[hes_address] AS a
LEFT JOIN (

	SELECT hes_hhd_hes_year_code
	,hes_hhd_year_nbr
	,hes_hhd_month_nbr
	,snz_hes_hhld_uid
	FROM [IDI_Clean_20191020].[hes_clean].[hes_household] 

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
IF OBJECT_ID('[DL-MAA2016-15].[chh_gss_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_gss_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_gss_notifications AS
SELECT DISTINCT j.snz_uid
	,CAST(k.notification_date AS DATE) AS notification_date
	,j.[snz_idi_address_register_uid] AS address_uid
	,k.[source]
	,'YES' as [validation]
	,1 AS [high_quality]
FROM (

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20191020].[gss_clean].[gss_household_2008]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20191020].[gss_clean].[gss_household_2010]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_start_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20191020].[gss_clean].[gss_household_2012]

	UNION ALL

	SELECT [snz_uid]
		  ,[gss_hq_interview_date] AS notification_date
		  ,[gss_hq_collection_code] AS [source]
		  ,[snz_gss_hhld_uid] AS household_uid
	FROM [IDI_Clean_20191020].[gss_clean].[gss_household]

) k
INNER JOIN [IDI_Clean_20191020].[gss_clean].[gss_identity] j
ON k.snz_uid = j.snz_uid
AND k.[source] = j.[gss_id_collection_code]
AND k.household_uid = j.snz_gss_hhld_uid

WHERE j.[snz_idi_address_register_uid] IS NOT NULL
AND k.notification_date IS NOT NULL;
GO

/* SNZ HLFS */
IF OBJECT_ID('[DL-MAA2016-15].[chh_hlfs_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_hlfs_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_hlfs_notifications AS
SELECT DISTINCT a.snz_uid
	,b.[hlfs_urd_interview_date] as notification_date
	,a.[snz_idi_address_register_uid] as address_uid
	,'HLFS'+cast(year(cast(a.hlfs_adr_quarter_date as datetime)) as varchar)  as [source]
	,'YES' as [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[hlfs_clean].[household_address] as a
INNER JOIN [IDI_Clean_20191020].[hlfs_clean].[data] as b
ON a.[snz_hlfs_uid] = b.[snz_hlfs_uid] 
AND a.[snz_hlfs_hhld_uid] = b.[snz_hlfs_hhld_uid]
AND a.[hlfs_adr_quarter_date] = b.[hlfs_urd_quarter_date] 
WHERE a.[snz_idi_address_register_uid] IS NOT NULL
AND b.[hlfs_urd_interview_date] IS NOT NULL
GO

/* NZTA motor vehicle registrations */
IF OBJECT_ID('[DL-MAA2016-15].[chh_nzta_mvr_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_nzta_mvr_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_nzta_mvr_notifications AS
SELECT DISTINCT snz_uid
	,nzta_mvr_start_date as notification_date
	,snz_idi_address_register_uid as address_uid
	,'NZTA' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[nzta_clean].[motor_vehicle_register]
WHERE snz_idi_address_register_uid IS NOT NULL
AND nzta_mvr_start_date IS NOT NULL
GO

/* NZTA driver's license registrations */
IF OBJECT_ID('[DL-MAA2016-15].[chh_nzta_dlr_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_nzta_dlr_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_nzta_dlr_notifications AS
SELECT DISTINCT snz_uid
	,nzta_dlr_licence_issue_date as notification_date
	,snz_idi_address_register_uid as address_uid
	,'NZTA' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]
FROM [IDI_Clean_20191020].[nzta_clean].[drivers_licence_register]
WHERE snz_idi_address_register_uid IS NOT NULL
AND nzta_dlr_licence_issue_date IS NOT NULL
GO

/* HNZ tenancies */
IF OBJECT_ID('[DL-MAA2016-15].[chh_hnz_notifications]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_hnz_notifications];
GO

CREATE VIEW [DL-MAA2016-15].chh_hnz_notifications AS
/* both new and legacy ids */
SELECT DISTINCT
	a.snz_uid
	,CAST(a.hnz_ths_snapshot_date AS DATE) AS notification_date
	,b.snz_idi_address_register_uid AS address_uid
	,'HNZ' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]

FROM [IDI_Clean_20191020].[hnz_clean].[tenancy_household_snapshot] as a
INNER JOIN [IDI_Clean_20191020].[hnz_clean].[houses_snapshot] as b
ON a.hnz_ths_snapshot_date = b.hnz_hs_snapshot_date 
AND a.snz_household_uid = b.snz_household_uid 
AND a.snz_legacy_household_uid = b.snz_legacy_household_uid
WHERE b.snz_idi_address_register_uid IS NOT NULL

UNION ALL

/* new id but not legacy id */
SELECT DISTINCT
	a.snz_uid
	,CAST(a.hnz_ths_snapshot_date AS DATE) AS notification_date
	,b.snz_idi_address_register_uid AS address_uid
	,'HNZ' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]

FROM [IDI_Clean_20191020].[hnz_clean].[tenancy_household_snapshot] as a
INNER JOIN [IDI_Clean_20191020].[hnz_clean].[houses_snapshot] as b
ON a.hnz_ths_snapshot_date = b.hnz_hs_snapshot_date 
AND a.snz_household_uid = b.snz_household_uid 
WHERE b.snz_idi_address_register_uid IS NOT NULL
AND a.snz_legacy_household_uid IS NULL

UNION ALL

/* legacy id but not new id */
SELECT DISTINCT
	a.snz_uid
	,CAST(a.hnz_ths_snapshot_date AS DATE) AS notification_date
	,b.snz_idi_address_register_uid AS address_uid
	,'HNZ' AS [source]
	,'NO' AS [validation]
	,1 AS [high_quality]

FROM [IDI_Clean_20191020].[hnz_clean].[tenancy_household_snapshot] as a
INNER JOIN [IDI_Clean_20191020].[hnz_clean].[houses_snapshot] as b
ON a.hnz_ths_snapshot_date = b.hnz_hs_snapshot_date 
AND a.snz_legacy_household_uid = b.snz_legacy_household_uid
WHERE b.snz_idi_address_register_uid IS NOT NULL
AND a.snz_household_uid IS NULL
GO
