/******************************************************************************************************************
Description: Establish views and tables as input to constructing enhanced household address information
These views capture non-residential spells

Input: IDI refresh

Output: Non-residential spells
 
Author: Simon Anastasiadis

Dependencies:

Notes:
1) Required columns: snz_uid, start_date, end_date (may be null), source, duration
 
Issues:
 
History (reverse order):
2019-08-13 SA updated to 2019-04-20 refresh
2019-08-09 SA v1
******************************************************************************************************************/

/* Establish database for writing views */
USE IDI_UserCode
GO

/* Deaths */
IF OBJECT_ID('[DL-MAA2016-15].[chh_death_non_residential]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_death_non_residential];
GO

CREATE VIEW [DL-MAA2016-15].chh_death_non_residential AS
SELECT snz_uid
	,DATEFROMPARTS(dia_dth_death_year_nbr, dia_dth_death_month_nbr, 28) AS [start_date]
	,'9999-01-01' AS [end_date]
	,'dia_deaths' AS [source]
	,1000000 AS duration
FROM IDI_Clean_20190420.dia_clean.deaths
WHERE dia_dth_death_year_nbr >= 2001
AND dia_dth_death_year_nbr IS NOT NULL
AND dia_dth_death_month_nbr IS NOT NULL
GO

/* Overseas */
IF OBJECT_ID('[DL-MAA2016-15].[chh_overseas_non_residential]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_overseas_non_residential];
GO

CREATE VIEW [DL-MAA2016-15].chh_overseas_non_residential AS
SELECT snz_uid
	,pos_applied_date AS [start_date]
	,pos_ceased_date AS [end_date]
	,'overseas' AS [source]
	,DATEDIFF(DAY, pos_applied_date, pos_ceased_date) AS duration
FROM IDI_Clean_20190420.data."person_overseas_spell"
WHERE pos_applied_date IS NOT NULL
AND pos_applied_date >= '2001-01-01'
AND pos_first_arrival_ind = 'n'
GO

/* Corrections */
IF OBJECT_ID('[DL-MAA2016-15].[chh_prison_non_residential]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_prison_non_residential];
GO

CREATE VIEW [DL-MAA2016-15].chh_prison_non_residential AS
SELECT snz_uid
	,cor_mmp_period_start_date AS [start_date]
	,cor_mmp_period_end_date AS [end_date]
	,'prison' AS [source]
	,DATEDIFF(DAY, cor_mmp_period_start_date, cor_mmp_period_end_date) AS duration
FROM IDI_Clean_20190420.cor_clean."ov_major_mgmt_periods"
WHERE cor_mmp_mmc_code = 'PRISON'
AND cor_mmp_period_start_date IS NOT NULL
AND cor_mmp_period_end_date IS NOT NULL
AND cor_mmp_period_start_date >= '2001-01-01'
GO