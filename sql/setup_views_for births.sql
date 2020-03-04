/******************************************************************************************************************
Description: Establish views and tables as input to constructing enhanced household address information

Input:

Output: View of dia births
 
Author: Simon Anastasiadis

Dependencies:
 
Notes:
1) Necessary as "SELECT *" no longer works on dia_births as select permissions have been removed from specific columns
2) Currently locked to 2019-10-20 refersh
 
Issues:
 
History (reverse order):
2020-01-29 SA v1
******************************************************************************************************************/

/* Establish database for writing views */
USE IDI_UserCode
GO

/* births */
IF OBJECT_ID('[DL-MAA2016-15].[chh_dia_births]','V') IS NOT NULL
DROP VIEW [DL-MAA2016-15].[chh_dia_births];
GO

CREATE VIEW [DL-MAA2016-15].chh_dia_births AS
SELECT snz_uid
	, dia_bir_birth_month_nbr
	, dia_bir_birth_year_nbr
	, parent1_snz_uid
	, parent2_snz_uid
FROM [IDI_Clean_20191020].[dia_clean].[births]
GO
