/*
Cross comparison of CHH input datasets
Simon Anastasiadis
2018-11-09

We expect all the records that appear in the gathered data to also appear in the validation data.
The purpose of this code is to run a comparison between the two to ensure this is the case.

This code should be run: - After running the following four scripts (in order) and in the case
that changes are made to any of the following four scripts:
1. setup_views_and_tables_individual_level.sql
2. setup_views_and_tables_household_level.sql
3. data_prep_notifications.R
4. data_prep_validation.R

If input and validation data is correct then output of this query should have:
- No NULL values
- Almost all values have matching/corresponding source in the 'gathered' and 'truth' columns
- A negligible number of records have non-matching/non-corresponding sources

*/
SELECT a.[source] AS gathered_source
,b.[source] AS truth_source
,count(*) AS num
FROM [IDI_Sandpit].[DL-MAA2016-15].[chh_gathered_data] a
FULL OUTER JOIN [IDI_Sandpit].[DL-MAA2016-15].[chh_household_validation] b
ON a.snz_uid = b.snz_uid
AND a.notification_date = b.notification_date
AND a.address_uid = b.address_uid
WHERE a.[validation] = 'YES'
OR a.[validation] IS NULL
GROUP BY a.[source], b.[source]
