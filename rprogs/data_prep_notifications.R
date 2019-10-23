###############################################################################
# Description: Prepare the data for household building
#
# Input: Prepared views from IDI Clean
#
# Output: SQL tables in the sandpit
# 
# Author: Simon Anastasiadis
# 
# Dependencies: setup_views_and_tables_individual_level.sql
# 
# Notes:
# Uses dbplyr to translate R to SQL
# Last recorded runtime = 15 minutes for load + 3 minutes for accuracy
#
# Editting:
# To add new datasets to the address notification approach, create a view or table in the SQL script
# before adding the new table or view to this script.
# Ensure that the new table you add has all the required columns as listed in "column_names".
# 
# Issues:
# 
# History (reverse order):
# 2019-09-26 SA clean to minimal production code
# 2019-08-13 SA updated to 2019-04-20 refresh
# 2019-06-13 SA moved loading views into a for loop to reduce repetition
# 2019-03-05 SA changed location views are saved to IDI_UserCode
# 2018-11-07 SA swapped from tables to views
# 2018-09-21 SA added all datasets produced by Craig
# 2018-08-23 SA restructure to make adding data easier
# 2018-08-22 SA v1
###############################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')
source('individual_address_analysis_functions.R')

## setup ----
db_con_IDI_sandpit = create_database_connection(database = 'IDI_Sandpit')
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_view = "[IDI_UserCode].[DL-MAA2016-15]"
# parameters
OUTPUT_TABLE = "chh_gathered_data"
ACCURACY_WINDOW = 90

## create table for output ----

notifications_columns = list(snz_uid = "[int] NOT NULL",
                             notification_date = "[date] NOT NULL",
                             address_uid = "[int] NOT NULL",
                             source = "[varchar](25) NULL",
                             validation = "[varchar](3) NOT NULL",
                             high_quality = "[int] NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_TABLE,
             named_list_of_columns = notifications_columns,
             OVERWRITE = TRUE)

## load each table withy key columns ----
run_time_inform_user("building notification table")

list_of_views_to_load <- c("chh_acc_notifications",
                           "chh_ird_applied_notifications",
                           "chh_ird_timestamp_notifications",
                           "chh_moe_notifications",
                           "chh_msd_residential_notifications",
                           "chh_msd_partner_notifications",
                           "chh_msd_child_notifications",
                           "chh_msd_postal_notifications",
                           "chh_moh_nhi_notifications",
                           "chh_moh_pho_notifications",
                           # "chh_census_ur_notifications",
                           "chh_census_ur5_notifications",
                           "chh_census_prev_notifications",
                           # "chh_hes_notifications",
                           # "chh_gss_notifications",
                           # "chh_hlfs_notifications",
                           "chh_nzta_mvr_notifications",
                           "chh_nzta_dlr_notifications",
                           "chh_hnz_notifications")

for(view in list_of_views_to_load){
  this_view <- create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view, tbl_name = view)
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, names(notifications_columns), this_view)
  run_time_inform_user(paste0(view," appended"))
}

run_time_inform_user("load complete, compressing")
compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE)
run_time_inform_user("compressed")

## table of accuracy measures ----

run_time_inform_user("obtain accuracy measures")
address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE)
person_details = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20190420].[data]", "personal_detail")
accuracy_measures = obtain_accuracy_measures(address_notifications, person_details, ACCURACY_WINDOW)
accuracy_measures = write_to_database(accuracy_measures, db_con_IDI_sandpit, our_schema,
                                      "chh_accuracy_measures", OVERWRITE = TRUE)

## load non-residential spells ----
#
# Death, extended time overseas, and extended time in prison
# were used as indicators of non-residential spells.
#
# However their inclusion failed to improve accuracy,
# Hence they were removed.

## Complete ----

close_database_connection(db_con_IDI_sandpit)
run_time_inform_user("GRAND COMPLETION")
