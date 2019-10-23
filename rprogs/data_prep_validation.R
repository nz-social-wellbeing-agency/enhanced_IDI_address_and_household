##################################################################################################################
#' Description: Prepare the data for validation of household building
#'
#' Input: Prepared views from IDI Clean and RnD
#'
#' Output: SQL tables in the sandpit
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: setup_views_and_tables_household_level.sql
#' 
#' Notes:
#' Uses dbplyr to translate R to SQL
#' Last recorded runtime = 2 minutes
#'
#' Editting:
#' To add new datasets to the address notification approach, create a view or table in the SQL script
#' before adding the new table or view to this script.
#' Ensure that the new table you add has all the required columns as listed in "column_names".
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2019-03-05 SA views are now saved in IDI_UserCode
#' 2018-11-07 SA v1
##################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')

## setup ----
db_con_IDI_sandpit = create_database_connection(database = 'IDI_Sandpit')
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_view ="[IDI_UserCode].[DL-MAA2016-15]"
OUTPUT_TABLE = "chh_household_validation"

## create tables ----

notifications_columns = list(snz_uid = "[int] NOT NULL",
                             notification_date = "[date] NOT NULL",
                             address_uid = "[int] NOT NULL",
                             household_uid = "[int] NOT NULL",
                             source = "[varchar](25) NULL")

columns_for_notifications = names(notifications_columns)

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_TABLE,
             named_list_of_columns = notifications_columns,
             OVERWRITE = TRUE)

## load each table withy key columns ----
run_time_inform_user("building table connections")

list_of_views_to_load <- c("chh_census_validation",
                           "chh_hes_validation",
                           "chh_gss_validation",
                           "chh_hlfs_validation",
                           "chh_census_composition")

for(view in list_of_views_to_load){
  this_view <- create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view, tbl_name = view)
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, this_view)
  run_time_inform_user(paste0(view," appended"))
}

run_time_inform_user("indexing")
create_clustered_index(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, "snz_uid")
run_time_inform_user("load complete, compressing")
compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE)
run_time_inform_user("compressed")

## Complete ----

close_database_connection(db_con_IDI_sandpit)
run_time_inform_user("GRAND COMPLETION")


