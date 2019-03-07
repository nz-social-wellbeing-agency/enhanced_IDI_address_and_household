##################################################################################################################
#' Description: Undertakes validation of requested tables
#'
#' Input:
#' prepared sandpit data table with address notifications 
#' required column names = c("snz_uid", "notification_date", "address_uid", "source", "validation")
#' optional column names = c("accuracy", other columns to group by)
#'
#' Output:
#' 
#' 
#' Author: Simon Anastasiadis, Craig Wright
#' 
#' Dependencies:
#' 
#' Notes:
#' Original version in SQL by CW
#' Uses dbplyr to translate R to SQL
#' Last recorded runtime = 1 min in development mode, est. 10 hours in full
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2018-11-21 SA v1
#' 2018-11-09 SA v0
#'#################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/",
             "Datalab-MA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')
source('validation_suite_functions.R')
library(xlsx)

## parameters ----

# user settings
DEVELOPMENT_MODE = FALSE
CAUTIOUS_MODE = TRUE
HOUSEHOLD_MODE = TRUE
# selection paramters
# TABLE_TO_TEST = "chh_individual_refined"
# TABLE_TO_TEST = "chh_snz_address_notif_table1"
TABLE_TO_TEST = "chh_snz_address_notif_full_table"
# TABLE_TO_TEST = "chh_gathered_data"
# TABLE_TO_TEST = "chh_household_refined"
# TABLE_TO_TEST = "chh_address_changes"
# TABLE_TO_TEST = "chh_ex_household_refined"
TABLE_WITH_TRUTH = "chh_household_validation"

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_val")
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh2_val")

## load required tables ----
run_time_inform_user("building table connections")

address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit, schema =  our_schema,
                                            tbl_name = TABLE_TO_TEST)

validation_truth = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_schema,
                                            tbl_name = TABLE_WITH_TRUTH)

if(DEVELOPMENT_MODE & !HOUSEHOLD_MODE){
  # address_notifications = address_notifications %>% filter(snz_uid %in% c(9452806,10184037,6429832))
  # validation_truth = validation_truth %>% filter(snz_uid %in% c(9452806,10184037,6429832))
  
  address_notifications = address_notifications %>% filter(snz_uid %% 100 == 0)
  validation_truth = validation_truth %>% filter(snz_uid %% 100 == 0)
}

if(DEVELOPMENT_MODE & HOUSEHOLD_MODE){
  ta_address = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20181020].[data]", "address_notification", TRUE)
  ta_address = ta_address %>%
    filter(ant_ta_code == '019') %>%
    select(address_uid = snz_idi_address_register_uid) %>%
    distinct()
  
  address_notifications = address_notifications %>% semi_join(ta_address, by = "address_uid")
  validation_truth = validation_truth %>% semi_join(ta_address, by = "address_uid")
}

## check input tables have the required columns ----

required_address_notif_columns = c("snz_uid", "notification_date", "address_uid", "source", "validation")
check = table_contains_required_columns(address_notifications, required_address_notif_columns)
assert(check, "table to test must contain columns: snz_uid, notification_date, address_uid, source, validation")

required_validation_columns = c("snz_uid", "notification_date", "address_uid", "household_uid", "source")
check = table_contains_required_columns(validation_truth, required_validation_columns)
assert(check,"table to test must contain columns: snz_uid, notification_date, address_uid, household_uid, source")

run_time_inform_user("input tables checked")

## prepare for comparison ----

address_spells = convert_notifs_to_validation_spells(address_notifications, accuracy_threshold = NA)
address_spells = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                 "chh_val_address_spells", address_spells)

comparison_ready = compare_truth_and_spells(truth_tbl = validation_truth, spells_tbl = address_spells)
comparison_ready = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                   "chh_val_comparison_ready", comparison_ready, index_columns = "snz_uid")

run_time_inform_user("prepared for comparison")

## comparison and validation ----

out = summarise_individual_accuracy(comparison_ready)
individual_overall_result = out$overall_result
individual_source_result = out$source_result
run_time_inform_user("individual results obtained")

out = summarise_household_accuracy_NEW(db_con_IDI_sandpit, comparison_ready)
household_overall_result = out$overall_result
household_source_result = out$source_result
run_time_inform_user("household results obtained")

## write output ----

if(!dir.exists("./output xlsx"))
  dir.create("./output xlsx")
  
clean_time = gsub("[. :]","_",Sys.time())
file_name = paste0("./output xlsx/accuracy ",clean_time,".xlsx")

write.xlsx(data.frame(household_overall_result), file_name, sheetName = "hhld", row.names = FALSE)
write.xlsx(data.frame(household_source_result), file_name, sheetName = "hhld_src", row.names = FALSE, append = TRUE)
write.xlsx(data.frame(individual_overall_result), file_name, sheetName = "indiv", row.names = FALSE, append = TRUE)
write.xlsx(data.frame(individual_source_result), file_name, sheetName = "indiv_src", row.names = FALSE, append=TRUE)

run_time_inform_user("results written out to file")

## conclude ----

# purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_val")
# purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh2_val")

run_time_inform_user("GRAND COMPLETION")
