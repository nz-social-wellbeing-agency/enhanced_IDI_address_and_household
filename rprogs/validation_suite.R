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
#' Author: Simon Anastasiadis, Craig Wright, Akilesh Chokkanathapuram
#' 
#' Dependencies:
#' 
#' Notes:
#' Original version in SQL by CW
#' Uses dbplyr to translate R to SQL
#' Last recorded runtime = 6 min in development mode, 70 min in full
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2019-09-26 SA clean to minimal production code
#' 2019-08-13 SA updated to 2019-04-20 refresh
#' 2019-07-25 SA v1.1
#' 2018-11-21 SA v1
#' 2018-11-09 SA v0
#'#################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')
source('validation_suite_functions.R')
library(xlsx)

## parameters ----

# user settings
DEVELOPMENT_MODE = FALSE
REFRESH_SCHEMA = "[IDI_Clean_20190420].[data]"
EST_RESIDENTIAL_POP = "snz_res_pop"

# input tables
TRUE_ADDRESS = "chh_household_validation"
CURRENT_SNZ = "chh_current_address_table"
RAW_GATHERED = "chh_gathered_data"
INDIV_REFINED = "chh_individual_refined"
INDIV_REPLACED = "chh_replaced_notifications"
INDIV_CHANGES = "chh_address_changes"
HHLD_REFINED = "chh_household_refined"
HHLD_REPLACED = "chh_household_replaced_notifications"
# output table
INDIV_MATCH = "chh_individual_matched"

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_val")

## create output tables ----

INDIV_MATCH_COLS = list(snz_uid = "[int] NOT NULL",
                        notification_date_truth = "[date] NOT NULL",
                        source_truth = "[varchar](25) NULL",
                        match_status = "[int] NOT NULL",
                        phase = "[int] NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = INDIV_MATCH,
             named_list_of_columns = INDIV_MATCH_COLS,
             OVERWRITE = TRUE)

## load required tables ----
run_time_inform_user("building table connections")

CURRENT_SNZ = create_access_point(db_con_IDI_sandpit, our_view, CURRENT_SNZ)
TRUE_ADDRESS = create_access_point(db_con_IDI_sandpit, our_schema, TRUE_ADDRESS) %>%
  mutate(validation = 'YES') %>%
  select(snz_uid, notification_date, address_uid, source, validation)
RAW_GATHERED = create_access_point(db_con_IDI_sandpit, our_schema, RAW_GATHERED)
INDIV_REFINED = create_access_point(db_con_IDI_sandpit, our_schema, INDIV_REFINED)
INDIV_REPLACED = create_access_point(db_con_IDI_sandpit, our_schema, INDIV_REPLACED)
INDIV_CHANGES = create_access_point(db_con_IDI_sandpit, our_schema, INDIV_CHANGES)
HHLD_REFINED = create_access_point(db_con_IDI_sandpit, our_schema, HHLD_REFINED)
HHLD_REPLACED = create_access_point(db_con_IDI_sandpit, our_schema, HHLD_REPLACED)

est_residential_population = create_access_point(db_con_IDI_sandpit, REFRESH_SCHEMA, "snz_res_pop") %>%
  select('snz_uid')

if(DEVELOPMENT_MODE){
  CURRENT_SNZ = CURRENT_SNZ %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  TRUE_ADDRESS = TRUE_ADDRESS %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  RAW_GATHERED = RAW_GATHERED %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  INDIV_REFINED = INDIV_REFINED %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  INDIV_REPLACED = INDIV_REPLACED %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  INDIV_CHANGES = INDIV_CHANGES %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  HHLD_REFINED = HHLD_REFINED %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
  HHLD_REPLACED = HHLD_REPLACED %>% filter(snz_uid %% 1000 == 0 | address_uid %% 100 == 0)
}

# check input tables have the required columns
required_cols = c("snz_uid", "notification_date", "address_uid", "source", "validation")

assert(table_contains_required_columns(CURRENT_SNZ, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(TRUE_ADDRESS, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(RAW_GATHERED, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(INDIV_REFINED, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(INDIV_REPLACED, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(INDIV_CHANGES, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(HHLD_REFINED, required_cols), "an input table lacks a required column")
assert(table_contains_required_columns(HHLD_REPLACED, required_cols), "an input table lacks a required column")

run_time_inform_user("tables loaded and checked checked")

## setup truth table ----

truth_table = TRUE_ADDRESS %>%
  filter(validation == 'YES') %>%
  semi_join(est_residential_population, by = "snz_uid")

truth_table = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_val_truth", truth_table, index_columns = 'snz_uid')

## setup stages of comparison ----
#
# this is primarily business logic depending on how the construction took place

col_list = c("snz_uid", "address_uid", "notification_date", "source", "validation")

phase1_gathered = RAW_GATHERED

phase2_window = INDIV_REPLACED %>%
  filter(replaced != 'notification outside window') %>%
  union_all(INDIV_CHANGES, col_list)

phase3_simult = INDIV_REPLACED %>%
  filter(replaced != 'notification outside window') %>%
  filter(replaced != 'unsupported simulataneous') %>%
  union_all(INDIV_CHANGES, col_list)

phase4_quality = INDIV_REPLACED %>%
  filter(replaced != 'notification outside window') %>%
  filter(replaced != 'unsupported simulataneous') %>%
  filter(replaced != 'lower quality') %>%
  union_all(INDIV_CHANGES, col_list)

phase5_gaps = INDIV_REPLACED %>%
  filter(replaced != 'notification outside window') %>%
  filter(replaced != 'unsupported simulataneous') %>%
  filter(replaced != 'lower quality') %>%
  filter(replaced != 'in gap unsupported') %>%
  union_all(INDIV_CHANGES, col_list)

phase6_last_simult = INDIV_REPLACED %>%
  filter(replaced != 'notification outside window') %>%
  filter(replaced != 'unsupported simulataneous') %>%
  filter(replaced != 'lower quality') %>%
  filter(replaced != 'in gap unsupported') %>%
  filter(replaced != 'resolve last simultaneous') %>%
  union_all(INDIV_CHANGES, col_list)

phase7_changes = INDIV_CHANGES

phase8_hhld_start = HHLD_REFINED %>%
  union_all( HHLD_REPLACED, col_list) %>%
  filter(source != 'standardise move in date'
         & source != 'sync child to parents'
         & source != 'consistent hand overs')

phase9_sync_all = HHLD_REFINED %>%
  filter(source != 'sync child to parents'
         & source != 'consistent hand overs') %>%
  union_all( filter(HHLD_REPLACED, replaced == 'consistent hand overs') , col_list)

phase10_sync_kids = HHLD_REFINED %>%
  filter(source != 'consistent hand overs') %>%
  union_all( filter(HHLD_REPLACED, replaced == 'consistent hand overs') , col_list)

phase11_final = HHLD_REFINED

phase12_current_address = CURRENT_SNZ # did we do better?

run_time_inform_user("phase tables prepared")

## household comparison and validation ----

source_list = list(phase1_gathered,
                   phase2_window,
                   phase3_simult,
                   phase4_quality,
                   phase5_gaps,
                   phase6_last_simult,
                   phase7_changes,
                   phase8_hhld_start,
                   phase9_sync_all,
                   phase10_sync_kids,
                   phase11_final,
                   phase12_current_address)

# validate each phase
results_table = data.frame(source = "place_holder", final_stat = "placeholder",
                           total_count = 0, percent_stat = 0, phase = 0, stringsAsFactors = FALSE)

for(phase in 1:12){
  admin_table = source_list[[phase]]
  run_time_inform_user(sprintf("start of phase %d", phase))
  
  # store tmp table
  admin_table = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_val_current_phase", admin_table,
                                index_columns = 'snz_uid')

  # compute accuracy (inner)
  truth_w_latest_admin = connect_truth_with_admin_to_compare(admin_table, truth_table)
  truth_w_latest_admin = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_val_truth_w_latest_admin",
                                         truth_w_latest_admin)
  # save household results (inner)
  hhld_results = address_validation_algorithm(truth_w_latest_admin)
  hhld_results = hhld_results %>% mutate(phase = phase)
  results_table = rbind(results_table, as.data.frame(hhld_results))
  
  # save individual results
  indiv_summary = truth_w_latest_admin %>%
    select(snz_uid, source_truth, notification_date_truth, notification_date, match_status) %>%
    mutate(phase = phase)
  append_database_table(db_con_IDI_sandpit, our_schema, INDIV_MATCH, names(INDIV_MATCH_COLS), indiv_summary)
  
}

compress_table(db_con_IDI_sandpit, our_schema, INDIV_MATCH)

## write output ----

run_time_inform_user("output to file")

if(!dir.exists("./output xlsx"))
  dir.create("./output xlsx")

clean_time = gsub("[. :]","_",Sys.time())
file_name = paste0("./output xlsx/accuracy ",clean_time,".xlsx")

write.xlsx(data.frame(results_table), file_name, sheetName = "hhld", row.names = FALSE)

## conclude ----

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_val")
close_database_connection(db_con_IDI_sandpit)
run_time_inform_user("GRAND COMPLETION")


