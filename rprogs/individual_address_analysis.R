##################################################################################################################
#' Description: Produce best individual level address change records
#'
#' Input:
#' prepared sandpit data table with address notifications 
#' required columns names = c("snz_uid", "notification_date", "address_uid", "source", "validation")
#'
#' Output:
#' two output tables:
#' - notifications that represent our best estimate of address changes
#' - all other notifications with their reason for exclusion
#' intermediate tables of address notifications
#' - with coded accuracy
#' - with unsupported simultaneous notifications removed
#' - with lower quality notifications removed
#' - with unsupported concurrent notifications removed
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies:
#' 
#' Notes:
#' Uses dbplyr to translate R to SQL
#' Last recorded runtime = 10 min in development mode, 13 hours in full
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2018-11-20 SA v1
#' 2018-09-18 SA v0
#'#################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')
source('individual_address_analysis_functions.R')

## parameters ----

# user settings
DEVELOPMENT_MODE = FALSE
CAUTIOUS_MODE = TRUE
NUM_SUBSETS = 400
# tuning parameters
QUALITY_THRESHOLD = 0.7 #0.8
YA_QUALITY_THRESHOLD = 0.6 #0.63
YA_AGE_START = 18
YA_AGE_END = 28 #26
CASCADE_LIMIT = 3
CONCURRENT_MAX_DAYS_WINDOW = 140
CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS = 130

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_views = "[IDI_UserCode].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

## load required tables ----
#
# Note if CAUTIOUS_MODE == TRUE then you need to rebuild the connection
# to all tables that are used in a section at the start of the section.
# Otherwise code will freeze on a join as tables are from separate connections.

run_time_inform_user("building table connections")

address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                            schema =  our_schema,
                                            tbl_name = "chh_gathered_data")

person_details = create_access_point(db_connection = db_con_IDI_sandpit,
                                     schema = "[IDI_Clean_20181020].[data]",
                                     tbl_name = "personal_detail")

if(DEVELOPMENT_MODE){
  address_notifications = address_notifications %>% filter(snz_uid %% 100 == 0)
  person_details = person_details %>% filter(snz_uid %% 100 == 0)
}

## create output tables ----

columns_for_replacement_table = c("snz_uid", "notification_date", "address_uid", "source", "validation", "replaced")

replaced_notifications_columns = list(snz_uid = "[int] NOT NULL",
                                      notification_date = "[date] NOT NULL",
                                      address_uid = "[int] NOT NULL",
                                      source = "[varchar](25) NULL",
                                      validation = "[varchar](3) NOT NULL",
                                      replaced = "[varchar](50) NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = "chh_replaced_notifications",
             named_list_of_columns = replaced_notifications_columns,
             OVERWRITE = TRUE)

columns_for_refined_tables = c("snz_uid", "notification_date", "address_uid", "source",
                               "validation", "concurrent_flag")

refined_columns = list(snz_uid = "[int] NOT NULL",
                       notification_date = "[date] NOT NULL",
                       address_uid = "[int] NOT NULL",
                       source = "[varchar](25) NULL",
                       validation = "[varchar](3) NOT NULL",
                       concurrent_flag = "[int] NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = "chh_individual_refined",
             named_list_of_columns = refined_columns,
             OVERWRITE = TRUE)

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = "chh_address_changes",
             named_list_of_columns = refined_columns,
             OVERWRITE = TRUE)

## clip to dates ----

run_time_inform_user("---- clipping to dates ----")

out = partition_by_dates(address_notifications, earliest_date = '2001-01-01', latest_date = '2017-12-31')
address_notifications = out$notifs_in_date_window
notifs_out_of_date_window = out$notifs_out_of_date_window

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, notifs_out_of_date_window)

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## add accuracy measures ----

run_time_inform_user("---- adding accuracy measures ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
  # reload person details
  person_details = create_access_point(db_connection = db_con_IDI_sandpit,
                                       schema = "[IDI_Clean_20181020].[data]",
                                       tbl_name = "personal_detail")
  if(DEVELOPMENT_MODE)
    person_details = person_details %>% filter(snz_uid %% 100 == 0)
}

out = add_accuracy_measures(db_con_IDI_sandpit, address_notifications, person_details)

all_notifs_with_accuracy = out$all_notifs_with_accuracy %>%
  select(snz_uid, notification_date, address_uid, source, validation, accuracy)

run_time_inform_user("writing accuracy coded notifications")
address_notifications = write_to_database(all_notifs_with_accuracy,
                                          db_con_IDI_sandpit, our_schema, "chh_accuracy_coded", OVERWRITE = TRUE)
run_time_inform_user("complete")

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

#### this is the point at which subsetting could begin ----

# purge as refinement is separate
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

if(DEVELOPMENT_MODE){
  modulo = 0
} else {
  modulo = 0:(NUM_SUBSETS-1)
}

for(jj in modulo){
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = "chh_accuracy_coded")

  address_notifications = address_notifications %>% filter(snz_uid %% NUM_SUBSETS == jj)
  
  if(!DEVELOPMENT_MODE)
    run_time_inform_user(paste0("-------- modulo ",jj," begun --------"))


## exclude unsupported simultaneous notifications ----

run_time_inform_user("---- exclude unsupported simult notifs ----")

# deactivate while iterating over modulo
# if(CAUTIOUS_MODE){
#   # recreate connection
#   db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
#   # reload current working table
#   address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
#                                               schema =  our_schema,
#                                               tbl_name = cautious_table)
# }

out = partition_out_unsupported_simultaneous(db_con_IDI_sandpit, address_notifications)
unsupported_simultaneous_notifications = out$unsupported_simultaneous_notifications
address_notifications = out$all_other_notifications

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, unsupported_simultaneous_notifications)

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## exclude low quality notifications ----

run_time_inform_user("---- exclude unsupported low qual notifs ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
  # reload person details
  person_details = create_access_point(db_connection = db_con_IDI_sandpit,
                                       schema = "[IDI_Clean_20181020].[data]",
                                       tbl_name = "personal_detail")
  if(DEVELOPMENT_MODE)
    person_details = person_details %>% filter(snz_uid %% 100 == 0)
}

out = partition_out_low_quality(db_con_IDI_sandpit, address_notifications, person_details,
                                QUALITY_THRESHOLD, YA_QUALITY_THRESHOLD, YA_AGE_START, YA_AGE_END, CASCADE_LIMIT)
address_notifications = out$high_quality_notifs
low_quality_notifs = out$low_quality_notifs

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, low_quality_notifs)

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## exclude echos ----

run_time_inform_user("---- exclude echos ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
}

out = partition_out_echoes(db_con_IDI_sandpit, address_notifications,
                           CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS)
notif_in_gap_for_discard = out$notif_in_gap_for_discard
address_notifications = out$notifications_no_echoes

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, notif_in_gap_for_discard)

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## add concurrency flags to notifications ----

run_time_inform_user("---- add concurrency flags ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
}

out = add_concurrency_flag(db_con_IDI_sandpit, address_notifications,
                           CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS)
address_notifications = out$address_notifications

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## resolve all remaining simultaneous notifications ----

run_time_inform_user("---- resolve simultaneous ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
}

out = resolve_remaining_simultaneous(db_con_IDI_sandpit, address_notifications)
address_notifications = out$all_other_notifications
unsupported_simultaneous_notifications = out$unsupported_simultaneous_notifications

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, unsupported_simultaneous_notifications)


append_database_table(db_con_IDI_sandpit, our_schema, "chh_individual_refined",
                      columns_for_refined_tables, address_notifications)

if(CAUTIOUS_MODE){
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  # output current working table
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
}

## convert notifications to address changes ----

run_time_inform_user("---- convert to address changes ----")

if(CAUTIOUS_MODE){
  # recreate connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
}

out = partition_notifications_to_address_changes(address_notifications)
address_notifications = out$address_changes
not_address_changes = out$not_address_changes

append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, not_address_changes)

run_time_inform_user("appending address changes")
append_database_table(db_con_IDI_sandpit, our_schema, "chh_address_changes",
                      columns_for_refined_tables, address_notifications)
run_time_inform_user("complete")

#### end of subsetting loop ----

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

if(!DEVELOPMENT_MODE)
  run_time_inform_user(paste0("-------- modulo ",jj," complete --------"))
}

## completion ----

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

run_time_inform_user("-------- GRAND COMPLETION --------")

