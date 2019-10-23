##################################################################################################################
#' Description: Produce best individual level address change records
#'
#' Input:
#' prepared sandpit data table with address notifications 
#' required columns names = c("snz_uid", "notification_date", "address_uid", "source", "validation", "high_quality")
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
#' Last recorded runtime = 5 min in development mode, 4.5 hours in full
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2019-09-26 SA clean to minimal production code
#' 2019-08-13 SA updated to 2019-04-20 refresh
#' 2019-06-21 SA v1.1
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
SQL_MODE = TRUE # = FALSE not implemented
NUM_SUBSETS = 50
# inputs and outputs
INPUT_TABLE = "chh_gathered_data"
OUTPUT_REPLACED = "chh_replaced_notifications"
OUTPUT_REFINED = "chh_individual_refined"
OUTPUT_CHANGES = "chh_address_changes"
# tuning parameters
LATEST_DATE = '2019-12-31'
DAYS_SUPPORT = 720
DAYS_SPREAD = 60
MAX_GAP_WIDTH = 720
SUPPORT_WITHIN_DAYS = 180

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_views = "[IDI_UserCode].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

## create output tables ----

replaced_notif_columns = list(snz_uid = "[int] NOT NULL",
                              notification_date = "[date] NOT NULL",
                              address_uid = "[int] NOT NULL",
                              source = "[varchar](25) NULL",
                              validation = "[varchar](3) NOT NULL",
                              replaced = "[varchar](50) NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_REPLACED,
             named_list_of_columns = replaced_notif_columns,
             OVERWRITE = TRUE)

refined_notif_columns = list(snz_uid = "[int] NOT NULL",
                             notification_date = "[date] NOT NULL",
                             address_uid = "[int] NOT NULL",
                             source = "[varchar](25) NULL",
                             validation = "[varchar](3) NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_REFINED,
             named_list_of_columns = refined_notif_columns,
             OVERWRITE = TRUE)

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_CHANGES,
             named_list_of_columns = refined_notif_columns,
             OVERWRITE = TRUE)

# wrapper for ease of storing un-trusted notifications in OUTPUT_REPLACED archive
append_discarded_notifs_to_archive = function(notifs_to_discard){
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_REPLACED,
                        names(replaced_notif_columns), notifs_to_discard)
}

#### begin subsetting ----

# only one remainder if development mode, otherwise all of them
modulo = if(DEVELOPMENT_MODE){
  modulo = 0
} else {
  modulo = 0:(NUM_SUBSETS-1)
}

for(jj in modulo){
  run_time_inform_user(paste0("-------- modulo ",jj," begun --------"))
  
  ## load required tables ----
  
  run_time_inform_user("building table connections")
  
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = INPUT_TABLE)

  address_notifications = address_notifications %>% filter(snz_uid %% NUM_SUBSETS == jj)
  
  ## clip to dates ----
  
  run_time_inform_user("---- clipping to dates ----")
  
  out = partition_by_dates(address_notifications, earliest_date = '2001-01-01', latest_date = LATEST_DATE)
  address_notifications = out$keep
  notifs_out_of_date_window = out$discard
  
  append_discarded_notifs_to_archive(notifs_out_of_date_window)
  if(SQL_MODE){ rebuild_connection() }
  
  ## exclude unsupported simultaneous notifications ----
  
  run_time_inform_user("---- exclude unsupported simult notifs ----")
  
  out = partition_out_unsupported_simultaneous(address_notifications, DAYS_SUPPORT)
  address_notifications = out$keep
  unsupported_simultaneous_notifications = out$discard

  append_discarded_notifs_to_archive(unsupported_simultaneous_notifications)
  if(SQL_MODE){ rebuild_connection() }
  
  ## exclude low quality notifications ----
  
  run_time_inform_user("---- exclude unsupported low qual notifs ----")
  
  out = partition_out_low_quality(address_notifications, DAYS_SPREAD)
  address_notifications = out$keep
  low_quality_notifs = out$discard
  
  append_discarded_notifs_to_archive(low_quality_notifs)
  if(SQL_MODE){ rebuild_connection() }
  
  ## exclude echos ----
  
  run_time_inform_user("---- exclude echos ----")
  
  notifications_in_gap = obtain_notifications_within_a_gap(address_notifications, MAX_GAP_WIDTH)
  notifications_in_gap = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_tmp_notifications_in_gap",
                                         notifications_in_gap, "snz_uid")
  
  out = partition_out_echoes(address_notifications, notifications_in_gap, SUPPORT_WITHIN_DAYS)
  address_notifications = out$keep
  notif_in_gap_for_discard = out$discard
  
  append_discarded_notifs_to_archive(notif_in_gap_for_discard)
  if(SQL_MODE){ rebuild_connection() }
  
  ## resolve all remaining simultaneous notifications ----
  
  run_time_inform_user("---- resolve simultaneous ----")
  
  out = resolve_remaining_simultaneous(address_notifications, DAYS_SUPPORT)
  address_notifications = out$keep
  unsupported_simultaneous_notifs = out$discard
  
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_REFINED,
                        names(refined_notif_columns), address_notifications)
  
  append_discarded_notifs_to_archive(unsupported_simultaneous_notifs)
  if(SQL_MODE){ rebuild_connection() }
  
  ## convert notifications to address changes ----
  
  run_time_inform_user("---- convert to address changes ----")
  
  out = partition_notifications_to_address_changes(address_notifications)
  address_notifications = out$keep
  not_address_changes = out$discard
  
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_CHANGES,
                        names(refined_notif_columns), address_notifications)
  
  append_discarded_notifs_to_archive(not_address_changes)
  run_time_inform_user("complete")
  
  #### end of subsetting loop ----
  
  purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")
  
  if(!DEVELOPMENT_MODE)
    run_time_inform_user(paste0("-------- modulo ",jj," complete --------"))
}

## completion ----

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")
if(!DEVELOPMENT_MODE){
  compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_REPLACED)
  compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_REFINED)
  compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_CHANGES)
}
close_database_connection(db_con_IDI_sandpit)

run_time_inform_user("-------- GRAND COMPLETION --------")

