##################################################################################################################
#' Description: Produce best group level address change records
#'
#' Input:
#' sandpit data table with refined individual level address notifications
#' required columns names = c("snz_uid", "notification_date", "address_uid", "source")
#' sandpit data table with truth individual level address notifications
#' required column names = c("snz_uid", "notification_date", "address_uid", "household_uid", "source")
#' This is used to identify multi-dwelling addresses
#'
#' Output:
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies:
#' 
#' Notes:
#' Uses dbplyr to translate R to SQL
#' Last recorded runtime = 3 min in development mode, 50 minutes in full
#' 
#' Issues:
#' When validating output produced with "DEVELOPMENT_MODE = TRUE" accuracy will be much lower than when validating
#' output produced with "DEVELOPMENT_MODE = FALSE". This is because people who change TAs will not have address
#' notifications outside the TA.
#' 
#' History (reverse order):
#' 2019-09-26 SA clean to minimal production code
#' 2019-08-13 SA updated to 2019-04-20 refresh
#' 2019-07-16 SA v1
#' 2018-11-13 SA v0
#'#################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')
source('group_address_analysis_functions.R')

## parameters ----

# user settings
DEVELOPMENT_MODE = FALSE
CAUTIOUS_MODE = TRUE
IDI_CLEAN = "[IDI_Clean_20190420]"
# inputs and outputs
INPUT_ADDRESS_CHANGE_TABLE = "chh_address_changes"
OUTPUT_REPLACED = "chh_household_replaced_notifications"
OUTPUT_REFINED = "chh_household_refined"
INTERIM_BIRTH_RECORD = "chh_tmp_birth_record"
# tuning parameters
MOVE_TO = 30 # max days gap when 2 people are moving to same address (30 prev, 70 revised)
MOVE_FROM = 20 # max days gap when 2 people are moving from same address (20 prev, 60 revised)
MOVE_BOTH = 60 # max days gap when 2 people moving both from and to same address (60 prev. 90 revised)
DEPENDENT_AGE = 15 # max age for children to count as dependents (15 prev, 18 revised)
AVOIDED_OVERLAP = 23 # max gap between move-in and move-out of two different tenancies (23 prev, 30 revised)
MINIMUM_TENANCY = 180 # minimum length of two different tenancies for comparison (180 prev, 90 revised)

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_view = "[IDI_UserCode].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

columns_for_replacement_table = c("snz_uid", "notification_date", "address_uid", "source", "validation", "replaced")
columns_for_notification_table = c("snz_uid", "notification_date", "address_uid", "source", "validation")

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

## load required tables ----
run_time_inform_user("building table connections")

address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                            schema =  our_schema,
                                            tbl_name = INPUT_ADDRESS_CHANGE_TABLE)

if(DEVELOPMENT_MODE){
  ta_address = create_access_point(db_con_IDI_sandpit, paste0(IDI_CLEAN,".[data]"), "address_notification")
  ta_address = ta_address %>%
    filter(ant_ta_code == '019') %>%
    select(address_uid = snz_idi_address_register_uid) %>%
    distinct()
  
  address_notifications = address_notifications %>% semi_join(ta_address, by = "address_uid")
}

## standardise people with nearby notifications for moving ----

run_time_inform_user("Standardise within household")

# controls
from_match   = c(TRUE     , FALSE  , TRUE     )
to_match     = c(TRUE     , TRUE   , FALSE    )
max_days_gap = c(MOVE_BOTH, MOVE_TO, MOVE_FROM)

for(ii in 1:length(from_match)){
  # pair up old and new dates for notifications requiring update
  recored_for_change = out_of_sync_records(address_notifications, from_match[ii], to_match[ii], max_days_gap[ii])
  recored_for_change = write_for_reuse(db_con_IDI_sandpit, our_schema, tbl_name = "chh_tmp_recored_for_change",
                                      tbl_to_save = recored_for_change, index_columns = "snz_uid")
  
  # new and old notifications correcting out-of-sync-ness
  out = consistent_address_change_dates(recored_for_change)
  original_records_for_discard = out$discard
  new_records_for_addition = out$add
  
  # store replaced notifications
  append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_REPLACED,
                        columns_for_replacement_table, original_records_for_discard)
  
  # remove replaced and add updated notifications
  address_notifications = address_notifications %>%
    anti_join(original_records_for_discard, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
    union_all(new_records_for_addition, columns_for_notification_table)
  
  # write for reuse
  temp_table = sprintf("chh_tmp_consistent_moves_%d",ii)
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          tbl_name = temp_table,
                                          address_notifications, index_columns = "snz_uid")
  
  if(CAUTIOUS_MODE){ cautious_reconnect(address_notifications, purge = TRUE) }
  run_time_inform_user(sprintf("- iteration %d complete",ii))
}

## sync children with parents ----

run_time_inform_user("Syncing parents and children")

birth_records = create_access_point(db_con_IDI_sandpit, paste0(IDI_CLEAN,".[dia_clean]"), "[births]")
birth_records = obtain_birth_record(birth_records)
birth_records = write_for_reuse(db_con_IDI_sandpit, our_schema, INTERIM_BIRTH_RECORD,
                                birth_records, index_columns = "snz_uid")

new_child_notifs = sync_dependent_children(address_notifications, birth_records, DEPENDENT_AGE)

new_table = union_all(address_notifications, new_child_notifs, columns_for_notification_table)
address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_tmp_sync_child",
                                        new_table, index_columns = "snz_uid")

if(CAUTIOUS_MODE){ cautious_reconnect(address_notifications, purge = TRUE) }

## standardise address transitions ----

run_time_inform_user("Ensuring consistent handovers")

records_for_change = get_inconsistent_hand_overs(address_notifications, AVOIDED_OVERLAP, MINIMUM_TENANCY)
records_to_change = write_for_reuse(db_con_IDI_sandpit, our_schema, tbl_name = "chh_tmp_inconsistent_hand_overs",
                                    tbl_to_save = records_for_change, index_columns = "snz_uid")

out = ensure_consistent_hand_overs(records_for_change)
move_ins_for_discard = out$discard
new_move_ins_to_add = out$add

# store replaced notifications
append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_REPLACED,
                      columns_for_replacement_table, move_ins_for_discard)

# remove replaced and add updated notifications
address_notifications = address_notifications %>%
  anti_join(move_ins_for_discard, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
  union_all(new_move_ins_to_add, columns_for_notification_table)

# write for reuse
address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_tmp_consistent_handovers",
                                        address_notifications, index_columns = "snz_uid")

if(CAUTIOUS_MODE){ cautious_reconnect(address_notifications, purge = TRUE) }

## non-residence notifications ----
#
# Death, extended time overseas, and extended time in prison
# were used as indicators of non-residential spells.
#
# However their inclusion failed to improve accuracy,
# Hence they were removed.
#
# The original process was:
# 1) at start of death/overseas/prison add a new notification with address -1
# 2) at end of overseas/prison add new notification with best guess of address
# 3) between start and end (or after start for death) remove all notifications

## completion ----

run_time_inform_user("Writing final table")
write_to_database(address_notifications, db_con_IDI_sandpit, our_schema, OUTPUT_REFINED, OVERWRITE = TRUE)
create_clustered_index(db_con_IDI_sandpit, our_schema, OUTPUT_REFINED, "snz_uid")
compress_table(db_con_IDI_sandpit, our_schema, OUTPUT_REFINED)

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")
dbDisconnect(db_con_IDI_sandpit)
run_time_inform_user("-------- GRAND COMPLETION --------")

