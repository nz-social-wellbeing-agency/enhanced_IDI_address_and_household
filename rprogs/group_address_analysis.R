##################################################################################################################
#' Description: Produce best individual level address change records
#'
#' Input:
#' sandpit data table with refined individual level address notifications
#' required columns names = c("snz_uid", "notification_date", "address_uid", "source", "concurrent_flag")
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
#' Last recorded runtime = 3 min in development mode, 90 minutes in full
#' 
#' Issues:
#' When validating output produced with "DEVELOPMENT_MODE = TRUE" accuracy will be much lower than when validating
#' output produced with "DEVELOPMENT_MODE = FALSE". This is because people who change TAs will not have address
#' notifications outside the TA.
#' 
#' History (reverse order):
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
# tuning parameters
MAX_DAYS_GAP_MOVE_TO = 30
MAX_DAYS_GAP_MOVE_FROM = 20
MAX_DAYS_GAP_MOVE_TO_AND_FROM = 60
MULTI_DWELLING_MINUS_DAYS = 5
CONCURRENCY_MINUS_DAYS = 5
DEPENDENT_AGE = 15
SYNC_CHILDREN_CASCADE_LIMIT = 3
AVOIDED_OVERLAP = 23
MIN_NONRESIDENT_SPELL = 350

## setup ----
db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_view = "[IDI_UserCode].[DL-MAA2016-15]"
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")
purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_hhld")

columns_for_replacement_table = c("snz_uid", "notification_date", "address_uid", "source", "validation", "replaced")
columns_for_notification_table = c("snz_uid", "notification_date", "address_uid", "source", "validation",
                                   "concurrent_flag")

## load required tables ----
run_time_inform_user("building table connections")

address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                            schema =  our_schema,
                                            tbl_name = "chh_address_changes")

household_records = create_access_point(db_connection = db_con_IDI_sandpit,
                                        schema = our_schema,
                                        tbl_name = "chh_household_validation")

if(DEVELOPMENT_MODE){
  ta_address = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20181020].[data]", "address_notification")
  ta_address = ta_address %>%
    filter(ant_ta_code == '019') %>%
    select(address_uid = snz_idi_address_register_uid) %>%
    distinct()
  
  address_notifications = address_notifications %>% semi_join(ta_address, by = "address_uid")
  household_records = household_records %>% semi_join(ta_address, by = "address_uid")
}

## Identify multi-dwelling addresses ----

run_time_inform_user("multi-dwelling addresses")
out = identify_multi_dwelling_addresses(db_con_IDI_sandpit, household_records)
multi_dwelling = out

## standardise people with nearby notifications for moving ----

run_time_inform_user("Standardise within household")

#### match from and to addresses ----
address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = TRUE,
                                                         concurrency = FALSE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO_AND_FROM)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = TRUE,
                                                         concurrency = TRUE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO_AND_FROM -
                                                           CONCURRENCY_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = TRUE,
                                                         concurrency = FALSE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO_AND_FROM -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = TRUE,
                                                         concurrency = TRUE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO_AND_FROM -
                                                           CONCURRENCY_MINUS_DAYS -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
  # purge
  purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp",
                         exclude = c(current_table, "chh_tmp_multi_dwelling"))
  run_time_inform_user("- Temporary tables removed")
}

run_time_inform_user("- Match from and to address complete")


#### match only from addresses ----
address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = FALSE,
                                                         concurrency = FALSE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_FROM)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = FALSE,
                                                         concurrency = TRUE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_FROM -
                                                           CONCURRENCY_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = FALSE,
                                                         concurrency = FALSE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_FROM -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = TRUE,
                                                         to_match = FALSE,
                                                         concurrency = TRUE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_FROM -
                                                           CONCURRENCY_MINUS_DAYS -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
  # purge
  purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp", 
                         exclude = c(current_table, "chh_tmp_multi_dwelling"))
  run_time_inform_user("- Temporary tables removed")
}

run_time_inform_user("- Match from address complete")

#### match only to addresses ----
address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = FALSE,
                                                         to_match = TRUE,
                                                         concurrency = FALSE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = FALSE,
                                                         to_match = TRUE,
                                                         concurrency = TRUE,
                                                         multi = FALSE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO -
                                                           CONCURRENCY_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = FALSE,
                                                         to_match = TRUE,
                                                         concurrency = FALSE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
}

address_notifications = consistent_address_change__dates(db_con_IDI_sandpit, address_notifications,
                                                         from_match = FALSE,
                                                         to_match = TRUE,
                                                         concurrency = TRUE,
                                                         multi = TRUE,
                                                         DAYS_GAP = MAX_DAYS_GAP_MOVE_TO -
                                                           CONCURRENCY_MINUS_DAYS -
                                                           MULTI_DWELLING_MINUS_DAYS)

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
  # purge
  purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp", 
                         exclude = c(current_table, "chh_tmp_multi_dwelling"))
  run_time_inform_user("- Temporary tables removed")
}

run_time_inform_user("- Match to address complete")

## sync children with parents ----

run_time_inform_user("Syncing parents and children")

for(ii in 1:SYNC_CHILDREN_CASCADE_LIMIT){
  new_child_notifs = sync_dependent_children(db_con_IDI_sandpit, address_notifications, DEPENDENT_AGE)

  tmp_tbl_name = paste0("chh_tmp_sync_child_",ii)
  new_table = union_all(address_notifications, new_child_notifs, columns_for_notification_table)

  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema, tmp_tbl_name,
                                          new_table, index_columns = "snz_uid")
  
  if(CAUTIOUS_MODE){
    current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
    current_table = current_table[[1]][3]
    # new connection
    db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
    address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
    # purge
    purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp", 
                           exclude = c(current_table, "chh_tmp_multi_dwelling"))
    run_time_inform_user("- Temporary tables removed")
  }
  
  run_time_inform_user(paste0("- Iteration ",ii," complete"))
}

## standardise address transitions ----

run_time_inform_user("Ensuring consistent handovers")

out = consistent_hand_overs(db_con_IDI_sandpit, address_notifications, AVOIDED_OVERLAP)
move_ins_for_discard = out$move_ins_for_discard
new_move_ins_to_add = out$new_move_ins_to_add

# store replaced notifications
append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                      columns_for_replacement_table, move_ins_for_discard)

# remove replaced and add updated notifications
address_notifications = address_notifications %>%
  anti_join(move_ins_for_discard, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
  union_all(new_move_ins_to_add, columns_for_notification_table)

# write for reuse
address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema, "chh_tmp_consistent_handovers",
                                        address_notifications, index_columns = "snz_uid")

if(CAUTIOUS_MODE){
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  # new connection
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
  # purge
  purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp", 
                         exclude = c(current_table, "chh_tmp_multi_dwelling"))
  run_time_inform_user("- Temporary tables removed")
}

## non-residence notifications ----

run_time_inform_user("Adding non-residence notifications infomration")

#### deaths ----

dia_deaths = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20181020].[dia_clean]", "deaths")
dia_deaths = dia_deaths %>%
  filter(!is.na(dia_dth_death_year_nbr),
         !is.na(dia_dth_death_month_nbr)) %>%
  mutate(notification_date = DATEFROMPARTS(dia_dth_death_year_nbr, dia_dth_death_month_nbr, 28),
         source = "dia_deaths",
         address_uid = as.integer(-1),
         concurrent_flag = 0,
         validation = "NO") %>%
  select(snz_uid, notification_date, address_uid, source, validation, concurrent_flag) %>%
  filter(notification_date >= '2001-01-01') %>%
  semi_join(address_notifications, by = "snz_uid")

#### overseas ----

overseas = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20181020].[data]", "person_overseas_spell")
overseas = overseas %>%
  filter(pos_day_span_nbr >= MIN_NONRESIDENT_SPELL,
         pos_first_arrival_ind == 'n',
         pos_applied_date >= '2001-01-01') %>%
  mutate(source = "overseas_spells",
         address_uid = as.integer(-1),
         concurrent_flag = 0,
         validation = "NO") %>%
  select(snz_uid, address_uid, concurrent_flag, validation, source, notification_date = pos_applied_date) %>%
  semi_join(address_notifications, by = "snz_uid")

#### corrections ----

prison = create_access_point(db_con_IDI_sandpit, "[IDI_Clean_20181020].[cor_clean]", "ov_major_mgmt_periods")
prison = prison %>%
  mutate(duration = DATEDIFF(DAY, cor_mmp_period_start_date, cor_mmp_period_end_date)) %>%
  filter(duration >= MIN_NONRESIDENT_SPELL,
         cor_mmp_mmc_code == 'PRISON',
         cor_mmp_period_start_date >= '2001-01-01') %>%
  mutate(source = "prison_spells",
         address_uid = as.integer(-1),
         concurrent_flag = 0,
         validation = "NO") %>%
  select(snz_uid, address_uid, concurrent_flag, validation, source, notification_date = cor_mmp_period_start_date)%>%
  semi_join(address_notifications, by = "snz_uid")
  
#### union all ----


address_notifications = address_notifications %>%
  union_all(dia_deaths, columns_for_notification_table) %>%
  union_all(overseas, columns_for_notification_table) %>%
  union_all(prison, columns_for_notification_table)

## completion ----

run_time_inform_user("Writing final table")
write_to_database(address_notifications, db_con_IDI_sandpit, our_schema, "chh_household_refined", OVERWRITE = TRUE)
run_time_inform_user("adding index")
create_clustered_index(db_con_IDI_sandpit, our_schema, "chh_household_refined", "snz_uid")

purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp")

run_time_inform_user("-------- GRAND COMPLETION --------")

