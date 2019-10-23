##################################################################################################################
#' Description: Functions that support producing the best household level address change records
#'
#' Input:
#'
#' Output: Functions to support analysis
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: utility_functions for dbplyr
#' 
#' Notes:
#' Uses dbplyr to translate R to SQL
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2019-09-26 SA clean to minimal production code
#' 2019-07-06 SA v1
#' 2018-11-13 SA v0
#'#################################################################################################################

#' Cautious model for group address analysis
#' 
#' Used when there are stability concerns, such as the database
#' connection timing-out.
#' 
cautious_reconnect <- function(address_notifications, purge = FALSE, exclude = c()){
  # get current table name
  current_table = address_notifications$ops$x %>% as.character() %>% strsplit("\\.")
  current_table = current_table[[1]][3]
  
  # renew connection
  dbDisconnect(db_con_IDI_sandpit)
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  # reconnect to table
  address_notifications = create_access_point(db_con_IDI_sandpit, our_schema, current_table)
  # assign into the parent environment
  assign("db_con_IDI_sandpit", db_con_IDI_sandpit, envir = parent.frame())
  assign("address_notifications", address_notifications, envir = parent.frame())
  
  if(purge){
    purge_tables_by_prefix(db_con_IDI_sandpit, our_schema, "chh_tmp", exclude = c(current_table, exclude))
    run_time_inform_user("- temporary tables removed")
  }
  
}

#' Produce a summary table identifying which addresses contain multiple dwellings
#'
#' Removed as minimal gain from distinguishing between multi-dwelling and uni-dwelling addresses.

#' Create address change notifications by multi-dwelling address status
#' 
#' Each row gives the [snz_uid] of a person who moved from [prev_address_uid]
#' to [address_uid] on [notification_date] as recorded by [source] and [validation].
#' 
#' Removed as minimal gain from distinguishing between multi-dwelling and uni-dwelling addresses.

#' Identify records where two people make a related move but on different dates.
#' Construct a composite notification from which deletion and addition to correct for
#' out-of-sync-ness can take place
#'
#' Each row gives the [snz_uid] of a person who moved to [address_uid] on [notification_date]
#' as reported by [source] and [validation], but for whom [notification_date_updated] would
#' be a better choice of date.
#'
out_of_sync_records = function(address_notifications, from_match, to_match, DAYS_GAP){
  # checks
  assert(is.logical(from_match), "from match must be TRUE or FALSE")
  assert(is.logical(to_match), "to match must be TRUE or FALSE")
  assert(is.numeric(DAYS_GAP), "days gap must be numeric")
  assert(from_match | to_match, "no reason to standardise without a match on from or to address")
  
  # add previous address
  full_address_moves = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(prev_address_uid = lag(address_uid, 1, order_by = "notification_date"),
           prev_notification_date = lag(notification_date, 1, order_by = "notification_date")) %>%
    filter(!is.na(prev_address_uid))
  # exclude records without previous address for clarity, else they are invisibly removed below

  # columns to join by
  join_column = c()
  if(to_match){ join_column = c(join_column, "address_uid") }
  if(from_match){ join_column = c(join_column, "prev_address_uid") }
  
  # pairs of addresses where refinement is possible
  address_pairs = full_address_moves %>%
    inner_join(full_address_moves, by = join_column, suffix = c("_x","_y")) %>%
    filter(snz_uid_x < snz_uid_y, # not the same person, count every pair at most once
           notification_date_x != notification_date_y, # no need to update if dates already identical
           prev_notification_date_x < notification_date_y, # no intervening notifications
           prev_notification_date_y < notification_date_x) %>%
    mutate(date_sync = DATEDIFF(day, notification_date_x, notification_date_y)) %>%
    mutate(date_sync = abs(date_sync)) %>% # days difference between move
    filter(date_sync <= DAYS_GAP)
  
  # focus on records to change
  records_for_change = address_pairs %>%
    mutate(x_first = ifelse(notification_date_x < notification_date_y, 1, 0)) %>%
    mutate(snz_uid = ifelse(x_first == 1, snz_uid_y, snz_uid_x),
           source = ifelse(x_first == 1, source_y, source_x),
           validation = ifelse(x_first == 1, validation_y, validation_x),
           notification_date = ifelse(x_first == 1, notification_date_y, notification_date_x),
           notification_date_updated = ifelse(x_first == 1, notification_date_x, notification_date_y))
  # address_uid will have suffix {_x, _y} if it was not used for matching
  if(!to_match)
    records_for_change = records_for_change %>%
    mutate(address_uid = ifelse(x_first == 1, address_uid_y, address_uid_x))
  # select
  records_for_change = records_for_change %>%
    select(snz_uid, address_uid, notification_date, source, validation, notification_date_updated)
  
  return(records_for_change)
}

#' Compare across individuals who share an addresses and ensure consistent move in/out dates
#' 
#' Identify address change notifications that should be updated as part of ensuring
#' move in/out dates are consistent across household members.
#' (Not intended to be called directly)
#' 
consistent_address_change_dates = function(records_for_change){
  
  # output tables
  original_records_for_discard = records_for_change %>%
    select(snz_uid, address_uid, notification_date, source, validation) %>%
    distinct() %>%
    mutate(replaced = "standardise move in date")
  
  new_records_for_addition = records_for_change %>%
    group_by(snz_uid, address_uid, notification_date, source, validation) %>%
    summarise(new_notification_date = min(notification_date_updated, na.rm = TRUE)) %>%
    mutate(source = "standardise move in date",
           validation = "NO") %>%
    ungroup() %>%
    select(snz_uid, address_uid, source, validation, notification_date = new_notification_date)
  
  # check equal number of notifications added & removed
  require_equal_length(original_records_for_discard, new_records_for_addition)
  
  return(list(add = new_records_for_addition, discard = original_records_for_discard))
}

#' De-duplicated birth records
#' 
#' Each row gives [snz_uid] born to [parent1_snz_uid] and [parent2_snz_uid]
#' on [birth_date]
#' 
obtain_birth_record = function(birth_records){
  # obtain birth records
  birth_records = birth_records %>%
    filter(!is.na(snz_uid),
           !is.na(dia_bir_birth_month_nbr),
           !is.na(dia_bir_birth_year_nbr),
           !is.na(parent1_snz_uid),
           !is.na(parent2_snz_uid),
           dia_bir_birth_year_nbr >= 1982) %>%
    mutate(birth_date = DATEFROMPARTS(dia_bir_birth_year_nbr, dia_bir_birth_month_nbr, 15)) %>%
    select("snz_uid", "birth_date", "parent1_snz_uid", "parent2_snz_uid") %>%
    distinct() # required as a small number of people have duplicate birth records
}

#' Synchronise dependent children addresses with their parents
#'
sync_dependent_children = function(address_notifications, birth_records, DEPENDENT_AGE){
  
  # notifications with notification details about the same date
  address_about_notifications = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(next_notification_date = lead(notification_date, 1, order_by = "notification_date"),
           next_address_uid = lead(address_uid, 1, order_by = "notification_date"),
           prev_notification_date = lag(notification_date, 1, order_by = "notification_date"),
           prev_address_uid = lag(address_uid, 1, order_by = "notification_date"))
  
  # child's notifications
  child_notifs = address_about_notifications %>%
    inner_join(birth_records, by = "snz_uid") %>%
    mutate(age_at_notif = DATEDIFF(year, birth_date, notification_date)) %>%
    filter(1 <= age_at_notif,
           age_at_notif <= DEPENDENT_AGE)
  
  # add parent notifications
  child_parent_notifs = child_notifs %>%
    inner_join(address_about_notifications, by = c("parent1_snz_uid" = "snz_uid"), suffix = c("","_p1")) %>%
    inner_join(address_about_notifications, by = c("parent2_snz_uid" = "snz_uid"), suffix = c("","_p2")) %>%
    filter(address_uid_p1 == address_uid_p2, # parents share the same address
           notification_date_p1 < next_notification_date_p2,
           notification_date_p2 < next_notification_date_p1, # parents are at the address at the same time
           (prev_address_uid == prev_address_uid_p1
            | prev_address_uid == prev_address_uid_p2), # child was at same address as at least one parent
           address_uid != address_uid_p1, # child notifies to a different address
           address_uid == next_address_uid_p1,
           address_uid == next_address_uid_p2, # child notifies to both parents' next addresses
           prev_notification_date < notification_date_p1,
           prev_notification_date < notification_date_p2, # parents notif is in gap where child is missing notif
           notification_date_p1 < notification_date,
           notification_date_p2 < notification_date) # parents notif is in gap where child is missing notif
  
  # form into new child notification
  new_child_notifs = child_parent_notifs %>%
    mutate(new_address_uid = address_uid_p1,
           new_notification_date = ifelse(notification_date_p1 < notification_date_p2,
                                          notification_date_p2, notification_date_p1),
           source = "sync child to parents",
           validation = "NO") %>%
    select(snz_uid, source, validation,
           address_uid = new_address_uid,
           notification_date = new_notification_date)
  
  # output
  return(new_child_notifs)
}

#' Ensure that move out dates for a address occur before move in dates
#' Necessarily limited to move outs that are within "AVOIDED OVERLAP" days of move ins
#' 
#' More likely that people report change of address late than that it gets reported early.
#' Hence in an overlap, we shift the move out date backwards, not the move in date forwards.
#'
#' Each row gives [snz_uid] who moved out of [address_uid] on [notification_date_old]
#' as repored by [source] and [validation]
#' would be better recorded as moving out on [notification_Date_new]
#'
get_inconsistent_hand_overs = function(address_notifications, AVOIDED_OVERLAP, MINIMUM_TENANCY){

  # get previous addresses
  full_address_moves = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(prev_address_uid = lag(address_uid, 1, order_by = "notification_date"),
           prev_notification_date = lag(notification_date, 1, order_by = "notification_date"))
  # join
  linked_addresses = full_address_moves %>%
    inner_join(full_address_moves, by = c("prev_address_uid" = "address_uid"), suffix = c("_out","_in")) %>%
    mutate(days_diff = DATEDIFF(DAY, notification_date_out, notification_date_in),
           days_stay_out = DATEDIFF(DAY, prev_notification_date_out, notification_date_out),
           days_stay_in = DATEDIFF(DAY, prev_notification_date_in, notification_date_in)) %>%
    filter(snz_uid_out != snz_uid_in, # not the same person
           notification_date_in <= notification_date_out, # move in happens before move out
           prev_notification_date_out < notification_date_in, # exclude people moving out moved twice
           ABS(days_diff) <= AVOIDED_OVERLAP, # move out & in are close together
           days_stay_out > MINIMUM_TENANCY, # move out must be from a long term stay
           days_stay_in > MINIMUM_TENANCY, # move in must be for a long term stay
           validation_out == "NO")
  
  # focus on records to change
  records_for_change = linked_addresses %>%
    mutate(notification_date_new = DATEADD(DAY, -1, notification_date_in)) %>%
    ungroup() %>%
    select(snz_uid = snz_uid_out,
           address_uid,
           notification_date_old = notification_date_out,
           notification_date_new,
           source = source_out) %>%
    mutate(validation = "NO")
  
  return(records_for_change)
}

#' Resolve inconsistent handovers in address change.
#'
ensure_consistent_hand_overs = function(records_for_change){

  # prepare output
  late_notifs_for_discard = records_to_change %>%
    select(snz_uid, address_uid, notification_date = notification_date_old,
           source, validation) %>%
    distinct() %>%
    mutate(replaced = "consistent hand overs")
  
  earlier_notifs_to_add = records_to_change %>%
    group_by(snz_uid, address_uid, notification_date_old, source, validation) %>%
    summarise(notification_date = min(notification_date_new, na.rm = TRUE)) %>%
    mutate(source = "consistent hand overs") %>%
    select(snz_uid, address_uid, source, validation, notification_date)
  
  # check equal number of notifications
  require_equal_length(late_notifs_for_discard, earlier_notifs_to_add)
  
  return(list(discard = late_notifs_for_discard,
              add = earlier_notifs_to_add))
}

#' Check two tables have equal numbers of records
#' 
require_equal_length = function(table_A, table_B){
  num_A = table_A %>%
    ungroup() %>%
    summarise(num = n()) %>%
    collect() %>%
    unlist(use.names = FALSE)
  
  num_B = table_B %>%
    ungroup() %>%
    summarise(num = n()) %>%
    collect() %>%
    unlist(use.names = FALSE)
  
  assert(num_A == num_B, "different number of records in tables that should match")
}

#' At end of non-residential spell, add new notification for
#' resumption of New Zealand address.
#'
#' Removed as non-residential spells did not improve accuracy

#' At start of non-residential spell, add notifications for end
#' of residence at New Zealand address.
#'
#' Removed as non-residential spells did not improve accuracy

#' Notifications during non-residence spells for removal
#'
#' Removed as non-residential spells did not improve accuracy.
