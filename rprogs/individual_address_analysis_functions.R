###############################################################################
#' Description: Functions that support producing the best individual level address change records
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
#' 2010-08-13 SA updated to 2019-04-20 refresh
#' 2019-06-21 SA v1
#' 2018-09-18 SA v0
#'#############################################################################

#' Rebuild connection and reconnect to tables
#' 
#' Used when there are stability concerns, such as the database
#' connection timing-out.
#' 
rebuild_connection <- function(){
  
  # output current working table
  cautious_table = paste0("chh_tmp_cautious_",floor(runif(1)*1E10))
  address_notifications = write_for_reuse(db_con_IDI_sandpit, our_schema,
                                          cautious_table, address_notifications, index_columns = "snz_uid")
  
  # recreate connection
  dbDisconnect(db_con_IDI_sandpit)
  db_con_IDI_sandpit = create_database_connection(database = "IDI_Sandpit")
  
  # reload current working table
  address_notifications = create_access_point(db_connection =  db_con_IDI_sandpit,
                                              schema =  our_schema,
                                              tbl_name = cautious_table)
  
  # assignment into the parent environment
  assign("db_con_IDI_sandpit", db_con_IDI_sandpit, envir = parent.frame())
  assign("address_notifications", address_notifications, envir = parent.frame())
}

#' Partition list of notifications by dates into
#' - list of notifications within dates
#' - list of notifications outside dates
#'
partition_by_dates = function(address_notifications, earliest_date, latest_date = NA){
  
  # latest date defaults to today
  if(is.na(latest_date))
    latest_date = as.character(Sys.Date())
  
  # notifications to exclude
  exclude_notifs = address_notifications %>%
    filter(notification_date < earliest_date
           | latest_date < notification_date) %>%
    mutate(replaced = 'notification outside window')
  
  # valid notifications
  in_date_notifs = address_notifications %>%
    filter(earliest_date <= notification_date & notification_date <= latest_date)
  
  # return
  return(list(keep = in_date_notifs,
              discard = exclude_notifs))
}

#' Obtain accuracy measures from notifications
#'
#' Used by data_prep_notifications
#' 
obtain_accuracy_measures = function(address_notifications, person_details, ACCURACY_WINDOW){
  
  # truth records to validate off
  true_notifs = address_notifications %>%
    filter(validation == "YES")
  
  # records to validate + age + month of notification
  notifs_to_validate = address_notifications %>%
    filter(validation == "NO") %>%
    inner_join(person_details, by = "snz_uid") %>%
    mutate(birth_date = DATEFROMPARTS(snz_birth_year_nbr, snz_birth_month_nbr, 15)) %>%
    mutate(age_at_notice = DATEDIFF(YEAR, birth_date, notification_date)) %>%
    # censor age > 90 to 90 and age < 1 to 1
    mutate(age_at_notice = ifelse(age_at_notice >= 90,90,age_at_notice)) %>%
    mutate(age_at_notice = ifelse(age_at_notice <= 1,1,age_at_notice))
  
  # number of matches
  accuracy_measures = true_notifs %>%
    inner_join(notifs_to_validate, by = "snz_uid", suffix = c("_true", "_to_validate")) %>%
    filter(notification_date_to_validate <= notification_date_true
           & DATEDIFF(DAY, notification_date_to_validate, notification_date_true) <= ACCURACY_WINDOW) %>%
    mutate(match = ifelse(address_uid_to_validate == address_uid_true,1,0)) %>%
    group_by(source_to_validate, validation_to_validate, high_quality_to_validate, age_at_notice) %>%
    summarise(num_match = sum(match, na.rm = TRUE),
              num = n()) %>%
    rename(source = source_to_validate, validation = validation_to_validate, high_quality = high_quality_to_validate)
  
  return(accuracy_measures)
}

#' Partition out unsupported simultaneous notifications
#'
partition_out_unsupported_simultaneous = function(address_notifications, DAYS_SUPPORT){
  
  # identify simultaneous notification dates
  simultaneous_notifications = address_notifications %>%
    group_by(snz_uid, notification_date) %>%
    summarise(share_date = n_distinct(address_uid))
  
  # identify supported notifications
  supported_notifications = address_notifications %>%
    inner_join(address_notifications, by = c("snz_uid", "address_uid"), suffix = c("","_support")) %>%
    filter(notification_date < notification_date_support
           & DATEDIFF(DAY, notification_date, notification_date_support) <= DAYS_SUPPORT) %>%
    group_by(snz_uid, notification_date, address_uid) %>%
    summarise(supports = n())
  
  # rejoin
  combined = address_notifications %>%
    left_join(simultaneous_notifications, by = c("snz_uid", "notification_date")) %>%
    left_join(supported_notifications, by = c("snz_uid", "address_uid", "notification_date")) %>%
    mutate(exclude = ifelse(share_date >= 2 & is.na(supports), 1, 0))
  
  unsupported_simultaneous_notifications = combined %>%
    filter(exclude == 1) %>%
    mutate(replaced = "unsupported simulataneous")
  
  all_other_notifications = combined %>%
    filter(exclude == 0)
  
  # return
  return(list(keep = all_other_notifications,
              discard = unsupported_simultaneous_notifications))
}

#' Add indicator for notification preceeded by high quality notification
#' 
preceeded_by_high_qulaity = function(address_notifications){
  # is notif preceeded by any high quality notifications
  address_notif_with_indicator = address_notifications %>%
    inner_join(address_notifications, by = "snz_uid", suffix = c("","_H")) %>%
    filter(notification_date_H <= notification_date) %>%
    group_by(snz_uid, address_uid, notification_date, source, validation, high_quality) %>%
    summarise(preceeding_high_qual = sum(high_quality_H, na.rm = TRUE)) %>%
    mutate(preceeding_high_qual = ifelse(preceeding_high_qual > 0, 1, 0))
}

#' Partition out low quality notifications
#'
partition_out_low_quality = function(address_notifications, DAYS_SPREAD){
  
  # low quality notifications
  low_qual = address_notifications %>%
    filter(high_quality == 0 & validation == "NO" & preceeding_high_qual == 1)
  # high quality notifications
  high_qual = address_notifications %>%
    filter(high_quality == 1 | validation == "YES" | preceeding_high_qual == 0)
  
  # spread high quality to nearby low quality
  spread = low_qual %>%
    inner_join(high_qual, by = c("snz_uid", "address_uid"), suffix = c("_L","_H")) %>%
    filter(notification_date_L < notification_date_H
           & DATEDIFF(DAY, notification_date_L, notification_date_H) <= DAYS_SPREAD) %>%
    select(snz_uid, address_uid, notification_date_L) %>%
    distinct() %>%
    mutate(spread_ind = 1)
  
  address_notifications = address_notifications %>%
    left_join(spread, by = c("snz_uid", "address_uid", "notification_date" = "notification_date_L")) %>%
    mutate(high_quality = ifelse(is.na(spread_ind), high_quality, spread_ind))
  
  # low quality notifications
  low_quality_notifs = address_notifications %>%
    filter(high_quality == 0 & validation == "NO" & preceeding_high_qual == 1) %>%
    select(snz_uid, notification_date, address_uid, source, validation) %>%
    mutate(replaced = "lower quality")
  
  # high quality notifications
  high_quality_notifs = address_notifications %>%
    filter(high_quality == 1 | validation == "YES" | preceeding_high_qual == 0)
  
  # return
  return(list(keep = high_quality_notifs,
              discard = low_quality_notifs))
}

#' Identify notifications within a gap at the same address
#' E.g. if an individiaul's address notifications were A, A, B, B, A, A
#' Then the notifications at address 'B' are in a gap between notifications of address A.
#'
obtain_notifications_within_a_gap = function(address_notifications, MAX_GAP_WIDTH){
  
  # get gaps that could contain concurrent addresses
  # only need to check one direction (forward)
  notifications_with_gap = address_notifications %>%
    group_by(snz_uid, address_uid) %>%
    mutate(next_date = lead(notification_date, 1, order_by = "notification_date")) %>%
    mutate(days_gap = DATEDIFF(DAY, notification_date, next_date)) %>%
    filter(0 <= days_gap & days_gap <= MAX_GAP_WIDTH) %>%
    ungroup()
  
  # get notifications within gaps
  # used <= instead of < to capture simultaneous notifs
  notifications_in_gap = address_notifications %>%
    inner_join(notifications_with_gap, by = "snz_uid", suffix = c("", "_G")) %>%
    filter(notification_date_G <= notification_date
           & notification_date <= next_date
           & address_uid != address_uid_G
           & validation != 'YES') %>%
    select(snz_uid, notification_date, address_uid, source, validation) %>%
    distinct()
  
  # return
  return(notifications_in_gap)
}

#' Partition out notifications that represent "echoes" of old addresses
#'
partition_out_echoes = function(address_notifications, notifications_in_gap, SUPPORT_WITHIN_DAYS){
  
  # determine whether notification has support
  unsupported = notifications_in_gap %>%
    left_join(address_notifications, by = c("snz_uid", "address_uid"), suffix = c("","_S")) %>%
    mutate(support = ifelse(!is.na(notification_date_S)
                            & notification_date < notification_date_S
                            & DATEDIFF(DAY, notification_date, notification_date_S) <= SUPPORT_WITHIN_DAYS
                            , 1, 0)) %>%
    group_by(snz_uid, address_uid, notification_date, source, validation) %>%
    summarise(has_support = sum(support, na.rm = TRUE)) %>%
    filter(has_support == 0) %>%
    mutate(replaced = "in gap unsupported")
  
  # notifications to keep
  notifications_without_echoes = address_notifications %>%
    anti_join(unsupported, by = c("snz_uid", "address_uid", "notification_date", "source"))
  
  # return
  return(list(keep = notifications_without_echoes,
              discard = unsupported))
}

#' Resolve any remaining simultaneous notifications
#' In our refined individual notification table we want there to be 
#' no simultaneous notifications unless they are concurrent
#' 
resolve_remaining_simultaneous = function(address_notifications, DAYS_SUPPORT){
  
  selected_simultaneous = address_notifications %>%
    group_by(snz_uid, notification_date) %>%
    summarise(num = n(), max_address = max(address_uid, na.rm = TRUE)) %>%
    filter(num > 1) %>%
    inner_join(address_notifications, by = c("snz_uid", "notification_date",
                                             "max_address" = "address_uid"), suffix = c("","_ref")) %>%
    group_by(snz_uid, notification_date, max_address) %>%
    summarise(max_source = max(source, na.rm = TRUE))
  
  rejected_simultaneous = address_notifications %>%
    inner_join(selected_simultaneous, by = c("snz_uid", "notification_date")) %>%
    filter(address_uid != max_address | source != max_source) %>%
    mutate(replaced = "resolve last simultaneous")
  
  all_other_notifs = address_notifications %>%
    anti_join(rejected_simultaneous, by = c("snz_uid", "notification_date", "address_uid", "source"))
  
  # return
  return(list(keep = all_other_notifs,
              discard = rejected_simultaneous))
}

#' Partition list of notifications into
#' - list of address changes
#' - list of notifications that don't reflect address changes
#'
partition_notifications_to_address_changes = function(address_notifications){
  
  # get address of most recent notification at the same address
  notifications_w_prev = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(lag_notification_date = lag(notification_date, 1, order_by = "notification_date"),
           lag_address_uid = lag(address_uid, 1, order_by = "notification_date"))
  
  # if address match then not address change
  not_address_changes = notifications_w_prev %>%
    ungroup() %>%
    filter(address_uid == lag_address_uid) %>%
    select(snz_uid, notification_date, address_uid, source, validation) %>%
    mutate(replaced = "not address change")
  
  # remainder are address changes
  address_changes = notifications_w_prev %>%
    ungroup() %>%
    filter(is.na(lag_notification_date)
           | address_uid != lag_address_uid) %>%
    select(snz_uid, notification_date, address_uid, source, validation)
  
  # combine all address changes
  column_names = c("snz_uid", "notification_date", "address_uid", "source", "validation")
  
  # return
  return(list(keep = address_changes,
              discard = not_address_changes))
}

