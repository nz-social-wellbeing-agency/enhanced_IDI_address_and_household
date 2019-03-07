##################################################################################################################
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
#' 2018-09-18 SA v0
#'#################################################################################################################

#' Partition list of notifications by dates into
#' - list of notifications within dates
#' - list of notifications outside dates
#'
partition_by_dates = function(address_notifications, earliest_date, latest_date = NA){
  
  # latest date defaults to today
  if(is.na(latest_date))
    latest_date = as.character(Sys.Date())
  
  # prior notifications to exclude
  prior_notifs = address_notifications %>%
    filter(notification_date < earliest_date) %>%
    mutate(replaced = paste0('notification is prior to ',earliest_date))
  
  # post notifications to exclude
  post_notifs = address_notifications %>%
    filter(latest_date < notification_date) %>%
    mutate(replaced = paste0('notification is post to ',latest_date))
  
  # all notifications to exclude
  column_names = c("snz_uid", "notification_date", "address_uid", "source", "validation", "replaced")
  
  out_date_notifs = prior_notifs %>%
    union_all(post_notifs, column_names)
  
  # valid notifications
  in_date_notifs = address_notifications %>%
    filter(earliest_date <= notification_date & notification_date <= latest_date)

  # return
  return(list(notifs_in_date_window = in_date_notifs,
              notifs_out_of_date_window = out_date_notifs))
}

#' Assign accuracy measures to notifications
#'
add_accuracy_measures = function(db_connection, address_notifications, person_details){
  
  # need birth date for each person
  person_details = person_details %>%
    mutate(birth_date = DATEFROMPARTS(snz_birth_year_nbr, snz_birth_month_nbr, 15)) %>%
    select("snz_uid", "birth_date")
  
  # truth records to validate off
  true_notifs = address_notifications %>%
    filter(validation == "YES")
  
  # records to validate + age
  notifs_to_validate = address_notifications %>%
    filter(validation == "NO") %>%
    inner_join(person_details, by = "snz_uid") %>%
    mutate(age_at_notice = DATEDIFF(YEAR, birth_date, notification_date)) %>%
    mutate(censored_age_step = ifelse(age_at_notice >= 90,90,age_at_notice)) %>%
    mutate(censored_age_at_notice = ifelse(censored_age_step <= 1,1,censored_age_step))
  
  # comparison + age
  comparison = true_notifs %>%
    inner_join(notifs_to_validate, by = "snz_uid", suffix = c("_true", "_to_validate")) %>%
    filter(notification_date_to_validate <= notification_date_true)
  
  comparison = write_for_reuse(db_connection, 
                               schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                               "chh_tmp_add_accuracy",
                               comparison,
                               index_columns = "snz_uid")
  
  # keep records closest to validation date
  greatest_date = comparison %>%
    group_by(snz_uid, notification_date_true, address_uid_true, source_true) %>%
    summarise(max_date = max(notification_date_to_validate, na.rm = TRUE)) %>%
    ungroup()
  
  comparison = comparison %>%
    inner_join(greatest_date,
               by = c("snz_uid",
                      "notification_date_true",
                      "address_uid_true",
                      "source_true",
                      "notification_date_to_validate" = "max_date"))
  
  # count matches and records
  matches_and_count = comparison %>%
    mutate(match = ifelse(address_uid_to_validate == address_uid_true,1,0)) %>%
    group_by(source_to_validate, censored_age_at_notice) %>%
    summarise(num_match = sum(match, na.rm = TRUE),
              num = n())
  
  matches_and_count = write_for_reuse(db_connection, 
                                      schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                      "chh_tmp_accuracy_matches_and_count",
                                      matches_and_count)
  
  accuracy_measures_detailed = matches_and_count %>%
    mutate(accuracy_ratio_detailed = 1.0 * num_match / num)
  
  accuracy_measures_coarse = matches_and_count %>%
    ungroup() %>%
    group_by(source_to_validate) %>%
    summarise(accuracy_ratio_coarse = 1.0 * sum(num_match, na.rm = TRUE) / sum(num, na.rm = TRUE))

  # add accuracy measures
  notifs_to_validate_with_accuracy = notifs_to_validate %>%
    left_join(accuracy_measures_detailed,
              by = c("source" = "source_to_validate", "censored_age_at_notice")) %>%
    left_join(accuracy_measures_coarse,
              by = c("source" = "source_to_validate")) %>%
    mutate(accuracy = ifelse(is.na(accuracy_ratio_detailed),
                             accuracy_ratio_coarse,
                             accuracy_ratio_detailed)) %>%
    select("snz_uid", "notification_date", "address_uid", "source", "validation", "accuracy")
  
  true_notifs_with_accuracy = true_notifs %>%
    mutate(accuracy = 1) %>%
    select("snz_uid", "notification_date", "address_uid", "source", "validation", "accuracy")
  
  # combine
  column_names = c("snz_uid", "notification_date", "address_uid", "source", "validation", "accuracy")
  
  all_notifs_with_accuracy = notifs_to_validate_with_accuracy %>%
    union_all(true_notifs_with_accuracy, column_names)
  
  return(list(all_notifs_with_accuracy = all_notifs_with_accuracy))
}

#' Partition out unsupported simultaneous notifications
#'
partition_out_unsupported_simultaneous = function(db_connection, address_notifications){
  
  # identify simultaneous notifications
  simultaneous_notifications = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(lag_date = lag(notification_date, 1, order_by = "notification_date"),
           lead_date = lead(notification_date, 1, order_by = "notification_date")) %>%
    filter(notification_date == lag_date | notification_date == lead_date)
  
  # identify simultaneous that are unsupported
  supported_simultaneous_notifications = simultaneous_notifications %>%
    left_join(address_notifications, by = c("snz_uid", "address_uid"), suffix = c("","_support")) %>%
    filter(notification_date < notification_date_support)
  
  # unsupported simultaneous notifications
  unsupported_simultaneous_notifications = simultaneous_notifications %>%
    anti_join(supported_simultaneous_notifications, by = c("snz_uid", "notification_date", "address_uid")) %>%
    mutate(replaced = "unsupported simultaneous")
  
  unsupported_simultaneous_notifications = write_for_reuse(db_connection, 
                                                           schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                                           "chh_tmp_unsupported_simultaneous_notifications",
                                                           unsupported_simultaneous_notifications,
                                                           index_columns = c("snz_uid", "notification_date"))
  
  # all other notifications
  all_other_notifications = address_notifications %>%
    anti_join(unsupported_simultaneous_notifications, by = c("snz_uid", "notification_date", "address_uid"))
  
  # return
  return(list(all_other_notifications = all_other_notifications,
              unsupported_simultaneous_notifications = unsupported_simultaneous_notifications))
}

#' Partition out low quality notifications
#'
partition_out_low_quality = function(db_connection, address_notifications, person_details,
                                     QUALITY_THRESHOLD, YA_QUALITY_THRESHOLD,
                                     YA_AGE_START, YA_AGE_END, CASCADE_LIMIT){
  
  # birth date
  person_details = person_details %>%
    mutate(birth_date = DATEFROMPARTS(snz_birth_year_nbr, snz_birth_month_nbr, 15)) %>%
    select("snz_uid", "birth_date")
  
  # initial indicator of high quality
  address_notifications = address_notifications %>%
    left_join(person_details, by = "snz_uid") %>%
    mutate(age_at_notice = DATEDIFF(YEAR, birth_date, notification_date)) %>%
    mutate(high_quality = ifelse(!is.na(age_at_notice)
                                 & accuracy >= YA_QUALITY_THRESHOLD
                                 & YA_AGE_START <= age_at_notice
                                 & age_at_notice <= YA_AGE_END, 1, 0)) %>%
    mutate(high_quality = ifelse(high_quality == 0
                                 & accuracy >= QUALITY_THRESHOLD, 1, high_quality))
  
  address_notifications = write_for_reuse(db_connection, 
                                          schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                          "chh_tmp_quality_iteration_0",
                                          address_notifications,
                                          index_columns = c("snz_uid", "notification_date"))
  # allow for cascades
  for(ii in 1:CASCADE_LIMIT){
    
    # spread indicators one step
    intermediate_results = spread_high_quality_indicators_to_adjacent(address_notifications)
    
    # save step
    intermediate_table_name = paste0("chh_tmp_quality_iteration_",ii)
    
    address_notifications = write_for_reuse(db_connection, 
                                            schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                            intermediate_table_name,
                                            intermediate_results,
                                            index_columns = c("snz_uid", "notification_date"))
  }
  
  # low quality notifications
  low_quality_notifs = address_notifications %>%
    filter(high_quality == 0 & validation == "NO") %>%
    select(snz_uid, notification_date, address_uid, source, validation) %>%
    mutate(replaced = "lower quality")
  
  # high quality notifications
  high_quality_notifs = address_notifications %>%
    filter(high_quality == 1 | validation == "YES")
  
  # return
  return(list(high_quality_notifs = high_quality_notifs,
              low_quality_notifs = low_quality_notifs))
}

#' Spread high quality status from high quality notifications
#' to adjacent low quality notifications
#' 
spread_high_quality_indicators_to_adjacent = function(address_notifications){
  
  input_columns = colnames(address_notifications)
  
  # We use 5 lags and 5 leads as simultaneous notifications can occur, AND
  # Almost all instances of simultaneous notifications consist of at most three concurrent notifications.
  # Hence 5 lag/lead allows for two adjacent dates, each with three simultaneous notifications.
  address_notifications = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(lag1_date = lag(notification_date, 1, order_by = "notification_date"),
           lag2_date = lag(notification_date, 2, order_by = "notification_date"),
           lag3_date = lag(notification_date, 3, order_by = "notification_date"),
           lag4_date = lag(notification_date, 4, order_by = "notification_date"),
           lag5_date = lag(notification_date, 5, order_by = "notification_date"),
           lag1_address = lag(address_uid, 1, order_by = "notification_date"),
           lag2_address = lag(address_uid, 2, order_by = "notification_date"),
           lag3_address = lag(address_uid, 3, order_by = "notification_date"),
           lag4_address = lag(address_uid, 4, order_by = "notification_date"),
           lag5_address = lag(address_uid, 5, order_by = "notification_date"),
           lag1_quality = lag(high_quality, 1, order_by = "notification_date"),
           lag2_quality = lag(high_quality, 2, order_by = "notification_date"),
           lag3_quality = lag(high_quality, 3, order_by = "notification_date"),
           lag4_quality = lag(high_quality, 4, order_by = "notification_date"),
           lag5_quality = lag(high_quality, 5, order_by = "notification_date"),
           
           lead1_date = lead(notification_date, 1, order_by = "notification_date"),
           lead2_date = lead(notification_date, 2, order_by = "notification_date"),
           lead3_date = lead(notification_date, 3, order_by = "notification_date"),
           lead4_date = lead(notification_date, 4, order_by = "notification_date"),
           lead5_date = lead(notification_date, 5, order_by = "notification_date"),
           lead1_address = lead(address_uid, 1, order_by = "notification_date"),
           lead2_address = lead(address_uid, 2, order_by = "notification_date"),
           lead3_address = lead(address_uid, 3, order_by = "notification_date"),
           lead4_address = lead(address_uid, 4, order_by = "notification_date"),
           lead5_address = lead(address_uid, 5, order_by = "notification_date"),
           lead1_quality = lead(high_quality, 1, order_by = "notification_date"),
           lead2_quality = lead(high_quality, 2, order_by = "notification_date"),
           lead3_quality = lead(high_quality, 3, order_by = "notification_date"),
           lead4_quality = lead(high_quality, 4, order_by = "notification_date"),
           lead5_quality = lead(high_quality, 5, order_by = "notification_date")
    )
  
  # back looking
  address_notifications = address_notifications %>%
    mutate(high_quality = ifelse(lag1_address == address_uid
                                 & lag1_quality == 1, 1, high_quality)) %>%
    mutate(high_quality = ifelse(lag2_address == address_uid 
                                 & (lag1_date == lag2_date 
                                    | notification_date == lag1_date)
                                 & lag2_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lag3_address == address_uid 
                                 & (lag1_date == lag3_date  
                                    | (notification_date == lag1_date & lag2_date == lag3_date) 
                                    | notification_date == lag2_date)
                                 & lag3_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lag4_address == address_uid 
                                 & (lag1_date == lag4_date  
                                    | (notification_date == lag1_date & lag2_date == lag4_date) 
                                    | (notification_date == lag2_date & lag3_date == lag4_date) 
                                    | notification_date == lag3_date)
                                 & lag4_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lag5_address == address_uid 
                                 & (lag1_date == lag5_date  
                                    | (notification_date == lag1_date & lag2_date == lag5_date) 
                                    | (notification_date == lag2_date & lag3_date == lag5_date) 
                                    | (notification_date == lag3_date & lag4_date == lag5_date) 
                                    | notification_date == lag4_date)
                                 & lag5_quality == 1
                                 , 1, high_quality))
  
  # forward looking
  address_notifications = address_notifications %>%
    mutate(high_quality = ifelse(lead1_address == address_uid 
                                 & lead1_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lead2_address == address_uid 
                                 & (lead1_date == lead2_date 
                                    | notification_date == lead1_date)
                                 & lead2_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lead3_address == address_uid
                                 & (lead1_date == lead3_date  
                                    | (notification_date == lead1_date & lead2_date == lead3_date) 
                                    | notification_date == lead2_date)
                                 & lead3_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lead4_address == address_uid 
                                 & (lead1_date == lead4_date  
                                    | (notification_date == lead1_date & lead2_date == lead4_date) 
                                    | (notification_date == lead2_date & lead3_date == lead4_date) 
                                    | notification_date == lead3_date)
                                 & lead4_quality == 1
                                 , 1, high_quality)) %>%
    mutate(high_quality = ifelse(lead5_address == address_uid 
                                 & (lead1_date == lead5_date  
                                    | (notification_date == lead1_date & lead2_date == lead5_date) 
                                    | (notification_date == lead2_date & lead3_date == lead5_date) 
                                    | (notification_date == lead3_date & lead4_date == lead5_date) 
                                    | notification_date == lead4_date)
                                 & lead5_quality == 1
                                 , 1, high_quality))
  
  # return
  return(address_notifications %>% select(input_columns))
}

#' Identify notifications within a gap at the same address
#' E.g. if an individiaul's address notifications were A, A, B, B, A, A
#' Then the notifications at address 'B' are in a gap between notifications of address A.
#'
identify_notifications_within_a_gap = function(db_connection, address_notifications,
                                               CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS){
  
  # get gaps that could contain concurrent addresses
  # only need to check one direction
  notifications_with_gap = address_notifications %>%
    group_by(snz_uid, address_uid) %>%
    mutate(next_date = lead(notification_date, 1, order_by = "notification_date")) %>%
    mutate(days_gap = DATEDIFF(DAY, notification_date, next_date))
  
  next_gap = notifications_with_gap %>%
    filter(0 <= days_gap 
           & days_gap <= CONCURRENT_MAX_DAYS_WINDOW) %>%
    ungroup()

  # get notifications within gaps
  # used <= instead of < to capture simultaneous notifs
  notifications_with_gap_filled = next_gap  %>%
    inner_join(address_notifications, by = "snz_uid", suffix = c("", "_in")) %>%
    filter(notification_date <= notification_date_in
           & notification_date_in <= next_date 
           & address_uid != address_uid_in)
  
  # notifications in a gap
  notifications_in_gaps = notifications_with_gap_filled %>%
    select(snz_uid,
           notification_date = notification_date_in,
           address_uid = address_uid_in,
           source = source_in,
           gap_end = next_date) %>%
    group_by(snz_uid, notification_date, address_uid, source) %>%
    summarise(gap_end = min(gap_end, na.rm = TRUE)) %>%
    distinct() %>%
    mutate(need_support_before = DATEADD(DAY, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS, gap_end))
  
  # random suffix for table name to reduce collisions & overrights
  tbl_name = paste0("chh_tmp_notifications_in_gaps_",floor(runif(1)*1E10))
  
  notifications_in_gaps = write_for_reuse(db_connection, 
                                          schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                          tbl_name,
                                          notifications_in_gaps,
                                          index_columns = c("snz_uid", "notification_date"))
  
  # return
  return(list(notifications_with_gap = notifications_with_gap,
              notifications_in_gaps = notifications_in_gaps))
}

#' Partition out notifications that represent "echoes" of old addresses
#'
partition_out_echoes = function(db_connection, address_notifications,
                                CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS){
  
  # notifications with gaps
  out = identify_notifications_within_a_gap(db_connection, address_notifications, 
                                            CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS)
  notifications_with_gap = out$notifications_with_gap
  notifications_in_gaps = out$notifications_in_gaps
  
  # partition out notifications in gaps if unsupported
  notif_in_gap_for_discard = notifications_in_gaps %>%
    inner_join(notifications_with_gap, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
    filter((is.na(next_date) | need_support_before < next_date) & validation == "NO") %>%
    mutate(replaced = "in gap unsupported")
  
  # notificaitions to keep
  notifications_no_echoes = address_notifications %>%
    anti_join(notif_in_gap_for_discard, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
    select(snz_uid, notification_date, address_uid, source, validation, accuracy)
  
  # return
  return(list(notifications_no_echoes = notifications_no_echoes,
              notif_in_gap_for_discard = notif_in_gap_for_discard))
}

#' Add concurrency flag to notifications
#'
add_concurrency_flag = function(db_connection, address_notifications,
                                CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS){
  
  # notifications with gaps
  out = identify_notifications_within_a_gap(db_connection, address_notifications,
                                            CONCURRENT_MAX_DAYS_WINDOW, CONCURRENT_REQUIRE_SUPPORT_WITHIN_DAYS)
  notifications_with_gap = out$notifications_with_gap
  notifications_in_gaps = out$notifications_in_gaps
  
  # assign concurrency flag to notifications in gaps
  notifications_in_gaps = notifications_in_gaps %>%
    distinct() %>%
    mutate(concurrent_flag = 1)

  # table of values to keep
  address_notifications = address_notifications %>%
    left_join(notifications_in_gaps, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
    mutate(concurrent_flag = ifelse(is.na(concurrent_flag),0,concurrent_flag))
  
  return(list(address_notifications = address_notifications))
}

#' Resolve any remaining simultaneous notifications
#' In our refined individual notification table we want there to be 
#' no simultaneous notifications unless they are concurrent
#' 
resolve_remaining_simultaneous = function(db_connection, address_notifications){
  
  # identify simultaneous (non-concurrent) notifications
  simultaneous_notifications = address_notifications %>%
    filter(concurrent_flag == 0) %>%
    group_by(snz_uid) %>%
    mutate(lag_date = lag(notification_date, 1, order_by = "notification_date"),
           lead_date = lead(notification_date, 1, order_by = "notification_date")) %>%
    filter(notification_date == lag_date | notification_date == lead_date)
  
  # get distance to closest collaborator (as crude sorter)
  distance_to_closest_collaborator = simultaneous_notifications %>%
    left_join(address_notifications, by = c("snz_uid", "address_uid"), suffix = c("","_support")) %>%
    mutate(days_gap = DATEDIFF(DAY, notification_date, notification_date_support)) %>%
    # this is a crude way of doing a compact sorter
    mutate(tie_breaker = CHECKSUM(NEWID())/100000.0 %% 0.5) %>%
    mutate(closeness = ifelse(days_gap > 0, days_gap, 1000 - days_gap)) %>%
    mutate(closeness = ifelse(days_gap == 0, 2000, closeness)) %>% # must at least join with self
    mutate(closeness = closeness + tie_breaker) %>%
    group_by(snz_uid, notification_date, address_uid, source) %>%
    summarise(closest_per_address = min(closeness, na.rm = TRUE))
  
  distance_to_closest_collaborator = write_for_reuse(db_connection, 
                                                     schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                                     "chh_tmp_distance_to_closest_collaborator",
                                                     distance_to_closest_collaborator,
                                                     index_columns = c("snz_uid", "notification_date"))
  
  # closest
  closest_colaborator = distance_to_closest_collaborator %>%
    group_by(snz_uid, notification_date) %>%
    summarise(closest_per_date = min(closest_per_address, na.rm = TRUE))
  
  # supported simultaneous notifications
  supported_simultaneous_notifications = distance_to_closest_collaborator %>%
    semi_join(closest_colaborator, by = c("snz_uid", "notification_date",
                                          "closest_per_address" = "closest_per_date"))
  
  # unsupported simultaneous notifications
  unsupported_simultaneous_notifications = simultaneous_notifications %>%
    anti_join(supported_simultaneous_notifications, by = c("snz_uid", "notification_date",
                                                           "address_uid", "source")) %>%
    mutate(replaced = "resolve last simultaneous")
  
  unsupported_simultaneous_notifications = write_for_reuse(db_connection, 
                                                           schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                                           "chh_tmp_last_unsupported_simultaneous_notifications",
                                                           unsupported_simultaneous_notifications,
                                                           index_columns = c("snz_uid", "notification_date"))
  
  # all other notifications
  all_other_notifications = address_notifications %>%
    anti_join(unsupported_simultaneous_notifications, by = c("snz_uid", "notification_date",
                                                             "address_uid", "source"))
  
  # return
  return(list(all_other_notifications = all_other_notifications,
              unsupported_simultaneous_notifications = unsupported_simultaneous_notifications))

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
           lag_address_uid = lag(address_uid, 1, order_by = "notification_date"),
           lag_concurrent_flag = lag(concurrent_flag, 1, order_by = "notification_date"))
  
  # if address and concurrency flag match then not address change
  not_address_changes = notifications_w_prev %>%
    ungroup() %>%
    filter(address_uid == lag_address_uid
           & concurrent_flag == lag_concurrent_flag) %>%
    select(snz_uid, notification_date, address_uid, source, validation, concurrent_flag) %>%
    mutate(replaced = "not address change")
  
  # remainder are address changes
  address_changes = notifications_w_prev %>%
    ungroup() %>%
    filter(is.na(lag_notification_date)
           | address_uid != lag_address_uid
           | concurrent_flag != lag_concurrent_flag) %>%
    select(snz_uid, notification_date, address_uid, source, validation, concurrent_flag)
    
  # combine all address changes
  column_names = c("snz_uid", "notification_date", "address_uid", "source", "validation", "concurrent_flag")

  # return
  return(list(address_changes = address_changes,
              not_address_changes = not_address_changes))
}

