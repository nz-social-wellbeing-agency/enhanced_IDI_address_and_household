##################################################################################################################
#' Description: Functions that support validating individual and group level address information
#'
#' Input: Specified tables for checking
#'
#' Output: Measures of coverage and accuracy, by groups
#' 
#' Author: Simon Anastasiadis, Craig Wright
#' 
#' Dependencies: utility_functions for dbplyr
#' 
#' Notes:
#' Original version in SQL by CW
#' Uses dbplyr to translate R to SQL
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2018-11-08 SA v1
#' 2018-10-09 SA v0
#'#################################################################################################################

library(tidyr)

#' Convert to spells
#'
convert_notifs_to_validation_spells = function(address_notifications, accuracy_threshold = NA){
  # checks
  assert(is.na(accuracy_threshold) | (0 < accuracy_threshold & accuracy_threshold < 1),
         msg = "accuracy threshold must be between [0,1]")
  
  # filter to accuracy level if it applies
  if(!is.na(accuracy_threshold)
     & 0 < accuracy_threshold
     & accuracy_threshold < 1
     & "accuracy" %in% names(address_notifications)){
    
    address_notifications = address_notifications %>%
      filter(accuracy >= accuracy_threshold,
             validation == 'NO')
  }
  
  # conversion
  address_spells = address_notifications %>%
    filter(validation != "YES") %>%
    group_by(snz_uid) %>%
    mutate(next_date = lead(notification_date, 1, order_by = "notification_date"),
           next_date2 = lead(notification_date, 2, order_by = "notification_date"),
           next_date3 = lead(notification_date, 3, order_by = "notification_date"),
           next_date4 = lead(notification_date, 4, order_by = "notification_date") ) %>%
    mutate(next_date = ifelse(notification_date == next_date, next_date2, next_date)) %>%
    mutate(next_date = ifelse(notification_date == next_date, next_date3, next_date)) %>%
    mutate(next_date = ifelse(notification_date == next_date, next_date4, next_date)) %>%
    mutate(next_date = ifelse(is.na(next_date), '9999-01-01', next_date)) %>%
    filter(notification_date != next_date) %>%
    select(snz_uid, notification_date, address_uid, source, validation, next_date)
  
  return(address_spells)
}

#' Compare truth and spells
#'
compare_truth_and_spells = function(truth_tbl, spells_tbl){
  
  # we use true_date %in% [start_date, end_date)
  # because the notification date is assumed to be the move in date
  comparison = truth_tbl %>%
    left_join(spells_tbl, by = "snz_uid", suffix = c("_truth","")) %>%
    filter(!is.na(notification_date_truth),
           !is.na(address_uid_truth),
           (is.na(notification_date) | notification_date <= notification_date_truth),
           (is.na(notification_date) | notification_date_truth < next_date)) %>%
    mutate(covered = ifelse(!is.na(address_uid), 1, 0),
           matched = ifelse(address_uid == address_uid_truth, 1, 0)) %>%
    select(snz_uid, notification_date, address_uid, source, validation, next_date,
           notification_date_truth, address_uid_truth, source_truth, household_uid,
           covered, matched)
  
  return(comparison)
}

#' Summarise individual level accuracy and coverage
#' - might need to include subsetting to spine or residential population
#' 
summarise_individual_accuracy = function(comparison){
  
  # accuracy by source
  source_result = comparison %>%
    ungroup() %>%
    group_by(snz_uid, notification_date_truth, source_truth) %>%
    summarise(accuracy_num = mean(matched, na.rm = TRUE),
              coverage_num = mean(covered, na.rm = TRUE),
              record_num = n()) %>%
    ungroup() %>%
    group_by(source_truth) %>%
    summarise(accuracy_num = sum(accuracy_num, na.rm = TRUE),
              coverage_num = sum(coverage_num, na.rm = TRUE),
              record_num = sum(record_num, na.rm = TRUE)) %>%
    mutate(accuracy = ifelse(coverage_num == 0, NA, 1.0 * accuracy_num / coverage_num),
           coverage = ifelse(record_num == 0, NA, 1.0 * coverage_num / record_num)) %>%
    mutate(eff_accuracy = accuracy * coverage,
           eff_error = (1 - accuracy) * coverage,
           eff_missing = 1 - coverage)
  
  
  
  save_to_sql(sql_render(source_result), "individual_accuracy")
  
  source_result = source_result %>% collect() %>%
    arrange(source_truth)
  
  # overall accuracy
  overall_result = source_result %>%
    ungroup() %>%
    summarise(accuracy_num = sum(accuracy_num, na.rm = TRUE),
              coverage_num = sum(coverage_num, na.rm = TRUE),
              record_num = sum(record_num, na.rm = TRUE)) %>%
    mutate(accuracy = ifelse(coverage_num == 0, NA, 1.0 * accuracy_num / coverage_num),
           coverage = ifelse(record_num == 0, NA, 1.0 * coverage_num / record_num)) %>%
    mutate(eff_accuracy = accuracy * coverage,
           eff_error = (1 - accuracy) * coverage,
           eff_missing = 1 - coverage)
  
  return(list(overall_result = overall_result, source_result = source_result))
}

#' Summarise household level accuracy and coverage
#' - might need to include subsetting to spine or residential population
#' 
summarise_household_accuracy = function(comparison){
  
  # accuracy by source
  source_result = comparison %>%
    ungroup() %>%
    group_by(snz_uid, notification_date_truth, source_truth, address_uid_truth, household_uid) %>%
    summarise(accuracy_num = mean(matched, na.rm = TRUE),
              coverage_num = mean(covered, na.rm = TRUE),
              record_num = n()) %>%
    ungroup() %>%
    group_by(source_truth, notification_date_truth, address_uid_truth, household_uid) %>%
    summarise(accuracy_num = sum(accuracy_num, na.rm = TRUE),
              coverage_num = sum(coverage_num, na.rm = TRUE),
              record_num = sum(record_num, na.rm = TRUE)) %>%
    mutate(accuracy = ifelse(coverage_num == 0, NA, 1.0 * accuracy_num / coverage_num)) %>%
    mutate(accuracy_type = ifelse(accuracy > 1.0, "superperfect", NA)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy == 1.0, "perfect", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy >= 0.5, "partial", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type), "poor", accuracy_type)) %>%
    ungroup() %>%
    group_by(source_truth, accuracy_type) %>%
    summarise(count = n())
  
  save_to_sql(sql_render(source_result), "household_accuracy")
   
  source_result = source_result %>% collect()
  
  # overall accuracy
  overall_result = source_result %>%
    ungroup() %>%
    group_by(accuracy_type) %>%
    summarise(count = sum(count)) %>%
    spread(key = accuracy_type, value = count)
  
  source_result = source_result  %>% spread(key = accuracy_type, value = count)
  
  return(list(overall_result = overall_result, source_result = source_result))
}

#' varient of summarise household accuracy to better match SNZ process
#'
summarise_household_accuracy_NEW = function(db_connection, comparison){
  
  # remove non-coverage
  # comparison = comparison %>%
  #   filter(covered == 1)
  
  # accuracy in each direction
  result_by_truth = comparison %>%
    ungroup() %>%
    group_by(snz_uid, notification_date_truth, source_truth, address_uid_truth, household_uid) %>%
    summarise(accuracy_num = mean(matched, na.rm = TRUE),
              coverage_num = mean(covered, na.rm = TRUE)) %>%
    ungroup() %>%
    group_by(source_truth, notification_date_truth, address_uid_truth, household_uid) %>%
    summarise(accuracy_num = sum(accuracy_num, na.rm = TRUE),
              coverage_num = sum(coverage_num, na.rm = TRUE)) %>%
    mutate(accuracy = ifelse(coverage_num == 0, NA, 1.0 * accuracy_num / coverage_num)) %>%
    mutate(accuracy_type = ifelse(accuracy > 1.0, "superperfect", NA)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy == 1.0, "perfect", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy >= 0.5, "partial", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type), "poor", accuracy_type)) %>%
    select("source_truth", "notification_date_truth", "address_uid_truth", "household_uid", "accuracy_type")
  
  result_by_truth = write_for_reuse(db_connection, our_schema, "chh_val_result_by_truth",
                                    result_by_truth, index_columns = "household_uid")
  
  result_by_admin = comparison %>%
    ungroup() %>%
    group_by(snz_uid, notification_date_truth, source_truth, address_uid, household_uid) %>%
    summarise(accuracy_num = mean(matched, na.rm = TRUE),
              coverage_num = mean(covered, na.rm = TRUE)) %>%
    ungroup() %>%
    group_by(source_truth, notification_date_truth, address_uid, household_uid) %>%
    summarise(accuracy_num = sum(accuracy_num, na.rm = TRUE),
              coverage_num = sum(coverage_num, na.rm = TRUE)) %>%
    mutate(accuracy = ifelse(coverage_num == 0, NA, 1.0 * accuracy_num / coverage_num)) %>%
    mutate(accuracy_type = ifelse(accuracy > 1.0, "superperfect", NA)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy == 1.0, "perfect", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & accuracy >= 0.5, "partial", accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type), "poor", accuracy_type)) %>%
    select("source_truth", "notification_date_truth", "address_uid", "household_uid", "accuracy_type")

  result_by_admin = write_for_reuse(db_connection, our_schema, "chh_val_result_by_admin",
                                    result_by_admin, index_columns = "household_uid")
  
  # result_by_admin = comparison %>%
  #   select("source_truth", "notification_date_truth", "address_uid", "household_uid") %>%
  #   distinct() %>%
  #   mutate(accuracy_type = "perfect")
  
  # accuracy by source
  source_result = result_by_admin %>%
    right_join(result_by_truth, suffix = c("_t","_a"),
               by = c("source_truth", "notification_date_truth", "household_uid",
                      "address_uid" = "address_uid_truth")) %>%
    mutate(accuracy_type = ifelse(accuracy_type_t == "superperfect"
                                  | accuracy_type_a == "superperfect", "superperfect", NA)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & (accuracy_type_t == 'poor'
                                     | accuracy_type_a == 'poor'), 'poor', accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & (accuracy_type_t == 'partial'
                                     | accuracy_type_a == 'partial'), 'partial', accuracy_type)) %>%
    mutate(accuracy_type = ifelse(is.na(accuracy_type)
                                  & (accuracy_type_t == 'perfect'
                                     | accuracy_type_a == 'perfect'), 'perfect', accuracy_type)) %>%
    ungroup() %>%
    group_by(source_truth, accuracy_type) %>%
    summarise(count = n())

  save_to_sql(sql_render(source_result), "household_accuracy")
  
  source_result = source_result %>% collect()
  
  # overall accuracy
  overall_result = source_result %>%
    ungroup() %>%
    group_by(accuracy_type) %>%
    summarise(count = sum(count)) %>%
    spread(key = accuracy_type, value = count)
  
  source_result = source_result  %>% spread(key = accuracy_type, value = count)
  
  return(list(overall_result = overall_result, source_result = source_result))
}
