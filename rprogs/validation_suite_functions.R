##################################################################################################################
#' Description: Functions that support validating individual and group level address information
#'
#' Input: Specified tables for checking
#'
#' Output: Measures of coverage and accuracy, by groups
#' 
#' Author: Simon Anastasiadis, Craig Wright, Akilesh Chokkanathapuram
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
#' 2019-10-02 SA fixed bug duplicating number of output addresses
#' 2019-09-26 SA clean to minimal production code
#' 2019-07-09 SA addition of hhld validation code
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

#' Summarise individual level accuracy and coverage
#' - might need to include subsetting to spine or residential population
#' 
#' Removed as integrated with household accuracy in later version.

#' Attach to each validation/truth record
#' the admin record that immediately preceeds it.
#' 
#' Assumes:
#' 1) only truth in truth_table and only admin in admin_table
#' 2) all values in tables are non-null
#' 
#' by Akilesh CHOKKANATHAPURAM
#'
connect_truth_with_admin_to_compare <- function(admin_table, truth_table){
  # The columns required in each of the table are validated here.
  required_columns_source_table <- c("snz_uid", "address_uid", "notification_date")
  required_columns_truth_table <- c(required_columns_source_table, "source")
  
  # Validate inputs
  assert(table_contains_required_columns(admin_table, required_columns_source_table),
         "Source table provided lacks required column(s)")
  assert(table_contains_required_columns(truth_table, required_columns_truth_table),
         "Truth table provided lacks required column(s)")
  assert(is.tbl(admin_table), "input must be of type table")
  assert(is.tbl(truth_table), "input must be of type table")
  
  # ACHOKKANATHAPURAM WAS HERE
  # Combine truth and admin tables - find date of latest admin notification
  truth_w_latest_admin_date <- truth_table %>%
    inner_join(admin_table, by = "snz_uid", suffix = c("_truth", "_admin")) %>%
    filter(notification_date_admin < notification_date_truth) %>% # struct inequality
    group_by(snz_uid, source_truth, address_uid_truth, notification_date_truth) %>%
    summarize(notification_date = max(notification_date_admin, na.rm = TRUE))
  # logic behind strict inequality: when new information arrives (e.g. validation notifications)
  # we first check whether existing/previous dates agree with it before adding the new notification.
  # Another way to think of it is: validation happens at 9am and address change happens at 11:59pm.
  
  # Fetch the admin record matching the latest date
  truth_w_latest_admin <- truth_w_latest_admin_date %>%
    inner_join(admin_table, by = c("snz_uid", "notification_date"), suffix = c("", "_final")) %>%
    mutate(match_status = if (address_uid_truth == address_uid) 1 else 0)
  
  # truth_w_latest_admin (table 2) is significant table and sees multiple re-use
  # writing this table for reuse improves runtime from 11 min to 1.5 min
  
  # output for writing & reuse
  return(truth_w_latest_admin)
}

#' Compare admin and truth (SNZ) address tables by household
#' and construct triple-P classification by source.
#' 
#' Assumes:
#' 1) only truth in truth_table and only admin in admin_table
#' 2) all values in tables are non-null
#' 
#' by Akilesh CHOKKANATHAPURAM
#'
address_validation_algorithm <- function(truth_w_latest_admin){
  # compare in both directions
  # ACHOKKANATHAPURAM WAS HERE
  compare_against_truth <- one_way_comparison(truth_w_latest_admin, "address_uid_truth")
  compare_against_admin <- one_way_comparison(truth_w_latest_admin, "address_uid")
  
  # reorganizing the query results to perform an efficient join on address_uid
  # the join also suffixes the tables with _truth_1 and _admin_1 respectively for truth and admin table variables
  both_comparisons <- compare_against_truth %>%
    inner_join(compare_against_admin, by = c("source", "address_uid"), suffix = c("_truth", "_admin"))
  
  # ACHOKKANATHAPURAM WAS HERE
  # Doing a combination assignment for Perfect, Partial and Poor based on the results from the previous sub-queries.
  # Each address gets the worst of the two values.
  final_classification <- both_comparisons %>% 
    mutate(final_stat = NA) %>%
    mutate(final_stat = ifelse(is.na(final_stat)
                               & (stat_truth == 'POOR' | stat_admin == 'POOR'), 'POOR', final_stat)) %>%
    mutate(final_stat = ifelse(is.na(final_stat)
                               & (stat_truth == 'PARTIAL' | stat_admin == 'PARTIAL'), 'PARTIAL', final_stat)) %>%
    mutate(final_stat = ifelse(is.na(final_stat)
                               & (stat_truth == 'PERFECT' & stat_admin == 'PERFECT'), 'PERFECT', final_stat)) %>%
    group_by(source, final_stat) %>%
    summarise(total_count = n())
  # appologies the above is clumsy, case_when would be more elegant but does not work on some versions of dbpylr
  # final_stat = case_when(
  #   stat_truth == 'PERFECT' & stat_admin == 'PERFECT' ~ 'PERFECT',
  #   stat_truth == 'PERFECT' & stat_admin == 'PARTIAL' ~ 'PARTIAL',
  #   stat_truth == 'PERFECT' & stat_admin == 'POOR' ~ 'POOR',
  #   stat_truth == 'PARTIAL' & stat_admin == 'PERFECT' ~ 'PARTIAL',
  #   stat_truth == 'PARTIAL' & stat_admin == 'PARTIAL' ~ 'PARTIAL',
  #   stat_truth == 'PARTIAL' & stat_admin == 'POOR' ~ 'POOR',
  #   stat_truth == 'POOR' & stat_admin == 'PERFECT' ~ 'POOR',
  #   stat_truth == 'POOR' & stat_admin == 'PARTIAL' ~ 'POOR',
  #   stat_truth == 'POOR' & stat_admin == 'POOR' ~ 'POOR'
  # )
  
  # load into R
  save_to_sql(sql_render(final_classification), "household_accuracy")
  final_classification <- final_classification %>% collect()
  run_time_inform_user("data transfered from SQL into R")
  
  # Result per source 
  # mutated to calculate percentage results and arrange the results in the order Perfect, Partial and Poor.
  # ACHOKKANATHAPURAM WAS HERE
  total_by_source <- final_classification %>%
    group_by(source) %>%
    summarise(sum_total_count = sum(total_count, na.rm = TRUE)) %>%
    select(source, sum_total_count)
  
  summary_by_source <- final_classification %>%
    left_join(total_by_source, by = "source") %>%
    mutate(percent_stat = total_count / sum_total_count) %>%
    arrange(source, final_stat) %>%
    select(source, final_stat, total_count, percent_stat)
  
  
  #the final results with overall numbers from table_12 is returned for quick reference. 
  return(summary_by_source)
}

#' Compare address results again one of the address columns (admin OR truth)
#'
#' by Akilesh Chokkanathapuram
#'
one_way_comparison <- function(input_table, address_column){
  # checks
  assert(is.tbl(input_table), "input table format not recognised")
  assert(is.character(address_column), "address column for comparison must be text string")
  required_columns = c(address_column, "source_truth", "match_status")
  assert(table_contains_required_columns(input_table, required_columns), "input table lacks required columns")
  
  # standardise input column name
  input_table <- input_table %>% mutate(address_uid = !!sym(address_column))
  
  # total_residents = the total residents in a truth table address
  # matched_residents = the total residents who have a matching address in the admin table
  # percent_match = percentage match based on the above numbers.
  # ACHOKKANATHAPURAM WAS HERE
  ready_for_classification <- input_table %>%
    group_by(source_truth, address_uid) %>%
    summarize(total_residents = n(),
              matched_residents = sum(match_status, na.rm = TRUE)) %>%
    mutate(percent_match = (matched_residents / total_residents))
  
  # results are mutate to form 'triple-P categories'
  # A 100% matched household is PERFECT
  # 50-99 inclusive is PARTIAL
  # and less than 50 is POOR.
  # Any households missing a category are also termed POOR to reduce algorithmic errors
  ppp_classified <- ready_for_classification %>%
    mutate(stat = if(percent_match == 1) "PERFECT"
           else if(percent_match >= 0.5) "PARTIAL"
           else "POOR") %>%
    select(source = source_truth, address_uid, total_residents, matched_residents, percent_match, stat)
  
  return(ppp_classified)
}
