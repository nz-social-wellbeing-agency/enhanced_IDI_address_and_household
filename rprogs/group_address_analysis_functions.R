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
#' 2018-11-13 SA v0
#'#################################################################################################################

#' Produce a summary table identifying which addresses contain multiple dwellings
#'
identify_multi_dwelling_addresses = function(db_connection, household_records){

  # number of dwelling
  num_dwellings = household_records %>%
    select(address_uid, household_uid, source) %>%
    distinct() %>%
    group_by(address_uid, source) %>%
    summarise(num_hhlds = n()) %>%
    ungroup() %>%
    group_by(address_uid) %>%
    summarise(num_hhlds = max(num_hhlds, na.rm = TRUE))
  
  # multi-dwelling
  multi_dwelling = num_dwellings %>%
    ungroup() %>%
    mutate(multi_ind = ifelse(num_hhlds > 1, 'Y', 'N'))

  # write out  
  multi_dwelling = write_for_reuse(db_connection, 
                                   schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                   tbl_name = "chh_tmp_multi_dwelling",
                                   tbl_to_save = multi_dwelling,
                                   index_columns = c("address_uid"))
  
  # return
  return(multi_dwelling)
}

#' Identify address change notifications that should be updated as part of ensuring
#' move in/out dates are consistent across household members.
#' (Not intended to be called directly)
#'
standardise_address_change_on_similar_dates = function(db_connection, address_notifications,
                                                       from_match, to_match, concurrency, multi, DAYS_GAP){
  # initial filtering, concurrency = F
  if(!concurrency)
    address_notifications = address_notifications %>% filter(concurrent_flag == 0)
  
  # add previous address & concurrency
  full_address_moves = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(prev_address_uid = lag(address_uid, 1, order_by = "notification_date"),
           prev_concurrent_flag = lag(concurrent_flag, 1, order_by = "notification_date")) %>%
    filter(!is.na(prev_address_uid))
  # We explicitly exclude records without a previous address for clarity.
  # Otherwise they implicitly/invisibly are removed during the joins below.
  
  # initial filtering, concurrency = T
  if(concurrency)
    full_address_moves = full_address_moves %>%
      filter(prev_concurrent_flag == 1, concurrent_flag == 1)
  
  # multi-dwelling data source
  multi_dwelling = create_access_point(db_connection, schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                       tbl_name = "chh_tmp_multi_dwelling") %>%
    filter(multi_ind == 'Y')
  
  # initial filtering, multi-dwelling address
  if(!multi)
    ready_table = full_address_moves %>%
      anti_join(multi_dwelling, by = "address_uid") %>%
      anti_join(multi_dwelling, by = c("prev_address_uid" = "address_uid"))
  
  if(multi){
    partA = full_address_moves %>%
      semi_join(multi_dwelling, by = "address_uid")
    partB = full_address_moves %>%
      anti_join(multi_dwelling, by = "address_uid") %>%
      semi_join(multi_dwelling, by = c("prev_address_uid" = "address_uid"))
    ready_table = union_all(partA, partB, colnames(partA))
  }
    
    # write for reuse
  join_column = c()
  if(to_match)
    join_column = append(join_column, "address_uid")
  if(from_match)
    join_column = append(join_column, "prev_address_uid")
  
  full_address_moves = write_for_reuse(db_connection, 
                                   schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                   tbl_name = "chh_tmp_full_address_moves",
                                   tbl_to_save = ready_table,
                                   index_columns = join_column)
  
  # pairs of addresses where refinement is possible
  address_pairs = full_address_moves %>%
    inner_join(full_address_moves, by = join_column, suffix = c("_x","_y")) %>%
    filter(snz_uid_x < snz_uid_y, # not the same person, count every pair at most once
           notification_date_x != notification_date_y) %>% # no need to update if dates already identical
    mutate(date_sync = DATEDIFF(day, notification_date_x, notification_date_y)) %>%
    mutate(date_sync = abs(date_sync)) %>% # days difference between move
    filter(date_sync <= DAYS_GAP)
    
  # focus on records to change
  records_to_change = address_pairs %>%
    mutate(snz_uid = ifelse(notification_date_x < notification_date_y,
                            snz_uid_y, snz_uid_x),
           source = ifelse(notification_date_x < notification_date_y,
                           source_y, source_x),
           concurrent_flag = ifelse(notification_date_x < notification_date_y,
                                    concurrent_flag_y, concurrent_flag_x),
           notification_date = ifelse(notification_date_x < notification_date_y,
                                      notification_date_y, notification_date_x),
           notification_date_updated = ifelse(notification_date_x < notification_date_y,
                                              notification_date_x, notification_date_y))
  # address_uid will have suffix {_x, _y} if it was not used for matching
  if(!to_match)
    records_to_change = records_to_change %>%
      mutate(address_uid = ifelse(notification_date_x < notification_date_y,
                                  address_uid_y, address_uid_x))
  # select
  records_to_change = records_to_change %>%
    mutate(validation = "NO") %>%
    select(snz_uid, address_uid, notification_date, source, validation, concurrent_flag, notification_date_updated)
  
  records_to_change = write_for_reuse(db_connection, 
                                      schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                      tbl_name = "chh_tmp_records_to_change",
                                      tbl_to_save = records_to_change,
                                      index_columns = "snz_uid")
  
  # output tables
  original_records_for_discard = records_to_change %>%
    select(snz_uid, address_uid, notification_date, source, validation, concurrent_flag) %>%
    distinct() %>%
    mutate(replaced = "standardise move in date")
  
  new_records_for_addition = records_to_change %>%
    group_by(snz_uid, address_uid, notification_date, source, validation, concurrent_flag) %>%
    summarise(new_notification_date = min(notification_date_updated, na.rm = TRUE)) %>%
    mutate(source = "standardise move in date") %>%
    ungroup() %>%
    select(snz_uid, address_uid, source, validation, concurrent_flag, notification_date = new_notification_date)
  
  return(list(original_records_for_discard = original_records_for_discard,
              new_records_for_addition = new_records_for_addition))
}

#' Compare across individuals who share an addresses and ensure consistent move in/out dates
#' 
consistent_address_change__dates = function(db_connection, address_notifications,
                                             from_match, to_match, concurrency, multi, DAYS_GAP){
  # checks
  assert(is.logical(from_match), "from match must be TRUE or FALSE")
  assert(is.logical(to_match), "to match must be TRUE or FALSE")
  assert(is.logical(concurrency), "concurrency must be TRUE or FALSE")
  assert(is.logical(multi), "multi must be TRUE or FALSE")
  assert(is.numeric(DAYS_GAP), "days gap must be numeric")
  assert(from_match | to_match, "no reason to standardise without a match on from or to address")
  
  # early exit
  if(DAYS_GAP > 0){
    # get notifications to update
    out = standardise_address_change_on_similar_dates(db_connection, address_notifications,
                                                      from_match, to_match, concurrency, multi, DAYS_GAP)
    original_records_for_discard = out$original_records_for_discard
    new_records_for_addition = out$new_records_for_addition
    
    # check equal number of notifications
    num_for_discard = original_records_for_discard %>% ungroup %>% summarise(num = n()) %>% collect()
    num_for_addition = new_records_for_addition %>% ungroup() %>% summarise(num = n()) %>% collect()
    assert(num_for_discard[[1,1]] == num_for_addition[[1,1]], "different number of records added and removed")
    
    # store replaced notifications
    append_database_table(db_con_IDI_sandpit, our_schema, "chh_replaced_notifications",
                          columns_for_replacement_table, original_records_for_discard)
    
    # remove replaced and add updated notifications
    address_notifications = address_notifications %>%
      anti_join(original_records_for_discard, by = c("snz_uid", "notification_date", "address_uid", "source")) %>%
      union_all(new_records_for_addition, columns_for_notification_table)
  }
  
  # write for reuse
  temp_tbl_name = paste0("chh_tmp_consistent_moves_",floor(runif(1)*1E10))
  address_notifications = write_for_reuse(db_connection, our_schema, temp_tbl_name,
                                          address_notifications, index_columns = "snz_uid")
  
  return(address_notifications)
}

#' Syncronise dependent children addresses with their parents
#'
sync_dependent_children = function(db_connection, address_notifications, DEPENDENT_AGE){
  
  # obtain birth records
  birth_records = create_access_point(db_connection, "[IDI_Clean_20181020].[dia_clean]", "[births]")
  birth_records = birth_records %>%
    filter(!is.na(snz_uid),
           !is.na(dia_bir_birth_month_nbr),
           !is.na(dia_bir_birth_year_nbr),
           !is.na(parent1_snz_uid),
           !is.na(parent2_snz_uid),
           dia_bir_birth_year_nbr >= 1982) %>%
    select("snz_uid", "dia_bir_birth_month_nbr", "dia_bir_birth_year_nbr", "parent1_snz_uid", "parent2_snz_uid") %>%
    distinct()
  
  # notifications with notification details about the same date
  address_about_notifications = address_notifications %>%
    group_by(snz_uid) %>%
    mutate(next_notification_date = lead(notification_date, 1, order_by = "notification_date"),
           prev_notification_date = lag(notification_date, 1, order_by = "notification_date"),
           prev_address_uid = lag(address_uid, 1, order_by = "notification_date"))
  
  # child's notifications
  child_notifs = address_about_notifications %>%
    inner_join(birth_records, by = "snz_uid") %>%
    mutate(birth_date = DATEFROMPARTS(dia_bir_birth_year_nbr, dia_bir_birth_month_nbr, 15)) %>%
    mutate(age_at_notif = DATEDIFF(year, birth_date, notification_date)) %>%
    filter(0 <= age_at_notif,
           age_at_notif <= DEPENDENT_AGE)
  
  # add parent notifications
  child_parent_notifs = child_notifs %>%
    inner_join(address_about_notifications, by = c("parent1_snz_uid" = "snz_uid"), suffix = c("","_p1")) %>%
    inner_join(address_about_notifications, by = c("parent2_snz_uid" = "snz_uid"), suffix = c("","_p2")) %>%
    filter(address_uid_p1 == address_uid_p2, # parents share the same address
           concurrent_flag_p1 == concurrent_flag_p2, # parents share same concurrency
           address_uid != address_uid_p1, # child is at a different address
           notification_date_p1 <= next_notification_date_p2,
           notification_date_p2 <= next_notification_date_p1, # parents are at the address at the same time
           (prev_address_uid == prev_address_uid_p1
            | prev_address_uid == prev_address_uid_p2), # child was at same address at at least one parent
           prev_notification_date <= notification_date_p1,
           prev_notification_date <= notification_date_p2, # parents notif is in gap where child is missing notif
           notification_date_p1 <= notification_date,
           notification_date_p2 <= notification_date) # parents notif is in gap where child is missing notif
  
  # intermediate output
  child_parent_notifs = write_for_reuse(db_connection, our_schema, "chh_tmp_child_parent_notifs",
                                        child_parent_notifs, index_columns = "snz_uid")
  
  # form into new child notification
  new_child_notifs = child_parent_notifs %>%
    mutate(new_address_uid = address_uid_p1,
           new_notification_date = ifelse(notification_date_p1 < notification_date_p2,
                                          notification_date_p1, notification_date_p2),
           new_concurrent_flag = concurrent_flag_p1,
           source = "sync child to parents",
           validation = "NO") %>%
    select(snz_uid, source, validation,
           address_uid = new_address_uid,
           notification_date = new_notification_date,
           concurrent_flag = new_concurrent_flag)
  
  # output
  return(new_child_notifs)
}

#' Ensure that move out dates for a address occur before move in dates
#' Necessarily limited to move outs that are within "AVOIDED OVERLAP" days of move ins
#'
consistent_hand_overs = function(db_connection, address_notifications, AVOIDED_OVERLAP){
  # multi-dwelling addresses
  multi_dwelling = create_access_point(db_connection, schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                       tbl_name = "chh_tmp_multi_dwelling") %>%
    filter(multi_ind == 'Y')
  
  # get previous addresses
  full_address_moves = address_notifications %>%
    filter(concurrent_flag == 0) %>%
    group_by(snz_uid) %>%
    mutate(prev_address_uid = lag(address_uid, 1, order_by = "notification_date"))
  
  # join
  linked_addresses = full_address_moves %>%
    inner_join(full_address_moves, by = c("prev_address_uid" = "address_uid"), suffix = c("_out","_in")) %>%
    anti_join(multi_dwelling, by = c("prev_address_uid_out" = "address_uid")) %>%
    mutate(days_diff = DATEDIFF(DAY, notification_date_out, notification_date_in)) %>%
    filter(snz_uid_out != snz_uid_in, # not the same person
           notification_date_in <= notification_date_out, # move in happens before move out
           ABS(days_diff) <= AVOIDED_OVERLAP) # move out & in are close together
  
  # focus on records to change
  records_to_change = linked_addresses %>%
    mutate(notification_date_new = DATEADD(DAY, -1, notification_date_out)) %>%
    ungroup() %>%
    select(snz_uid = snz_uid_in,
           address_uid = prev_address_uid_out,
           notification_date_old = notification_date_in,
           notification_date_new,
           source = source_in,
           concurrent_flag = concurrent_flag_in) %>%
    mutate(validation = "NO")
    
    # write
    records_to_change = write_for_reuse(db_connection, 
                                        schema = "[IDI_Sandpit].[DL-MAA2016-15]",
                                        tbl_name = "chh_tmp_consistent_hand_overs",
                                        tbl_to_save = records_to_change,
                                        index_columns = "snz_uid")
    
    # prepare output
    move_ins_for_discard = records_to_change %>%
      select(snz_uid, address_uid, notification_date = notification_date_old,
             source, validation, concurrent_flag) %>%
      distinct() %>%
      mutate(replaced = "consistent hand overs")
    
    new_move_ins_to_add = records_to_change %>%
      group_by(snz_uid, address_uid, notification_date_old, source, validation, concurrent_flag) %>%
      summarise(notification_date = min(notification_date_new, na.rm = TRUE)) %>%
      mutate(source = "consistent hand overs") %>%
      select(snz_uid, address_uid, source, validation, concurrent_flag, notification_date)
    
    # check equal number of notifications
    num_for_discard = move_ins_for_discard %>% ungroup %>% summarise(num = n()) %>% collect()
    num_for_addition = new_move_ins_to_add %>% ungroup() %>% summarise(num = n()) %>% collect()
    assert(num_for_discard[[1,1]] == num_for_addition[[1,1]], "different number of records added and removed")
    
    return(list(move_ins_for_discard = move_ins_for_discard,
                new_move_ins_to_add = new_move_ins_to_add))
}

