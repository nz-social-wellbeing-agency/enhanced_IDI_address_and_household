##################################################################################################################
#' Description: Unit tests for validation_suit_functions
#'
#' Input: validation_suite_functions & data to test _ .xlsx files
#'
#' Output: Confirmation that functions perform as expected
#' 
#' Author: Simon Anastasiadis
#' 
#' Dependencies: utility_functions.R
#' 
#' Notes:
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2020-03-24 SA v1
#'#################################################################################################################

## compare truth with admin ----
context("connect_truth_with_admin_to_compare")

testthat::test_that("output matches manual construction",{
  
  data_for_test = "./tests/data to test connect_truth_with_admin_to_compare.xlsx"
  
  input_admin = readxl::read_xlsx(data_for_test, sheet = "input_admin")
  input_truth = readxl::read_xlsx(data_for_test, sheet = "input_truth")
  expected_output = readxl::read_xlsx(data_for_test, sheet = "output")
  
  testthat::expect_true(all.equal(expected_output,
                                  connect_truth_with_admin_to_compare(input_admin, input_truth),
                                  ignore.row.order = TRUE,
                                  ignore.col.order = TRUE))
})

## one way comparison ----
context("one_way_comparison")

testthat::test_that("output matches manual construction",{
  
  data_for_test = "./tests/data to test one_way_comparison.xlsx"
  
  input = readxl::read_xlsx(data_for_test, sheet = "input")
  expected_output_truth = readxl::read_xlsx(data_for_test, sheet = "output_truth")
  expected_output_truth$total_residents = as.integer(expected_output_truth$total_residents)
  expected_output_admin = readxl::read_xlsx(data_for_test, sheet = "output_admin")
  expected_output_admin$total_residents = as.integer(expected_output_admin$total_residents)
  
  testthat::expect_true(all.equal(expected_output_truth,
                                  one_way_comparison(input, "address_uid_truth"),
                                  ignore.row.order = TRUE,
                                  ignore.col.order = TRUE))
  
  testthat::expect_true(all.equal(expected_output_admin,
                                  one_way_comparison(input, "address_uid"),
                                  ignore.row.order = TRUE,
                                  ignore.col.order = TRUE))
})

## address validation algorithm ----

context("address_validation_algorithm")

testthat::test_that("output matches manual construction",{
  
  data_for_test = "./tests/data to test address_validation_algorithm.xlsx"
  
  input = readxl::read_xlsx(data_for_test, sheet = "input")
  expected_output = readxl::read_xlsx(data_for_test, sheet = "output")
  expected_output$total_count = as.integer(expected_output$total_count)
  
  testthat::expect_true(all.equal(expected_output,
                                  address_validation_algorithm(input),
                                  ignore.row.order = TRUE,
                                  ignore.col.order = TRUE))
})
