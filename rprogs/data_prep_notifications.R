##################################################################################################################
# Description: Prepare the data for household building
#
# Input: Prepared views from IDI Clean and Adhoc
#
# Output: SQL tables in the sandpit
# 
# Author: Simon Anastasiadis
# 
# Dependencies: setup_views_and_tables_individual_level.sql
# 
# Notes:
# Uses dbplyr to translate R to SQL
# Last recorded runtime = 15 minutes
#
# Editting:
# To add new datasets to the address notification approach, create a view or table in the SQL script
# before adding the new table or view to this script.
# Ensure that the new table you add has all the required columns as listed in "column_names".
# 
# Issues:
# 
# History (reverse order):
# 2019-03-05 SA changed location views aresaved to IDI_UserCode
# 2018-11-07 SA swapped from tables to views
# 2018-09-21 SA added all datasets produced by Craig
# 2018-08-23 SA restructure to make adding data easier
# 2018-08-22 SA v1
##################################################################################################################

## source ----
setwd(paste0("/home/STATSNZ/dl_sanastasia/Network-Shares/DataLabNas/",
             "MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs"))
source('utility_functions.R')

## setup ----
db_con_IDI_sandpit = create_database_connection(database = 'IDI_Sandpit')
our_schema = "[IDI_Sandpit].[DL-MAA2016-15]"
our_view = "[IDI_UserCode].[DL-MAA2016-15]"
OUTPUT_TABLE = "chh_gathered_data"

## create tables ----

columns_for_notifications = c("snz_uid", "notification_date", "address_uid", "source", "validation")

notifications_columns = list(snz_uid = "[int] NOT NULL",
                             notification_date = "[date] NOT NULL",
                             address_uid = "[int] NOT NULL",
                             source = "[varchar](25) NULL",
                             validation = "[varchar](3) NOT NULL")

create_table(db_con_IDI_sandpit,
             schema = our_schema,
             tbl_name = OUTPUT_TABLE,
             named_list_of_columns = notifications_columns,
             OVERWRITE = TRUE)

## load each table withy key columns ----
run_time_inform_user("building table connections")

#### ACC ----
acc = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                          tbl_name = "chh_acc_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, acc)
run_time_inform_user("acc appended")
  
#### IRD applied date ----
ird_applied = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                  tbl_name = "chh_ird_applied_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, ird_applied)
run_time_inform_user("ird_applied appended")

#### IRD timestamped date ----
ird_timestamped = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                      tbl_name = "chh_ird_timestamp_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, ird_timestamped)
run_time_inform_user("ird_timestamped appended")

#### MOE ----
moe = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                          tbl_name = "chh_moe_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, moe)
run_time_inform_user("moe appended")

#### MSD residential location ----
msd_residential = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                      tbl_name = "chh_msd_residential_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, msd_residential)
run_time_inform_user("msd_residential appended")

#### MSD partner residential location ----
msd_partner = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                      tbl_name = "chh_msd_partner_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, msd_partner)
run_time_inform_user("msd_partner appended")

#### MSD child residential location ----
msd_child = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                      tbl_name = "chh_msd_child_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, msd_child)
run_time_inform_user("msd_child appended")

#### MSD postal location ----
msd_postal = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                 tbl_name = "chh_msd_postal_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, msd_postal)
run_time_inform_user("msd_postal appended")

#### MOH nhi address ----
moh_nhi = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                              tbl_name = "chh_moh_nhi_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, moh_nhi)
run_time_inform_user("moh_nhi appended")

#### MOH pho address ----
moh_pho = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                              tbl_name = "chh_moh_pho_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, moh_pho)
run_time_inform_user("moh_pho appended")

#### SNZ census usual residence ----
snz_census_UR = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                    tbl_name = "chh_census_ur_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_census_UR)
run_time_inform_user("snz_census_UR appended")

#### SNZ census usual residence 5 years ago ----
snz_census_UR5 = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                     tbl_name = "chh_census_ur5_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_census_UR5)
run_time_inform_user("snz_census_UR5 appended")

#### SNZ census years at current address ----
snz_census_prev = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                                      tbl_name = "chh_census_prev_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_census_prev)
run_time_inform_user("snz_census_prev appended")

#### SNZ hes ----
snz_hes = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                              tbl_name = "chh_hes_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_hes)
run_time_inform_user("snz_hes appended")

#### SNZ gss ----
snz_gss = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                              tbl_name = "chh_gss_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_gss)
run_time_inform_user("snz_gss appended")

#### SNZ hlfs ----
snz_hlfs = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                               tbl_name = "chh_hlfs_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, snz_hlfs)
run_time_inform_user("snz_hlfs appended")

#### NZTA motor vehicle registrations ----
nzta_mvr = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                               tbl_name = "chh_nzta_mvr_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, nzta_mvr)
run_time_inform_user("nzta_mvr appended")

#### NZTA drivers license registrations ----
nzta_dlr = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                               tbl_name = "chh_nzta_dlr_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, nzta_dlr)
run_time_inform_user("nzta_dlr appended")

#### MSD HNZ tenancies ----
msd_hnz = create_access_point(db_connection = db_con_IDI_sandpit, schema = our_view,
                              tbl_name = "chh_hnz_notifications")

append_database_table(db_con_IDI_sandpit, our_schema, OUTPUT_TABLE, columns_for_notifications, msd_hnz)
run_time_inform_user("msd_hnz appended")

#### tenancies from bond records ----
#
# currently not prepared

## Complete ----
run_time_inform_user("GRAND COMPLETION")


