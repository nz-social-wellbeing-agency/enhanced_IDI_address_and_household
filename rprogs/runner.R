rm(list=ls())
path = '~/Network-Shares/DataLabNas/MAA/MAA2016-15 Supporting the Social Investment Unit/constructing households/rprogs/'

file_order = c(
  "data_prep_notifications",
  "data_prep_validation",
  "individual_address_analysis",
  "group_address_analysis",
  "validation_suite"
)

for(file in file_order){
  source(paste0(path, file, ".R"))
}