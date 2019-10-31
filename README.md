# enhanced_IDI_address_and_household
Improving the quality of address and household information in the IDI

## Overview
This analysis seeks to improve the accuracy of address information within the IDI. It applies a series of rules to ensure address information from admin sources is consistent across admin sources and with Stats NZ surveys & censuses, and constructs a more accurate table of address.

This code should be read and used alongside the accompanying report **Constructing households from linked administrative data: Improving address information in the IDI**. As running the code requires both significant memory and processing, IDI researchers are advised to talk with SNZ or SIA about getting access to an existing (shared) version of the table before running this code to create their own version.

## Dependencies
It is necessary to have an IDI project if you wish to run the code. Visit the Stats NZ website for more information about this. This analysis has been developed for the IDI_Clean_20190420 refresh of the IDI. As changes in database structure can occur between refreshes, the initial preparation of the input information may require updating to run the code in other refreshes.

The R code makes use of several publicly available R packages. The version of some of these packages may be important. This analysis was conducted using `odbc` version 1.1.5, `DBI` version 0.8.0, `dplyr` version 0.7.6, and `dbplyr` version 1.2.2.

If applying this code to another environment other than the IDI, several features of the environment are required: First, R and some database manager (such as SQL Server) must be installed. Second, these must be configured such that R can pass commands to, and and retrieve results from, the database. Once the environment is configured correctly, then adjustments to the code in response to the new environment can be considered.

## Folder descriptions
This repositry contains all the core code to assemble the data and run the analysis.

* **documentation:** This folder contains documentation outlining the key files and processing.
* **rprogs:** This folder contains all the R scripts for executing each stage of the analysis and constructing the improved address table.
* **sql:** Several setup scripts and one closing script are stored here.

## Instructions to run the project

Prior to running the project be sure to review the associated report and documentation. The code flow document in the documentation file provides the run order for the project. The order is:

1. Setup SQL views for input data (administrative and validation).
	* setup_views_and_tables_individual_level.sql
	* setup_views_and_tables_household_level.sql
2. Load the input data into project tables for the purposes of analysis.
	* data_prep_notifications.R
	* data_prep_validation.R
3. Construct the improved address table.
	* individual_address_analysis.R
	* group_address_analysis.R
4. Assess the accuracy of the improved table, and create an output table that matches the format commonly used in the IDI.
	* validation_suite.R
	* final_table_to_snz_format.sql

The main purpose of step 1 is to convert the raw data into a standardised format that will be used for analysis. The final step in the process is to output the results to the format expected by other data users. Between these two points, the analysis expects and requires a standardised format.

Note that a variety of intermediate tables are saved during the construction process. Some of these tables are temporary and are deleted automatically once they are no longer required. Others persist beyond the end of the construction. We recommend that the database have at least four times the storage space of the input data available before conducting the analysis. Otherwise the analysis is likely to crash or freeze due to lack of memory. As most of the processing is pushed to the database, R memory is unlikely to be a constraint. Once the final table is produced manual removal of key intermediate tables is recommended.

## Getting Help
If you have any questions email info@sia.govt.nz

