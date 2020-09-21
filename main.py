import load_vendor_to_datalake, delete_data_datalake

#loading data from vendor source to our data mart
load_vendor_to_datalake

#deleting data from data marts that is older than X days
delete_data_datalake
