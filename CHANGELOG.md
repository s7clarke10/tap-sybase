# Changelog

## 1.0.3
 * Correctly output Date, Time, and Timestamp - use SQL to work-around TDS limitations
 * Working option to emit Dates as Dates without Timestamp: "use_date_datatype": true
 
## 1.0.2
 * Cleaning up imports
 * Supporting Sybase ASA / IQ data dictionary as well as ASE.
 * Putting in default replication method
 * Using common.get_key_properties function to get primary keys
 * Passing config as a parameter to functions
 * General Hygine
## 1.0.1
 * Raise Error if LOG_BASED replication method is used. Log based is not available under Sybase.

## 1.0.0
 * Original Release supporting extracts from Sybase ASE / Sybase 16.