# Setup Microsoft SQL Server Change Data Capture

## Why Log Based replication

Log based replication has a number of advantages over incremental key and full table replication.

Some of the key features include
1. Smaller batches of data to transfer - you are only shipping the changed data.
2. Incremental key can't detect deletes. If the application / database has deleted records. Then you must do a full table or CDC log based replication.
3. There is no special logic required to detect deletes. Delete records are shipped through when they occur.
4. There is an accurate timestamp available to indicate when records are changed in the database / application.

If you would like to learn more about Change Data Capture, please refer to the Microsoft Documentation below.

https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server

## Background

Microsoft has introduced two technologies for identifying and capturing changes to tables. The first technology introduced was Change Tracking with SQL Server 2005. This inbuilt technology
identifies rows which have changed. This approach has some downsides however as it doesn't capture the actual change data and it is a synchronous operation.

Change Data Capture (CDC) was introduced with SQL Server 2016 and is a free feature if you are licensed with an Enterprise Edition. The benefit of asynchronous processing and the ability to capture a
full image of the record is extremely useful. 

## Log_Based Approach

This implication of MSSQL log_based replication approach uses Change Data Capture only. It does not support Change Tracking due to the obvious limitations and performance overheads.

## Setup Example

The following example works with the MSSQL AdventureWorks2016 database. If you would like to try the example, please download and install the AdventureWorks database from here.

https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure

Feel free to adjust a number of the steps as required (for example more restrictive privileges, database ownership, files, and roles). To access the cdc tables use the CDC user which has privileges
to obtain the CDC data.

```sql

-- ====  
-- Enable Database for CDC template   
-- ====  

USE AdventureWorks2016 ;  

EXEC sp_changedbowner 'sa';

EXEC sys.sp_cdc_enable_db;  

-- ====  
-- Create a new File Group for the CDC tables   
-- ====  

USE AdventureWorks2016;

ALTER DATABASE AdventureWorks2016 ADD FILEGROUP CDC_FILEGROUP;

-- Add files to the filegroup

ALTER DATABASE AdventureWorks2016 ADD FILE ( NAME = cdc_files, FILENAME = 'C:\MySQLFiles\CDC_FILES' )to FILEGROUP CDC_FILEGROUP;

-- ====  
-- Enable Snapshot Isolation - Highly recommended for read consistency and to avoid dirty commits.   
-- ====  

-- https://www.sqlservercentral.com/articles/what-when-and-who-auditing-101-part-2
-- Consider running one of these options to ensure read consistency. Do consider the impact however of enabling these features.

ALTER DATABASE AdventureWorks2016
SET READ_COMMITTED_SNAPSHOT ON;

ALTER DATABASE AdventureWorks2016
SET ALLOW_SNAPSHOT_ISOLATION ON;

SELECT DB_NAME(database_id), 
    is_read_committed_snapshot_on,
    snapshot_isolation_state_desc
FROM sys.databases
WHERE database_id = DB_ID();

-- ====  
-- Create a new Role for accessing the CDC tables   
-- ====

USE AdventureWorks2016;

-- Create a new login

CREATE LOGIN CDC_USER WITH PASSWORD = '<cdc_user password>';

ALTER SERVER ROLE sysadmin ADD MEMBER CDC_USER;

-- Create a new user associated with the new login and access to the database

CREATE USER CDC_USER FOR LOGIN CDC_USER;

ALTER USER CDC_USER WITH DEFAULT_SCHEMA=dbo;

-- ====  
-- Create a new Role for accessing the CDC tables   
-- ====

CREATE ROLE CDC_Role;

ALTER ROLE CDC_Role ADD MEMBER CDC_USER;

-- Add explicit permissions to tracked tables, use this script to generate the required grant permissions

-- ====  
-- Add required CDC permissions to the CDC role   
-- ====



-- =========  
-- Enable CDC Tracking on tables Specifying Filegroup Option Template  
-- =========  
USE AdventureWorks2016  
;  
  
EXEC sys.sp_cdc_enable_table  
@source_schema = N'HumanResources',
@source_name   = N'Department',  
@role_name     = N'CDC_Role',  
@filegroup_name = N'CDC_FILEGROUP',  
@supports_net_changes = 1  
; 

EXEC sys.sp_cdc_enable_table  
@source_schema = N'HumanResources',
@source_name   = N'Employee',  
@role_name     = N'CDC_Role',  
@filegroup_name = N'CDC_FILEGROUP',  
@supports_net_changes = 1  
; 

EXEC sys.sp_cdc_enable_table  
@source_schema = N'Person',
@source_name   = N'Person',  
@role_name     = N'CDC_Role',  
@filegroup_name = N'CDC_FILEGROUP',  
@supports_net_changes = 1  
; 

EXEC sys.sp_cdc_enable_table  
@source_schema = N'Person',
@source_name   = N'BusinessEntity',  
@role_name     = N'CDC_Role',  
@filegroup_name = N'CDC_FILEGROUP',  
@supports_net_changes = 1  
; 

-- =========  
-- Fixing the logging if it was not working  
-- =========  

-- Stopping the Capture Process

-- Flushing the logs
EXEC sp_repldone @xactid = NULL, @xact_segno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sp_replflush;

-- Start the Capture Process

-- =========  
-- CDC Queries to check what is happening  
-- =========  

-- Check for CDC errors
select *
from sys.dm_cdc_errors;

-- Stop the CDC Capture Job
exec sys.sp_cdc_stop_job;

-- Start the CDC Capture Job
EXEC sys.sp_cdc_start_job;

-- Check that CDC is enabled on the database
SELECT name, is_cdc_enabled
FROM sys.databases WHERE database_id = DB_ID();

-- Check tables which have CDC enabled on them
select s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc, t.object_id
from sys.tables t
join sys.schemas s on (s.schema_id = t.schema_id)
where t.is_tracked_by_cdc = 1
;

```