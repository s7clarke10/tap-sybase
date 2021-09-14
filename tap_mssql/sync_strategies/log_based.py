#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression

import copy
import singer
from singer import metadata
from singer.schema import Schema
import binascii

import tap_mssql.sync_strategies.common as common

from tap_mssql.connection import connect_with_backoff, MSSQLConnection

LOGGER = singer.get_logger()

def py_bin_to_mssql(binary_value):
    return "CONVERT(BINARY(10),'0x" + binary_value + "',1)"

def verify_change_data_capture_table(connection, schema_name, table_name):
    cur = connection.cursor()
    cur.execute("""select s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc, t.object_id
                   from sys.tables t
                   join sys.schemas s on (s.schema_id = t.schema_id)
                   and  t.name = '{}'
                   and  s.name = '{}'""".format(table_name,schema_name)
               )
    row = cur.fetchone()
        
    return row[2]

def verify_change_data_capture_databases(connection):
    cur = connection.cursor()
    cur.execute("""SELECT name, is_cdc_enabled
                   FROM sys.databases WHERE database_id = DB_ID()"""
               )
    row = cur.fetchone()

    LOGGER.info(
        "CDC Databases enable : Database %s, Enabled %s", *row,
    )
    return row

# def get_object_id_by_table(connection, dbname, schema_name, table_name):
#     cur = connection.cursor()
#     query = "SELECT OBJECT_ID(N'" + dbname + "." + schema_name + "." + table_name + "') AS 'Object_ID'"
#     print(query)
#     cur.execute(query)
#     row = cur.fetchone()

#     LOGGER.info(
#         "Current Object ID : %s", *row,
#     )
#     return row

def get_lsn_extract_range(connection, schema_name, table_name,last_extract_datetime):
    cur = connection.cursor()
    query = """DECLARE @load_timestamp datetime
               SET @load_timestamp = '{}'

               SELECT min(sys.fn_cdc_map_lsn_to_time(__$start_lsn)) lsn_from_datetime
                    , max(sys.fn_cdc_map_lsn_to_time(__$start_lsn)) lsn_to_datetime
                    , min(__$start_lsn) lsn_from
                    , max(__$start_lsn) lsn_to
                    , max(replace(replace( convert(varchar(8),sys.fn_cdc_map_lsn_to_time(__$start_lsn),112) + convert(varchar(12),sys.fn_cdc_map_lsn_to_time(__$start_lsn),114), ':',''), ' ','')) lsn_to_string
               FROM cdc.{}_{}_CT
               WHERE __$operation != 3
               AND __$Start_lsn >= sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @load_timestamp )
               ;
            """.format(str(last_extract_datetime),schema_name,table_name)
    cur.execute(query)
    row = cur.fetchone()

    if row[2] is None:   # Test that the lsn_from is not NULL i.e. there is change data to process
       ### TO_DO:  1. Modify the query to not have a where clause
       ###         2. Use a SCN rather that date time as it is more accurate
       ###         3. Check if the SCN from the state file is between the lsn_from and lsn_to
       ###         4. Raise an error if last SCN is outside the range and there is change data available. This means the CDC has been purged in the source system. A full extract will be required.
       LOGGER.info("No data available to process in CDC table cdc.%s_%s_CT", schema_name, table_name)
    else:
       LOGGER.info("Data available in cdc table cdc.%s_%s_CT from lsn %s", schema_name, table_name, row[3])

    return row

# def get_from_lsn(connection, capture_instance_name ):
#     cur = connection.cursor()
#     query = """select sys.fn_cdc_get_min_lsn ( '{}' ) """.format(capture_instance_name)
#     print(query)
#     cur.execute(query)
#     row = cur.fetchone()

#     LOGGER.info(
#         "Current LSN ID : %s", *row,
#     )
#     return row 

def get_to_lsn(connection):
    cur = connection.cursor()
    query = """select sys.fn_cdc_get_max_lsn () """

    cur.execute(query)
    row = cur.fetchone()

    LOGGER.info(
        "Max LSN ID : %s", *row,
    )
    return row 

def add_synthetic_keys_to_schema(catalog_entry):
   catalog_entry.schema.properties['_cdc_operation_type'] = Schema(
            description='Source operation I=Insert, D=Delete, U=Update', type=['null', 'string'], format='string')  
   catalog_entry.schema.properties['_cdc_lsn_commit_timestamp'] = Schema(
            description='Source system commit timestamp', type=['null', 'string'], format='date-time')
   catalog_entry.schema.properties['_cdc_lsn_deleted_at'] = Schema(
            description='Source system delete timestamp', type=['null', 'string'], format='date-time')
   catalog_entry.schema.properties['_cdc_lsn_hex_value'] = Schema(
            description='Source system log sequence number (LSN)', type=['null', 'string'], format='string')            

   return catalog_entry          

# def get_min_valid_version(connection, dbname, schema_name, table_name):
#     cur = connection.cursor()
#     query = "SELECT OBJECT_ID(N'" + dbname + "." + schema_name + "." + table_name + "') AS 'Object_ID'"
#     print(query)
#     cur.execute(query)
#     row = cur.fetchone()

#     LOGGER.info(
#         "Current Object ID : %s", *row,
#     )
#     return row        

#    with connect_with_backoff(conn_config) as open_conn:
#        try:
#            with open_conn.cursor() as cur:
#                cur.execute("""select name, is_tracked_by_cdc
#                               from sys.tables t
#                               where is_tracked_by_cdc = 1"""
#                           )
#                row = cur.fetchone()
#                LOGGER.info(
#                    "CDC Tables : Table %s, Enabled %s", *row,
#                )
#                return row
#        except:
#            LOGGER.warning("Encountered error returning CDC tracking tables. Error: (%s) %s", *e.args)    

def generate_bookmark_keys(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get("replication-method")

    ### TO_DO: 
    ### 1. check the use of the top three values above and the parameter value, seem to not be required.
    ### 2. check the base_bookmark_keys required
    base_bookmark_keys = {
        "last_lsn_fetched",
        "max_lsn_values",
        "lsn",
        "version",
        "initial_full_table_complete",
    }

    bookmark_keys = base_bookmark_keys

    return bookmark_keys

def sync_historic_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    # Add additional keys to the columns
    extended_columns = columns + ['_cdc_operation_type', '_cdc_lsn_commit_timestamp', '_cdc_lsn_deleted_at', '_cdc_lsn_hex_value']    

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    initial_full_table_complete = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete"
    )

    state_version = singer.get_bookmark(state, catalog_entry.tap_stream_id, "version")

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream, version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:

            escaped_columns = [common.escape(c) for c in columns]
            table_name      = catalog_entry.table
            schema_name     = common.get_database_name(catalog_entry)

            if not verify_change_data_capture_table(mssql_conn,schema_name,table_name):
               raise Exception("Error {}.{}: does not have change data capture enabled. Call EXEC sys.sp_cdc_enable_table with relevant parameters to enable CDC.".format(schema_name,table_name))            

            # Store the current database lsn number, will use this to store at the end of the initial load.
            # Note: Recommend no transactions loaded when the initial loads are performed.
            lsn_to = str(get_to_lsn(mssql_conn)[0].hex())

            select_sql = """
                            SELECT {}
                                ,'I' _cdc_operation_type
                                , '1900-01-01 00:00:00' _cdc_lsn_commit_timestamp
                                , null _cdc_lsn_deleted_at
                                , 'Testing' _cdc_lsn_hex_value
                            FROM {}.{}
                            ;""".format(",".join(escaped_columns), schema_name, table_name)          
            params = {}

            common.sync_query(
                cur, catalog_entry, state, select_sql, extended_columns, stream_version, params
            )
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'lsn', lsn_to)

    # store the state of the table lsn's after the initial load ready for the next CDC run
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_pk_values")
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_pk_fetched")

    singer.write_message(activate_version_message)

def sync_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    # Add additional keys to the columns
    extended_columns = columns + ['_cdc_operation_type', '_cdc_lsn_commit_timestamp', '_cdc_lsn_deleted_at', '_cdc_lsn_hex_value']

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    initial_full_table_complete = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete"
    )

    state_version = singer.get_bookmark(state, catalog_entry.tap_stream_id, "version")

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream, version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:

            # start_lsn = min([singer.get_bookmark(state, s.tap_stream_id, 'lsn') for s in catalog_entry.stream])
            state_last_lsn = singer.get_bookmark(state, catalog_entry.tap_stream_id, "lsn")
            
            escaped_columns = [common.escape(c) for c in columns]
            escaped_table   = (catalog_entry.tap_stream_id).replace("-","_")
            table_name      = catalog_entry.table
            schema_name     = common.get_database_name(catalog_entry)

            if not verify_change_data_capture_table(mssql_conn,schema_name,table_name):
               raise Exception("Error {}.{}: does not have change data capture enabled. Call EXEC sys.sp_cdc_enable_table with relevant parameters to enable CDC.".format(schema_name,table_name))

            lsn_range = get_lsn_extract_range(mssql_conn, schema_name, table_name,"2016-08-01T23:00:00")

            if lsn_range[2] is not None:   # Test to see if there are any change records to process
               lsn_from = str(lsn_range[2].hex())
               lsn_to   = str(lsn_range[3].hex())

               if lsn_from <= state_last_lsn:
                  LOGGER.info("The last lsn processed as per the state file %s, minimum available lsn for extract table %s", state_last_lsn, lsn_from)
               else:
                  raise Exception("Error {}.{}: CDC changes have expired, the minimum lsn is %s, the last processed lsn is %s. Recommend a full load as there may be missing data.".format(schema_name, table_name, lsn_from, state_last_lsn ))                

               LOGGER.info("Here is the tracking tables results from_lsn_datetime %s, to_lsn_datetime %s, from_lsn %s, to_lsn %s, lsn_to_string %s", lsn_range[0],lsn_range[1],lsn_range[2],lsn_range[3],lsn_range[4])

               select_sql = """DECLARE @from_lsn binary (10), @to_lsn binary (10)
                    
                               SET @from_lsn = {}
                               SET @to_lsn = {}

                               SELECT {}
                                   ,case __$operation
                                       when 2 then 'I'
                                       when 4 then 'U'
                                       when 1 then 'D'
                                   end _cdc_operation_type
                                   , sys.fn_cdc_map_lsn_to_time(__$start_lsn) _cdc_lsn_commit_timestamp
                                   , case __$operation
                                       when 1 then sys.fn_cdc_map_lsn_to_time(__$start_lsn)
                                       else null
                                       end _cdc_lsn_deleted_at
                                   , 'Testing' _cdc_lsn_hex_value
                               FROM cdc.fn_cdc_get_all_changes_{}(@from_lsn, @to_lsn, 'all')
                               WHERE __$start_lsn > {} and __$start_lsn <= {}
                               ORDER BY __$seqval
                               ;""".format(py_bin_to_mssql(state_last_lsn), py_bin_to_mssql(lsn_to), ",".join(escaped_columns), escaped_table, py_bin_to_mssql(lsn_from), py_bin_to_mssql(lsn_to) )

               params = {}

               common.sync_query(
                   cur, catalog_entry, state, select_sql, extended_columns, stream_version, params
               )
            
               state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'lsn', lsn_to)
               singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max lsn value and last lsn fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_lsn_values")
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_lsn_fetched")

    singer.write_message(activate_version_message)