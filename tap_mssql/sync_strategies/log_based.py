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
        
    # LOGGER.info(
    #     "CDC Tables : Schema %s, Table %s, Enabled %s, Object %s", *row,
    # )
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

def get_object_id_by_table(connection, dbname, schema_name, table_name):
    cur = connection.cursor()
    query = "SELECT OBJECT_ID(N'" + dbname + "." + schema_name + "." + table_name + "') AS 'Object_ID'"
    print(query)
    cur.execute(query)
    row = cur.fetchone()

    LOGGER.info(
        "Current Object ID : %s", *row,
    )
    return row

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

    return row

def get_from_lsn(connection, capture_instance_name ):
    cur = connection.cursor()
    query = """select sys.fn_cdc_get_min_lsn ( '{}' ) """.format(capture_instance_name)
    print(query)
    cur.execute(query)
    row = cur.fetchone()

    LOGGER.info(
        "Current LSN ID : %s", *row,
    )
    return row 

def get_to_lsn(connection):
    cur = connection.cursor()
    query = """select sys.fn_cdc_get_max_lsn () """
    print(query)
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

def get_min_valid_version(connection, dbname, schema_name, table_name):
    cur = connection.cursor()
    query = "SELECT OBJECT_ID(N'" + dbname + "." + schema_name + "." + table_name + "') AS 'Object_ID'"
    print(query)
    cur.execute(query)
    row = cur.fetchone()

    LOGGER.info(
        "Current Object ID : %s", *row,
    )
    return row        

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

    base_bookmark_keys = {
        "last_pk_fetched",
        "max_pk_values",
        "version",
        "initial_full_table_complete",
    }

    bookmark_keys = base_bookmark_keys

    return bookmark_keys


def sync_table(mssql_conn, config, catalog_entry, state, columns, extended_columns, stream_version):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    ### BEGIN NEW LOGIC FOR CDC
    # result = get_change_data_capture_tables(mssql_conn)    
    # LOGGER.info("Here is the tracking tables results %s", result)

    # result2 = get_change_data_capture_databases(mssql_conn)
    # LOGGER.info("Here is the tracking databases results %s", result2)

    # AdventureWorks2016.HumanResources.Department
    # result3 = get_object_id_by_table(mssql_conn, "AdventureWorks2016", "HumanResources", "Department")
    # LOGGER.info("Here is the table object id result %s", result3)
    

    # from_lsn = get_from_lsn(mssql_conn, "HumanResources_Department")
    # LOGGER.info("Here is the table min lsn id result %s", from_lsn)

    # to_lsn = get_to_lsn(mssql_conn)
    # LOGGER.info("Here is the table max lsn id result %s", to_lsn)

    ### END LOGIC FOR CDC

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
            # select_sql = common.generate_select_sql(catalog_entry, columns)
            # start_lsn = min([singer.get_bookmark(state, s.tap_stream_id, 'lsn') for s in catalog_entry.stream])
            start_lsn = singer.get_bookmark(state, catalog_entry.tap_stream_id, "lsn")
            # print(start_lsn)
            
            escaped_columns = [common.escape(c) for c in columns]
            escaped_table   = (catalog_entry.tap_stream_id).replace("-","_")
            table_name      = catalog_entry.table
            schema_name     = common.get_database_name(catalog_entry)

            if not verify_change_data_capture_table(mssql_conn,schema_name,table_name):
               raise Exception("Error {}.{}: does not have change data capture enabled. Call EXEC sys.sp_cdc_enable_table with relevant parameters to enable CDC.".format(schema_name,table_name))

            lsn_range = get_lsn_extract_range(mssql_conn, schema_name, table_name,"2016-08-01T23:00:00")
            lsn_from = str(lsn_range[2].hex())
            lsn_to   = str(lsn_range[3].hex())

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
                            WHERE __$start_lsn > {}
                            ORDER BY __$seqval
                            ;""".format(py_bin_to_mssql(lsn_from), py_bin_to_mssql(lsn_to), ",".join(escaped_columns), escaped_table, py_bin_to_mssql(lsn_from) )

            params = {}

            common.sync_query(
                cur, catalog_entry, state, select_sql, extended_columns, stream_version, params
            )
            
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id, 'lsn', lsn_to)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max pk value and last pk fetched upon successful sync
    # singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_pk_values")
    # singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_pk_fetched")

    singer.write_message(activate_version_message)