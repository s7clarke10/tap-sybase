#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression

import copy
import singer
from singer import metadata

import tap_mssql.sync_strategies.common as common

from tap_mssql.connection import connect_with_backoff, MSSQLConnection

LOGGER = singer.get_logger()

def get_change_data_capture_tables(connection):
    cur = connection.cursor()
    cur.execute("""select name, is_tracked_by_cdc
                               from sys.tables t
                               where is_tracked_by_cdc = 1"""
               )
    row = cur.fetchone()
        
    LOGGER.info(
        "CDC Tables : Table %s, Enabled %s", *row,
    )
    return row

def get_change_data_capture_databases(connection):
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


def sync_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
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
            # start_scn = min([get_bookmark(state, s.tap_stream_id, 'scn') for s in streams])
            # print('Stream ID : ',catalog_entry.tap_stream_id)
            escaped_columns = [common.escape(c) for c in columns]
            escaped_table   = (catalog_entry.tap_stream_id).replace("-","_")
            # print('Escaped Table : ',escaped_table)
            select_sql = """DECLARE @from_lsn binary (10), @to_lsn binary (10)
            
                            SET @from_lsn = sys.fn_cdc_get_min_lsn('{}')
                            SET @to_lsn = sys.fn_cdc_get_max_lsn()

                            SELECT {}
                            FROM cdc.fn_cdc_get_all_changes_{}(@from_lsn, @to_lsn, 'all')
                            ORDER BY __$seqval
                            ;""".format(escaped_table,",".join(escaped_columns),escaped_table)
            # print("Catalog Entry : ",catalog_entry)
            # print("Columns : ",columns)
            # print("Select SQL : ",select_sql)
            params = {}

            common.sync_query(
                cur, catalog_entry, state, select_sql, columns, stream_version, params
            )

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_pk_values")
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_pk_fetched")

    singer.write_message(activate_version_message)