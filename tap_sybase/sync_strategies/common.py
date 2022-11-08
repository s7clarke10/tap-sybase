#!/usr/bin/env python3
# pylint: disable=too-many-arguments,duplicate-code,too-many-locals

import copy
import datetime
import singer
import time
import pytz
import uuid

import singer.metrics as metrics
from singer import metadata
from singer import utils

LOGGER = singer.get_logger()

def escape(string):
    if "`" in string:
        raise Exception(
            "Can't escape identifier {} because it contains a double quote".format(string)
        )
    return '"' + string + '"'


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + "-" + table_name


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, "version")

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), "selected")

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ("properties", property_name), "inclusion"),
        metadata.get(md_map, ("properties", property_name), "selected"),
        True,
    )


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get("is-view")


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get("database-name")


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get("view-key-properties", [])
    else:
        key_properties = stream_metadata.get("table-key-properties", [])

    return key_properties

def prepare_columns_sql(catalog_entry, c, use_date_data_type_format):
    """
    Places double quotes around the columns to be selected and does any required
    sql conversion of data to strings.

    Args:
        catalog_entry: required - The full catalog including the schema definition
        c: required - the column being converted
        use_date_data_type_format: required - indicates if dates should be timestamps
    
    Raises:
        Exception if it contains an invalid quote i.e. `

    Returns:
        Formatted column for database select statement.

    Notes:
    Converts Dates, Datetime, Timestamp, and Time to a string because the pymssql / tds
    support for these datatypes is limited.
    This function also uses an estoric conversion method of converting the datetimes to
    a string because of limited date formats on older versions of Sybase.
    This code works with Sybase ASA and very old versions of Sybase ASE.

    Note during the conversion of the datetimes there is a limit to three places in the
    microseconds. Internally there are six places but Sybase only displays up to three
    microseconds.
    """
    if "`" in c:
        raise Exception(
            "Can't escape identifier {} because it contains a double quote".format(c)
        )
    column_name = """ "{}" """.format(c)
    schema_property = catalog_entry.schema.properties[c]
    sql_data_type = ""
    # additionalProperties is used with singer.decimal to contain scale/precision
    # in those cases, there will not be an sql_data_type value in the schema
    if schema_property.additionalProperties:
        sql_data_type = schema_property.additionalProperties.get('sql_data_type',"")

    if 'string' in schema_property.type and schema_property.format == 'time':
        return "convert(char, {} , 140)".format(column_name)
    elif 'string' in schema_property.type and schema_property.format == 'date-time':
        if sql_data_type == 'date' and not use_date_data_type_format:
            return f"""case when {column_name} is not null then
                      substring(convert(Char, {column_name}, 102),1,4)||'-'
                    ||substring(convert(Char, {column_name}, 102),6,2)||'-'
                    ||substring(convert(Char, {column_name}, 102),9,2)||'T00:00:00+00:00'
                    else null end
                    """
        else:
            return f"""case when {column_name} is not null then
                     substring(convert(Char, {column_name}, 102),1,4)||'-'
                    ||substring(convert(Char, {column_name}, 102),6,2)||'-'
                    ||substring(convert(Char, {column_name}, 102),9,2)||'T'
                    ||substring(convert(Char, {column_name}, 108),1,2)
                    ||substring(convert(Char, {column_name}, 109),15,6)||'.'
                    ||substring(convert(Char, {column_name}, 109),22,3)||'+00:00'
                    else null end
                    """
    elif 'string' in schema_property.type and schema_property.format == 'date':
        if use_date_data_type_format:
            return "convert(char, {} , 140)".format(column_name)
        else:
            return f"""case when {column_name} is not null then
                      substring(convert(Char, {column_name}, 102),1,4)||'-'
                    ||substring(convert(Char, {column_name}, 102),6,2)||'-'
                    ||substring(convert(Char, {column_name}, 102),9,2)||'T00:00:00+00:00'
                    else null end
                    """
    return column_name

def generate_select_sql(catalog_entry, columns, config):
    use_date_data_type_format = config.get("use_date_datatype") or default_date_format()
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = map(lambda c: prepare_columns_sql(catalog_entry, c, use_date_data_type_format), columns)

    select_sql = "SELECT {} FROM {}.{}".format(",".join(escaped_columns), escaped_db, escaped_table)

    # escape percent signs
    select_sql = select_sql.replace("%", "%%")

    return select_sql

def default_date_format():
    return False

def row_to_singer_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        property_format = catalog_entry.schema.properties[columns[idx]].format
        if isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            row_to_persist += (timedelta_from_epoch.isoformat() + "+00:00",)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False, 1 as True and anything else as hex
            if elem == b"\x00":
                boolean_representation = False
                row_to_persist += (boolean_representation,)
            elif elem == b"\x01":
                boolean_representation = True
                row_to_persist += (boolean_representation,)
            else:
                row_to_persist += (str(elem.hex()),)

        elif "boolean" in property_type or property_type == "boolean":
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)
        elif isinstance(elem, uuid.UUID):
            row_to_persist += (str(elem),)
        elif property_format == 'singer.decimal':
            if elem is None:
                row_to_persist += (elem,)
            else:
                row_to_persist += (str(elem),)
        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=catalog_entry.stream, record=rec, version=version, time_extracted=time_extracted
    )


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [
        non_whitelisted_bookmark_key
        for non_whitelisted_bookmark_key in state.get("bookmarks", {}).get(tap_stream_id, {}).keys()
        if non_whitelisted_bookmark_key not in bookmark_key_set
    ]:
        singer.clear_bookmark(state, tap_stream_id, bk)

def ResultIter(cursor, arraysize):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params, config):
    replication_key = singer.get_bookmark(state, catalog_entry.tap_stream_id, "replication_key")

    arraysize = int(config.get("cursor_array_size", "1"))
    select_sql = select_sql.replace('"', '')

    time_extracted = utils.now()
    cursor.execute(select_sql, params)

    LOGGER.info(f"{arraysize=}")
    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    with metrics.record_counter(None) as counter:
        counter.tags["database"] = database_name
        counter.tags["table"] = catalog_entry.table

        for row in ResultIter(cursor,arraysize):
            counter.increment()
            rows_saved += 1
            record_message = row_to_singer_record(
                catalog_entry, stream_version, row, columns, time_extracted
            )
            singer.write_message(record_message)
            md_map = metadata.to_map(catalog_entry.metadata)
            stream_metadata = md_map.get((), {})
            replication_method = stream_metadata.get("replication-method")

            if replication_method == "FULL_TABLE":
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = singer.get_bookmark(
                    state, catalog_entry.tap_stream_id, "max_pk_values"
                )

                if max_pk_values:
                    last_pk_fetched = {
                        k: v for k, v in record_message.record.items() if k in key_properties
                    }

                    state = singer.write_bookmark(
                        state, catalog_entry.tap_stream_id, "last_pk_fetched", last_pk_fetched
                    )

            elif replication_method == "LOG_BASED":
                key_properties = get_key_properties(catalog_entry)

                max_lsn_values = singer.get_bookmark(
                    state, catalog_entry.tap_stream_id, "max_lsn_values"
                )

                if max_lsn_values:
                    last_lsn_fetched = {
                        k: v for k, v in record_message.record.items() if k in key_properties
                    }

                    state = singer.write_bookmark(
                        state, catalog_entry.tap_stream_id, "last_lsn_fetched", last_lsn_fetched
                    )

            elif replication_method == "INCREMENTAL":
                if replication_key is not None:
                    state = singer.write_bookmark(
                        state, catalog_entry.tap_stream_id, "replication_key", replication_key
                    )

                    state = singer.write_bookmark(
                        state,
                        catalog_entry.tap_stream_id,
                        "replication_key_value",
                        record_message.record[replication_key],
                    )
            if rows_saved % 1000 == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
