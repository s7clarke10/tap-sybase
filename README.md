# tap-sybase

[Singer](https://www.singer.io/) tap that extracts data from a [sybase](https://infocenter.sybase.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md). This TAP was built against Sybase Version 16 also known as Adapter Server Enterprise or officially as SAP Adaptive Server Enterprise (ASE) 16.0. Compatibility with older versions of Sybase is unknown, I would recommend testing the TAP for compatibility.


This is a [PipelineWise](https://transferwise.github.io/pipelinewise) and [Meltano](https://meltano.com) compatible tap connector.

[![Open in Visual Studio Code](https://github.com/s7clarke10/tap-sybase/blob/master/static_images/Open_Visual_Studio_Code.svg)](https://open.vscode.dev/s7clarke10/tap-sybase)

## How to use it

If you want to run this [Singer Tap](https://singer.io) independently please read further.

## Usage

This section dives into basic usage of `tap-sybase` by walking through extracting
data from a table. It assumes that you can connect to and read from a Sybase
database.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
```

Then build the tap_sybase Python Application
```bash
  python setup.py install
```

### Have a source database

There's some important business data siloed in this sybase database -- we need to
extract it. Here's the table we'd like to sync:

```
sybase> select * from example_db.animals;
+----|----------|----------------------+
| id | name     | likes_getting_petted |
+----|----------|----------------------+
|  1 | aardvark |                    0 |
|  2 | bear     |                    0 |
|  3 | cow      |                    1 |
+----|----------|----------------------+
3 rows in set (0.00 sec)
```

### Create the configuration file

Create a config file containing the database connection credentials, e.g.:

```json
{
  "host": "localhost",
  "port": "2638",
  "user": "root",
  "password": "password",
  "database": "databasename"
}
```

Optional:

To filter the discovery to a particular schema within a database. This is useful if you have a large number of schemas and wish to speed up the discovery.

```json
{
  "filter_dbs": "your database schema name",
}
```

Optional:

To emit a date as a date without a time component or time without an UTC offset. This is helpful to avoid time conversions or to just work with a date datetype in the target database. If this boolean config item is not set, the default behaviour is `false` i.e. emit date datatypes as a datetime. It is recommended to set this on if you have time datetypes and are having issues uploading into into a target database.
```json
{
  "use_date_datatype": true
}
```

Optional:

Set the version of TDS to use when communicating with Sybase Server (the default is None). This is used by pymssql with connecting and fetching data from Sybase databases. See the [pymssql](https://pymssql.readthedocs.io/en/stable/index.html) documentation and [FreeTDS](https://www.freetds.org/) documentation for more details.
```json
{
  "tds_version": "7.3"
}
```

Optional:

The characterset for the database / source system. The default is `utf8`, however older databases might use a charactersets like [cp1252](https://en.wikipedia.org/wiki/Windows-1252) for the encoding. If you have errors with a `UnicodeDecodeError: 'utf-8' codec can't decode byte ....` then a solution is examine the characterset of the source database / system and make an appropriate substitution for utf8 like cp1252. 
```json
{
  "characterset": "utf8"
}
```

Optional:
To make use of fetchmany(x) instead of fetchone(), use cursor_array_size with an integer value indicating the number of rows to pull. This can help in some architectures by pulling more rows into memory. The default if omitted is 1, the tap will still use fetchmany, but with an argument of 1, under the assumption that fetchmany(1) === fetchone().
Usage:
```json
{
  "cursor_array_size": 10000
}
```

These are the same basic configuration properties used by the sybase command-line
client (`tap-sybase`).

### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-sybase --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": {
        "type": "object",
        "properties": {
          "name": {
            "inclusion": "available",
            "type": [
              "null",
              "string"
            ],
            "maxLength": 255
          },
          "id": {
            "inclusion": "automatic",
            "minimum": -2147483648,
            "maximum": 2147483647,
            "type": [
              "null",
              "integer"
            ]
          },
          "likes_getting_petted": {
            "inclusion": "available",
            "type": [
              "null",
              "boolean"
            ]
          }
        }
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": [
              "id"
            ],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
          }
        },
        {
          "breadcrumb": [
            "properties",
            "id"
          ],
          "metadata": {
            "sql-datatype": "int(11)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": [
            "properties",
            "name"
          ],
          "metadata": {
            "sql-datatype": "varchar(255)",
            "selected-by-default": true
          }
        },
        {
          "breadcrumb": [
            "properties",
            "likes_getting_petted"
          ],
          "metadata": {
            "sql-datatype": "tinyint(1)",
            "selected-by-default": true
          }
        }
      ],
      "stream": "animals"
    }
  ]
}

```

### Field selection

In sync mode, `tap-sybase` consumes the catalog and looks for tables and fields
have been marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-sybase -c config.json --discover > properties.json
```

Then edit `properties.json` to make selections. In this example we want the
`animals` table. The stream's metadata entry (associated with `"breadcrumb": []`)
gets a top-level `selected` flag, as does its columns' metadata entries. Additionally,
we will mark the `animals` table to replicate using a `FULL_TABLE` strategy. For more,
information, see [Replication methods and state file](#replication-methods-and-state-file).

```json
[
  {
    "breadcrumb": [],
    "metadata": {
      "row-count": 3,
      "table-key-properties": [
        "id"
      ],
      "database-name": "example_db",
      "selected-by-default": false,
      "is-view": false,
      "selected": true,
      "replication-method": "FULL_TABLE"
    }
  },
  {
    "breadcrumb": [
      "properties",
      "id"
    ],
    "metadata": {
      "sql-datatype": "int(11)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": [
      "properties",
      "name"
    ],
    "metadata": {
      "sql-datatype": "varchar(255)",
      "selected-by-default": true,
      "selected": true
    }
  },
  {
    "breadcrumb": [
      "properties",
      "likes_getting_petted"
    ],
    "metadata": {
      "sql-datatype": "tinyint(1)",
      "selected-by-default": true,
      "selected": true
    }
  }
]
```

### Sync mode

With a properties catalog that describes field and table selections, the tap can be invoked in sync mode:

```bash
$ tap-sybase -c config.json --properties properties.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

```json
{"value": {"currently_syncing": "example_db-animals"}, "type": "STATE"}

{"key_properties": ["id"], "stream": "animals", "schema": {"properties": {"name": {"inclusion": "available", "maxLength": 255, "type": ["null", "string"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}, "id": {"inclusion": "automatic", "minimum": -2147483648, "type": ["null", "integer"], "maximum": 2147483647}}, "type": "object"}, "type": "SCHEMA"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"record": {"name": "aardvark", "likes_getting_petted": false, "id": 1}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "bear", "likes_getting_petted": false, "id": 2}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"record": {"name": "cow", "likes_getting_petted": true, "id": 3}, "stream": "animals", "version": 1509133344771, "type": "RECORD"}

{"stream": "animals", "version": 1509133344771, "type": "ACTIVATE_VERSION"}

{"value": {"currently_syncing": "example_db-animals", "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}

{"value": {"currently_syncing": null, "bookmarks": {"example_db-animals": {"initial_full_table_complete": true}}}, "type": "STATE"}
```

## Replication methods and state file

In the above example, we invoked `tap-sybase` without providing a _state_ file
and without specifying a replication method. The two ways to replicate a given
table are `FULL_TABLE`, and `INCREMENTAL`. Note: `tap-sybase` was cloned from `tap-mssql`, and the `LOG_BASED` based replication is not available. Code and documentation left for a potential future update.

### Full Table

Full-table replication extracts all data from the source table each time the tap
is invoked.

### Log Based

NOTE: NOT IMPLEMENTED! Notes below are from TAP-MSSQL.

Log_Based replication extracts change data from the MS SQL Server Change Data Capture (CDC) tables, are a MS SQL feature. This code and documentation has been left in with the view that Sybase may have a CDC Log based replication method.

This method allows you to replicate just the changes to a table e.g. the Inserts, Deletes, and Updates. For this method to work you
must enrol the database in question and tables that you wish to replicate.

See : https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server for
more details.

Please Note: CDC is different to Change Tracking which is a older approach for tracking change. Log Based only works with CDC, it does
not work with Change Tracking!

To find out more about setting up CDC, refer to this page [MSSQL CDC Setup](MS_CDC_SETUP.md)

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked. This requires a replication key to be
specified in the table's metadata as well.

#### Example

Let's sync the `animals` table again, but this time using incremental
replication. The replication method and replication key are set in the
table's metadata entry in properties file:

```json
{
  "streams": [
    {
      "tap_stream_id": "example_db-animals",
      "table_name": "animals",
      "schema": { ... },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 3,
            "table-key-properties": [
              "id"
            ],
            "database-name": "example_db",
            "selected-by-default": false,
            "is-view": false,
            "replication-method": "INCREMENTAL",
            "replication-key": "id"
          }
        },
        ...
      ],
      "stream": "animals"
    }
  ]
}
```

We have no meaningful state so far, so just invoke the tap in sync mode again
without a state file:

```bash
$ tap-sybase -c config.json --properties properties.json
```

The output messages look very similar to when the table was replicated using the
default `FULL_TABLE` replication method. One important difference is that the
`STATE` messages now contain a `replication_key_value` -- a bookmark or
high-water mark -- for data that was extracted:

```json
{"type": "STATE", "value": {"currently_syncing": "example_db-animals"}}

{"stream": "animals", "type": "SCHEMA", "schema": {"type": "object", "properties": {"id": {"type": ["null", "integer"], "minimum": -2147483648, "maximum": 2147483647, "inclusion": "automatic"}, "name": {"type": ["null", "string"], "inclusion": "available", "maxLength": 255}, "likes_getting_petted": {"type": ["null", "boolean"], "inclusion": "available"}}}, "key_properties": ["id"]}

{"stream": "animals", "type": "ACTIVATE_VERSION", "version": 1509135204169}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 1, "name": "aardvark", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 2, "name": "bear", "likes_getting_petted": false}}

{"stream": "animals", "type": "RECORD", "version": 1509135204169, "record": {"id": 3, "name": "cow", "likes_getting_petted": true}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"version": 1509135204169, "replication_key_value": 3, "replication_key": "id"}}, "currently_syncing": null}}
```

Note that the final `STATE` message has a `replication_key_value` of `3`,
reflecting that the extraction ended on a record that had an `id` of `3`.
Subsequent invocations of the tap will pick up from this bookmark.

Normally, the target will echo the last `STATE` after it's finished processing
data. For this example, let's manually write a `state.json` file using the
`STATE` message:

```json
{
  "bookmarks": {
    "example_db-animals": {
      "version": 1509135204169,
      "replication_key_value": 3,
      "replication_key": "id"
    }
  },
  "currently_syncing": null
}
```

Let's add some more animals to our farm:

```
sybase> insert into animals (name, likes_getting_petted) values ('dog', true), ('elephant', true), ('frog', false);
```

```bash
$ tap-sybase -c config.json --properties properties.json --state state.json
```

This invocation extracts any data since (and including) the
`replication_key_value`:

```json
{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 3}}, "currently_syncing": "example_db-animals"}}

{"key_properties": ["id"], "schema": {"properties": {"name": {"maxLength": 255, "inclusion": "available", "type": ["null", "string"]}, "id": {"maximum": 2147483647, "minimum": -2147483648, "inclusion": "automatic", "type": ["null", "integer"]}, "likes_getting_petted": {"inclusion": "available", "type": ["null", "boolean"]}}, "type": "object"}, "type": "SCHEMA", "stream": "animals"}

{"type": "ACTIVATE_VERSION", "version": 1509135204169, "stream": "animals"}

{"record": {"name": "cow", "id": 3, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "dog", "id": 4, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "elephant", "id": 5, "likes_getting_petted": true}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}
{"record": {"name": "frog", "id": 6, "likes_getting_petted": false}, "type": "RECORD", "version": 1509135204169, "stream": "animals"}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": "example_db-animals"}}

{"type": "STATE", "value": {"bookmarks": {"example_db-animals": {"replication_key": "id", "version": 1509135204169, "replication_key_value": 6}}, "currently_syncing": null}}
```

---

Based on Stitch documentation

## Build Instructions

This section dives into basic commands to build `tap-sybase` if an alteration is made to the code.

### Setup Tools

You may need a copy of setup tools or an up to date version of setup tools to build `tap-sybase`

To do this follow these instructions.

```bash
  # Ensure you have first sourced the python virtual environment e.g.
  source venv/bin/activate

  python -m pip install --upgrade setuptools
```

### To build the tap

Run the following command each time you need to rebuild the tap.

```bash
$ python setup.py install
```

### Debugging in Visual Studio Code

To run the __init__.py python program in debug mode, you need to do the following two steps. Note: This was run within a Docker Container in Visual Studio Code.

1. Create a .vscode/launch.json file. Note: The parameters config.json and properties.json should point to the files you have generated in previous steps above.
   If you want to test state, include the state parameter as shown below and prepare an appropriate state file as per the instructions in an earlier section.

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Program",
            "type": "python",
            "request": "launch",
            "program": "${workspaceRoot}/tap_sybase/__init__.py",
            "args": [
                "-c", "config.json",
                "--properties", "properties.json"
                "--state", "state.json"
            ]
        }
    ]
}
```
2. Add a main entry to the __init__.py file to run interactively

Add the following lines to the end of the __init__.py in the tap_sybase directory.

```python

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
```
