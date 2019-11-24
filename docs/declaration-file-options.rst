Data Asset Declaration Files
============================

This page aims to give a short reference on the options for Airtunnel's data asset declaration files. Most importantly,
the name of each declaration file has to exactly reflect to the name we want to call each individual data asset.

The :doc:`tutorial <tutorial>` will also walk through the usage of these declarations in a more interactive approach.

type
~~~~
With any data asset declaration a type definition is mandatory.

Allowed values for the ``type`` property are: **ingested** and **derived**

type: derived
+++++++++++++
A declaration for a data asset of type *derived* does not need any further sections, i.e. a declaration file with
only

.. code:: yaml

   type: derived

will be valid.

However, for *derived* data assets the sections *load* and *extra* can be optionally used:

.. code:: yaml

   type: derived

   load:
         out_storage_format: csv
         out_compression_codec: gzip

   extra:
         maintainer: Tom


type: ingested
++++++++++++++
For ingested data assets, the section **ingest** is required. Additionally, the sections *load*, *transformation* and
*extra* are optional. Refer to the following chapters that explain these sections and give examples.


sections
~~~~~~~~
This chapter will give a reference on the declaration file sections with their properties and defaults.


ingest
++++++
Required declarations:

  -  *file_input_glob*: a string with a glob pattern to retrieve matching input files from the landing directory

Optional declarations:

  -  *in_storage_format*: The storage format of the input files. **csv|xls|parquet** , will default to: csv
  -  *archive_ingest*: Should the ingested data be archived? **True|False**, will default to: True


load
++++

Optional declarations:

  -  *out_storage_format*: The storage format to use when writing into the ready layer. **csv|parquet**, will default to:
     parquet
  -  *out_compression_codec*: The compression codec to use when writing into the ready layer. **gzip|snappy|none**,
     will default to: gzip
  -  *archive_ready*: Whether the pre-existing ready data should be archived on loaded. **True|False**, will default
     to: True
  -  *run_ddl*: Only for SQLDataAssets, and used by the SQLTransformationOperator. Indicates if a DDL file should
     always be run before running a DML script. **True|False**, will default to True
  -  *key_columns*: A list of key columns for this data asset. No default value.

Consider this example of a load section:

.. code:: yaml

    load:
      out_storage_format: parquet
      key_columns:
        - student_id
      out_compression_codec: gzip
      archive_ready: yes


transformation
++++++++++++++

Optional declarations:

  -  *in_column_renames*: Can be used to specify a rename map from source column names to target column names.
  -  *in_date_formats*: Can be used to specify a map of input data format strings per source column.

Consider this example of a transformation section to define a column rename from *student* to *student_first_name*:

.. code:: yaml

    transformation:
      in_column_renames:
        student_name: student_first_name


extra
+++++
Schemaless, non verified additional properties one wants to declare.
