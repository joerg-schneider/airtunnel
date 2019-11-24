Physical data store setup
=========================

Folder setup
~~~~~~~~~~~~
In plain terms of a folder hierarchy, please lay out your physical data store as follows (read section below on why):

.. code:: shell

    ├── archive
    ├── ingest
    │   ├── archive
    │   └── landing
    ├── ready
    └── staging
        ├── intermediate
        ├── pickedup
        └── ready


Folder meanings and motivation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each data layer must store data persistently and efficiently. This is why the physical data store - be it a SQL system,
an S3 bucket or cloud-enabled data lake service - is a key building block, worthy of a rigorous structure.
In a scalable, filesystem-like storage medium, for example, a common approach to structure includes:

**ingest**: Raw files as received from sources

  -  **landing**: Incoming data files will be 1:1 loaded in here and suffixed with a timestamp of arrival
  -  **archive**: Files from landing that have been consumed by a pipeline will be moved in here

**ready**: Processed data assets that are ready to be read from, each one being in its own folder.
Ready assets may possibly be further partitioned using a Hive/Spark style folder convention, according to which
each subfolder is named using the partition predicate (useful to seamlessly read data assets using Hive,
Spark or PyArrow).

**staging**: Files in staging are not for general consumption because they are incomplete or currently being worked on.
It is further broken down into **pickedup** (data that was moved there from ingest/landing), **intermediate** (any
kind of temporary data for intermediate processing temps) and **ready** (where the next version for the ready layer
is produced).

Airtunnel's load operator will use an atomic move operation (or SQL transaction) to push finished data from
*staging/ready* to *ready*, so consumers will never run into access issues or tap into half-finished files.

**archive**: Whenever a new version of an asset in ready has been computed and it is valuable to keep a copy of
the previous run, move it here under [asset-name]/[load-time]/.

**export** (optional): This is for files to be exported to other consumers and that will never be re-introduced
into data assets contained in the folders above. Examples include final csv-exports, front-end specific
data and reports.

Physical data store adapters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By default Airtunnel currently uses the local filesystem as the physical data store, it can be however extended to use
for example cloud storage providers, see :doc:`Extending Airtunnel <extending>`.

