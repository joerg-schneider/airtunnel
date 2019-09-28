# airtunnel
Airtunnel is a means of supplementing [Apache Airflow](https://github.com/apache/airflow), a platform for workflow
automation in Python which is angled at analytics/data pipelining. It was born out of years of project experience in
data science, and the hardships of running large data platforms in real life businesses. Hence, Airtunnel is both a
set of principles (read more on them [in the official airtunnel announcement](https://medium.com)) and a lightweight
Python library *to tame your airflow*!

## Why choose airtunnel? Because you will…
:heart: …stop worrying and love the uncompromised consistency

:rocket: …need a clean codebase with separated concerns to be scalable

:memo: …get metadata for ingested files, load status and lineage out-of-the-box

:running: …have it up and running in minutes

:beer: …spend less time debugging Airflow DAGs doing worthwhile things instead

## Getting started
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

## Table of Contents
- [Installation requirements](#installation-requirements)
- [Installation and setup](#installation-and-setup)
- [Tutorial: loading the university data model](#tutorial-loading-the-university-data-model)
- [Known limitations](#known-limitations)
- [Design Principles](#design-principles)
- [Architecture: modules & classes](architecture-modules-classes)
- [Contributing to Airtunnel](#contributing-to-airtunnel)

### Installation requirements
- **Python >= 3.6 and Airflow >=1.10**: we assume Airtunnel is implemented best early on in a project, which is why going
with a recent Python and Airflow version makes the most sense. In the future we might do more tests and include coverage
for older Airflow versions.

### Installation and setup
1) We suppose you have installed Apache Airflow in a Python virtual environment. From there, simply do a 
`pip install airtunnel` to get the package.

2) Configure your codebase according to the Airtunnel principles: You need to add three folders for a
declaration store, a scripts store and finally the data store:

    2.1) The declaration store folder has no subfolders. It is where your data asset declarations
     (YAML files) will reside
     
    2.2) The scripts store folder is where all your Python & SQL scripts to process data assets will reside.
     It should be broken down by subfolders `py` for Python scripts and `sql` for SQL scripts. Please further add
     subfolders `dml` and `ddl` into the `sql` script folder.
     
    2.3) The data store folder follows a convention as well. 

3) Configure Airtunnel by extending your existing `airflow.cfg`

    3.1) Add the configuration section `[airtunnel]` in which,
    you need to add three configuration keys.

    3.2) add `declarations_folder` which takes the absolute path to the folder you set up in 2.1
    
    3.3) add `scripts_folder` which takes the absolute path to the folder you set up in 2.2
 
    3.4) add `data_store_folder`, which takes the absolute path to the folder you set up in 2.3
    for your data store

### Tutorial: loading the university data model
In the following section we will show a toy example, on how a very simple DAG using Airtunnel can be built.

#### Objective of the DAG workflow
For the ingested data assets, the example DAG will:
- sense for input data for a Data Asset in the form of csv, using a glob pattern
- move the input file(s) to a staging location
- prepare a new version of the Data Asset by applying the specified transformations, which are deduplication and renames
- store the new version on a staging location
- atomically load the new data into the physical data store
- archive the previous version of the data asset, using the DAG's execution date
- update Airtunnel's load status, file ingestion and lineage metadata
- archive the ingested files using the DAG's execution date

Subsequently, it will update the derived data asset.

#### Data Assets
The four relevant data assets for the university toy example are: Student, Programme, Enrollment and Enrollment Summary.

Student, Programme and Enrollment are ingested assets we receive as a flat file csv, "Enrollment summary" is a derived
asset we can build by joining them together and then counting per student major subject and program, 
how many students are enrolled. 

We start by declaring the three ingested data assets. The first one is `student`, therefore let's place a `student.yaml`
in the declaration store with the following contents:

```yaml
type: ingested

ingest:
  in_storage_format: csv
  file_input_glob: student*
  archive_ingest: yes

transformation:
  in_column_renames:
    student_name: student_first_name

load:
  out_storage_format: parquet
  key_columns:
    - student_id
  out_compression_codec: gzip
  archive_ready: yes
```

:+1:Great! This is a very readable description of important parameters, that influence how we process this data asset.
Actually, we can leverage some of the defaults and can be less explicit when we describe `programme` (in its own
`programme.yaml`):
````yaml
type: ingested

ingest:
  file_input_glob: programme*

load:
  key_columns:
    - programme_id
````

...and `enrollment` (in its own `enrollment.yaml`):

````yaml
type: ingested

ingest:
  file_input_glob: enrollment_*


load:
  key_columns:
    - student_id
    - programme_id
````
Here we have specified some key-columns we want to later use for deduplication purposes.

The last data asset to describe is `enrollment_summary`, a derived data asset with the following description:

```yaml
type: derived

```

This description is super compact as we simply inherit all the defaults (parquet, gzip and archival) for loading.

Wonderful, this is it in terms of declarations and we are ready to go to write some Pandas scripts! :rocket:

#### Data Asset scripts
To process data assets, we want to use Pandas. Hence, we will create four Python modules - one for each asset. All of
these need to implement a method of the following signature:

```python
from airtunnel import PandasDataAsset
def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    pass
```

Let's start by specifying the script for the student asset, `student.py`:

```python
from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):

    student_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    student_data = asset.rename_fields_as_declared(student_data)

    PandasDataAssetIO.write_data_asset(asset=asset, data=student_data)
``` 

We can see, that we make use of the lightweight class `PandasDataAssetIO` which helps to translate data asset declarations
around storage into Pandas commands. Similarly, column renames based upon declarations are a one-liner delegated to
the PandasDataAsset implementation. Don't worry - in case you need special properties, both `read_data_asset()` and
`write_data_asset()` optionally except additional keyword arguments that will be passed to the Pandas function. Or, just
do not use `PandasDataAssetIO` at all - `rebuild_for_store` can be implemented as you wish.

Mostly similar, the script `programme.py` looks like this:

````python
from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    programme_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )
    programme_data = programme_data.drop_duplicates(
        subset=asset.declarations.key_columns
    )
    PandasDataAssetIO.write_data_asset(asset=asset, data=programme_data)
````

Here we can see, that the script makes use of the declared key-columns to de-duplicate the inputs.

The Python script `enrollment.py` looks like this:

```python
from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    enrollment_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    PandasDataAssetIO.write_data_asset(asset=asset, data=enrollment_data)
```

This is as straight forward as it gets, just reading in input data and writing it with the output format.

More interesting is the script `enrollment_summary.py` that performs the aggregation:

````python
import pandas as pd
from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    student = PandasDataAsset(name="student")
    programme = PandasDataAsset(name="programme")
    enrollment = PandasDataAsset(name="enrollment")

    student_df = student.retrieve_from_store(airflow_context, consuming_asset=asset)
    programme_df = programme.retrieve_from_store(airflow_context, consuming_asset=asset)
    enrollment_df = enrollment.retrieve_from_store(
        airflow_context, consuming_asset=asset
    )

    enrollment_summary: pd.DataFrame = enrollment_df.merge(
        right=student_df, on=student.declarations.key_columns
    ).merge(right=programme_df, on=programme.declarations.key_columns)

    enrollment_summary = (
        enrollment_summary.loc[:, ["student_major", "programme_name", "student_id"]]
        .groupby(by=["student_major", "programme_name"])
        .count()
    )

    PandasDataAssetIO.write_data_asset(asset=asset, data=enrollment_summary)
````

Several things happen there:
- we can see how easy it is, to actually retrieve data from the *ready* layer of the data store: we define the data
asset instance and call the `retrieve_from_store()` method.
- additionally, when doing the above, we pass in the consuming asset - this will trigger a lineage collection and record
data source, data target, dag id and task id.
- finally we do the aggregation by joining (on the keys that we can retrieve from the declaration) and store the data

#### The final DAG
Here comes the great part - assembling the scripts we prepared above into the final DAG. For this, we leverage the data
assets with their declarations, in addition to several custom operators (introduced in detail below) that Airtunnel
provides.

````python
from datetime import datetime
from airflow.models import DAG
from airtunnel import PandasDataAsset
from airtunnel.operators.archival import DataAssetArchiveOperator, IngestArchiveOperator
from airtunnel.operators.ingestion import IngestOperator
from airtunnel.operators.loading import StagingToReadyOperator
from airtunnel.operators.transformation import PandasTransformationOperator
from airtunnel.sensors.ingestion import SourceFileIsReadySensor

student = PandasDataAsset("student")
programme = PandasDataAsset("programme")
enrollment = PandasDataAsset("enrollment")
enrollment_summary = PandasDataAsset("enrollment_summary")

with DAG(
    dag_id="university",
    schedule_interval=None,
    start_date=datetime(year=2019, month=9, day=1),
) as dag:
    ingested_ready_tasks = set()

    # a common stream of tasks for all ingested assets:
    for ingested_asset in (student, programme, enrollment):
        source_is_ready = SourceFileIsReadySensor(asset=ingested_asset)
        ingest = IngestOperator(asset=ingested_asset)
        transform = PandasTransformationOperator(asset=ingested_asset)
        archive = DataAssetArchiveOperator(asset=ingested_asset)
        staging_to_ready = StagingToReadyOperator(asset=ingested_asset)
        ingest_archival = IngestArchiveOperator(asset=ingested_asset)

        dag >> source_is_ready >> ingest >> transform >> archive >> staging_to_ready >> ingest_archival

        ingested_ready_tasks.add(staging_to_ready)

    # upon having loaded the three ingested assets, connect the aggregation downstream to them:
    build_enrollment_summary = PandasTransformationOperator(asset=enrollment_summary)
    build_enrollment_summary.set_upstream(ingested_ready_tasks)

    staging_to_ready = StagingToReadyOperator(asset=enrollment_summary)

    dag >> build_enrollment_summary >> staging_to_ready
````
**Look how clean this DAG is** - it fully conveys what actually happens and with which dependencies. 

Notice something special? Yes - we have never actually defined a `task_id` with these custom Airtunnel operators. If we don't, Airtunnel will derive
the operator task_ids from the given data asset's name. An easy way that yields consistent naming! :+1:

Graphically the finished DAG looks like this:

![alt text](docs/assets/university-dag.png "University DAG")

## Known limitations
Airtunnel is still a very young project - there are several limitations:
- ff
- ff
- ff

## Design Principles
Taken from the [the official airtunnel announcement](https://medium.com).

### Uncompromised Consistency

**todo: add finished section of article in here**

### Declarative first and low-code redundancy

**todo: add finished section of article in here**


### Metadata driven

**todo: add finished section of article in here**

## Architecture: modules & classes
The airtunnel architecture in terms of Python modules and classes is as follows:
![alt text](docs/assets/airtunnel-arch.png "Airtunnel architecture")

- **Data Assets**: The data asset abstraction is at the heart of airtunnel - its base class bundles several common
operations, like finding data asset paths (by strictly inferring them from data asset names, and DAG-execution dates, if
versioned). Therefor by following the convention, the unique data asset name suffices to instantiate a data asset.
The BaseDataAsset is subclassed by: *Pandas Data Asset*, *SQL Data Asset* and *PySpark Data Asset*. The striking difference
there being obviously the means of accessing/processing the data asset instance at hand. For that the core abstract
functions `rebuild_for_store` and `retrieve_from_store` are implemented differently.

    - *PandasDataAsset*: To rebuild from store, this class will check in the Airtunnel scripts store in the subfolder
    `py` for a Python module named `[data_asset_name].py`. It will dynamically import this module and will expect and 
    call the function `rebuild_for_store(asset: PandasDataAsset, airflow_context)` to perform the rebuilding. As we
    want this to be mostly run by Airflow DAGs, we pass in the Airflow context to be able to leverage that.
     
    - *SQLDataAsset*: This data asset class will take an Airflow `DbApiHook` instance in its constructor in addition to
    the data asset name. It will leverage this hook for all operations in interaction with the database where
    this data asset resides. To rebuild from store, it will dynamically fetch SQL scripts from the scripts store under
    the subfolder SQL in each `ddl` and `dml` respectively, that are named as `[data_asset_name].sql`.
    
    - *PySparkDataAsset*: Similar to PandasDataAsset, but would work with `pyspark.sql.DataFrame` - to be developed

- **Data Asset Declarations**: To safely interact with the declaration store made up of YAML files, the class 
`DataAssetDeclaration` wraps all operations. The entrypoint to it is simply by calling the constructor with the name
of the data asset one would like to retrieve declarations for. Then, the YAML file is fetched and validated using 
several defined `DeclarationSchemas`, which make use of the helper library [schema](https://github.com/keleshev/schema).

- **Metadata Entities & Adapter**: Airtunnel attempts to make Airflow metadata-driven. To accomplish this, we add two
concepts: metadata entities (the types of metadata information we want to interact with) and metadata adapters
 (means of persistently storing and retrieving metadata entities). As of now, Airtunnel adds metadata entities `Lineage`,
 `LoadStatus` and `IngestedFileMetadata` which should be self-explanatory. The current sole implementation of
 `BaseMetaAdapter` is `SQLMetaAdapter`, which takes an Airflow `DbApiHook` and uses SQLAlchemy to read/write metadata.
 We believe it makes a lot of sense to simply store this metadata on the Airflow DB, to have everything in one place.
 This class model though is intended to, by subclassing, to read/write metadata using other means.
- **Custom Operators**: Airtunnel ships various custom operators; the most important ones for you to know being:
`PandasTransformationOperator`, `SQLTransformationOperator` and `PySparkTransformationOperator` that relate one-by-one
to the corresponding data asset types introduced earlier. They are very lightweight and do what you can guess from their
name: each one is instantiated (as part of a DAG) with an Data Asset instance of their type and on operator execute,
will call the `rebuild_for_store` function, with the current Airflow context as a parameter. 
*Other custom operators Airtunnel provides*, that interact with the file-based local physical data store, are:
  - SourceFileIsReadySensor
  - IngestOperator
  - StagingToReadyOperator
  - IngestArchiveOperator
  - DataAssetArchiveOperator
  
  To interact with Airtunnels metadata model, there are sensors `AwaitLoadStatusSensor` (pokes for an data asset to be
  updated in a recent specified timedelta) and `AwaitAssetAncestorsUpdatedSensor` (using lineage, pokes for updates on
  a Data Asset's ancestors). These can help build out more pull-based workflows, if you do not want/or can't leverage
  Airflows push-based approaches, like DAG triggering.

  Additionally there is `SQLOperator`, which allows you to run generic SQL scripts from the scripts store using an Airflow
`DbApiHook` – good for maintenance scripts that are not Data Asset centered.

## Contributing to Airtunnel
We love any contributions, be it feedback, issues or PRs on GitHub!
