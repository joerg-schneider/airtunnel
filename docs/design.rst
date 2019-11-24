Design Principles
=================

Uncompromised Consistency
~~~~~~~~~~~~~~~~~~~~~~~~~

Consistency is at the heart of any successful software project. In data processing, consistency is achieved primarily
by strict naming conventions and a clear codebase structure. Such consistency enables new team members to quickly
onboard, and has the added benefit of highlighting inconsistencies. Unit tests are a good way to
actually ensure consistency.

Given that the data asset is at the center of the project, consistency should span:

 - naming (& placement) of data assets
 - naming of script file(s) to process said data asset
 - naming of workflows; as in Airflow DAG & operator IDs
 - a strict 1:1 relationship between data scripts/tasks and data assets. In other words, do not load three
   tables using one script, rather use three scripts

Consistency will help team members form a complete mental image of how aspects of the project are connected.
Ideally, it also yields a structure, such that no one has to do time-consuming searches for scripts or
perform similar actions. Instead, developers are enabled to quickly infer names and locations
just from a data asset name at hand.

Declarative first and low-code redundancy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow is a "workflows as code" type framework. While we absolutely love this flexibility, it may lead developers
to write redundant code and/or bury configuration properties in it. There is no need to write DAGs and processing
scripts completely from scratch. In fact, doing so can cause problems - especially if you have hundreds of such DAGs
and scripts.

As is the nature of data pipelining, process-oriented task types such as ingesting, loading, and moving data are
constantly recurring along the data asset lifecycle. The only difference per instance is what a particular data asset
requires: Ingest from where and using which mechanism? Load delta or in batch? Etc.
This is why we postulate to have a central declaration file (as in YAML or JSON) per data asset, capturing all these
properties required to run a generalized task (carried out by a custom operator). In other words, operators are designed
in a generic way and receive the name of a data asset, from which they can grab its declaration file and learn how to
parameterize and carry out the specific task.

Key benefits of this approach are that it:

 - Enables standardization of basic data pipelining tasks, which makes it possible for even novice developers to build workflows simply by providing the correct declaration.
 - Leads to the creation of a "single source of truth" through extraction of data asset properties into declaration.
 - Allows easy declaration-testing for consistency and validity using unit tests.
 - Enables cross-programming language use of declaration files.

Metadata driven
~~~~~~~~~~~~~~~

Metadata is knowledge, and knowledge is power. In this case, "power" refers to how to best execute a workflow,
analyze it later and maybe even optimize it. We propose a low-overhead metadata interface that should extend upon
Airflow's internal data model.

In particular, we need to add the dimension of the data asset, since Airflow's data model is based only on its own
objects such as DAGs, Tasks, and others. Here, a link between data assets and operators or DAGs needs to be
established because a stakeholder will probably never ask you: "<Your Name>, when did DAG load_all_the_data(fast)
finish today?" Instead, they have a particular data asset in mind.

To be truly metadata driven, one should also collect information on input files (such as sizes and modification times)
as well as on internally constructed data assets (such as which partitions were updated and at what time).

Downstream jobs then can easily leverage this information to construct efficient delta mechanisms such as recalculating
and replacing a partition whose input data has changed, or running efficient data-quality
checks based on updated data.

It is worth considering how much developer time needs to be invested in a framework like this.
If, for example, all imaginable jobs on a given day can work as a full load since data volumes are low,
there is no need to optimize delta updates based on metadata.
