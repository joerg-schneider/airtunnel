Extending Airtunnel
===================

Airtunnel was built to be easily extendable. To give some inspiration of
what can be done and how, this section shows some common use-cases.

Extra Declaration Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As noted, data asset declaration options are still a (too) small set of sensible defaults.

To still enable the use of the declaration store for any properties you like, we created the
(schemaless) ``extra`` section.

Example
+++++++

An example use case for this would be that one would like to additionally declare maintainers alongside
with their data asset declaration.

We can easily define this in a ``my_asset.yaml`` using extra declarations:

.. code:: yaml

   type: derived

   extra:
         maintainer:
            Tom

And the usage of this value is as simple as:

.. code:: python

    from airtunnel.declaration_store import DataAssetDeclaration
    d = DataAssetDeclaration("my_asset")
    # access the declared extra section:
    d.extra_declarations

…where the value of ``d.extra_declarations`` will simply be: ``{'maintainer': 'Tom'}``


Custom MetaAdapter
~~~~~~~~~~~~~~~~~~
As introduced in the :doc:`architecture section <architecture>`, Airtunnel's metadata is stored using
so called metadata adapters – the default being ``SQLMetaAdapter`` which uses SQLAlchemy to interact with an
Airflow connection. To use your custom adapter, implement it by basing ``airtunnel.metadata.adapter.BaseMetaAdapter``
and specify this new class in the [airtunnel] section of your ``airflow.cfg`` using the full reference, like this:

.. code::

    metadata_store_adapter_class = yourpackage.yoursubpackage.RedisMetaAdapter

You can verify it is being properly loaded, by running:

.. code:: python

    from airtunnel.metadata.adapter import get_configured_meta_adapter
    get_configured_meta_adapter()

…which should then return: *<class 'yourpackage.yoursubpackage.RedisMetaAdapter'>*

Custom DataStoreAdapter
~~~~~~~~~~~~~~~~~~~~~~~
Airtunnel's physical data store is by default on the local filesystem, for which `LocalDataStoreAdapter` is a bridge
to carry out all commands like move, copy or list files.

Since we expect a large number of users wanting to have their data store located on i.e. cloud storage, Airtunnel
can be easily extended to support it.

To use a custom DataStoreAdapter, implement it by basing ``airtunnel.data_store.BaseDataStoreAdapter`` and
specify this new class in the [airtunnel] section of your ``airflow.cfg`` using the full reference, like this:

.. code::

    data_store_adapter_class = yourpackage.yoursubpackage.S3DataStoreAdapter

You can verify it is being properly loaded, by running:

.. code:: python

    from airtunnel.data_store import get_configured_data_store_adapter
    get_configured_data_store_adapter()


…which should then return: *<class 'yourpackage.yoursubpackage.S3DataStoreAdapter'>*
