Configuration and Setup
=======================

Configuring Airtunnel is as easy as configuring Airflow – simply add a section in your ``airflow.cfg`` for Airtunnel:

.. code:: yaml

    [airtunnel]
    declarations_folder = <absolute-path-to-your-declarations-folder>
    data_store_folder = <absolute-path-to-your-data-store-folder>
    scripts_folder = <absolute-path-to-your-scripts-folder>

    # we use the default settings for a local data store & metadata being written on the Airflow DB:
    data_store_adapter_class = airtunnel.data_store.LocalDataStoreAdapter
    meta_adapter_class = airtunnel.metadata.adapter.SQLMetaAdapter
    meta_adapter_hook_factory = airtunnel.metadata.adapter.DefaultSQLHookFactory

See :doc:`Physical data store setup <data-store>` on how to set up your physical data store for Airtunnel.