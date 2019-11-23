""" Module for Airtunnel's paths, i.e. to the declaration, data and scripts store folders. """
from os import path

from airflow import conf

P_DECLARATIONS = conf.get(section="airtunnel", key="declarations_folder")
P_DATA = conf.get(section="airtunnel", key="data_store_folder")
P_SCRIPTS = conf.get(section="airtunnel", key="scripts_folder")
P_SCRIPTS_SQL = path.join(P_SCRIPTS, "sql")
P_SCRIPTS_PY = path.join(P_SCRIPTS, "py")

# define data paths based on data store root:
P_DATA_ARCHIVE = path.join(P_DATA, "archive")
P_DATA_INGEST = path.join(P_DATA, "ingest")
P_DATA_READY = path.join(P_DATA, "ready")
P_DATA_STAGING = path.join(P_DATA, "staging")
P_DATA_STAGING_PICKEDUP = path.join(P_DATA_STAGING, "pickedup")
P_DATA_STAGING_READY = path.join(P_DATA_STAGING, "ready")
P_DATA_STAGING_INTERMEDIATE = path.join(P_DATA_STAGING, "intermediate")
P_DATA_INGEST_LANDING = path.join(P_DATA_INGEST, "landing")
P_DATA_INGEST_ARCHIVE = path.join(P_DATA_INGEST, "archive")
