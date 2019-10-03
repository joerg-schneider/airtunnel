import logging
import os
import typing
from os import path
from typing import Dict, Any, Iterable, List

import yaml
from schema import Schema, And, Optional

# get declarations folder:
from airtunnel.paths import P_DECLARATIONS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# expected file suffix
DECL_FILE_SUFFIX = ".yaml"

# CONSTANTS

# allowed sections
SECTION_LOAD = "load"
SECTION_TRANSFORMATION = "transformation"
SECTION_INGEST = "ingest"
SECTION_EXTRA = "extra"

# allowed YAML property keys - prefixed by K_
K_IN_RENAMES = "in_column_renames"
K_ARCH_INGEST = "archive_ingest"
K_IN_STORAGE_FORMAT = "in_storage_format"
K_ARCH_READY = "archive_ready"
K_OUT_COMP_CODEC = "out_compression_codec"
K_FILE_INPUT_GLOB = "file_input_glob"
K_OUT_FORMAT = "out_storage_format"
K_ASSET_TYPE = "type"
K_IN_DATE_FORMATS = "in_date_formats"
K_STAGING_ASSETS = "staging_assets"
K_KEY_COLUMNS = "key_columns"
K_RUN_DDL = "run_ddl"


# allowed YAML property values - prefixed by V_
V_TYPE_INGESTED = "ingested"
V_TYPE_DERIVED = "derived"
V_TYPE_STAGING = "staging"


V_FORMAT_CSV = "csv"
V_FORMAT_PARQUET = "parquet"
V_FORMAT_EXCEL = "xls"


V_COMP_GZIP = "gzip"
V_COMP_SNAPPY = "snappy"
V_COMP_NONE = "none"

# for easier checking if value is in allowed group, combine into tuples:
V_ALLOWED_COMP = (V_COMP_GZIP, V_COMP_SNAPPY)
V_ALLOWED_FORMATS = (V_FORMAT_CSV, V_FORMAT_PARQUET, V_FORMAT_EXCEL)
V_ALLOWED_TYPES = (V_TYPE_DERIVED, V_TYPE_INGESTED)

DEFAULT_LOAD_SECTION = {
    K_OUT_FORMAT: V_FORMAT_PARQUET,
    K_OUT_COMP_CODEC: V_COMP_GZIP,
    K_ARCH_READY: True,
    K_RUN_DDL: True,
}


class DeclarationSchemas:
    """ Wrapper to hold all our YAML schemas to validate declaration files. """

    # remark: we could use Optional() with a default on some fields, like compression.
    #         (it is nice to have them explicit though)

    # to reuse: sub-schemas that are common
    LOAD_SCHEMA = Schema(
        {
            Optional(K_OUT_FORMAT, default=V_FORMAT_PARQUET): And(
                lambda f: f in V_ALLOWED_FORMATS
            ),
            Optional(K_OUT_COMP_CODEC, default=V_COMP_GZIP): And(
                lambda o: o in V_ALLOWED_COMP
            ),
            Optional(K_ARCH_READY, default=True): lambda a: a in (True, False),
            Optional(K_RUN_DDL, default=True): lambda a: a in (True, False),
            Optional(K_KEY_COLUMNS): Schema([Schema(str)]),
        }
    )
    # Staging assets schema:
    STAGING_ASSETS = Schema([Schema(str)])

    # the main schemas for the two data asset types:
    # 1.) a schema for files airtunnel ingests from external sources
    INGESTED_DATA_ASSET = Schema(
        {
            K_ASSET_TYPE: And(str, lambda t: t == V_TYPE_INGESTED),
            SECTION_INGEST: Schema(
                {
                    Optional(K_IN_STORAGE_FORMAT, default=V_FORMAT_CSV): lambda f: f
                    in V_ALLOWED_FORMATS,
                    K_FILE_INPUT_GLOB: And(str, len),
                    Optional(K_ARCH_INGEST, default=True): lambda a: a in (True, False),
                }
            ),
            Optional(SECTION_TRANSFORMATION): Schema(
                {
                    Optional(K_IN_RENAMES): And(
                        dict,
                        # no duplicates on the target column name side:
                        lambda rename_map: len(set(rename_map.values()))
                        == len(rename_map.values()),
                        # never allow to map a new column name onto an pre-existing one (column duplication):
                        lambda rename_map: all(
                            new_name not in rename_map.keys()
                            for new_name in rename_map.values()
                        ),
                    ),
                    Optional(K_IN_DATE_FORMATS): And(
                        dict,
                        # all keys and values in this Dict should be strings:
                        lambda datetime_map: all(
                            [
                                isinstance(k, str) and isinstance(v, str)
                                for k, v in datetime_map.entries()
                            ]
                        ),
                    ),
                }
            ),
            Optional(SECTION_LOAD, default=DEFAULT_LOAD_SECTION): LOAD_SCHEMA,
            Optional(K_STAGING_ASSETS): STAGING_ASSETS,
            Optional(SECTION_EXTRA): Schema(object),
        }
    )

    # 2.) a schema for files airtunnel builds from combinations from its own data assets
    DERIVED_DATA_ASSET = Schema(
        {
            K_ASSET_TYPE: And(str, lambda t: t == V_TYPE_DERIVED),
            Optional(SECTION_LOAD, default=DEFAULT_LOAD_SECTION): LOAD_SCHEMA,
            Optional(K_STAGING_ASSETS): STAGING_ASSETS,
            Optional(SECTION_EXTRA): Schema({object: object}),
        }
    )


class DataAssetDeclaration:
    __slots__ = ["_yaml", "_asset_name", "_file", "_data_asset_decl", "_asset_type"]

    def __init__(self, data_asset: str):

        if data_asset != data_asset.strip() or data_asset != data_asset.lower():
            raise ValueError(
                f"All data assets should be lower-cased and without leading/trailing space"
            )

        self._asset_name = data_asset
        self._yaml = self.__load_from_yaml()

        if self._yaml is None or K_ASSET_TYPE not in self._yaml:
            raise AttributeError(
                f"The mandatory type field is missing in '{self._file}'"
            )

        defined_type = self._yaml[K_ASSET_TYPE]

        if defined_type == V_TYPE_INGESTED:
            self._data_asset_decl = DeclarationSchemas.INGESTED_DATA_ASSET.validate(
                data=self._yaml
            )
            logger.info(f"Got valid declaration for ingested asset {self._asset_name}")
        elif defined_type == V_TYPE_DERIVED:
            self._data_asset_decl = DeclarationSchemas.DERIVED_DATA_ASSET.validate(
                data=self._yaml
            )
            logger.info(f"Got valid declaration for derived asset {self._asset_name}")
        else:
            raise AttributeError(f"Unsupported type '{defined_type}' in {self._file}")

        self._asset_type = defined_type

    def __load_from_yaml(self) -> Dict[str, Any]:
        decl_file = path.join(P_DECLARATIONS, self._asset_name + DECL_FILE_SUFFIX)

        self._file = decl_file

        with open(decl_file) as f:
            return yaml.safe_load(f)

    @property
    def asset_name(self) -> str:
        return self._asset_name

    @property
    def all(self):
        return self._data_asset_decl

    def _ingestion_decls(self):
        if self._asset_type == V_TYPE_DERIVED:
            raise ValueError(
                f"No ingestion declarations for asset of type {V_TYPE_DERIVED}"
            )
        return self.all[SECTION_INGEST]

    @property
    def ingest_file_glob(self) -> str:
        return self._ingestion_decls()[K_FILE_INPUT_GLOB]

    @property
    def in_storage_format(self) -> str:
        return self._ingestion_decls()[K_IN_STORAGE_FORMAT]

    @property
    def archive_ingest(self) -> bool:
        return self._ingestion_decls()[K_ARCH_INGEST]

    def _transform_decls(self) -> Dict:
        if self._asset_type == V_TYPE_DERIVED:
            raise ValueError(
                f"No transform declarations for asset of type {V_TYPE_DERIVED}"
            )

        if SECTION_TRANSFORMATION not in self.all:
            raise ValueError("No transformation properties defined for data asset.")

        return self.all[SECTION_TRANSFORMATION]

    @property
    def transform_renames(self) -> Dict[str, str]:
        return self._transform_decls()[K_IN_RENAMES]

    @property
    def transform_date_formats(self) -> Dict[str, str]:
        return self._transform_decls()[K_IN_DATE_FORMATS]

    def _load_decls(self):
        return self.all[SECTION_LOAD]

    @property
    def out_storage_format(self) -> str:
        return self._load_decls()[K_OUT_FORMAT]

    @property
    def out_comp_codec(self) -> str:
        return self._load_decls()[K_OUT_COMP_CODEC]

    @property
    def archive_ready(self) -> bool:
        return self._load_decls()[K_ARCH_READY]

    @property
    def staging_assets(self) -> typing.Optional[List[str]]:
        if K_STAGING_ASSETS in self.all:
            return self.all[K_STAGING_ASSETS]

    @property
    def key_columns(self) -> typing.Optional[List[str]]:
        if K_KEY_COLUMNS in self.all:
            return self.all[K_KEY_COLUMNS]

    @property
    def extra_declarations(self) -> Any:
        return self.all[SECTION_EXTRA]

    @property
    def is_parquet_output(self) -> bool:
        return self.out_storage_format == V_FORMAT_PARQUET

    @property
    def is_csv_output(self) -> bool:
        return self.out_storage_format == V_FORMAT_CSV

    @property
    def is_parquet_input(self) -> bool:
        return self.in_storage_format == V_FORMAT_PARQUET

    @property
    def is_csv_input(self) -> bool:
        return self.in_storage_format == V_FORMAT_CSV

    @property
    def is_xls_input(self) -> bool:
        return self.in_storage_format == V_FORMAT_EXCEL

    @property
    def run_ddl(self) -> bool:
        return self._load_decls()[K_RUN_DDL]


def fetch_all_declarations() -> Iterable[DataAssetDeclaration]:
    for decl_file in os.listdir(P_DECLARATIONS):
        if decl_file.endswith(DECL_FILE_SUFFIX):
            try:
                yield DataAssetDeclaration(
                    data_asset=decl_file.replace(DECL_FILE_SUFFIX, "")
                )
            except Exception as e:
                logger.warning(f"Declaration file '{decl_file}' is broken!: {e}")
