""" Module for Airtunnel DataStore abstractions. """
import glob
import importlib
import logging
import os
import shutil
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TextIO, List, Dict, Tuple

from airflow import conf
from airflow.exceptions import AirflowConfigException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_ADAPTER_CLASS = "airtunnel.data_store.LocalDataStoreAdapter"


class BaseDataStoreAdapter(ABC):
    """ Base class for all Airtunnel DataStoreAdapters. """

    @staticmethod
    @abstractmethod
    def move(source: str, destination: str, recursive: bool = False) -> None:
        """ Move file(s) from source to destination. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def copy(source: str, destination: str, recursive: bool = False) -> None:
        """ Copy file(s) from source to destination. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def delete(path: str, recursive: bool = False) -> None:
        """ Delete file(s) at a given path. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def makedirs(path: str, exist_ok: bool = False, **kwargs) -> None:
        """ Make directories along the given path. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def open(file: str, mode: str, **kwargs) -> TextIO:
        """ Open a file-handle with the given path & mode. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def exists(path: str, **kwargs) -> bool:
        """ Checks the given path exists. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def glob(pattern: str, **kwargs) -> List[str]:
        """ Fetch file list using the provided glob pattern. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def listdir(path: str, **kwargs) -> List[str]:
        """ List entries of a given path. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def modification_times(files: List[str]) -> Dict[str, int]:
        """ For a given file list, fetch modification times as int Unix timestamps. """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def inspect(files: List[str]) -> Dict[str, Tuple[datetime, datetime, int]]:
        """ For a given file list, fetch create & modification datetime and byte size. """
        raise NotImplementedError


class LocalDataStoreAdapter(BaseDataStoreAdapter):
    """ DataStoreAdapter implementation targeting the local filesystem. """

    @staticmethod
    def move(source: str, destination: str, recursive: bool = False) -> None:
        """ Move file(s) from source to destination. """
        shutil.move(src=source, dst=destination)

    @staticmethod
    def copy(source: str, destination: str, recursive: bool = False) -> None:
        """ Copy file(s) from source to destination. """
        if recursive:
            shutil.copytree(src=source, dst=destination)
        else:
            shutil.copy(src=source, dst=destination)

    @staticmethod
    def delete(path: str, recursive: bool = False) -> None:
        """ Delete file(s) at a given path. """
        if recursive:
            shutil.rmtree(path=path)
        else:
            os.remove(path)

    @staticmethod
    def makedirs(path: str, exist_ok: bool = False, **kwargs) -> None:
        """ Make directories along the given path. """
        os.makedirs(path, exist_ok=exist_ok, **kwargs)

    @staticmethod
    def open(file: str, mode: str, **kwargs) -> TextIO:
        """ Open a file-handle with the given path & mode. """
        return open(file=file, mode=mode, **kwargs)

    @staticmethod
    def exists(path: str, **kwargs) -> bool:
        """ Checks the given path exists. """
        return os.path.exists(path=path)

    @staticmethod
    def glob(pattern: str, **kwargs) -> List[str]:
        """ Fetch file list using the provided glob pattern. """
        return glob.glob(pattern)

    @staticmethod
    def listdir(path: str, **kwargs) -> List[str]:
        """ List entries of a given path. """
        return os.listdir(path)

    @staticmethod
    def modification_times(files: List[str]) -> Dict[str, int]:
        """ For a given file list, fetch modification times as int Unix timestamps. """
        return {f: os.stat(f).st_mtime for f in files}

    @staticmethod
    @abstractmethod
    def inspect(files: List[str]) -> Dict[str, Tuple[datetime, datetime, int]]:
        """ For a given file list, fetch create & modification datetime and byte size. """
        inspected = {}
        for f in files:
            f_stat = os.stat(f)
            inspected[f] = (
                datetime.fromtimestamp(f_stat.st_ctime),
                datetime.fromtimestamp(f_stat.st_mtime),
                f_stat.st_size,
            )

        return inspected


def get_configured_data_store_adapter() -> BaseDataStoreAdapter:
    """ Gets the configured DataStoreAdapter. """
    try:
        data_store_adapter_class = conf.get(
            section="airtunnel", key="data_store_adapter_class"
        )
    except AirflowConfigException:
        logger.warning(
            f"'data_store_adapter_class' for Airtunnel not configured in airflow.cfg – using default"
        )
        # set the default config as part of the environment, to hide future AirflowConfigExceptions:
        os.environ[
            "AIRFLOW__AIRTUNNEL__DATA_STORE_ADAPTER_CLASS"
        ] = DEFAULT_ADAPTER_CLASS
        data_store_adapter_class = DEFAULT_ADAPTER_CLASS

    module, cls = data_store_adapter_class.rsplit(".", maxsplit=1)
    mod = importlib.import_module(name=module)
    return getattr(mod, cls)
