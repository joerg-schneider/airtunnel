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
    @staticmethod
    @abstractmethod
    def move(source: str, destination: str, recursive: bool = False) -> None:
        pass

    @staticmethod
    @abstractmethod
    def copy(source: str, destination: str, recursive: bool = False) -> None:
        pass

    @staticmethod
    @abstractmethod
    def delete(path: str, recursive: bool = False) -> None:
        pass

    @staticmethod
    @abstractmethod
    def makedirs(path: str, exist_ok: bool = False, **kwargs) -> None:
        pass

    @staticmethod
    @abstractmethod
    def open(file: str, mode: str, **kwargs) -> TextIO:
        pass

    @staticmethod
    @abstractmethod
    def exists(path: str, **kwargs) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def glob(pattern: str, **kwargs) -> List[str]:
        pass

    @staticmethod
    @abstractmethod
    def listdir(path: str, **kwargs) -> List[str]:
        pass

    @staticmethod
    @abstractmethod
    def modification_times(files: List[str]) -> Dict[str, int]:
        pass

    @staticmethod
    @abstractmethod
    def inspect(files: List[str]) -> Dict[str, Tuple[datetime, datetime, int]]:
        pass


class LocalDataStoreAdapter(BaseDataStoreAdapter):
    @staticmethod
    def move(source: str, destination: str, recursive: bool = False) -> None:
        shutil.move(src=source, dst=destination)

    @staticmethod
    def copy(source: str, destination: str, recursive: bool = False) -> None:
        if recursive:
            shutil.copytree(src=source, dst=destination)
        else:
            shutil.copy(src=source, dst=destination)

    @staticmethod
    def delete(path: str, recursive: bool = False) -> None:
        if recursive:
            shutil.rmtree(path=path)
        else:
            os.remove(path)

    @staticmethod
    def makedirs(path: str, exist_ok: bool = False, **kwargs) -> None:
        os.makedirs(path, exist_ok=exist_ok, **kwargs)

    @staticmethod
    def open(file: str, mode: str, **kwargs) -> TextIO:
        return open(file=file, mode=mode, **kwargs)

    @staticmethod
    def exists(path: str, **kwargs) -> bool:
        return os.path.exists(path=path)

    @staticmethod
    def glob(pattern: str, **kwargs) -> List[str]:
        return glob.glob(pattern)

    @staticmethod
    def listdir(path: str, **kwargs) -> List[str]:
        return os.listdir(path)

    @staticmethod
    def modification_times(files: List[str]) -> Dict[str, int]:
        return {f: os.stat(f).st_mtime for f in files}

    @staticmethod
    @abstractmethod
    def inspect(files: List[str]) -> Dict[str, Tuple[datetime, datetime, int]]:
        inspected = {}
        for f in files:
            f_stat = os.stat(f)
            inspected[f] = (
                datetime.fromtimestamp(f_stat.st_ctime),
                datetime.fromtimestamp(f_stat.st_mtime),
                f_stat.st_size,
            )

        return inspected


def get_configured_adapter() -> BaseDataStoreAdapter:
    try:
        data_store_adapter_class = conf.get(
            section="airtunnel", key="data_store_adapter_class"
        )
    except AirflowConfigException:
        data_store_adapter_class = DEFAULT_ADAPTER_CLASS

    module, cls = data_store_adapter_class.rsplit(".", maxsplit=1)
    mod = importlib.import_module(name=module)
    return getattr(mod, cls)
