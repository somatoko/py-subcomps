import os
import abc
from pathlib import Path

DATA_BASE_DIR = '_tmp'


class Dataset:

    def __init__(self, name, delegate, subdir=None):
        self.name = name
        self.delegate = delegate
        self.subdir = subdir

        # ensure data dir exists
        Path(self.base_subdir).mkdir(parents=True, exist_ok=True)

    def create_inverted_file(self, docs):
        pass

    @property
    def base_subdir(self):
        if self.subdir is not None:
            return f'{DATA_BASE_DIR}/{self.subdir}'
        else:
            return DATA_BASE_DIR

    @property
    def inverted_file_path(self):
        return f'{self.base_subdir}/{self.name}.inf'

    @property
    def lexicon_file_path(self):
        return f'{self.base_subdir}/{self.name}.lex'

    @property
    def temp_file_path(self):
        return f'{self.base_subdir}/{self.name}.tmp'


class DatasetDelegateInterface(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'load_data_source') and
                callable(subclass.load_data_source) and
                hasattr(subclass, 'extract_text') and
                callable(subclass.extract_text) or
                NotImplemented)

    @abc.abstractmethod
    def load_data_source(self, path: str, file_name: str):
        """Load in the data set"""
        raise NotImplementedError

    @abc.abstractmethod
    def extract_text(self, full_file_path: str):
        """Extract text from the data set"""
        raise NotImplementedError
