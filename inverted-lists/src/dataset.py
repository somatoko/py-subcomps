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

    def collect_documents(self):
        self.delegate.collect_documents(self.docs_file_path)

    def create_inverted_file(self):

        method = self.delegate.strategy(
            self.lexicon_file_path,
            self.inverted_file_path,
            self.temp_file_path)

        docs_gen = self.delegate.docs_gen(self.docs_file_path)
        method.create_invreted_file(docs_gen)

    def search(self, query):
        method = self.delegate.strategy(
            self.lexicon_file_path,
            self.inverted_file_path,
            self.temp_file_path)

        docs_gen = self.delegate.docs_gen(self.docs_file_path)
        doc_ids = method.retrieve_docs(query)
        docs = [d for d in docs_gen if d.id in doc_ids]
        print(docs)

    @property
    def base_subdir(self):
        if self.subdir is not None:
            return f'{DATA_BASE_DIR}/{self.subdir}'
        else:
            return DATA_BASE_DIR

    @property
    def docs_file_path(self):
        return f'{self.base_subdir}/{self.name}.json'

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
