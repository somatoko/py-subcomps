import os
import sys
# from glob import glob
from pathlib import Path
import json
from src import memory_based, sort_based
import pprint
import pickle
from timeit import timeit

BOOKS_ROOT = '/Volumes/storeA/books'


def main():
    # generate_docs()
    docs = read_docs()
    # construct_index_1(docs)

    # _ = construct_index_2(docs)
    find_index_2()


def read_docs():
    with open('docs.json', 'r') as sin:
        return json.load(sin, object_hook=lambda d: Document(None, d))


def generate_docs():
    # files = glob('**/*.(epub|pdf)', root_dir='/Volumes/storeA/books')
    docs = []
    for p in (p for p in Path(BOOKS_ROOT).glob("**/*") if p.suffix in {".pdf", ".epub"}):
        d = Document(p)
        docs.append(d)

    with open('docs.json', 'w') as sout:
        # use default=vars to utilize @property __dict__ of the custom classes
        json.dump(docs, sout, sort_keys=True, indent=2, default=vars)


def construct_index_1(docs):
    def bar():
        index = {}
        for d in docs:
            index = memory_based.consume_doc(d.id, d.name, index)
    # pprint.pprint(index)
    # print(f'= index pickled size: {sys.getsizeof(pickle.dumps(index))} bytes')

    t = timeit(lambda: bar(), number=100)
    print('- timing:', t)


def construct_index_2(docs):
    def bar():
        _ = sort_based.consume_docs(docs)

    # t = timeit(lambda: bar(), number=100)
    # print('- timing:', t)
    bar()
    # sort_based.dump_tmp_file()
    # sort_based.ensure_tmp_file_is_sorted()


def find_index_2():
    terms = 'swift'
    doc_ids = sort_based.find_docs(terms)
    # print(doc_ids)
    docs = read_docs()
    docs = list(filter(lambda d: d.id in doc_ids, docs))
    print(docs)


class Document:
    oid = 0

    def __init__(self, abs_path, _dict=None):
        if _dict is not None:
            self.id = _dict['id']
            self.path = _dict['path']
            self.name = _dict['name']
            self.suffix = _dict['suffix']
        else:
            p = Path(abs_path)
            self.__class__.oid += 1
            self.id = self.__class__.oid
            self.path = abs_path
            self.name = self.__normalize_name(p.stem)
            self.suffix = p.suffix

    def __normalize_name(self, name):
        return (name
                .replace('.', ' ')
                .replace('-', ' ')
                .replace('_', ' ')
                .replace('[', '')
                .replace(']', '')
                .replace('  ', ' ')
                )

    def __str__(self):
        return f'{self.id} - {self.name}\ntype: {self.suffix}\npath: {self.path}'

    __repr__ = __str__

    @property
    def __dict__(self):
        return dict(
            id=self.id,
            name=self.name,
            suffix=self.suffix,
            path=str(self.path.relative_to(BOOKS_ROOT)),
        )


if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
    main()
