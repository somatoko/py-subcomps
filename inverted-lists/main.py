import os
import sys
# from glob import glob
from pathlib import Path
import json
from src import memory_based, sort_based, dataset
from src.document import Document
import pprint
import pickle
from timeit import timeit

BOOKS_ROOT = '/Volumes/storeA/books'


def main():
    # generate_docs()
    docs = read_docs()
    # construct_index_1(docs)

    construct_index_2(docs)


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
    def create_inverted_file():
        lexicon = sort_based.consume_docs(docs)
        with open(sort_based.LEXICON_FILE, 'wb') as fout:
            pickle.dump(lexicon, fout)

    # t = timeit(lambda: bar(), number=100)
    # print('- timing:', t)
    # create_inverted_file()
    # sort_based.dump_tmp_file()
    # sort_based.ensure_tmp_file_is_sorted()

    ds = dataset.Dataset('torrents', None, 'torrent_set')
    method = sort_based.SortBasedIndex(
        ds.lexicon_file_path, ds.inverted_file_path, ds.temp_file_path)
    method.create_invreted_file(docs)

    doc_ids = method.retrieve_docs('swift')
    docs = read_docs()
    docs = list(filter(lambda d: d.id in doc_ids, docs))
    print(docs)


if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
    main()
