import os
import math
import struct
import shutil
import pickle

ENTRY_FORMAT_TMP = '=III'
ENTRY_FORMAT_INF = '=II'
RUN_SIZE = 4


class SortBasedIndex:

    def __init__(self, lexicon_path: str, inverted_file_path: str, tmp_file: str = None):
        self.lexicon_path = lexicon_path
        self.inverted_file_path = inverted_file_path
        self.tmp_file = tmp_file if tmp_file is not None else inverted_file_path + '.tmp'

    def create_invreted_file(self, docs):
        index = {}
        temp_file = ''
        term_id = 0

        tmp_file = open(self.tmp_file, 'wb')

        def get_term_id(term):
            nonlocal term_id
            if term not in index:
                term_id += 1
                index[term] = term_id
            return index[term]

        # -2- write frequencies to tmp file

        doc_count = 0
        entry_count = 0
        for doc in docs:
            doc_count += 1
            stats = self.__extract_doc_terms(doc.content)
            for (term, freq) in stats.items():
                _term_id = get_term_id(term)
                entry = self.__freq_entry_pack(_term_id, doc.id, freq)
                tmp_file.write(entry)
                entry_count += 1
        tmp_file.close()

        # tmp_file_size = os.path.getsize(self.tmp_file)
        # print(f'- size of tmp file: {tmp_file_size}')

        # -3- internal (in-file) sorting of runs
        # ASC term_id + ASC doc_id
        # n records = 1 run. Read 1 run sort it and write it back.

        with open(self.tmp_file, 'r+b') as fin:
            entry_size = self.__freq_entry_size()
            while True:
                run_bytes = fin.read(entry_size * RUN_SIZE)
                bytes_read = len(run_bytes)
                if bytes_read == 0:
                    break

                run = self.__freq_entry_run_unpack(run_bytes)
                # sort in-place (as opposed to using sorted())
                # print('--', len(run), run)
                run.sort(key=lambda u: (u[0], u[1]))
                # print('  ', len(run), run)

                # -- rewind back and write sorted run
                run_bytes = self.__pack_run(run)
                # print(f'R: {bytes_read}; W: {len(run_bytes)}')
                fin.seek(-bytes_read, os.SEEK_CUR)
                fin.write(run_bytes)

        # -4- merge sorted runs
        self.__merge_runs(entry_count)

        # -5- construct the inverted file
        lexicon = self.__construct_inverted_file(index)
        os.remove(self.tmp_file)

        # -6- persist the lexicon file
        with open(self.lexicon_path, 'wb') as fout:
            pickle.dump(lexicon, fout)

        print(f'= Ready with {doc_count} docs and {len(lexicon.keys())} terms')

        return lexicon

    def retrieve_docs(self, terms):
        terms = terms.lower()
        with open(self.lexicon_path, 'rb') as fin:
            lexicon = pickle.load(fin)
        entry = lexicon.get(terms, None)
        if entry is None:
            return []

        (freq, pos) = entry
        with open(self.inverted_file_path, 'rb') as fin:
            entry_size = struct.calcsize(ENTRY_FORMAT_INF)
            size = entry_size * freq
            fin.seek(pos)
            list_raw = fin.read(size)
            list_ids = [struct.unpack(ENTRY_FORMAT_INF, list_raw[u: u + entry_size])
                        for u in range(0, size, entry_size)]
            # TODO: sort docs somehow

            # Return doc ids
            return list(map(lambda u: u[0], list_ids))

    def __extract_doc_terms(self, content):
        ''' Construct a dictonary mappind text terms to their frequency.
            Terms are normalized first, as of now only lower-cased.
        '''
        stats = {}
        for word in content.split():
            word = word.lower()
            if word in stats:
                stats[word] += 1
            else:
                stats[word] = 1
        return stats

    def __freq_entry_pack(self, term_id, doc_id, freq):
        '''Construct a bytearray that consists of the three given argyments.
        '''
        bytes = struct.pack(ENTRY_FORMAT_TMP, term_id, doc_id, freq)
        return bytes

    def __freq_entry_size(self):
        return struct.calcsize(ENTRY_FORMAT_TMP)

    def __freq_entry_run_unpack(self, bytes):
        # bytes = struct.unpack(ENTRY_FORMAT_TMP, bytes)
        entry_size = self.__freq_entry_size()
        run = [bytes[i:i+entry_size] for i in range(0, len(bytes), entry_size)]
        run = [u for u in map(
            lambda u: struct.unpack(ENTRY_FORMAT_TMP, u), run)]
        return run

    def __pack_run(self, run):
        result = bytearray()
        for u in run:
            result += bytearray(self.__freq_entry_pack(u[0], u[1], u[2]))
        return result

    def __merge_runs(self, entry_count):
        # print(f'- merging for {entry_count} terms')
        run_size = RUN_SIZE

        # Number of runs can be oneven in the beginning, but it will not pose
        # a problem because at some point run_count will be
        # sufficiently large to form a pair.
        while run_size < entry_count:
            self.__run_merge_step_phase(run_size, entry_count)
            run_size *= 2

    def __run_merge_step_phase(self, run_size, entry_count):
        # print(f'\n-- run of size {run_size}')
        runs_count = math.ceil(entry_count / run_size)
        # operating by run index is simpler than directly by term index
        left = [i for i in range(0, runs_count, 2)]
        right = [i for i in range(1, runs_count, 2)]
        pairs = zip(left, right)

        out_filename = self.tmp_file + '_aux'
        shutil.copy(self.tmp_file, out_filename)

        entry_size = self.__freq_entry_size()
        with open(self.tmp_file, 'rb') as fin:
            # open for writing but do not truncate
            with open(out_filename, 'r+b') as fout:
                for p in pairs:
                    l_b, l_e = (p[0] * run_size, (p[0] + 1) * run_size)
                    r_b, r_e = (p[1] * run_size, (p[1] + 1) * run_size)
                    r_e = min(r_e, entry_count)
                    # print(p, l_b, l_e, r_b, r_e)
                    pL = l_b
                    pR = r_b
                    pO = l_b

                    fout.seek(pO * entry_size)

                    while pL < l_e or pR < r_e:
                        # print(f'- {pL}:{l_e} {pR}:{r_e}')
                        if pL >= l_e:
                            while pR < r_e:
                                fin.seek(pR * entry_size)
                                entry_bytes = fin.read(entry_size)
                                fout.write(entry_bytes)
                                pR += 1
                        elif pR >= r_e:
                            while pL < l_e:
                                fin.seek(pL * entry_size)
                                entry_bytes = fin.read(entry_size)
                                fout.write(entry_bytes)
                                pL += 1
                        else:
                            fin.seek(pL * entry_size)
                            l_entry_bytes = fin.read(entry_size)
                            l_entry = struct.unpack(
                                ENTRY_FORMAT_TMP, l_entry_bytes)
                            fin.seek(pR * entry_size)
                            r_entry_bytes = fin.read(entry_size)
                            # print('- ', pR, len(r_entry_bytes), fin.tell())
                            r_entry = struct.unpack(
                                ENTRY_FORMAT_TMP, r_entry_bytes)
                            if l_entry[0] < r_entry[0] or (l_entry[0] == r_entry[0] and l_entry[1] <= r_entry[1]):
                                fout.write(l_entry_bytes)
                                pL += 1
                            else:
                                fout.write(r_entry_bytes)
                                pR += 1
        os.rename(out_filename, self.tmp_file)

    def __construct_inverted_file(self, term_index):
        pos_to_term = {i: k for (k, i) in term_index.items()}
        lexicon = {}

        entry_size = self.__freq_entry_size()

        with open(self.inverted_file_path, 'wb') as fout:
            with open(self.tmp_file, 'rb') as fin:
                entry_prev = struct.unpack(
                    ENTRY_FORMAT_TMP, fin.read(entry_size))
                entry_count = 1
                term_list_size = 1

                lexicon[pos_to_term[entry_prev[0]]] = 0

                fout.write(struct.pack(ENTRY_FORMAT_INF,
                                       entry_prev[1], entry_prev[2]))

                while True:
                    bytes_read = fin.read(entry_size)
                    if len(bytes_read) == 0:
                        break

                    entry = struct.unpack(ENTRY_FORMAT_TMP, bytes_read)

                    if entry_prev[0] != entry[0]:
                        # update prev record with count and pos
                        pos = lexicon[pos_to_term[entry_prev[0]]]
                        lexicon[pos_to_term[entry_prev[0]]] = (
                            term_list_size, pos)
                        # start current record with pos
                        lexicon[pos_to_term[entry[0]]] = fout.tell()
                        # reset term count
                        term_list_size = 1
                    else:
                        term_list_size += 1
                    entry_count += 1

                    fout.write(struct.pack(
                        ENTRY_FORMAT_INF, entry[1], entry[2]))

                    entry_prev = entry

                # update last record with count and pos
                pos = lexicon[pos_to_term[entry_prev[0]]]
                lexicon[pos_to_term[entry_prev[0]]] = (term_list_size, pos)
            fout.flush()

        return lexicon


def ensure_tmp_file_is_sorted(temp_file_path):
    entry_size = struct.calcsize(ENTRY_FORMAT_TMP)

    with open(temp_file_path, 'rb') as fin:
        prev = struct.unpack(ENTRY_FORMAT_TMP, fin.read(entry_size))
        pos = 1

        while True:
            curr_bytes = fin.read(entry_size)
            if len(curr_bytes) == 0:
                break

            curr = struct.unpack(ENTRY_FORMAT_TMP, curr_bytes)
            if prev[0] < curr[0] or (prev[0] == curr[0] and prev[1] <= curr[1]):
                prev = curr
                pos += 1
            else:
                print('- error broken ordering detected:', pos, prev, curr)
                exit(1)


def dump_tmp_file(temp_file_path):
    entry_size = __freq_entry_size()

    i = 0
    with open(temp_file_path, 'rb') as fin:
        while True:
            curr_bytes = fin.read(entry_size)
            if len(curr_bytes) == 0:
                break
            curr = struct.unpack(ENTRY_FORMAT_TMP, curr_bytes)
            print(f'{i} - {curr}')
            i += 1
