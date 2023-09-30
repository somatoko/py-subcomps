import sys
from typing import NamedTuple


class State(NamedTuple):
    is_dict: bool = False
    is_list: bool = False

    def __str__(self):
        d = 'D' if self.is_dict else ''
        l = 'L' if self.is_list else ''
        return f'{d}{l}'

    __repr__ = __str__


class FileDecoder:

    def __init__(self, path):
        self.path = path

    def process(self):
        with open(self.path, 'rb') as fin:
            term = fin.read()
        return parse(term)


def parse(term):
    term = term
    state = []
    values = []

    while len(term) > 0:
        if len(term) == 0:
            return values[0]

        elif term[0] == ord('e'):
            if len(values) == 1:
                if state[-1].is_dict:
                    return _list_to_dict(values[0])
                else:
                    return values[0]
            # nested structs
            else:
                if state[-1].is_dict:
                    state.pop()
                    dict = _list_to_dict(values.pop())
                    # Refer to last because we have already popped the dict above
                    values[-1].append(dict)
                    term = term[1:]
                else:
                    state.pop()
                    values[-2].append(values.pop())
                    term = term[1:]

        elif term[0] == ord('d'):
            state.append(State(is_dict=True))
            values.append([])
            term = term[1:]
        elif term[0] == ord('l'):
            state.append(State(is_list=True))
            values.append([])
            term = term[1:]
        elif chr(term[0]).isdigit():
            sep = term.find(b':')
            size = int(term[:sep])
            stop = sep+1+size
            val = term[sep+1:stop]
            last_state = state[-1]
            if last_state.is_dict or last_state.is_list:
                values[-1].append(val)
            else:
                state.append(State())
                values.append(val)
            term = term[stop:]
        elif term[0] == ord('i'):
            stop = term.find(b'e')
            val = int(term[1:stop])
            last_state = state[-1]
            if last_state.is_dict or last_state.is_list:
                values[-1].append(val)
            else:
                state.append(State())
                values.append(val)
            term = term[stop+1:]


def _list_to_dict(lst):
    # keys must be strings
    keys = [k.decode() for k in lst[::2]]
    vals = lst[1::2]
    return dict(zip(keys, vals))
