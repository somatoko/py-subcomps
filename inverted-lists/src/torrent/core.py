import numbers

def bencode(entity):
    if isinstance(entity, str):
        return f'{len(entity)}:{entity}'.encode()
    elif isinstance(entity, numbers.Number):
        return f'i{entity}e'.encode()
    elif isinstance(entity, bytes):
        result = f'{len(entity)}:'.encode() + entity
        return result
    elif isinstance(entity, list):
        acc = bytearray(b'l')
        for u in entity:
            acc += bencode(u)
        acc += b'e'
        return acc
    elif isinstance(entity, dict):
        acc = bytearray(b'd')
        keys = [k for k in entity.keys()]
        keys.sort()
        # print('- keys', keys)
        for k in keys:
            v = entity[k]
            v = v.decode() if (isinstance(v, bytes) and k != 'pieces') else v
            if k == 'pieces':
                encoded = bencode(v)
                # print('- entry', k, v.hex())
                # print('- encoded:', encoded)
            acc += bencode(k)
            acc += bencode(v)
        acc += b'e'
        return bytes(acc)

class State:
    in_dict = False

    def __init__(self, in_dict=False):
        self.in_dict = in_dict

def bencoding_decode(term, acc=[], stack=[]):
    # print(term, acc, stack)
    if len(term) == 0:
        return stack[0]
    if term[0] == ord('e'):
        if len(stack) == 1:
            if acc[-1].in_dict:
                return _list_to_dict(stack[0])
            else:
                return stack[0]
        # nested structs
        else:
            if acc[-1].in_dict:
                acc.pop()
                dict = _list_to_dict(stack.pop())
                # Refer to last because we have already popped the dict above
                stack[-1].append(dict)
                return bencoding_decode(term[1:], acc, stack)
            else:
                acc.pop()
                stack[-2].append(stack.pop())
                return bencoding_decode(term[1:], acc, stack)

    if chr(term[0]).isdigit():
        sep = term.find(b':')
        size = int(term[:sep])
        stop = sep+1+size
        val = term[sep+1:stop]
        # assert (term[stop] == b'e')
        if len(stack) == 0:
            stack.append(val)
        else:
            stack[-1].append(val)
        return bencoding_decode(term[stop:], acc, stack)

    if term[0] == ord('i'):
        stop = term.find(b'e')
        val = int(term[1:stop])
        if len(stack) == 0:
            stack.append(val)
        else:
            stack[-1].append(val)
        return bencoding_decode(term[stop+1:], acc, stack)

    if term[0] == ord('l'):
        acc.append(State())
        stack.append([])
        return bencoding_decode(term[1:], acc, stack)

    if term[0] == ord('d'):
        acc.append(State(in_dict=True))
        stack.append([])
        return bencoding_decode(term[1:], acc, stack)

def _list_to_dict(lst):
    # keys must be strings
    keys = [k.decode() for k in lst[::2]]
    vals = lst[1::2]
    return dict(zip(keys, vals))