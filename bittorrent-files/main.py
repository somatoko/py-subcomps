import json
import sys
import numbers
import hashlib
import app.peers as peers
import argparse

class State:
    in_dict = False

    def __init__(self, in_dict=False):
        self.in_dict = in_dict

# import bencodepy - available if you need it!
# import requests - available if you need it!

# Examples:
#
# - decode_bencode(b"5:hello") -> b"hello"
# - decode_bencode(b"10:hello12345") -> b"hello12345"
def decode_bencode(bencoded_value):
    return parse(bencoded_value)

def parse(term, acc=[], stack=[]):
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
                return parse(term[1:], acc, stack)
            else:
                acc.pop()
                stack[-2].append(stack.pop())
                return parse(term[1:], acc, stack)

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
        return parse(term[stop:], acc, stack)

    if term[0] == ord('i'):
        stop = term.find(b'e')
        val = int(term[1:stop])
        if len(stack) == 0:
            stack.append(val)
        else:
            stack[-1].append(val)
        return parse(term[stop+1:], acc, stack)

    if term[0] == ord('l'):
        acc.append(State())
        stack.append([])
        return parse(term[1:], acc, stack)

    if term[0] == ord('d'):
        acc.append(State(in_dict=True))
        stack.append([])
        return parse(term[1:], acc, stack)

def _list_to_dict(lst):
    # keys must be strings
    keys = [k.decode() for k in lst[::2]]
    vals = lst[1::2]
    return dict(zip(keys, vals))


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

# Returns decoded torrent file
def _read_torrent_file(path):
    with open(path, 'rb') as input:
        return decode_bencode(input.read())


def main():
    command = sys.argv[1]

    if command == "decode":
        bencoded_value = sys.argv[2].encode()

        # json.dumps() can't handle bytes, but bencoded "strings" need to be
        # bytestrings since they might contain non utf-8 characters.
        #
        # Let's convert them to strings for printing to the console.
        def bytes_to_str(data):
            if isinstance(data, bytes):
                return data.decode()

            raise TypeError(f"Type not serializable: {type(data)}")

        print(json.dumps(decode_bencode(bencoded_value), default=bytes_to_str))

    elif command == "info":
        file_path = sys.argv[2]
        bt = _read_torrent_file(file_path)
        info_bencoded = bencode(bt['info'])
        hash = hashlib.sha1(info_bencoded).hexdigest()
        url = bt["announce"].decode()
        result = f'Tracker URL: {url}\nLength: {bt["info"]["length"]}\nInfo Hash: {hash}'
        print(result)

        # -- piece length
        piece_length = bt["info"]["piece length"]
        print(f'Piece Length: {piece_length}')

        # -- pieces
        pieces = bt["info"]["pieces"]
        _from = [u for u in range(0, len(pieces) // 20)]
        _from = [u * 20 for u in _from]
        _to = _from[1:] + [len(pieces)]

        print('Piece Hashes:')
        for i, j in zip(_from, _to):
            print(pieces[i:j].hex())

    elif command == "peers":
        file_path = sys.argv[2]
        bt = _read_torrent_file(file_path)
        peers.peers_command(bt)

    elif command == "handshake":
        file_path = sys.argv[2]
        peer_address = sys.argv[3]

        bt = _read_torrent_file(file_path)
        peers.handshake_command(bt, peer_address)

    elif command == "download_piece":
        del sys.argv[1]
        parser = argparse.ArgumentParser(description='Simple BitTorrent implementation')
        parser.add_argument('torrent_file', help='Which torrent file to use')
        parser.add_argument('piece_index', type=int, help='Which piece index to download')
        parser.add_argument('-o', '--output', dest='output_file')
        args = parser.parse_args()

        torrent = _read_torrent_file(args.torrent_file)
        peers.command_download_piece(torrent, args.output_file, args.piece_index)

    else:
        raise NotImplementedError(f"Unknown command {command}")


if __name__ == "__main__":
    main()
