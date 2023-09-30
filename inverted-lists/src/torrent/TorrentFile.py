import os
import time
import requests
import hashlib
import socket
from .core import bencoding_decode, _list_to_dict
from .utils import humanbytes
from .FileDecoder import FileDecoder


class TorrentFile:

    def __init__(self, path):
        self.path = path
        self.__read()

    @property
    def link_size(self):
        return os.path.getsize(self.path)

    @property
    def filename(self):
        return os.path.basename(self.path)

    @property
    def created_fmt(self):
        return _format_epoch_time(self.created)

    @property
    def created(self):
        return os.path.getctime(self.path)

    @property
    def total_size(self):
        return self.count_total_length()

    @property
    def total_size_formatted(self):
        return humanbytes(self.total_size)

    def quick_info(self):
        keys = self.meta.keys()
        print(keys)

    def __read(self):
        # print('==== processing' + os.path.basename(self.path))
        meta = FileDecoder(self.path).process()

        self.announce = meta['announce'].decode()
        self.announce_list = meta['announce-list']
        self.comment = meta['comment'].decode()
        if 'created by' in meta:
            self.created_by = meta['created by'].decode()
        self.creation_date = meta['creation date']

        # -- info
        if 'files' in meta['info']:
            self.files = meta['info']['files']
        else:
            self.length = meta['info']['length']
        self.name = meta['info']['name'].decode()
        self.piece_length = meta['info']['piece length']
        self.pieces = meta['info']['pieces']

    def count_total_length(self):
        if hasattr(self, 'length'):
            return self.length
        else:
            total = 0
            for u in self.files:
                total += u['length']
            return total

    def created_by_line(self):
        created_at = time.strftime(
            '%d %b %Y %H:%M:%S', time.gmtime(self.creation_date))
        if hasattr(self, 'created_by'):
            return f'created by {self.created_by} on {created_at}'
        else:
            return f'created on {created_at}'

    def __str__(self):
        return f'''
        {self.name}
        announce: {self.announce}
        comment: {self.comment}
        {self.created_by_line()}
        total size: {self.total_size_formatted}
    '''


def _format_epoch_time(et):
    _t = time.gmtime(et)
    return time.strftime('%d %b %Y %H:%M:%S', _t)


class TorrentFile2:

    def __init__(self, content):
        self.content = content

    @ property
    def url(self):
        return self.content['announce'].decode()

    @ property
    def hash_as_bytes(self):
        info_bencoded = core.bencode(self.content['info'])
        return hashlib.sha1(info_bencoded).digest()

    @ property
    def hash_as_hex(self):
        info_bencoded = core.bencode(self.content['info'])
        return hashlib.sha1(info_bencoded).hexdigest()

    @ property
    def piece_length(self):
        return self.content['info']['piece length']

    def get_peers(self):
        params = {}
        params['info_hash'] = self.hash_as_bytes
        params['peer_id'] = '00112233445566778899'
        params['port'] = 6881
        params['uploaded'] = 0
        params['downloaded'] = 0
        params['left'] = self.content['info']['length']
        params['compact'] = 1

        r = requests.get(self.url, params=params)
        resp = core.bencoding_decode(r.content)
        return self._parse_peers(resp['peers'])

    def _parse_peers(self, raw):
        peers = [raw[i*6:(i+1)*6] for i in range(len(raw) // 6)]
        results = []

        for p in peers:
            ip = '.'.join([str(u) for u in p[:4]])
            port = int.from_bytes(p[4:], 'big')
            results.append(f'{ip}:{port}')

        return results

    def send_handshake(self, peer_address):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        parts = peer_address.split(':')
        host = parts[0]
        port = int(parts[1])
        s.connect((host, port))
        hs = self._construct_handshake()
        sent = s.sendall(hs)
        response = s.recv(4096)
        s.close()
        peer_id = response[48:]
        print(f'Peer ID: {peer_id.hex()}')

    def _construct_handshake(self):
        hs = (
            bytes([19])
            + b"BitTorrent protocol"
            + b"\x00" * 8
            + self.hash_as_bytes
            + b"00112233445566778899"
        )
        return hs

    def download_piece(self, index):
        peers = self.get_peers()
        peer = Peer(peers[0])
        blocks = peer.obtain_piece(
            self._construct_handshake(), 0, self.piece_length)
        if blocks is None:
            print(f"Error: failed to obtain piece at index {index}")
        # TODO: verify sha-1 of each piece
        piece = b''
        for (_, _, data) in blocks:
            piece += data
        with open('output.txt', 'wb') as out:
            out.write(piece)
