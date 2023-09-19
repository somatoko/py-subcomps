import requests
import hashlib
import socket
import app.core as core


def peers_command(torrent):
    t = TorrentFile(torrent)
    for p in t.get_peers():
        print(p)

def handshake_command(torrent, peer_address):
    t = TorrentFile(torrent)
    t.send_handshake(peer_address)

def command_download_piece(torrent, out_name, piece_index):
    print('- obtaining', torrent, out_name, piece_index)
    t = TorrentFile(torrent)
    t.download_piece(piece_index)


class TorrentFile:

    def __init__(self, content):
        self.content = content

    @property
    def url(self):
        return self.content['announce'].decode()

    @property
    def hash_as_bytes(self):
        info_bencoded = core.bencode(self.content['info'])
        return hashlib.sha1(info_bencoded).digest()

    @property
    def hash_as_hex(self):
        info_bencoded = core.bencode(self.content['info'])
        return hashlib.sha1(info_bencoded).hexdigest()

    @property
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
        blocks = peer.obtain_piece(self._construct_handshake(), 0, self.piece_length)
        if blocks is None:
            print(f"Error: failed to obtain piece at index {index}")
        # TODO: verify sha-1 of each piece
        piece = b''
        for (_, _, data) in blocks:
            piece += data
        with open('output.txt', 'wb') as out:
            out.write(piece)


class Peer:

    def __init__(self, addr_str):
        parts = addr_str.split(':')
        self.host = parts[0]
        self.port = int(parts[1])
    
    def __str__(self):
        return f'{self.id}:{self.port}'
    
    def handshake(self, payload):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        sent = s.sendall(payload)
        response = s.recv(4096)
        s.close()
        peer_id = response[48:]
        print(f'Peer ID: {peer_id.hex()}')
    
    def obtain_piece(self, handshake, piece_i, piece_length):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))

        # -> handshake 
        s.sendall(handshake)
        # <- handshake : peer id 
        response = s.recv(4096)
        print(f'Peer ID: {response[48:].hex()}')
        
        # Note: message layout \4bytes:length/\1byte:messageID/\Nbytes:payload/
        # length in bytes includes the size of payload + 1 for the messageID

        # <- bitfield message: mid == 5
        _prefix = s.recv(4)
        _len = int.from_bytes(_prefix)
        _mid = int.from_bytes(s.recv(1))
        if _mid != 5:
            print('...', _len, _mid)
            return
        _payload = s.recv(_len - 1)
        print('>>> bitfield', _len, _mid, _payload.hex())

        # -> interested message
        msg = _mk_message(2)
        print('=>', msg.hex())
        s.sendall(msg)

        # <- unchoke message: mid == 1
        _prefix = s.recv(4)
        _len = int.from_bytes(_prefix)
        _mid = int.from_bytes(s.recv(1))
        if _mid != 1:
            print('[E] - missed unchoke', _len, _mid)
            return None

        # -> block requests
        piece_blocks = piece_length // (16 * 1024)
        blocks = [u for u in range(piece_blocks)]
        # last block has no uniform size of 2^14
        blocks.pop()
        for i in blocks:
            msg = _mk_block_req(piece_i, i * 2**14, 2**14)
            s.sendall(msg)

        last_block_length = piece_length - (len(blocks) * 2**14)
        msg = _mk_block_req(piece_i, len(blocks) * 2**14, last_block_length)
        s.sendall(msg)

        # A list of tuples (piece_index, block_offset, block_bytes)
        blocks = []
        
        received = 0
        while received < len(blocks) + 1:
            _prefix = s.recv(4)
            _len = int.from_bytes(_prefix)
            _mid = int.from_bytes(s.recv(1))
            if _mid != 7:
                print('[E] - missed block response', _len, _mid)
                return None

            # Reliably receive proposed length
            # https://stackoverflow.com/a/55825906
            buff = bytearray(_len - 1)
            pos = 0
            while pos < _len - 1:
                cr = s.recv_into(memoryview(buff)[pos:])
                if cr == 0:
                    raise EOFError
                pos += cr
            _payload = buff
            _index = int.from_bytes(_payload[:4])
            _begin = int.from_bytes(_payload[4:8])
            _block = _payload[8:]
            blocks.append((_index, _begin, _block))
            received += 1
        
        s.close()
        return blocks

def _mk_message(id, payload=bytes([])):
    prefix = (len(payload)+1).to_bytes(4, byteorder='big')
    msg = prefix + bytes([id])
    if len(payload) > 0:
        msg += payload
    return msg

def _mk_block_req(piece_i, begin, length):
    payload = (
        piece_i.to_bytes(4, byteorder='big')
        + begin.to_bytes(4, byteorder='big')
        + length.to_bytes(4, byteorder='big')
    )
    return _mk_message(6, payload)