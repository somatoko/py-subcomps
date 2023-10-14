import os
import struct


class Block:

    def __init__(self, _id, fd, header_size=10, block_size=1024):
        self.id = _id
        self._fd = fd
        self.header_size = header_size
        self.block_size = block_size

        self.record_length = 0
        self.next_id = 0
        self.prev_id = 0
        self.is_deleted = False
        self.is_leaf = False

    def get_header(self):
        self._set_cursor(src_offset)
        return self._fd.read(self.header_size)

    @property
    def content_length(self):
        return self.block_size - self.header_size

    def load_header(self):
        self._set_cursor()
        fmt = '>IHH??'
        buf = self._fd.read(self.header_size)
        a, b, c, d, e = struct.unpack(fmt, buf)
        self.record_length = a
        self.next_id = b
        self.prev_id = c
        self.is_deleted = d
        self.is_leaf = e

    def flush_header(self):
        self._set_cursor()
        fmt = '>IHH??'
        bytes = struct.pack(fmt, self.record_length, self.next_id, self.prev_id, self.is_deleted, self.is_leaf)
        self._fd.write(bytes)

    def set_header(self, value):
        self._set_cursor(src_offset)
        self._fd.write(value)

    def read_bytes(self, count, src_offset=0):
        self._set_cursor(self.header_size + src_offset)
        return self._fd.read(count)

    def write_bytes(self, bytes, dst_offset=0):
        self._set_cursor(self.header_size + dst_offset)
        self._fd.write(bytes)

    def _set_cursor(self, extra=0):
        ''' Positions file cursor at the block start + any extra offset given.
        '''
        final_offset = self.block_size * self.id + extra
        self._fd.seek(final_offset, os.SEEK_SET)
        # try:
        # except:
        #     import pdb; pdb.set_trace()
