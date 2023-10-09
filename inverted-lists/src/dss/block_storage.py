import os
from .block import Block


class BlockStorage:
    ''' Class that manages a single file that contains blocks.
        - Each block has a fixed size.
        - The block consists of Header and Body section.
        - Header is a Long (8 bytes) reserved for whatever reason.
        - Body is the container of actual bytes.
    '''

    def __init__(self, path, header_size=10, block_size=1024):
        ''' It's best to align block_size with underlying file system block size.
            Valid examples 256B, 512B, 1024B, ..., 4KB.
        '''
        self.filename = os.path.basename(path)
        self.header_size = header_size
        self.block_size = block_size
        self.block_count = 0

        self.file = self._activate_file(path)
        self.block_count = self._count_blocks()

    # ----------------- Properties

    @property
    def content_size(self):
        return self.block_size - self.header_size

    # ----------------- Public API

    def find_block(self, block_id):
        if block_id >= self.block_count:
            # raise ValueError('Block ID exceeds present blocks')
            return None
        block = Block(block_id, self.file,
                      self.header_size, self.block_size)
        return block

    def create_block(self):
        zeroes = bytearray(b'\x00') * self.block_size
        self.file.seek(0, os.SEEK_END)
        self.file.write(zeroes)
        self.block_count += 1
        block = Block(self.block_count - 1, self.file,
                      self.header_size, self.block_size)
        return block

    # ----------------- Internal API

    def cleanup(self):
        if self.file is not None:
            print(f'- file close for {self.filename}')
            self.file.close()

    # ----------------- Private methods

    def _activate_file(self, path):
        if not os.path.exists(path):
            open(path, 'w').close()
        file = open(path, 'r+b')
        print(f'- file open for {self.filename}')
        return file

    def _count_blocks(self):
        ''' According to storage specifications and underlying file size
            compute the number of available blocks.
        '''
        if self.file is not None:
            curr_pos = self.file.tell()
            self.file.seek(0, os.SEEK_END)
            byte_count = self.file.tell()
            self.file.seek(curr_pos, os.SEEK_SET)

            num_blocks = byte_count // self.block_size
            return num_blocks

        raise ValueError(
            f'BlockStorage for {self.filename} has no corresponding storage file.')
