import math
from .block_storage import BlockStorage


class RecordStorage:

    def __init__(self, path, header_size=10, block_size=1024):
        self.path = path
        self.header_size = header_size
        self.block_size = block_size
        self._block_storage = BlockStorage(path, header_size, block_size)

    # ----------------- Public API

    def insert(self, entity):
        bytes = entity.serialize()

        first_block = self._allocate_fresh_block()
        first_block.record_length = len(bytes)

        if len(bytes) == 0:
            return first_block.id

        data_written = 0
        total_bytes = len(bytes)
        block_capacity = first_block.content_length
        first_block.flush_header()

        write_count = min(block_capacity, total_bytes)
        first_block.write_bytes(bytes[:write_count])
        data_written = write_count

        prev_block = first_block
        while data_written < total_bytes:
            block = self._allocate_fresh_block()
            block.prev_id = prev_block.id
            block.next_id = 0
            prev_block.next_id = block.id
            prev_block.flush_header()

            write_count = min(block_capacity, total_bytes - data_written)
            block.write_bytes(bytes[data_written:data_written + write_count])
            block.flush_header()

            data_written += write_count
            prev_block = block

        return first_block.id

    def find_by_id(self, block_id):
        first_block = self._block_storage.find_block(block_id)
        if first_block is None:
            return None

        first_block.load_header()
        total_bytes = first_block.record_length
        block_cap = first_block.content_length

        bytes_read = min(block_cap, total_bytes)
        result_bytes = bytearray(first_block.read_bytes(bytes_read))

        block = first_block
        while block.next_id > 0:
            block = self._block_storage.find_block(block.next_id)
            block.load_header()
            read_count = min(block_cap, total_bytes - bytes_read)

            result_bytes.extend(block.read_bytes(read_count))
            bytes_read += read_count
        
        return result_bytes

    def update(self, block_id, entity):
        data = entity.serialize()
        total_length = len(data)

        first_block = self._block_storage.find_block(block_id, True)
        if first_block is None:
            raise ValueError('Unable to load block with a given block id.')

        block_capacity = first_block.content_length
        blocks_needed = math.ceil(total_length / block_capacity)

        reuse_blocks = []
        if first_block.next_id:
            reuse_blocks.append(first_block.next_id)

        # write first block setting
        first_block.record_length = total_length
        first_block.next_id = 0
        first_block.prev_id = 0
        first_block.data = data[:block_capacity]

        if blocks_needed == 1:
            first_block.flush()
            return first_block.id
        
        def get_free_block():
            if reuse_blocks:
                b = self._block_storage.find_block(reuse_blocks.pop(), True)
                if b.next_id:
                    reuse_blocks.append(b.next_id)
                return b
            else:
                return self._allocate_fresh_block()
        
        offset = block_capacity
        prev_block = first_block
        for i in range(1, blocks_needed):
            curr_block = get_free_block()

            curr_block.prev_id = prev_block.id
            curr_bytes_size = min(block_capacity, total_length - offset)
            curr_block.data = data[offset : offset + curr_bytes_size]
            offset += curr_bytes_size

            prev_block.next_id = curr_block.id
            prev_block.flush()

            prev_block = curr_block
        
        prev_block.next_id = 0
        prev_block.flush()

        freed_blocks = []
        while reuse_blocks:
            next_id = reuse_blocks.pop()
            freed_blocks.append(next_id)
            b = load_block(reuse_blocks.pop())
            b.load_header()
            if b.next_id:
                reuse_blocks.append(b.next_id)

            b.record_length = 0
            b.next_id = 0
            b.prev_id = 0
            b.is_deleted = True
            b.is_leaf = False
            b.data = [0] * block_capacity
            b.flush()
        
        # TODO: register freed_block with the zero-block
        # print(f' = {len(freed_blocks)} block(s) became free')
        # print(f' = record updated; bytes written: {offset}/{total_length}')
        return first_block.id


    def delete(self, block_id):
        raise NotImplementedError

    # ----------------- Internal methods

    def _allocate_fresh_block(self):
        block = self._find_recycled_block()
        if block is not None:
            return block

        block = self._block_storage.create_block()
        return block

    def _find_recycled_block(self):
        space_blocks = self._find_blocks(0)

        # Block 0 will always be empty to be a root of deleted blocks
        # and to point to the next available block.
        if len(space_blocks) < 2:
            return None

        recycled = space_blocks.pop()
        last_block = space_blocks[-1]
        last_block.next_id = 0

        return recycled

    def _find_blocks(self, record_id):
        ''' Collect all blocks for a given record in correct order.
        '''
        blocks = []

        block = self._block_storage.find_block(record_id)
        if block is None:
            if record_id == 0:
                # Special case for 0-tracking block which was never created
                block = self._block_storage.create_block()
            else:
                raise ValueError(f'Unable to locate record by id: {record_id}')

        if block.is_deleted:
            raise ValueError(
                f'One record`s block {record_id} is marked as deleted!')

        blocks.append(block)
        curr_id = block.next_id

        while curr_id > 0:
            block = self._block_storage.find_block(curr_id)
            if block is None:
                raise ValueError(f'Unable to locate record by id: {record_id}')
            if block.is_deleted:
                raise ValueError(
                    f'One record`s block {record_id} is marked as deleted!')
            blocks.append(block)
            curr_id = block.next_id

        return blocks

    def cleanup(self):
        self._block_storage.cleanup()
