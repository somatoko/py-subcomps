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
        print(f'- inserting: {entity.id}:{entity.title}')

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
            print(f' wrote: {data_written}/{total_bytes}')
            prev_block = block
        print('= done')

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
