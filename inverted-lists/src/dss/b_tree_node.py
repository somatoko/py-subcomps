import struct

class BTreeNode:
    key_size = 4
    val_size = 4
    ref_size = 2 # ref to child or child id number size

    def __init__(self, block):
        self._block = block

        self.id = self._block.id
        self.length = 0
        self.is_leaf = False
        self.capacity = self._calc_capacity()
        self.keys = []
        self.vals = []
        self.refs = []
        self._init_data()
    

    def get_child_node(self, index):
        pass
    
    # ----------------- public API

    def persist(self):
        # --- block - header

        self._block.is_leaf = self.is_leaf
        self._block.record_length = self.length
        self._block.flush_header()

        # --- block - body

        # fill empty elements with zeroes so we have correct file bytes alignments
        keys_out = self.keys + [0] * (self.capacity - len(self.keys))
        vals_out = self.vals + [0] * (self.capacity - len(self.vals))
        refs_out = self.refs + [0] * (self.capacity + 1 - len(self.refs))

        key_fmt = '<%dL' % (self.capacity)
        val_fmt = '<%dL' % (self.capacity)
        ref_fmt = '<%dH' % (self.capacity + 1)

        key_bytes = struct.pack(key_fmt, *keys_out)
        val_bytes = struct.pack(val_fmt, *vals_out)
        ref_bytes = struct.pack(ref_fmt, *refs_out)

        self._block.write_bytes(key_bytes + val_bytes + ref_bytes)

    # ----------------- Internal API

    # ----------------- Private methdods

    def _init_data(self):
        # --- block - header

        self._block.load_header()
        self.is_leaf = self._block.is_leaf
        self.length = self._block.record_length

        # --- block - body

        total_bytes = self.capacity * (self.key_size + self.val_size + self.ref_size) + self.ref_size
        bytes = self._block.read_bytes(total_bytes)

        len_bytes_keys = self.capacity * self.key_size
        len_bytes_vals = self.capacity * self.val_size
        len_bytes_refs = (self.capacity+1) * self.ref_size

        key_bytes = bytes[:len_bytes_keys]
        val_bytes = bytes[len_bytes_keys:len_bytes_keys + len_bytes_vals]
        ref_bytes = bytes[len_bytes_keys + len_bytes_vals:len_bytes_keys + len_bytes_vals + len_bytes_refs]

        key_fmt = '<%dL' % (self.capacity)
        val_fmt = '<%dL' % (self.capacity)
        ref_fmt = '<%dH' % (self.capacity + 1)

        self.keys = list(struct.unpack(key_fmt, key_bytes))
        self.vals = list(struct.unpack(val_fmt, val_bytes))
        self.refs = list(struct.unpack(ref_fmt, ref_bytes))

    def _calc_capacity(self):
        avalable_bytes = self._block.block_size - self._block.header_size
        approx = (avalable_bytes - self.ref_size) // (self.key_size + self.val_size + self.ref_size)

        # num children must be even, num elements - odd
        if approx % 2 == 0:
            return approx - 1
        return approx
