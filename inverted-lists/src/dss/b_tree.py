import struct

from .b_tree_node import BTreeNode

class BTree:
    # key-value pair entry
    fmt = '<LLH'
    block_ref_size = 2
    element_size = struct.calcsize(fmt)

    def __init__(self, block_storage):
        self._block_storage = block_storage

        # ensure we have a root node; must always be in-memory
        if self._block_storage.block_count == 0:
            self.root = BTreeNode(self._block_storage.create_block())
            self.root.is_leaf = True
            self.root.persist()
        else:
            block = self._block_storage.find_block(0)
            block.load_header()
            if block.next_id != 0:
                block = self._block_storage.find_block(block.next_id)
            self.root = BTreeNode(block)
    
    def put(self, key, value):
        if self.root.length == self.root.capacity:
            self.split_root()
        self.put_non_full(self.root, key, value)

    def find(self, key):
        return self.tree_search(self.root, key)

    def delete(self, key):
        return self.tree_delete(self.root, key)

    # ----------------- Internal API

    @property
    def block_capacity(self):
        possible = (self._block_storage.content_size - self.block_ref_size) // self.element_size
        if possible % 2 == 0:
            return possible - 1
        return possible

    def tree_delete(self, node, key):
        if node.is_leaf:
            i = 0
            while i < node.length and key > node.keys[i]:
                i += 1
            if i < node.length and key == node.keys[i]:
                for j in range(i+1, node.length):
                    node.keys[j-1] = node.keys[j]
                    node.vals[j-1] = node.vals[j]
                node.length -= 1
                node.persist()
        else:
            i = 0
            while i < node.length and key > node.keys[i]:
                i += 1
            
            if i < node.length and key == node.keys[i]:
                min_keys = (node.capacity + 1) // 2

                left = self.get_child(node, i)
                if left.length >= min_keys:
                    key, val = self.delete_predecessor(left)
                    node.keys[i] = key
                    node.vals[i] = val
                    node.persist()
                    return

                right = self.get_child(node, i+1)
                if right.length >= min_keys:
                    key, val = self.delete_successor(left)
                    node.keys[i] = key
                    node.vals[i] = val
                    node.persist()
                    return
                
                # merge k and `right`` into `left`
                left.keys[left.length] = node.keys[i]
                left.vals[left.length] = node.vals[i]
                for j in range(0, right.length):
                    left.keys[left.length + 1 + j] = right.keys[j]
                    left.vals[left.length + 1 + j] = right.vals[j]
                    left.refs[left.length + 1 + j] = right.refs[j]
                left.refs[left.length + 1 + right.length] = right.refs[right.length]
                left.length = left.length + 1 + right.length
                left.persist()
                # right.id is a free block id now
                # shift current node key-vals-refs
                for j in range(i, node.length-1):
                    node.keys[j] = node.keys[j+1]
                    node.vals[j] = node.vals[j+1]
                    node.refs[j+1] = node.refs[j+1+1]
                node.keys[node.length-1] = 0
                node.vals[node.length-1] = 0
                node.refs[node.length] = 0
                node.persist()
                self.tree_delete(left, key)
            
            else:
                # this node doesn't contain the key; continue searching ensuring that each
                # node visited has at least (capacity + 1) // 2 keys
                min_keys = (node.capacity + 1) // 2
                target = self.get_child(node, i)
                if target.length >= min_keys:
                    return self.tree_delete(target, key)
                
                left = None if i == 0 else self.get_child(node, i-1)
                if left is not None and left.length >= min_keys:
                    # prepare left sibling
                    key_from_left = left.keys[left.length - 1]
                    val_from_left = left.vals[left.length - 1]
                    ref_from_left = left.refs[left.length]
                    left.keys[left.length - 1] = 0
                    left.vals[left.length - 1] = 0
                    left.refs[left.length] = 0
                    left.length -= 1
                    left.persist()

                    # adjust target
                    for j in range(target.length-1, -1, -1):
                        target.keys[j+1] = target.keys[j]
                        target.vals[j+1] = target.vals[j]
                        target.refs[j+1+1] = target.refs[j+1]
                    target.refs[1] = target.refs[0]

                    # insert into target extra entry
                    target.keys[0] = node.keys[i]
                    target.vals[0] = node.vals[i]
                    target.refs[0] = ref_from_left
                    target.length += 1
                    target.persist()

                    # adjust parent
                    node.keys[i] = key_from_left
                    node.vals[i] = val_from_left
                    node.persist()
                    return self.tree_delete(target, key)

                right = None if i == node.length else self.get_child(node, i+1)
                if right is not None and right.length >= min_keys:
                    # prepare right sibling
                    key_from_right = right.keys[0]
                    val_from_right = right.vals[0]
                    ref_from_right = right.refs[0]
                    for j in range(1, right.length):
                        right.keys[j-1] = right.keys[j]
                        right.vals[j-1] = right.vals[j]
                        right.refs[j-1] = right.refs[j]
                    right.refs[right.length-1] = right.refs[right.length]
                    right.length -= 1
                    right.persist()

                    # adjust target
                    target.keys[target.length] = node.keys[i]
                    target.vals[target.length] = node.vals[i]
                    target.refs[target.length+1] = ref_from_right
                    target.length += 1
                    target.persist()

                    # adjust parent
                    node.keys[i] = key_from_right
                    node.vals[i] = val_from_right
                    node.persist()
                    return self.tree_delete(target, key)
                
                # Nor target nor its siblings have enough keys, we need to merge.
                if left is not None:
                    # make room in target to put new values from left
                    for j in range(target.length-1, -1, -1):
                        target.keys[j + 1 + left.length] = target.keys[j]
                        target.vals[j + 1 + left.length] = target.vals[j]
                        target.refs[j + 1 + left.length + 1] = target.refs[j+1]
                    target.refs[left.length + 1] = target.refs[0]
                    # put the parent's key in place
                    target.keys[left.length] = node.keys[i-1]
                    target.vals[left.length] = node.vals[i-1]
                    for j in range(i, node.length):
                        node.keys[j-1] = node.keys[j] 
                        node.vals[j-1] = node.keys[j] 
                        node.refs[j-1] = node.refs[j] 
                    node.refs[node.length-1] = node.refs[node.length] 
                    node.length -= 1
                    node.persist()

                    for j in range(left.length):
                        target.keys[j] = left.keys[j]
                        target.vals[j] = left.vals[j]
                        target.refs[j] = left.refs[j]
                    target.refs[left.length] = left.refs[left.length]
                    target.length += 1 + left.length
                    target.persist()
                    # print left.id is now free

                    if self.root == node and node.length == 0:
                        # print node.id is now free
                        self.root = target

                    return self.tree_delete(target, key)


                if right is not None:
                    # put the parent's entry in place
                    target.keys[target.length] = node.keys[i]
                    target.vals[target.length] = node.vals[i]

                    for j in range(i+1, node.length):
                        node.keys[j-1] = node.keys[j] 
                        node.vals[j-1] = node.keys[j] 
                        node.refs[j-1] = node.refs[j] 
                    node.refs[node.length-1] = node.refs[node.length] 
                    node.length -= 1
                    node.persist()

                    for j in range(right.length):
                        target.keys[target.length + 1 + j] = right.keys[j]
                        target.vals[target.length + 1 + j] = right.vals[j]
                        target.refs[target.length + 1 + j] = right.refs[j]
                    target.refs[target.length + 1 + right.length] = right.refs[right.length]
                    target.length += 1 + right.length
                    target.persist()
                    # print right.id is now free

                    if self.root == node and node.length == 0:
                        # print node.id is now free
                        self.root = target

                    return self.tree_delete(target, key)
    

    def tree_search(self, node, key):
        i = 0
        while i < node.length and key > node.keys[i]:
            i += 1

        if i < node.length and node.keys[i] == key:
            return (f'- node({node.id}) - {node.vals[i]}')
        elif node.is_leaf:
            return None
        else:
            child = self.get_child(node, i)
            return self.tree_search(child, key)

    def put_non_full(self, node, key, value):
        i = node.length - 1

        if node.is_leaf:
            while i >= 0 and key < node.keys[i]:
                node.keys[i+1] = node.keys[i]
                node.vals[i+1] = node.vals[i]
                i -= 1
            node.keys[i+1] = key
            node.vals[i+1] = value
            node.length += 1
            node.persist()
        else:
            # find appropriate child sub-range
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            child = self.get_child(node, i)
            if child.length == child.capacity:
                self.split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            child = self.get_child(node, i)
            self.put_non_full(child, key, value)
    
    def delete_predecessor(self, node):
        # just find the right-most element
        if node.is_leaf:
            key = node.keys[node.length-1]
            val = node.vals[node.length-1]
            node.length -= 1
            node.persist()
            return (key, val)
        
        return self.delete_predecessor(self.get_child(node, node.length))
    
    def delete_successor(self, node):
        # just find the left-most element
        if node.is_leaf:
            key = node.keys[0]
            val = node.vals[0]
            for i in range(0, node.length-1):
                node.keys[i] = node.keys[i+1]
                node.vals[i] = node.vals[i+1]
            node.length -= 1
            node.persist()
            return (key, val)
        
        return self.delete_successor(self.get_child(node, 0))

    def allocate_node(self):
        block = self._block_storage.create_block()
        node = BTreeNode(block)
        return node
    
    def split_root(self):
        s = self.allocate_node()
        s.is_leaf = False
        s.length = 0
        s.refs[0] = self.root.id
        s.persist()

        self.root = s
        # The root block we store in the 0-th block under next_id property
        zero_block = self._block_storage.find_block(0)
        zero_block.load_header()
        zero_block.next_id = s.id
        zero_block.flush_header()

        self.split_child(s, 0)

    def split_child(self, parent_node, child_index):
        ''' The child parameter is a full node that needs to be split.
        '''

        x = parent_node

        y = self.get_child(x, child_index)

        z = self.allocate_node()
        z.is_leaf = y.is_leaf

        half = y.capacity // 2
        z.length = half

        # Copy upper half of Y to new node
        for i in range(half+1, y.capacity):
            left_i = i - half - 1
            z.keys[left_i] = y.keys[i]
            z.vals[left_i] = y.vals[i]

        if not y.is_leaf:
            for i in range(half+1, y.capacity+1):
                left_i = i - half - 1
                z.refs[left_i] = y.refs[i]

        y.length = half

        # Make room for Z child in X by shifting prev elements after i to right.
        # Note: range(a,b,c) b is not inclusive event if c < 0!
        for j in range(x.length, child_index, -1):
            x.refs[j+1] = x.refs[j]
        x.refs[child_index+1] = z.id

        # Shift the corresponding keys and vals in X
        for j in range(x.length-1, child_index-1, -1):
            x.keys[j+1] = x.keys[j]
            x.vals[j+1] = x.vals[j]
        x.keys[child_index] = y.keys[half]
        x.vals[child_index] = y.vals[half]

        # clear the copied upper half
        y.keys[half:] = [0]*half
        y.vals[half:] = [0]*half

        x.length += 1

        x.persist()
        y.persist()
        z.persist()

    def get_child(self, node, index):
        block_id = node.refs[index]
        block = self._block_storage.find_block(block_id)
        return BTreeNode(block)


