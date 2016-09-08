# coding: utf-8
import pickle

from logical import Value2Addr, LogicalBase

__author__ = 'Jux.Liu'


class BinaryNode(object):
    def __init__(self, left_child, right_child, key, value, length):
        self.left_child = left_child
        self.right_child = right_child
        self.key = key
        self.value = value
        self.length = length

    def store(self, storage):
        self.value.store(storage)
        self.left_child.store(storage)
        self.right_child.store(storage)

    @classmethod
    def from_node(cls, node, **kwargs):
        length = node.length
        for k, v in {'left_child': node.left_child, 'right_child': node.right_child}:
            if k in kwargs:
                length += kwargs[k].length - v.length

        left_child = kwargs.get('left_child', node.left_child)
        right_child = kwargs.get('right_child', node.right_child)
        key = kwargs.get('key', node.key)
        value = kwargs.get('value', node.value)

        return cls(left_child=left_child,
                   right_child=right_child,
                   key=key,
                   value=value,
                   length=length)


class BinaryNode2Addr(Value2Addr):
    def prepare_to_store(self, storage):
        if self._referent:
            self._referent.store(storage)

    @property
    def length(self):
        if self._referent is None and self._address:
            return -1
        elif self._referent:
            return self._referent.length
        else:
            return 0

    @staticmethod
    def referent_to_string(referent):
        return pickle.dumps({
            'left_child': referent.left_child.address,
            'right_child': referent.right_child.address,
            'key': referent.key,
            'value': referent.value,
            'length': referent.length,
        })

    @staticmethod
    def string_to_referent(string):
        d = pickle.loads(string)
        return BinaryNode(
            left_child=BinaryNode2Addr(address=d['left_child']),
            right_child=BinaryNode2Addr(address=d['right_child']),
            key=d['key'],
            value=Value2Addr(address=d['value']),
            length=d['length']
        )


class BinaryTree(LogicalBase):
    node_ref_class = BinaryNode2Addr

    def _get(self, node, key):
        while node is not None:
            if key < node.key:
                node = self._follow(node.left_ref)
            elif node.key < key:
                node = self._follow(node.right_ref)
            else:
                return self._follow(node.value_ref)
        raise KeyError

    def _insert(self, node, key, value_ref):
        if node is None:
            new_node = BinaryNode(
                self.node_ref_class(), key, value_ref, self.node_ref_class(), 1)
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._insert(
                    self._follow(node.left_ref), key, value_ref))
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._insert(
                    self._follow(node.right_ref), key, value_ref))
        else:
            new_node = BinaryNode.from_node(node, value_ref=value_ref)
        return self.node_ref_class(referent=new_node)

    def _delete(self, node, key):
        if node is None:
            raise KeyError
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._delete(
                    self._follow(node.left_ref), key))
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._delete(
                    self._follow(node.right_ref), key))
        else:
            left = self._follow(node.left_ref)
            right = self._follow(node.right_ref)
            if left and right:
                replacement = self._find_max(left)
                left_ref = self._delete(
                    self._follow(node.left_ref), replacement.key)
                new_node = BinaryNode(
                    left_ref,
                    replacement.key,
                    replacement.value_ref,
                    node.right_ref,
                    left_ref.length + node.right_ref.length + 1,
                )
            elif left:
                return node.left_ref
            else:
                return node.right_ref
        return self.node_ref_class(referent=new_node)

    def _find_max(self, node):
        while True:
            next_node = self._follow(node.right_ref)
            if next_node is None:
                return node
            node = next_node
