# coding: utf-8
from logical import Value2Addr

__author__ = 'Jux.Liu'


class BinaryNode(object):
    def __init__(self, left_child, right_child, key, value, length):
        self.left_child = left_child
        self.right_child = right_child
        self.key = key
        self.value  = value
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
            self._referent


class BinaryTree(object):
    pass
