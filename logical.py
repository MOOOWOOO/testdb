# coding: utf-8

__author__ = 'Jux.Liu'


class LogicalBase(object):
    err_msg = "{classname}.{methodname} must be implemented"
    node_ref_class = None
    value_ref_class = Value2Addr

    def __init__(self, storage):
        self._storage = storage
        self._refresh_tree_ref()

    def commit(self):
        self._tree_ref.store(self._storage)
        self._storage.commit_root_address(self._tree_ref.address)

    def _refresh_tree_ref(self):
        self._tree_ref = self.node_ref_class(address=self._storage.get_root_address())

    def _get(self, node, key):
        raise NotImplementedError(self.err_msg.format(classname=str(self.__class__), methodname='_get()'))

    def _insert(self, node, key, value_ref):
        raise NotImplementedError(self.err_msg.format(classname=str(self.__class__), methodname='_insert()'))

    def _delete(self, node, key):
        raise NotImplementedError(self.err_msg.format(classname=str(self.__class__), methodname='_delete()'))

    def get(self, key):
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def set(self, key, value):
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(self._follow(self._tree_ref), key, self.value_ref_class(value))

    def pop(self, key):
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._delete(self._follow(self._tree_ref), key)

    def _follow(self, ref):
        return ref.get(self._storage)

    def __len__(self):
        if not self._storage.locked:
            self._refresh_tree_ref()
        root = self._follow(self._tree_ref)
        if root:
            return root.length
        else:
            return 0


class Value2Addr(object):
    err_msg = "{classname}.{methodname} must be implemented"

    def __init__(self, referent=None, address=0):
        self._referent = referent
        self._address = address

    @staticmethod
    def referent_to_string(referent):
        return referent.encode('utf-8')

    @staticmethod
    def string_to_referent(string):
        return string.decode('utf-8')

    def prepare_to_store(self, storage):
        raise NotImplementedError(self.err_msg.format(classname=str(self.__class__), methodname='_get()'))

    def store(self, storage):
        if self._referent is None or self._address == 0:
            pass
        else:
            self.prepare_to_store(storage)
            self._address = storage.write(self.referent_to_string(self._referent))

    @property
    def address(self):
        return self._address

    def query(self, storage):
        if self._referent is None and self._address:
            self._referent = self.string_to_referent(storage.read(self._address))
        else:
            pass
        return self._referent
