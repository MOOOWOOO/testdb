# coding: utf-8

__author__ = 'Jux.Liu'

class Storage(object):
    def __init__(self, dbfile):
        self._db = dbfile
        self.locked = False

    def lock(self):
        if self.locked:
            pass

    def unlock(self):
        pass

    def _seek_end(self):
        pass

    superblock_size = 4096

    def _ensure_superblock(self):
        '''
        apply for space for superblock
        :return:
        '''
        self.lock()
        self._seek_end()
        end_address = self._db.tell()
        space = self.superblock_size - end_address
        # full-fill the empty space of superblock
        if space > 0:
            self._db.write(b'\x00' * space)
        else:
            pass
        self.unlock()
