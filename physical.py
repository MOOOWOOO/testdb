# coding: utf-8
import os

from portalocker import portalocker

__author__ = 'Jux.Liu'

class Storage(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.locked = False

        self.superblock_size = 4096
        self._ensure_superblock()

    def lock(self):
        if self.locked:
            return False
        else:
            portalocker.lock(self.dbfile, portalocker.LOCK_EX)
            self.locked = True
            return True

    def unlock(self):
        if self.locked:
            self.dbfile.flush()
            portalocker.unlock(self.dbfile)
            self.locked = False
            return True
        else:
            return False

    def _seek_end(self):
        self.dbfile.seek(0, os.SEEK_END)

    def _ensure_superblock(self):
        '''
        apply for space for superblock
        :return:
        '''
        self.lock()
        self._seek_end()
        end_address = self.dbfile.tell()
        empty_space = self.superblock_size - end_address
        # full-fill the empty space of superblock
        if empty_space > 0:
            self.dbfile.write(b'\x00' * empty_space)
        else:
            pass
        self.unlock()

    @property
    def closed(self):
        return self.dbfile.closed

    def close(self):
        self.unlock()
        self.dbfile.close()
