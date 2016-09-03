# coding: utf-8
import os
import struct

from portalocker import portalocker

__author__ = 'Jux.Liu'


class Storage(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.locked = False

        self.superblock_size = 4096
        self._ensure_superblock()

        self.integer_format = '!Q'
        self.integer_length = 8

    def lock(self):
        '''
        lock file, platform independence
        :return:
        '''
        if self.locked:
            return False
        else:
            portalocker.lock(self.dbfile, portalocker.LOCK_EX)
            self.locked = True
            return True

    def unlock(self):
        '''
        unlock file, platform independence
        :return:
        '''
        if self.locked:
            self.dbfile.flush()
            portalocker.unlock(self.dbfile)
            self.locked = False
            return True
        else:
            return False

    def _seek_end(self):
        self.dbfile.seek(0, os.SEEK_END)

    def _seek_superblock(self):
        self.dbfile.seek(0)

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
        '''
        close dbfile
        :return:
        '''
        self.unlock()
        self.dbfile.close()

    def _integer_to_bytes(self, integer):
        '''
        trans integers to bytes
        :param integer: integer from data
        :return:
        '''
        return struct.pack(self.integer_format, integer)

    def _write_integer(self, integer):
        '''
        write integer->bytes into dbfile
        :param integer: integer from data
        :return:
        '''
        self.dbfile.write(self._integer_to_bytes(integer))

    def write(self, data):
        '''
        write data into dbfile
        :param data:
        :return:
        '''
        self.lock()
        self._seek_end()
        object_address = self.dbfile.tell()
        self._write_integer(len(data))
        self.dbfile.write(data)
        self.unlock()
        return object_address

    def _bytes_to_integer(self, integer_bytes):
        '''
        trans bytes to integer tuple
        :param integer_bytes: bytes from dbfile
        :return:
        '''
        return struct.unpack(self.integer_format, integer_bytes)[0]

    def _read_integer(self):
        '''
        read bytes->integer from dbfile
        :return:
        '''
        return self._bytes_to_integer(self.dbfile.read(self.integer_length))

    def read(self, address):
        '''
        read data from dbfile
        :param address: read start address
        :return:
        '''
        self.dbfile.seek(address)
        length = self._read_integer()
        data = self.dbfile.read(length)
        return data

    def commit_root_address(self, root_address):
        '''
        commit changes
        :param root_address: data root address
        :return:
        '''
        self.lock()
        # write changes to dbfile
        self.dbfile.flush()
        # write root_address to superblock
        self._seek_superblock()
        self._write_integer(root_address)
        # write superblock changes to dbfile
        self.dbfile.flush()
        self.unlock()

    def get_root_address(self):
        '''
        get root_address from superblock
        :return:
        '''
        self._seek_superblock()
        root_address = self._read_integer()
        return root_address
