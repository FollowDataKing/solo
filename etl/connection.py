# -*- coding: utf-8 -*-

__author__ = 'baihe'
__author_email__ = 'baihe@xiaomei.com'
__mtime__ = '16/5/17'


class Connection(object):
    def __init__(self, context, host, port, user, password, db):
        self.context = context
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def load_data_frame(self, table, alias=None):
        raise NotImplementedError

    def write_data_frame(self, table):
        raise NotImplementedError 
