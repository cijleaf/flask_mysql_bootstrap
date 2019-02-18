#!/usr/bin/python
# -*- coding: utf-8 -*-


import MySQLdb
from settings import *


class Dbreader():
    def __init__(self):
        self.db = MySQLdb.connect(host=HOST,
                                  user=USER,
                                  passwd=PWD,
                                  db=DB,
                                  charset="utf8",
                                  use_unicode=True)
        self.cur = self.db.cursor()

    def execute(self, sql):
        # print sql
        self.cur.execute(sql)
        return self.cur.fetchall()

    def get_unic_urls(self, site):
        sql = "SELECT url FROM listings WHERE site = '{}';".format(site)
        return self.execute(sql)

    def get_buildings(self):
        sql = "SELECT building FROM buildings;"
        return self.execute(sql)

    def close(self):
        self.db.close()
