#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
from settings import *
import pandas as pd
from datetime import datetime
from common_func import *
import hashlib


class Dbsender():
    def __init__(self):

        self.db = MySQLdb.connect(host=HOST,    # your host, usually localhost
                     user=USER,         # your username
                     passwd=PWD,  # your password
                     db=DB,        # name of the data base
                    charset="utf8",
                    use_unicode=True)
        self.cur = self.db.cursor()

    def execute(self, sql):
        self.cur.execute(sql)

    def execute_resp(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()

    def write_session(self, user, ip):
        sql = """select id FROM users
                WHERE user='{}';""".format(user)
        user_id = self.execute_resp(sql)[0][0]
        date = datetime.datetime.today().strftime('%Y-%m-%d')
        endate = (datetime.datetime.today() + datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        session = hashlib.md5()
        session.update((user+ip+date).encode('utf8'))
        session = str(session.hexdigest())
        sql = """INSERT INTO `session`(user, ip, cookies, log_date, end_date) 
                 VALUES ({}, '{}', '{}',  '{}', '{}');""".format(user_id,
                                                                 ip,
                                                                 session,
                                                                 date,
                                                                 endate)
        self.execute(sql)
        return session

    def close(self):
        self.db.commit()
        self.db.close()

    def test(self):
        sql = "select * from users;"
        self.cur.execute(sql)


if __name__ == '__main__':
    DB = Dbsender()
    DB.test()
    DB.close()