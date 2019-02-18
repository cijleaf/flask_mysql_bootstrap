#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
from settings import *
import pandas as pd
from datetime import datetime
from common_func import *


class Dbsender():
    def __init__(self):

        self.db = MySQLdb.connect(host=HOST,  # your host, usually localhost
                                  user=USER,  # your username
                                  passwd=PWD,  # your password
                                  db=DB,  # name of the data base
                                  charset="utf8",
                                  use_unicode=True)
        self.cur = self.db.cursor()

    def execute(self, sql):
        # print sql
        self.cur.execute(sql)

    def execute_commit(self, sql):
        self.cur.execute(sql)
        self.cur.commit()

    def clean_broom(self, txt):

        pre = '0' if pd.isna(txt) else str(txt).lower().split(' ')[0].split('+')[0].replace('studio', '0')
        try:
            pre = str(int(float(pre)))
        except:
            pre = '0'
        return pre

    def get_place(self, row, poz):
        pre = row.get('Place_{}'.format(poz))
        out = 'NULL'
        if type(pre) == str:
            out = pre.replace("'", "''")
        return out

    def write_row(self, row, site):
        row.fillna('NULL', inplace=True)
        table = 'listings'

        date = datetime.datetime.strptime(row['Date'], '%d.%m.%Y').strftime('%Y-%m-%d')

        area_ft = str(row['Area ft']).split(' ')[0].replace(',', '').replace('-', '0').split('.')[0]
        url = row['URL']

        name = row['Named'].replace("'", "''").replace('\\', '/')

        try:
            city = row['City'].replace("'", "''")
            city = replace_city(city)
        except:
            city = 'NULL'

        try:
            latitude = round(float(row['Latitude']), 5)
            longitude = round(float(row['Longitude']), 5)
        except:
            latitude = 'NULL'
            longitude = 'NULL'

        types = row['Type']
        listing_type = row['Listing type']
        listing_category = row['Listing category']

        price = str(row['Price']).split(' ')[0].replace(',', '').replace('Ask', 'NULL')
        bathrooms = self.clean_broom(row['Bathrooms'])
        bedrooms = self.clean_broom(row['Bedrooms'])

        reference = row['Reference'] if row.get('Reference') else 'NULL'

        descript = str(row['Descript']).replace("'", "''").replace('\\', '/')

        agency_name = row['Company Name'].replace("'", "''") if row.get('Company Name') else 'NULL'

        agent = row['Agent Name'].replace("'", "''") if row.get('Agent Name') else 'NULL'
        phone = str(row['Phone']).split('.')[0] if row.get('Phone') else 'NULL'
        email = row['Email'] if row.get('Email') else 'NULL'
        building = row['Building'] if row.get('Building') else 'NULL'

        img = row['Img'] if row.get('Img') else 'NULL'

        if 'bayut' not in site:

            place_first = self.get_place(row, 1)
            place_sec = self.get_place(row, 2)
            place_thr = self.get_place(row, 3)
        else:
            place_first = self.get_place(row, 2)
            place_sec = self.get_place(row, 3)
            place_thr = self.get_place(row, 4)
        place_first = replace_place(place_first)
        # print(city, place_first,place_sec,place_thr,bedrooms)
        prehash = city + place_first + place_sec.split('Bayut')[0] + place_thr.split('Bayut')[0] + bedrooms

        hash = prehash.replace('NULL', '').lower().replace(' ', '')

        colls = 'hash,date,area_ft,name,city,place_first,place_sec,place_thr,latitude,longitude,type,listing_type,listing_category,price,bathrooms,bedrooms,reference,descript,agency_name,agent,phone,email,url,site'
        sql = "INSERT INTO {}(hash, date, area_ft, url, site, name, city, latitude, longitude,type,listing_type,listing_category,price,bathrooms,bedrooms,descript,agency_name,reference,agent,phone,email,place_first,place_sec,place_thr,building,img) " \
              "VALUES ('{}','{}',{},'{}','{}','{}','{}',{},{},'{}','{}','{}',{},{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
            table,
            hash,
            date,
            area_ft,
            url,
            site,
            name,
            city,
            latitude,
            longitude,
            types,
            listing_type,
            listing_category,
            price,
            bathrooms,
            bedrooms,
            descript,
            agency_name,
            reference,
            agent,
            phone,
            email,
            place_first,
            place_sec,
            place_thr,
            building,
            img
            )

        sql = sql.replace("'NULL'", 'NULL')
        # print(sql)
        self.cur.execute(sql)

    def write_build(self, row):
        city, build, short = row.split(' | ')
        sql = "INSERT INTO buildings (city, building, short) VALUES ('{}', '{}', '{}');".format(city, build, short)
        self.cur.execute(sql)

    def update_field(self, url, value):
        if value:
            sql = """UPDATE listings SET agency_name='{}' WHERE url='{}'""".format(value, url)
        else:
            sql = """UPDATE listings SET agency_name=NULL WHERE url='{}'""".format(url)
        sql = sql.replace("'NULL'", 'NULL')
        self.execute(sql)

    def update_buildings(self):
        date = datetime.datetime.now().date().strftime('%Y-%m-%d')
        print(date)
        sql = """UPDATE listings, (SELECT ls.id, bd.name FROM (SELECT * FROM listings WHERE date = '{}' ) ls
                LEFT JOIN buildcoord bd ON 
                ls.hash LIKE CONCAT('%', bd.hash, '%')) nn 
                SET listings.building=nn.name
                WHERE nn.id=listings.id AND nn.name IS NOT NULL;""".format(date)
        self.execute(sql)
        print('upd names')
        sql = """UPDATE listings, 
                (SELECT ls.id, bd.name FROM (SELECT * FROM listings 
                WHERE  building IS NULL AND date = '{}') ls
                LEFT JOIN buildcoord bd ON 
                ls.latitude<=bd.latitude+0.0003 AND ls.latitude>=bd.latitude-0.0003 AND
                ls.longitude<=bd.longitude+0.0003 AND ls.longitude>=bd.longitude-0.0003) nn
                SET listings.building=nn.name
                WHERE nn.id=listings.id and nn.name IS NOT NULL;""".format(date)
        self.execute(sql)
        print('upd coord')
        sql = """UPDATE listings, (SELECT ls.id, bd.building 
                FROM (SELECT * FROM listings WHERE building IS NULL AND date = '{}') ls
                LEFT JOIN buildings bd ON 
                ls.hash LIKE CONCAT('%', bd.hash, '%') AND LENGTH(bd.hash)>9 ) nn 
                SET listings.building= REPLACE(nn.building, '- ', '')
                WHERE nn.id=listings.id AND nn.building IS NOT NULL;""".format(date)
        self.execute(sql)
        print('upd other')

        sql = """UPDATE listings, 
                (SELECT ls.id, bd.area FROM listings ls
                INNER JOIN buildcoord bd ON ls.building=bd.name AND ls.place_first != bd.area AND ls.date='{}') nn
                SET listings.place_first = nn.area
                WHERE nn.id=listings.id;""".format(date)
        self.execute(sql)
        print('upd build')

    def update_fast_tabs(self):

        sql = 'TRUNCATE TABLE fast_sites;'
        self.execute(sql)

        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        wdate = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
        sql = """INSERT INTO fast_sites(site, today, week) 
                SELECT td.site, td.cnt, wk.cnt FROM 
                (SELECT site, COUNT(id) cnt FROM listings
                WHERE date='{}'
                GROUP BY site) td
                INNER JOIN (SELECT site, COUNT(id) cnt FROM listings
                WHERE date>='{}' AND date<='{}'
                GROUP BY site) wk
                ON td.site = wk.site;""".format(date, wdate, date)
        self.execute(sql)
        print('upd fast_tables')

    def close(self):
        self.db.commit()
        self.db.close()

    def test(self):
        sql = "CREATE INDEX buildings_building_IDX USING BTREE ON uaerealdb.buildings (building) ;"
        self.cur.execute(sql)


if __name__ == '__main__':
    DB = Dbsender()
    DB.update_buildings()
    DB.close()
