#!/usr/bin/python
# -*- coding: utf-8 -*-


import MySQLdb
from settings import *
import datetime


class Dbreader():
    def __init__(self):

        self.db = MySQLdb.connect(host=HOST,
                     user=USER,
                     passwd=PWD,
                     db=DB,
                    charset="utf8",
                    use_unicode=True)
        self.db.autocommit = True
        self.cur = self.db.cursor()

    def execute(self, sql):
        print(sql)
        self.db.commit()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def get_unic_urls(self, site):
        sql = "SELECT url FROM listings WHERE site = '{}';".format(site)
        return self.execute(sql)

    def get_buildings(self):
        sql = "SELECT building FROM buildings;"
        return self.execute(sql)

    def get_buildings2(self):
        sql = 'SELECT DISTINCT name FROM buildcoord;'
        return self.execute(sql)

    def get_cities(self):
        sql = """SELECT Distinct city FROM listings;"""
        return self.execute(sql)

    def get_rooms(self):
        sql = """SELECT DISTINCT bedrooms FROM listings;"""
        return self.execute(sql)

    def get_count_listings(self, city, place, rooms, types, category, minprice, maxprice, agency,
                           startdate, enddate, zeros='off'):

        presql = """SELECT building, COUNT(DISTINCT id) FROM listings
                WHERE building IS NOT NULL """

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if types and types != 'All':
            types = " AND listing_type='{}'".format(types)
        else:
            types = ''

        if place:
            place = " AND place_first='{}'".format(place)
        else:
            place = ''

        if rooms and rooms != 'All':
            rooms = " AND bedrooms={}".format(rooms)
        else:
            rooms = ''

        if category[0] != 'All':
            cat = " AND listing_category='{}'".format(category[0])
        else:
            cat = ''

        if category[1] != 'All':
            subcat = " AND `type`='{}'".format(category[1])
        else:
            subcat = ''

        if minprice:
            minprice = " AND price >= {}".format(minprice)
        else:
            minprice = ''

        if maxprice:
            maxprice = " AND price <= {}".format(maxprice)
        else:
            maxprice = ''

        if agency and agency != 'All':
            agency = " AND agency_name = '{}'".format(agency)
        else:
            agency = ''

        if startdate:
            startdate = " AND date >= '{}'".format(startdate)
        else:
            startdate = ''

        if enddate:
            enddate = " AND date <= '{}'".format(enddate)
        else:
            enddate = ''

        if zeros == 'on':
            zeros = " AND COUNT(DISTINCT listing_type)>1"
        else:
            zeros = ""

        templ = [city, types, place, rooms, cat, subcat,
                 minprice, maxprice,
                 agency, startdate, enddate]

        endsql = """ GROUP by building HAVING COUNT(DISTINCT site) > 0{};""".format(zeros)

        sql = presql + ' '.join(templ) + endsql

        return self.execute(sql)

    def check_zeros(self, city, build):
        sql = """SELECT COUNT(DISTINCT listing_type) FROM listings
                WHERE building IS NOT NULL  AND city='{}' AND building='{}' AND price IS NOT NULL
                group by listing_type;""".format(city, build)
        return self.execute(sql)

    def get_listings_aggr(self, city, build, rooms, startdate, enddate, category, maxprice, minprice):
        print('---date', startdate)

        if build:
            build = " AND building='{}'".format(build.replace("'", "''"))
        else:
            build = ''

        if category[0] != 'All':
            cat = " AND listing_category='{}'".format(category[0])
        else:
            cat = ''

        if category[1] != 'All':
            subcat = " AND `type`='{}'".format(category[1])
        else:
            subcat = ''

        if minprice:
            minprice = " AND price >= {}".format(minprice)
        else:
            minprice = ''

        if maxprice:
            maxprice = " AND price <= {}".format(maxprice)
        else:
            maxprice = ''

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if rooms and rooms != 'All':
            rooms = " AND bedrooms={}".format(rooms)
        else:
            rooms = ''

        if startdate:
            startdate = " AND date >= '{}'".format(startdate)
        else:
            startdate = ''

        if enddate:
            enddate = " AND date <= '{}'".format(enddate)
        else:
            enddate = ''

        templ = [city, build, rooms, cat, subcat,
                 maxprice, minprice,
                 startdate, enddate]

        presql = """SELECT * FROM listings WHERE building IS NOT NULL AND price IS NOT NULL """

        sql = presql + ' '.join(templ) + ' ORDER BY price DESC, name ASC;'

        return self.execute(sql)

    def get_listings_build(self, city, build):
        if build:
            build = "{}".format(build.replace("'", "''"))
        else:
            build = "'test'"

        if city and city != 'All':
            city = "{}".format(city)
        else:
            city = "test"

        sql = """SELECT *FROM listings
                WHERE city='{}' AND building='{}' AND bedrooms IN 
                (SELECT bedrooms FROM listings
                WHERE city='{}' AND building='{}'
                GROUP BY bedrooms
                HAVING COUNT(DISTINCT listing_type)=2);""".format(city, build, city, build)
        return self.execute(sql)

    def get_listings(self, city, build, rooms, types, category, minprice, maxprice, agency, startdate, enddate):
        print('---date', startdate)
        if types  and types != 'All':
            types = " AND listing_type='{}'".format(types)
        else:
            types = ''

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if build:
            build = " AND building='{}'".format(build.replace("'", "''"))
        else:
            build = ''

        if rooms and rooms != 'All':
            rooms = " AND bedrooms={}".format(rooms)
        else:
            rooms = ''

        if category[0] != 'All':
            cat = " AND listing_category='{}'".format(category[0])
        else:
            cat = ''

        if category[1] != 'All':
            subcat = " AND `type`='{}'".format(category[1])
        else:
            subcat = ''

        if minprice:
            minprice = " AND price >= {}".format(minprice)
        else:
            minprice = ''

        if maxprice:
            maxprice = " AND price <= {}".format(maxprice)
        else:
            maxprice = ''

        if agency and agency != 'All':
            agency = " AND agency_name = '{}'".format(agency)
        else:
            agency = ''

        if startdate:
            startdate = " AND date >= '{}'".format(startdate)
        else:
            startdate = ''

        if enddate:
            enddate = " AND date <= '{}'".format(enddate)
        else:
            enddate = ''

        templ = [city, types, build, rooms,
                 cat, subcat, minprice, maxprice, agency,
                 startdate, enddate]

        presql = """SELECT * FROM listings WHERE building IS NOT NULL """

        sql = presql + ' '.join(templ) + ' ORDER BY price DESC, name ASC;'

        return self.execute(sql)

    def get_agencies(self):
        sql = "SELECT * FROM agencies;"
        return self.execute(sql)

    def get_places(self, types, city, rooms, category, minprice, maxprice,
                                        agency, startdate, enddate, page):
        presql = """
                SELECT bd.place_first, COUNT(bd.id) FROM
                (SELECT place_first, id FROM listings WHERE building IS NOT NULL """

        if types and types != 'All':
            types = " AND listing_type='{}'".format(types)
        else:
            types = ''

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if rooms and rooms != 'All':
            rooms = " AND bedrooms={}".format(rooms)
        else:
            rooms = ''

        if category[0] != 'All':
            cat = " AND listing_category='{}'".format(category[0])
        else:
            cat = ''

        if category[1] != 'All':
            subcat = " AND `type`='{}'".format(category[1])
        else:
            subcat = ''

        if minprice:
            minprice = " AND price >= {}".format(minprice)
        else:
            minprice = ''

        if maxprice:
            maxprice = " AND price <= {}".format(maxprice)
        else:
            maxprice = ''

        if agency and agency != 'All':
            agency = " AND agency_name = '{}'".format(agency)
        else:
            agency = ''

        if startdate:
            startdate = " AND date >= '{}'".format(startdate)
        else:
            startdate = ''

        if enddate:
            enddate = " AND date <= '{}'".format(enddate)
        else:
            enddate = ''

        templ = [types, city, rooms,
                 cat, subcat, minprice, maxprice, agency,
                 startdate, enddate]

        endsql = """) bd GROUP by bd.place_first
                ORDER BY COUNT(bd.id) DESC;"""

        sql = presql + ' '.join(templ) + endsql

        return self.execute(sql)

    def get_prefile(self, city, rooms, types, category, minprice, maxprice, agency, startdate, enddate):
        if types:
            types = " AND listing_type='{}'".format(types)
        else:
            types = ''

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if rooms and rooms != 'All':
            rooms = " AND bedrooms={}".format(rooms)
        else:
            rooms = ''

        if category[0] != 'All':
            cat = " AND listing_category='{}'".format(category[0])
        else:
            cat = ''

        if category[1] != 'All':
            subcat = " AND `type`='{}'".format(category[1])
        else:
            subcat = ''

        if minprice:
            minprice = " AND price >= {}".format(minprice)
        else:
            minprice = ''

        if maxprice:
            maxprice = " AND price <= {}".format(maxprice)
        else:
            maxprice = ''

        if agency and agency != 'All':
            agency = " AND agency_name = '{}'".format(agency)
        else:
            agency = ''

        if startdate:
            startdate = " AND date >= '{}'".format(startdate)
        else:
            startdate = ''

        if enddate:
            enddate = " AND date <= '{}'".format(enddate)
        else:
            enddate = ''

        templ = [types, city, rooms,
                 cat, subcat, minprice, maxprice, agency,
                 startdate, enddate]

        presql = """SELECT * FROM listings WHERE building IS NOT NULL """

        sql = presql + ' '.join(templ) + ' ORDER BY price DESC, name ASC;'

        return self.execute(sql)

    def get_site_count(self):
        sql = """SELECT site, today, week FROM fast_sites;"""
        return self.execute(sql)

    def get_premap(self, city, today=True):

        if city and city != 'All':
            city = " AND city='{}'".format(city)
        else:
            city = ''

        if today:
            predate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            date = " AND date='{}'".format(predate)
        else:
            date = ''

        sql = """SELECT area_ft, latitude, longitude, bathrooms, bedrooms, img, id FROM listings
                WHERE building IS NOT NULL AND img IS NOT NULL 
                {}{};""".format(city, date)

        return self.execute(sql)

    def get_map_center(self, city):

        if city and city != 'All':
            city = " WHERE city = '{}'".format(city)
        else:
            city = ''

        sql = """SELECT AVG(DISTINCT latitude), AVG(longitude) FROM nonzero_build
                {};""".format(city)
        return self.execute(sql)

    def get_build_data(self, city, build):
        sql = """SELECT building, latitude, longitude FROM listings
                        WHERE building is NOT NULL AND city='Abu Dhabi'
                        GROUP by building 
                        HAVING COUNT(DISTINCT listing_type)>1;"""
        return self.execute(sql)

    def get_best_sale(self):
        date = (datetime.datetime.today() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        sql = """SELECT * FROM top_listings
                INNER JOIN listings
                ON top_listings.top_id = listings.id;"""
        return self.execute(sql)

    def check_pass(self, user, pwd):
        sql = """select count(id) FROM users
                WHERE user='{}' AND pass='{}';""".format(user, pwd)
        return self.execute(sql)

    def check_cook(self, cook):
        sql = """select count(id) FROM `session`
                WHERE cookies='{}';""".format(cook)
        return self.execute(sql)

    def get_full_places(self):
        sql = """SELECT city, place_first, building, AVG(latitude) lat, AVG(longitude) lon, COUNT(id) FROM listings
                GROUP BY city, place_first, building;"""
        return self.execute(sql)

    def close(self):
        self.db.close()

    def test(self):
        sql = """SELECT * FROM listings 
                WHERE date = '2019-01-03' AND site='propertyfinder.ae' 
                AND building IS NOT NULL AND listing_type='Sale'
                ORDER BY id ASC;"""
        return self.execute(sql)


if __name__ == '__main__':
    db = Dbreader()
    pre = db.test()
    for n in pre:
        print(n)
    db.close()

