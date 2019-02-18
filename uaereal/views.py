#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import render_template, url_for, request, send_from_directory, send_file, redirect, make_response
from uaereal import app
from uaereal import mysql_reader
from uaereal import mysql_writer
from uaereal.sort_func import *
from common_func import *
import csv
import os
import random
import time


@app.route('/')
@app.route('/index')
def index():
    cookie = request.cookies.get('sess')

    dbreader = mysql_reader.Dbreader()

    # ----------------------------- check user
    #iscook = dbreader.check_cook(cookie)[0][0]
    #print(iscook)
    #if not iscook:
    #    return 'Bad request'
    # ----------------------------------------

    #place_list = dbreader.get_premap('')
    place_list = []
    if len(place_list) > 100:
        small_place_list = place_list[:100]
    else:
        small_place_list = place_list

    if len(place_list) > 800:
        big_place_list = place_list[:800]
    else:
        big_place_list = place_list

    top_listings = choise_top(dbreader.get_best_sale())

    for top in top_listings:
        print(top)

    agencies = dbreader.get_agencies()
    center = dbreader.get_map_center('')[0]
    precity = dbreader.get_cities()[1:]
    precity = clean_city(precity)
    rooms = dbreader.get_rooms()[:12]
    sites_info = dbreader.get_site_count()
    # agencies = dbreader.get_agencies()    add table

    dbreader.close()

    return render_template('index2.html', cities=precity, rooms=rooms, sites_info=sites_info,
                           small_marks=small_place_list, big_marks=big_place_list, map_center=center,
                           agencies=agencies, top_listings=top_listings)


@app.route('/search')
def search():

    full_args = ['city', 'type', 'place', 'bedrooms', 'page', 'category',
                 'minprice', 'maxprice', 'agency', 'period', 'doubles']
    print(request.args)

    args = clean_args(request.args, full_args)
    dbreader = mysql_reader.Dbreader()

    city = args.get('city')
    types = args.get('type')
    place = args.get('place')
    bedrooms = args.get('bedrooms')
    page = args.get('page')
    category = args.get('category')
    minprice = args.get('minprice')
    maxprice = args.get('maxprice')
    agency = args.get('agency')
    drop = args.get('doubles')
    period = args.get('period')

    url = request.url

    if page:
        url = '='.join(url.split('=')[:-1])+'='
        page = int(page)
    else:
        url = url + '&page='
        page = 0

    startdate, enddate = separate_date(period)
    sum_category = separate_cat(category)

    if place:
        prelists = dbreader.get_count_listings(city, place, bedrooms, types, sum_category, minprice, maxprice,
                                                agency, startdate, enddate)
        data = []
        pages = find_num_pages(page, len(prelists))
        print(pages)
        for i in range(page*BUILD_ON_PAGE, (page+1)*BUILD_ON_PAGE):
            try:
                pre = prelists[i]
            except:

                break

            newtower = []
            predata = dbreader.get_listings(city, pre[0], bedrooms, types, sum_category, minprice, maxprice,
                                                agency, startdate, enddate)

            if drop == 'on':
                predata = drop_double(predata)

            for new in predata:
                newtower.append(replace_price(new))

            if len(newtower):
                data.append(newtower)
            else:
                break

        dbreader.close()
        for dt in data:
            print(dt)
        print(url)
        return render_template('results.html', city=city, data=data, types=types, beds=bedrooms, place=place,
                               maxprice=maxprice, minprice=minprice, category=category,
                               period=period, pages=pages, page=page, preurl=url)

    else:
        preplaces = dbreader.get_places(types, city, bedrooms, sum_category, minprice, maxprice,
                                        agency, startdate, enddate, page)

        prebuild = dbreader.get_buildings()
        builds = set()
        for pre in prebuild:
            builds.add(pre[0])

        places = []
        for place in preplaces:
            try:
                if place[0] not in builds and 'Tower' not in place[0]:
                    places.append(place)
            except:
                continue

        #------ add to base images
        templ_places = []
        n = 0
        for pl in places:
            templ_places.append((pl[0], pl[1], '/static/images/search-area/i{}.png'.format(random.choice(range(1, 9)))))

        templ_places = group_place(templ_places)
        for pl in templ_places:
            print(pl)
        # ------ add to base images

        dbreader.close()
        return render_template('presearch.html', city=city, places=templ_places, types=types, bedrooms=bedrooms,
                               category=category, maxprice=maxprice, minprice=minprice, agency=agency,
                               period=period, drop=drop)


@app.route('/download')
def download():
    dbreader = mysql_reader.Dbreader()
    city = request.args.get('city')
    types = request.args.get('type')
    bedrooms = request.args.get('bedrooms')
    category = request.args.get('category')
    minprice = request.args.get('minprice')
    maxprice = request.args.get('maxprice')
    agency = request.args.get('agency')
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')               #check

    predata = dbreader.get_prefile(city, bedrooms, types, category, minprice, maxprice,
                                            agency, startdate, enddate)
    dbreader.close()

    with open(DATAPATH + '/temp.csv', "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"')
        for line in predata:
            print(line)
            writer.writerow(line)
        return send_file(os.path.join(DATAPATH, 'temp.csv'))


@app.route('/aggregation')
def aggregation():
    full_args = ['city', 'place', 'bedrooms', 'page', 'category',
                 'minprice', 'maxprice', 'period', 'doubles',
                 'maxperc', 'minperc', 'zeros']
    print(request.args)

    args = clean_args(request.args, full_args)
    dbreader = mysql_reader.Dbreader()
    city = args.get('city')
    types = 'All'
    place = args.get('place')
    bedrooms = args.get('bedrooms')
    page = args.get('page')
    category = args.get('category')
    minprice = args.get('minprice')
    maxprice = args.get('maxprice')
    agency = ''
    maxperc = args.get('maxperc')
    minperc = args.get('minperc')
    zeros = args.get('zeros')
    drop = args.get('doubles')
    period = args.get('period')
    url = request.url

    if page:
        url = '='.join(url.split('=')[:-1])+'='
        page = int(page)
    else:
        url = url + '&page='
        page = 0

    startdate, enddate = separate_date(period)
    sum_category = separate_cat(category)

    if place:
        prelists = dbreader.get_count_listings(city, place, bedrooms, types, sum_category, minprice, maxprice,
                                                agency, startdate, enddate, zeros=zeros)
        data = []

        pages = find_num_pages(page, len(prelists))
        print(pages)
        for i in range(page*BUILD_ON_PAGE, (page+1)*BUILD_ON_PAGE):
            try:
                pre = prelists[i]
            except:
                break

            newtower = []
            predata = dbreader.get_listings_aggr(city, pre[0], bedrooms, startdate, enddate, sum_category, maxprice, minprice)
            #print(predata)

            if drop == 'on':
                predata = drop_double(predata)

            for new in predata:
                newtower.append(replace_price(new))

            if len(newtower):
                print('==========', zeros)
                dictrow = sale_rent_aggregation(newtower, zeros=zeros, minperc=minperc, maxperc=maxperc)
                if dictrow:
                    data.append((dictrow, pre[0]))
            else:
                break

        dbreader.close()
        #for dt in data:
        #    print(dt)
        print(data)
        return render_template('results_aggr.html', city=city, data=data, types=types, beds=bedrooms, place=place,
                               maxprice=maxprice, minprice=minprice, category=category,
                               period=period, pages=pages, page=page, preurl=url)

    else:
        preplaces = dbreader.get_places(types, city, bedrooms, sum_category, minprice, maxprice,
                                        agency, startdate, enddate,  page)
        #for pl in preplaces:
        #    print(pl)
        prebuild = dbreader.get_buildings()
        builds = set()
        for pre in prebuild:
            builds.add(pre[0])

        places = []
        for place in preplaces:
            try:
                if place[0] not in builds and 'Tower' not in place[0]:
                    places.append(place)
            except:
                continue
        # ------ add to base images
        templ_places = []
        for pl in places:
            templ_places.append(
                        (pl[0], pl[1], '/static/images/search-area/i{}.png'.format(random.choice(range(1, 9)))))
        templ_places = group_place(templ_places)
        #for pl in templ_places:
        #        print(pl)
        # ------ add to base images

        dbreader.close()
        return render_template('presearch_aggr.html', city=city, places=templ_places, bedrooms=bedrooms,
                               category=category, minprice=minprice, maxprice=maxprice,
                               period=period, minperc=minperc, maxperc=maxperc,
                               zeros=zeros, drop=drop)


@app.route('/presearch_map')
def pre_map():
    city = request.args.get('city')
    dbreader = mysql_reader.Dbreader()
    place_list = dbreader.get_premap(city)
    center = dbreader.get_map_center(city)[0]
    dbreader.close()
    if len(place_list) > 800:
        place_list = place_list[:800]
    return render_template('presearch_map.html', place_list=place_list, center=center, city=city)


@app.route('/get_data')
def get_data():
    city = request.args.get('city')
    building = request.args.get('building')
    dbreader = mysql_reader.Dbreader()
    place_list = dbreader.get_listings_build(city, building)
    predata = drop_double(place_list)
    dictrow = sale_rent_aggregation(predata, zeros=1)
    dbreader.close()
    print(dictrow)
    return render_template('build_data.html', data=dictrow, name=building)


@app.route('/stats')
def stats():
    dbreader = mysql_reader.Dbreader()
    dbreader.close()
    return 'No data'


@app.route('/login', methods=['POST', 'GET'])
def login():
    ip = request.remote_addr
    cookie = request.cookies.get('sess')
    if cookie:
        dbreader = mysql_reader.Dbreader()
        iscook = dbreader.check_cook(cookie)[0][0]
        dbreader.close()
        if iscook:
            return redirect("/", code=302)
    if request.method == 'POST':
        user = request.values.get('user')
        pwd = request.values.get('pwd')
    else:
        user = None
        pwd = None

    print(user, pwd)
    if user and pwd:
        dbreader = mysql_reader.Dbreader()
        isuser = dbreader.check_pass(user, pwd)[0][0]
        dbreader.close()

        if isuser:
            dbwriter = mysql_writer.Dbsender()
            sess = dbwriter.write_session(user, ip)
            dbwriter.close()
            resp = make_response(redirect('/', code=302))
            resp.set_cookie('sess', sess)
            return resp
        else:
            mess = 'Bad username or password'
            return render_template('login.html', mess=mess)
    mess = ''
    return render_template('login.html', mess=mess)


@app.route('/blog')
def blog():
    pagename = 'blog'
    return render_template('blog.html', pagename=pagename)


@app.route('/blog_detail')
def blog_detail():
    pagename = 'blog_detail'
    return render_template('blog_detail.html', pagename=pagename)


@app.route('/about')
def about():
    pagename = 'about'
    return render_template('about.html', pagename=pagename)


@app.route('/faq')
def faq():
    pagename = 'faq'
    return render_template('faq.html', pagename=pagename)


@app.route('/contacts')
def contacts():
    pagename = 'contacts'
    return render_template('contacts.html', pagename=pagename)


@app.route('/properties')
def properties():
    pagename = 'properties'
    return render_template('properties.html', pagename=pagename)


@app.route('/test')
def test():
    out = ['https://drive.google.com/open?id=1KBHonuJEhSYET9NukGhPyAfgLEUuyBLc',]


    return render_template('test.html', out=out)
