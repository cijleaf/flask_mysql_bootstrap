#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
from time import time
from common_func import *
import xlsxwriter
import aiohttp
import asyncio
import traceback
from mysql_writer import *
from mysql_reader import *
import logging
import pandas as pd
import json


def get_len_pages(html):
    JSON = json.loads(html.decode())
    return int(JSON.get('total_pages'))


def generate_urls(url, pages):
    out = []
    for page in range(1, pages + 1):
        out.append(url.format(page))
    return out


def get_start_url():
    outurls = []
    page = '?per_page=50&ajax=true&page={}'
    genurls = ['https://www.justproperty.com/en/rent/uae/commercial-for-rent/',
               'https://www.justproperty.com/en/buy/uae/commercial-for-sale/',
               'https://www.justproperty.com/en/buy/uae/properties-for-sale/',
               'https://www.justproperty.com/en/rent/uae/properties-for-rent/'
               ]

    for url in genurls:
        print(url)
        html = get_html(url + page.format(1))
        lenpage = get_len_pages(html)
        outurls += generate_urls(url + page, lenpage)
    return outurls


def get_target_urls(site):
    start_urls = get_start_url()

    return start_urls


def get_outdata(targ_urls, old_urls):
    outdata = []
    start = time()

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asynchronous(targ_urls, outdata, old_urls))
    # ioloop.close()
    print(time() - start)
    return outdata


async def asynchronous(urls, outdata, old_urls):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS else MAXTHREADS

        newurls = 0
        async with aiohttp.ClientSession() as session:

            tasks = [asyncio.ensure_future(fetch_async(session, pid, urls.pop(), n, outdata, old_urls)) for pid in
                     range(1, thr + 1)]

            row = await asyncio.gather(*tasks)

        worktime = round(time() - start, )

        if not worktime:
            break

        bigtime = round(time() - fulltime)
        fullspeed = round(len(outdata) / bigtime, 1)
        persec = round(thr / worktime, 1)
        print('------------------------------------------')
        print('Summary   ', len(outdata), bigtime, fullspeed)
        print('New       ', newurls, len(urls))
        print('One       ', thr, worktime, persec, '\n')
        print('OUT       ', len(outdata), '\n')


async def fetch_async(session, pid, URL, n, outdata, old_urls):
    mess = ''
    runs = 10
    print(URL)
    for _ in range(runs):
        try:
            async with session.get(URL, timeout=60) as response:
                mess = await response.text()
                if 'SqlException' not in mess:
                    break
                else:
                    sleep(0.1)
        except Exception as err:
            # pass
            # print(traceback.format_exc())
            # print(pid, '-------sleep', URL)
            sleep(0.1)

    parse_page(mess, URL, outdata, old_urls)

    return len(mess)


def parse_page(html, url, outdata, old_urls):
    host = 'https://www.justproperty.com'
    JSON = json.loads(html)
    rows = JSON.get('objects')

    if len(rows):
        for row in rows:
            URL = host + row.get('url')
            if URL not in old_urls:
                out_dict = {}
                out_dict['URL'] = URL
                out_dict['Date'] = datetime.datetime.today().strftime('%d.%m.%Y')
                out_dict['Latitude'] = row['coordinates'].get('lat')
                out_dict['Longitude'] = row['coordinates'].get('lon')

                out_dict['Listing type'] = 'Sale' if row.get('site_type') == 'sale' else 'Rent'
                out_dict['Listing category'] = 'Commercial' if row.get(
                    'category_label') == 'Commercial' else 'Residental'

                out_dict['Named'] = row.get('original_title')
                out_dict['Price'] = row.get('price')

                out_dict['Area ft'] = row.get('built_up_area')
                out_dict['Bathrooms'] = row.get('bathrooms')
                out_dict['Bedrooms'] = row.get('bedrooms_formatted')

                out_dict['Descript'] = row.get('description')

                out_dict['Building'] = ''

                out_dict['Reference'] = row.get('reference_number')
                out_dict['Company Name'] = row.get('company_profile_name')
                out_dict['Phone'] = row.get('phone_number')

                out_dict['Type'] = row.get('subcategory_label') if row.get('subcategory_label') else row.get(
                    'category_label')

                out_dict['City'] = row.get('city_name')
                out_dict['Place_1'] = row.get('location_name')
                out_dict['Place_2'] = row.get('sublocation_name')

                preimg = row.get('thumbnail_url')
                if preimg:
                    out_dict['Img'] = preimg.split('-')[0] + '-original.jpeg'
                # print(out_dict)
                outdata.append(out_dict)


def separate_place(inlist):
    outlist = []
    for s in inlist:
        outlist.append(s)
    while len(outlist) < 4:
        outlist.append('')
    return outlist


def format_data(dct):
    templ = get_templ(short=False)
    out = []
    for key in templ:
        if key != 'Place':
            out.append(dct.get(key))
        else:
            out = out + separate_place(dct[key])
    return out


def get_templ(short=True):
    poz = ['Place', 'Latitude', 'Longitude', 'Type', 'Listing type', 'Listing category', 'Price', 'Bathrooms',
           'Bedrooms', 'Reference', 'Company Name',
           'Descript', 'URL']
    midrow = ['Date', 'Area ft', 'Named']

    if short:
        firstrow = midrow + ['City', 'Place_1', 'Place_2', 'Place_3'] + poz[1:]
    else:
        firstrow = midrow + poz[:]
    return firstrow


def load_xlsx():
    urls = []
    for i in range(1, 4):
        df = pd.read_excel(DATAPATH + '/dubizzle_com_12_2018_{}.xlsx'.format(i))
        urls += df['URL'].tolist()
    return urls


def write2db(data, sitename):
    dbsender = Dbsender()
    for iter, row in data.iterrows():
        try:
            print('-- ', row.get('Named'))
            dbsender.write_row(row, sitename)
        except:
            pass
    dbsender.update_buildings()
    dbsender.update_fast_tabs()
    dbsender.close()


def read_db_urls(site):
    dbreader = Dbreader()
    unic_urls = dbreader.get_unic_urls(site)

    urls = set()
    for pre in unic_urls:
        urls.add(pre[0])
    dbreader.close()
    return urls


def run():
    sitename = 'justproperty_com'
    site = 'justproperty.com'
    log = start_log(sitename)
    log('Start parsing')

    old_urls = read_db_urls(site)
    log('Get {} old urls'.format(len(old_urls)))

    targ_urls = get_target_urls(site)[:]

    outdata = get_outdata(targ_urls, old_urls)
    # for out in outdata:
    #    print(out)
    log('Get {} new urls'.format(len(outdata)))

    df = dict2df(outdata)

    # writexlsx(pre_pd, sitename, get_templ())

    write2db(df, site)

    # write_new_urls(out_url_list, sitename)

    log('Done !\n')


if __name__ == '__main__':
    run()
