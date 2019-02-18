#!/usr/bin/python
# -*- coding: utf-8 -*-

# !/usr/bin/python
# -*- coding: utf-8 -*-

import re
from time import time
from common_func import *
# import xlsxwriter
import aiohttp
import asyncio
import traceback
from mysql_writer import *
from mysql_reader import *
import logging


def find_len_pages(soup):
    pre = soup.find('div', {'data-qs': "search-results-count"}).text
    return pre.replace('results', '').strip()


def get_start_url():
    urls = ['https://www.propertyfinder.ae/en/buy/properties-for-sale.html',
            'https://www.propertyfinder.ae/en/rent/properties-for-rent.html',
            'https://www.propertyfinder.ae/en/commercial-rent/properties-for-rent.html']
    outurls = []
    for url in urls:
        print(url)
        soup = get_soup(url)
        lenpage = int(find_len_pages(soup))
        print(lenpage)
        for i in range(1, int((lenpage / 25)) + 1):
            outurls.append(url + '?page=' + str(i))
    preset = set(outurls)
    outurls = list(preset)
    print(len(outurls), 'start urls')
    return outurls


async def asynchronous_target(urls, outdata, seturls):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 10 else MAXTHREADS * 10

        newurls = 0

        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(fetch_async_targ(session, pid, urls.pop(), n, outdata, urls, seturls)) for
                     pid in
                     range(1, thr + 1)]

            row = await asyncio.gather(*tasks)

        lenset = len(seturls) - len(outdata)

        worktime = time() - start

        if not len(urls):
            break
        if len(outdata) > MAXADS:
            break

        bigtime = time() - fulltime
        fullspeed = round(lenset / bigtime, 1)
        persec = round(thr / worktime, 1)
        print('------------------------------------------')
        print('Summary   ', lenset, round(bigtime), fullspeed)
        print('New       ', newurls, len(urls))
        print('One       ', thr, round(worktime), persec, '\n')
        print('OUT       ', len(outdata), '\n')


async def fetch_async_targ(session, pid, URL, n, outdata, addurls, seturls):
    mess = ''
    runs = 20
    print(URL)
    for _ in range(runs):

        try:
            async with session.get(URL, timeout=60) as response:
                mess = await response.text()
                break
        except Exception as err:
            pass
            # print(traceback.format_exc())
            print(pid, '-------sleep', URL)
            sleep(0.01)
    print(pid, len(outdata), len(mess))
    try:
        parse_page_target(mess, outdata, addurls, seturls)
    except:
        pass
    return len(mess)


def parse_page_target(mess, outdata, newurls, seturls):
    divs = BS(mess, 'lxml')
    alist = divs.findAll('div', {'class': "cardlist_item"})
    host = 'https://www.propertyfinder.ae'
    for div in alist:
        url = host + div.a['href']
        if url not in seturls:
            seturls.add(url)
            outdata.append(host + div.a['href'])


def get_target_urls(site, seturls):
    start_urls = get_start_url()

    targ_urls = []
    start = time()

    ioloop_targ = asyncio.get_event_loop()
    ioloop_targ.run_until_complete(asynchronous_target(start_urls, targ_urls, seturls))
    # ioloop_targ.close()
    print(time() - start)
    return targ_urls


def get_outdata(targ_urls, buildings):
    outdata = []
    start = time()

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asynchronous(targ_urls, outdata, buildings))
    # ioloop.close()
    print(time() - start)
    return outdata


async def asynchronous(urls, outdata, buildings):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < 50 else 50

        newurls = 0
        async with aiohttp.ClientSession() as session:

            tasks = [asyncio.ensure_future(fetch_async(session, pid, urls.pop(), n, outdata, buildings)) for pid in
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


async def fetch_async(session, pid, URL, n, outdata, buildings):
    mess = ''
    runs = 10
    for _ in range(runs):
        try:
            async with session.get(URL, timeout=50) as response:
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
    # print(mess)
    try:
        parse_page(mess, URL, outdata, buildings)
    except:
        pass
    return len(mess)


def separate_tmp(bb, table):
    out = ''
    try:
        trlist = table.findAll('tr')
        for tr in trlist:
            if bb in tr.th.text:
                out = tr.td.text.strip()
    except:
        pass
    return out


def clean(txt):
    while txt.count('  '):
        txt = txt.replace('  ', ' ')
    txt = txt.replace('\n', '').strip()
    return txt


def separate_place(inlist):
    outlist = []
    for s in inlist:
        outlist.append(s)
    while len(outlist) < 4:
        outlist.append('')
    return outlist


def get_building(pre_dict, buildings):
    preparse = ''
    out = 'NULL'
    for k, v in pre_dict.items():
        if 'Place' in k or 'City' in k:
            preparse += v

    for build in buildings:
        if build.replace(' ', '').lower() in preparse:
            out = build
            break
    return out


def parse_page(html, url, outdata, buildings):
    # print(html)
    soup = BS(html, 'lxml')
    out_dict = {}

    out_dict['Company Name'] = None
    pre = soup.findAll('div', {'class': "agent-info__detail-item"})

    for comp in pre:
        if 'Company:' in comp.text:
            out_dict['Company Name'] = comp.text.split('Company:')[-1].strip()
    if out_dict['Company Name']:
        outdata.append((url, out_dict['Company Name']))
        # print(out_dict['Company Name'])


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
           'Bedrooms', 'Reference', 'RERA Permit No.', 'Company Name', 'Phone',
           'Email', 'Descript', 'URL']
    midrow = ['Date', 'Area ft', 'Area m', 'Named']

    if short:
        firstrow = midrow + ['City', 'Place_1', 'Place_2', 'Place_3'] + poz[1:]
    else:
        firstrow = midrow + poz[:]
    return firstrow


def write2db(data, sitename):
    dbsender = Dbsender()
    for iter, row in data.iterrows():
        try:
            print('-- ', row.get('Named'))
            dbsender.write_row(row, sitename)
        except:
            pass
    dbsender.close()


def update_db(data):
    dbsender = Dbsender()
    for url, value in data:
        try:
            dbsender.update_field(url, value)
            print('-- ', value)
        except:
            print('------------someerrror')
    dbsender.close()
    print('DB update')


def read_db_urls(site):
    dbreader = Dbreader()
    unic_urls = dbreader.get_unic_urls(site)

    urls = set()
    for pre in unic_urls:
        urls.add(pre[0])
    dbreader.close()
    return urls


def get_buildings():
    dbreader = Dbreader()
    pre = dbreader.get_buildings()
    buildings = set()
    for name in pre:
        buildings.add(name[0])
    dbreader.close()
    return buildings


def run():
    sitename = 'propertyfinder_ae'
    site = 'propertyfinder.ae'
    log = start_log(sitename)
    log('Start parsing')

    old_urls = read_db_urls(site)
    print(len(old_urls))
    log('Get {} old urls'.format(len(old_urls)))
    # targ_urls = get_target_urls(site, old_urls)
    targ_urls = list(old_urls)

    buildings = get_buildings()
    log('Get {} buildings'.format(len(buildings)))
    log('Get {} new urls'.format(len(targ_urls)))

    for i in range(int(len(targ_urls) / 100)):
        pretarg = targ_urls[i * 100: (i + 1) * 100]
        outdata = get_outdata(pretarg, buildings)
        print(len(outdata))
        update_db(outdata)

    # df = dict2df(outdata)

    # writexlsx(pre_pd, sitename, get_templ())

    # write2db(df, sitename.replace('_', '.'))



    # write_new_urls(out_url_list, sitename)

    log('Done !\n')


if __name__ == '__main__':
    run()
