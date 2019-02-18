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


def find_len_pages(soup):
    try:
        pre = soup.find('div', {'class': "count"}).text.replace(',', '').split(' ')[0]
    except:
        pre = 0
    return int(pre)


def get_deep_1(url, outurls):
    adder = '&filters=(size>%3D{} AND size<%3D{})'
    adder2 = ' AND (price>%3D{} AND price<%3D{})'
    adder3 = ' AND (price>%3D{})'
    summary = 0
    for i in range(100):

        new_url = url + adder.format(i * 100, (i + 1) * 100)
        print(new_url)
        soup = get_soup(new_url)
        lenpage = find_len_pages(soup)
        summary += lenpage
        print(lenpage)
        if lenpage < 1000 and lenpage != 0:
            for i in range(0, int(lenpage / 25) + 1):
                outurls.append(new_url + '&page={}'.format(i))
                # print(new_url + '&page={}'.format(i))
            print(summary, len(outurls), i, 'start urls')

        else:
            if lenpage:
                BR = 10
                fourl = new_url + adder3.format(BR * 1000)
                for br in range(BR):
                    thrdurl = new_url + adder2.format(br * 10000, (br + 1) * 10000)

                    print(thrdurl)
                    soup = get_soup(thrdurl)
                    lenpage2 = find_len_pages(soup)
                    print(lenpage2)
                    if lenpage2 < 1000 and lenpage2 != 0:
                        for i in range(0, int(lenpage2 / 25) + 1):
                            outurls.append(thrdurl + '&page={}'.format(i))
                            print(thrdurl + '&page={}'.format(i))
                        print('second', summary, len(outurls), i, 'start urls')
                    else:
                        if lenpage2:
                            for i in range(0, int(lenpage2 / 25) + 1):
                                if i < 40:
                                    outurls.append(thrdurl + '&page={}'.format(i))
                            print('max ', summary, len(outurls), i, 'start urls')

                soup = get_soup(fourl)
                lenpage2 = find_len_pages(soup)
                print(lenpage2)
                if lenpage2 != 0:
                    for i in range(0, int(lenpage2 / 25) + 1):
                        outurls.append(fourl + '&page={}'.format(i))
                        print(fourl + '&page={}'.format(i))
                    print('third', summary, len(outurls), i, 'start urls')
                if 'rooms-for-rent' in url:
                    break


def get_start_url():
    urls = open(DATAPATH + '/dubizz.urls').read().split('\n')[:]

    outurls = []
    for url in urls:
        print(url)
        soup = get_soup(url)
        lenpage = find_len_pages(soup)
        # print(lenpage)
        if lenpage < 1000:
            for i in range(0, int(lenpage / 25) + 1):
                outurls.append(url + 'page={}'.format(i))
                # print(len(outurls), 'start urls')
        else:
            get_deep_1(url, outurls)
    for url in outurls:
        print(url)
    with open('endurls.urls', 'w') as f:
        f.write('\n'.join(outurls))

    # raise TypeError
    preset = set(outurls)
    outurls = list(preset)
    print(len(outurls), 'start urls')
    return outurls


def load_start_urls():
    preurls = open(DATAPATH + '/endurls.urls').read().split('\n')
    return preurls


async def asynchronous_target(urls, outdata, seturls):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 20 else MAXTHREADS * 20

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
        # if len(outdata) > 100:#MAXADS * 3:
        #    break

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
        # print(_, '------------', URL)
        try:
            async with session.get(URL, timeout=120) as response:
                mess = await response.text()
                if len(mess) > 100000:
                    break

        except Exception as err:
            # print(traceback.format_exc())
            print(pid, '-------sleep', URL)
            sleep(0.1)
            # if URL == 'https://sharjah.dubizzle.com/property-for-rent/residential/apartmentflat/?&filters%3D(size>%3D3000 AND size<%3D3100)&page%3D3':
            # print(mess)
            # raise TypeError
    print(pid, len(outdata), len(mess))
    try:
        parse_page_target(mess, outdata, addurls, seturls)
    except:
        pass
    return len(mess)


def parse_page_target(mess, outdata, newurls, seturls):
    divs = BS(mess, 'lxml')
    alist = divs.findAll('a', {'class': "listing-link"})

    for a in alist:
        outurl = a['href'].split('?')[0]
        if outurl not in seturls:
            seturls.add(outurl)
            outdata.append(outurl)


def get_target_urls(site):
    # start_urls = get_start_url()

    start_urls = load_start_urls()
    targ_urls = []
    seturls = set()  # read_db_urls(site)
    start = time()

    ioloop_targ = asyncio.get_event_loop()
    ioloop_targ.run_until_complete(asynchronous_target(start_urls, targ_urls, seturls))
    # ioloop_targ.close()
    print(time() - start)
    return targ_urls


def get_outdata(targ_urls):
    outdata = []
    start = time()
    builds = set()
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asynchronous(targ_urls, outdata, builds))
    # ioloop.close()
    print(time() - start)
    return outdata


async def asynchronous(urls, outdata, builds):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 30 else MAXTHREADS * 30

        newurls = 0
        async with aiohttp.ClientSession() as session:

            tasks = [asyncio.ensure_future(fetch_async(session, pid, urls.pop(), n, outdata, builds)) for pid in
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


async def fetch_async(session, pid, URL, n, outdata, builds):
    mess = ''
    runs = 10
    for _ in range(runs):
        try:
            async with session.get(URL, timeout=80) as response:
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
    try:
        if 'Building' in mess:
            parse_page(mess, URL, outdata, builds)
    except:
        pass


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


def parse_page(html, url, outdata, builds):
    soup = BS(html, 'lxml')
    out_dict = {}

    out_dict['Building'] = ''

    lilist = soup.find('ul', {'class': "normal-fields"}).findAll('li')
    for li in lilist:
        if 'Building' in li.text:
            out_dict['Building'] = li.find('strong').text.strip()

    alist = soup.find('div', {'class': "location-areas"}).findAll('a')
    out_dict['City'] = alist[1].text
    if out_dict['Building']:
        if out_dict['City'] + out_dict['Building'] not in builds:
            small = out_dict['Building'].lower().replace(' ', '')
            print(out_dict['City'] + ' | ' + out_dict['Building'])
            builds.add(out_dict['City'] + out_dict['Building'])
            outdata.append(out_dict['City'] + ' | ' + out_dict['Building'] + ' | ' + small)


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
    dbsender.close()


def read_db_urls(site):
    dbreader = Dbreader()
    unic_urls = dbreader.get_unic_urls(site)
    dbreader.close()
    urls = set()
    for pre in unic_urls:
        urls.add(pre[0])
    log = start_log(site.replace('.', '_'))
    log('Get {} old urls'.format(len(urls)))
    return urls


def run():
    sitename = 'dubizzle_com'

    targ_urls = read_db_urls(sitename.replace('_', '.'))
    print(len(targ_urls))
    outdata = get_outdata(targ_urls)

    with open('buildings.txt', 'w') as f:
        f.write('\n'.join(outdata).replace('Abudhabi', 'Abu Dhabi'))


if __name__ == '__main__':
    run()
