#!/usr/bin/python
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
import random
import logging


def find_len_pages(soup):
    # print('soup ====', soup)
    try:
        pre = soup.find('div', {'data-qs': "search-results-count"}).text
    except:
        print('BAD LENGHT')
    return pre.replace('results', '').strip()


def get_start_url(proxylist):
    urls = ['https://www.propertyfinder.ae/en/buy/properties-for-sale.html',
            'https://www.propertyfinder.ae/en/rent/properties-for-rent.html',
            'https://www.propertyfinder.ae/en/commercial-rent/properties-for-rent.html']
    outurls = []
    # print(proxylist)
    for url in urls:
        print(url)
        soup = get_soup(url, proxylist)
        while len(str(soup)) < 100000 or 'ERR_ACCESS_DENIED' in str(soup):
            soup = get_soup(url, proxylist)
            print('lensoup is small - ', len(str(soup)) if len(str(soup)) > 10 else soup)
        lenpage = int(find_len_pages(soup))
        print(lenpage)
        for i in range(1, int((lenpage / 25)) + 1):
            outurls.append(url + '?page=' + str(i))
    preset = set(outurls)
    outurls = list(preset)
    print(len(outurls), 'start urls')
    # for out in outurls:
    #    print(out)
    return outurls


async def asynchronous_target(urls, outdata, seturls, proxylist):
    print('proxy3  - ', len(proxylist))
    fulltime = time()
    while True:

        if not proxylist:
            print('NO PROXY')
            break

        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 10 else MAXTHREADS * 10

        newurls = 0

        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.ensure_future(fetch_async_targ(session, pid, urls.pop(), n, outdata, urls, seturls, proxylist))
                for pid in
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


async def fetch_async_targ(session, pid, URL, n, outdata, addurls, seturls, proxylist):
    mess = ''
    runs = 20
    errlist = ["'Bad Request'", "'Forbidden'", "'Internal Server Error'", "'Service Unavailable'",
               'CERTIFICATE_VERIFY_FAILED', 'WRONG_VERSION_NUMBER', '()', '(None,)']
    print(URL)
    for _ in range(runs):
        proxy = 'http://' + random.choice(proxylist)
        print(proxy)
        try:
            async with session.get(URL, timeout=60, proxy=proxy) as response:
                mess = await response.text()
                break
        except Exception as err:
            # print('DELETE ', proxy.replace('http://', ''))
            # proxylist.remove(proxy.replace('http://', ''))

            arg = str(err.args)
            for tmperr in errlist:
                if tmperr in arg:
                    print('DELETE ', proxy.replace('http://', ''))
                    try:
                        proxylist.remove(proxy.replace('http://', ''))
                    except:
                        pass
                    break

            # print(err.args, URL)
            # print(traceback.format_exc())
            print(pid, '-------sleep', 0, str(err.args), 0, URL)
            sleep(0.01)
    print(pid, len(proxylist), len(outdata), len(mess))
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


def get_target_urls(site, seturls, proxylist):
    start_urls = get_start_url(proxylist)
    print('start_urls ', len(start_urls))
    targ_urls = []
    start = time()
    print('proxy2  - ', len(proxylist))
    ioloop_targ = asyncio.get_event_loop()
    ioloop_targ.run_until_complete(asynchronous_target(start_urls, targ_urls, seturls, proxylist))
    # ioloop_targ.close()
    print(time() - start)
    return targ_urls


def get_outdata(targ_urls, buildings, proxylist):
    outdata = []
    start = time()

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asynchronous(targ_urls, outdata, buildings, proxylist))
    # ioloop.close()
    print(time() - start)
    return outdata


async def asynchronous(urls, outdata, buildings, proxylist):
    fulltime = time()
    while True:
        if not len(proxylist):
            break
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 10 else MAXTHREADS * 10

        newurls = 0
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(fetch_async(session, pid, urls.pop(), n, outdata, buildings, proxylist)) for
                     pid in
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


async def fetch_async(session, pid, URL, n, outdata, buildings, proxylist):
    mess = ''
    runs = 10
    errlist = ["'Bad Request'", "'Forbidden'", "'Internal Server Error'", "'Service Unavailable'",
               'CERTIFICATE_VERIFY_FAILED', 'WRONG_VERSION_NUMBER', '()', '(None,)']

    for _ in range(runs):
        proxy = 'http://' + random.choice(proxylist)
        try:

            async with session.get(URL, timeout=80, proxy=proxy) as response:
                mess = await response.text()
                if 'SqlException' not in mess:
                    break
                else:
                    sleep(0.1)
        except Exception as err:
            # print('DELETE ', proxy.replace('http://', ''))
            # proxylist.remove(proxy.replace('http://', ''))

            # pass
            arg = str(err.args)
            for tmperr in errlist:
                if tmperr in arg:
                    print('DELETE ', proxy.replace('http://', ''))
                    try:
                        proxylist.remove(proxy.replace('http://', ''))
                    except:
                        pass
                    break

            # print(err.args, URL)
            # print(traceback.format_exc())
            print(pid, '-------sleep', 0, str(err.args), 0, URL)
            sleep(0.01)
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
    soup = BS(html, 'lxml')
    out_dict = {}
    out_dict['URL'] = url
    out_dict['Date'] = datetime.datetime.today().strftime('%d.%m.%Y')
    try:
        latitude = re.findall('"latitude":\d+\.\d+', html)[0].split(':')[-1]
        longitude = re.findall('"longitude":\d+\.\d+', html)[0].split(':')[-1]
        out_dict['Latitude'] = latitude
        out_dict['Longitude'] = longitude
    except:
        out_dict['Latitude'] = None
        out_dict['Longitude'] = None
    out_dict['Listing type'] = 'Sale' if '/buy/' in url else 'Rent'
    out_dict['Listing category'] = 'Commercial' if 'commercial' in url else 'Residental'

    divlist = soup.find('div', {'class': 'facts__list'}).findAll('div', {'class': 'facts__list-item'})
    for div in divlist:
        pre = div.findAll('div')
        out_dict[pre[0].text] = clean(pre[1].text)

    named = soup.find('h1').text.strip()
    out_dict['Named'] = named

    alist = soup.find('div', {'class': "propertypage_breadcrumbarea"}).findAll('a')
    for poz, a in enumerate(alist):
        if len(a.text):
            if poz == 1:
                out_dict['City'] = a.text
            else:
                out_dict['Place_{}'.format(poz - 1)] = a.text

    predescript = str(soup.find('div', {'class': 'propertydescription_texttrim'})).replace('<br>', ' ').replace('<br/>',
                                                                                                                ' ')
    descript = BS(predescript, 'lxml').text
    out_dict['Descript'] = descript

    try:
        out_dict['Company Name'] = None
        pre = soup.findAll('div', {'class': "agent-info__detail-item"})
        for comp in pre:
            if 'Company:' in comp.text:
                out_dict['Company Name'] = comp.text.split('Company:')[-1].strip()
    except:
        out_dict['Company Name'] = None

    try:
        phone = re.findall('"type":"phone","value":"\+\d+', html)[0].split('+')[-1]
    except:
        phone = None
    out_dict['Phone'] = phone
    try:
        email = re.findall('mailto:\w+@\w+\.\w+', html)[0].split(':')[-1]
    except:
        email = None

    out_dict['Email'] = email
    out_dict['Area ft'], out_dict['Area m'] = out_dict['Area'].split(' / ')
    out_dict['City'] = out_dict['City'].replace('Abudhabi', 'Abu Dhabi')

    out_dict['Building'] = get_building(out_dict, buildings)

    try:
        preimg = soup.findAll('meta', {'property': 'og:image'})
        out_dict['Img'] = preimg[0]['content']
    except:
        out_dict['Img'] = None

    outdata.append(out_dict)
    print(out_dict.get('Named'))


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
            print('----errror')
            pass
    dbsender.update_buildings()
    dbsender.close()


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
    proxylist = load_proxy()
    sitename = 'propertyfinder_ae'
    site = 'propertyfinder.ae'
    log = start_log(sitename)
    log('Start parsing')

    old_urls = read_db_urls(site)
    log('Get {} old urls'.format(len(old_urls)))
    targ_urls = get_target_urls(site, old_urls, proxylist)

    buildings = get_buildings()
    log('Get {} buildings'.format(len(buildings)))
    log('Get {} new urls'.format(len(targ_urls)))
    proxylist = load_proxy()
    outdata = get_outdata(targ_urls, buildings, proxylist)

    df = dict2df(outdata)

    # writexlsx(pre_pd, sitename, get_templ())

    write2db(df, site)

    # write_new_urls(out_url_list, sitename)

    log('Done !\n')


if __name__ == '__main__':
    run()
