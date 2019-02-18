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


def find_len_pages(soup):
    pre = soup.find('span', {'aria-label': "Summary text"}).text
    pre = pre.split('of ')[-1].split(' ')[0].replace(',', '').strip()
    return pre


def get_start_url():
    resid = ["/for-sale/apartments/uae/",
             "/for-sale/villas/uae/",
             "/for-sale/townhouses/uae/",
             "/for-sale/residential-plots/uae/",
             "/for-sale/penthouse/uae/",
             "/for-sale/hotel-apartments/uae/",
             "/for-sale/residential-building/uae/",
             "/for-sale/residential-floors/uae/",
             "/for-sale/villa-compound/uae/",
             "/to-rent/apartments/uae/",
             "/to-rent/villas/uae/",
             "/to-rent/townhouses/uae/",
             "/to-rent/hotel-apartments/uae/",
             "/to-rent/penthouse/uae/",
             "/to-rent/residential-building/uae/",
             "/to-rent/villa-compound/uae/",
             "/to-rent/residential-floors/uae/"
             ]
    commerc = ["/for-sale/offices/uae/",
               "/for-sale/commercial-plots/uae/",
               "/for-sale/warehouses/uae/",
               "/for-sale/shops/uae/",
               "/for-sale/commercial-buildings/uae/",
               "/for-sale/labour-camps/uae/",
               "/for-sale/mixed-use-land/uae/",
               "/for-sale/industrial-land/uae/",
               "/for-sale/bulk-units/uae/",
               "/for-sale/commercial-floors/uae/",
               "/for-sale/commercial-villas/uae/",
               "/for-sale/factories/uae/",
               "/for-sale/commerical-properties/uae/",
               "/for-sale/showrooms/uae/",
               "/to-rent/offices/uae/",
               "/to-rent/warehouses/uae/",
               "/to-rent/shops/uae/",
               "/to-rent/labour-camps/uae/",
               "/to-rent/commercial-villas/uae/",
               "/to-rent/showrooms/uae/",
               "/to-rent/commercial-plots/uae/",
               "/to-rent/commercial-floors/uae/",
               "/to-rent/industrial-land/uae/",
               "/to-rent/commercial-buildings/uae/",
               "/to-rent/bulk-units/uae/",
               "/to-rent/commerical-properties/uae/",
               "/to-rent/factories/uae/",
               "/ar/to-rent/apartments/uae/",
               "/to-rent/studio-apartments/uae/"
               ]
    host = 'https://www.bayut.com'
    outurls = []
    outlen = 0
    for i, preurl in enumerate((resid, commerc)):
        for url in preurl:
            print(url)
            try:
                lenpage = int(find_len_pages(get_soup(host + url)))
            except:
                continue
            print(lenpage)
            outlen += int(lenpage)
            outurls.append((host + url, i))
            for u in range(2, int((lenpage / 22)) + 1):
                outurls.append((host + url + 'page-{}/'.format(u), i))
                print(host + url + 'page-{}/'.format(u))

    print(outlen, len(outurls), 'start urls')
    # raise TypeError
    return outurls


async def asynchronous_target(urls, outdata, seturls):
    fulltime = time()
    while True:
        start = time()
        n = 0

        lenurls = len(urls)
        thr = lenurls if lenurls < MAXTHREADS * 5 else MAXTHREADS * 5

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

        if len(outdata) > MAXADS * 1:
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
            async with session.get(URL[0], timeout=60) as response:
                mess = await response.text()
                break
        except Exception as err:
            pass
            print(traceback.format_exc())
            print(pid, '-------sleep', URL)
            sleep(0.01)
    print(pid, len(outdata))
    parse_page_target(mess, outdata, addurls, seturls, URL)
    return len(mess)


def parse_page_target(mess, outdata, newurls, seturls, URL):
    divs = BS(mess, 'lxml')
    preout = []
    alist = divs.findAll('li', {'aria-label': "Listing card"})
    host = 'https://www.bayut.com'
    for div in alist:

        href = div.find('a')['href']
        newurl = host + href

        if newurl not in seturls:
            seturls.add(newurl)
            # print(href)
            outdata.append((newurl, URL[-1]))


def get_target_urls(site, seturls):
    start_urls = get_start_url()

    targ_urls = []
    # seturls = read_db_urls(site)
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
        thr = lenurls if lenurls < MAXTHREADS * 10 else MAXTHREADS * 10

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
            async with session.get(URL[0], timeout=80) as response:
                mess = await response.text()
                if 'SqlException' not in mess:
                    break
                else:
                    sleep(0.05)
        except Exception as err:
            # pass
            # print(traceback.format_exc())
            print(pid, '-------sleep', URL)
            sleep(0.1)
    try:
        parse_page(mess, URL, outdata, buildings)
    except:
        print('=-=-=-= error parser')
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
    date = datetime.datetime.today()
    soup = BS(html, 'lxml')

    out_dict = {}

    named = soup.find('h1').text
    out_dict['Named'] = named

    headers = {'x-requested-with': 'XMLHttpRequest'}
    property_id = url[0].split('-')[-1].split('.')[0]
    data = {'property_id': property_id}

    try:
        ans = requests.post('https://www.bayut.com/show-numbers/', data=data, headers=headers, timeout=30).json()
    except:
        ans = requests.post('https://www.bayut.com/show-numbers/', data=data, headers=headers, timeout=30).json()

    out_dict['Date'] = date.strftime('%d.%m.%Y')
    try:
        phone = ans['result']['number']['mobile'].replace('-', '').replace('+', '').split(',')[0]
    except:
        phone = None
    out_dict['Phone'] = phone
    try:
        company = ans['result']['number']['agent_name']
    except:
        company = None
    out_dict['Company Name'] = company
    try:
        agent = ans['result']['number']['contact_person']
    except:
        agent = None
    out_dict['Agent Name'] = agent

    try:
        latitude = re.findall('"latitude":\d+\.\d+', html)[0].split(':')[-1]
        longitude = re.findall('"longitude":\d+\.\d+', html)[0].split(':')[-1]
        out_dict['Latitude'] = latitude
        out_dict['Longitude'] = longitude
    except:
        out_dict['Latitude'] = None
        out_dict['Longitude'] = None

    typedata = soup.find('li', {'aria-label': "Property detail type"}).findAll('span')[-1].text
    out_dict['Type'] = typedata

    price = soup.find('li', {'aria-label': "Property detail price"}).findAll('span')[-1].text
    cur, pr = price.split(' \xa0 ')
    out_dict['Price'] = ' '.join((pr, cur))

    area = soup.find('li', {'aria-label': "Property detail area"}).findAll('span')[-1].text
    out_dict['Area ft'] = area

    beds = soup.find('li', {'aria-label': "Property detail beds"}).findAll('span')[-1].text
    out_dict['Bedrooms'] = beds.replace('-', '')

    baths = soup.find('li', {'aria-label': "Property detail baths"}).findAll('span')[-1].text
    out_dict['Bathrooms'] = baths.replace('-', '')

    listing_type = soup.find('li', {'aria-label': "Property detail purpose"}).findAll('span')[-1].text.replace(
        'For ', '')
    out_dict['Listing type'] = listing_type

    location = soup.find('li', {'aria-label': "Property detail location"}).findAll('span')[-1].text
    out_dict['Community'] = location

    reference = soup.find('li', {'aria-label': "Property detail reference"}).findAll('span')[-1].text
    out_dict['Reference'] = reference

    if url[-1]:
        out_dict['Listing category'] = 'Residental'
    else:
        out_dict['Listing category'] = 'Commercial'

    predescript = str(soup.find('div', {'aria-label': "Property description"})).replace('<br/>', '\n')
    descript = BS(predescript, 'lxml').text
    out_dict['Descript'] = descript.strip()
    out_dict['URL'] = url[0]

    preplace = soup.findAll('span', {'itemprop': "name"})
    place = []
    for poz, a in enumerate(preplace):
        if len(a.text):
            if poz == 1:
                out_dict['City'] = a.text.replace('Abudhabi', 'Abu Dhabi')
            else:
                if poz == 0:
                    out_dict['Place_{}'.format(poz + 1)] = a.text
                else:
                    out_dict['Place_{}'.format(poz)] = a.text

    out_dict['Building'] = get_building(out_dict, buildings)

    try:
        preimg = soup.findAll('meta', {'property': 'og:image'})
        out_dict['Img'] = preimg[0]['content']
    except:
        out_dict['Img'] = None

    outdata.append(out_dict)


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
           'Bedrooms', 'Reference',
           'Company Name', 'Agent Name', 'Phone', 'Descript', 'URL']
    midrow = ['Date', 'Area ft', 'Community', 'Named']

    if short:
        firstrow = midrow + ['Place_1', 'City', 'Place_2', 'Place_3', 'Place_4', 'Place_5'] + poz[1:]
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
    sitename = 'bayut_com'
    site = 'bayut.com'
    log = start_log(sitename)
    log('Start parsing')

    old_urls = read_db_urls(site)

    log('Get {} old urls'.format(len(old_urls)))

    targ_urls = get_target_urls(site, old_urls)

    buildings = get_buildings()
    log('Get {} buildings'.format(len(buildings)))
    log('Get {} new urls'.format(len(targ_urls)))
    outdata = get_outdata(targ_urls, buildings)

    df = dict2df(outdata)

    # writexlsx(pre_pd, sitename, get_templ())

    write2db(df, sitename.replace('_', '.'))

    # write_new_urls(out_url_list, sitename)

    log('Done !\n')


if __name__ == '__main__':
    run()
