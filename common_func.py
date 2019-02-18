#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from settings import *
from time import sleep
import logging
import xlsxwriter
import datetime
import math
import traceback
import random
import re
import json


def get_html(url, proxy=None, timeout=30, **kwargs):
    if not proxy:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
    else:
        url = url.replace('https', 'http')
        proxy = {'http': 'http://' + proxy,
                 'https': proxy}
        resp = requests.get(url, headers=HEADERS, timeout=timeout * 2, proxies=proxy)
    return resp.content


def get_soup(url, proxylist=None):
    resp = ''
    for _ in range(MAX_RETRIES):
        try:
            if proxylist:
                proxy = random.choice(proxylist)
                resp = get_html(url, proxy)
            else:
                resp = get_html(url)
            break
        except Exception:
            print(traceback.format_exc())
            print('BAD RQUEST')
            sleep(SLEEP)
    return BS(resp, 'lxml')


def get_only_soup(html):
    return BS(html, 'lxml')


def start_log(sitename):
    logger = logging.getLogger(sitename)
    logger.setLevel(logging.INFO)
    flog = logging.FileHandler(LOGPATH + "/{}.log".format(sitename))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    return logger.info


def write_new_urls(urllist, name):
    with open(DATAPATH + '/{}.urls'.format(name), 'a') as f:
        f.write('\n'.join(urllist))


def get_old_urls(name):
    old_urls = open(DATAPATH + '/{}.urls'.format(name), 'r').read()
    return old_urls


def writexlsx(indata, sitename, templ):
    date = datetime.datetime.today()

    numfiles = int(math.ceil(len(indata) / LENXLSX))

    for part in range(1, numfiles + 1):

        fname = '{}_{}_{}_{}.xlsx'.format(sitename, date.month, date.year, part)
        workbook = xlsxwriter.Workbook(DATAPATH + '/' + fname)
        worksheet = workbook.add_worksheet()
        predata = indata[(part - 1) * LENXLSX:part * LENXLSX]
        predata.insert(0, templ)
        for r, row in enumerate(predata):
            for c, coll in enumerate(row):
                worksheet.write(r, c, coll)
        workbook.close()


def dict2df(data):
    return pd.DataFrame(data)


def get_free_proxy():  # use two sites
    print('GET FREE PROXY')
    outdata = []
    preprox = []
    soup = get_soup(PROXYURLS[0])
    pretd = soup.findAll('tr')
    for td in pretd[1:]:
        td = td.findAll('td')
        try:
            preprox.append(td[0].text + ':' + td[1].text)
        except:
            continue
    for _ in range(20):
        try:
            firstprox = random.choice(preprox)

            html = get_html(PROXYURLS[1], proxy=firstprox).decode()
            templ = re.findall('gp.insertPrx\({.+}\)', html)
            for prejson in templ:
                prejson = prejson.replace('gp.insertPrx(', '').replace('})', '}')
                JSON = json.loads(prejson)
                outdata.append(JSON.get('PROXY_IP') + ':' + str(int(JSON.get('PROXY_PORT'), 16)))
            outdata.append(firstprox)
            break
        except:
            continue
    print(outdata)


def get_hideme_proxy():
    url = HIDEME_PROXY_URL
    pre = get_soup(url).text.split('\r\n')
    print(pre)
    return pre


def load_proxy():
    # outdata = get_free_proxy()
    outdata = get_hideme_proxy()
    return outdata


def find_num_pages(page, lendata):
    if lendata < BUILD_ON_PAGE:
        return [0, ]
    if lendata < BUILD_ON_PAGE * PAGES:
        return list(range(int(lendata / BUILD_ON_PAGE) + 0 if lendata % BUILD_ON_PAGE == 0 else 1))
    total = int(lendata / BUILD_ON_PAGE)
    print(total)
    page = int(page)
    out = []
    if page < PAGES / 2:
        out += list(range(PAGES))
    elif page > total - PAGES / 2:
        out += list(range(page - 2, total + 1))
    else:
        out += list(range(page - 2, page + 3))
    for i in out:
        if i > total:
            out.remove(i)
    return out


def replace_city(city):
    city_dict = {'Alain': 'Al Ain',
                 'Dubai Apartments': 'Dubai',
                 'Dubai Villas': 'Dubai',
                 'Rak': 'Ras Al Khaimah',
                 'Umm Al Quwain Mixed Use Land': 'Umm Al Quwain'}
    city = city_dict.get(city) if city_dict.get(city) else city
    return city


def replace_place(place):
    place_dict = {'JVC Jumeirah Village Circle': 'Jumeirah Village Circle',
                  'Jumeirah Golf Estate': 'Jumeirah Golf Estates',
                  'Jumeirah Village Triangle (JVT)': 'Jumeirah Village Triangle',
                  'Al Khalidiya': 'Al Khalidiyah',
                  'Mohamed Bin Zayed City': 'Mohammed Bin Zayed City'}

    place = place_dict.get(place) if place_dict.get(place) else place
    return place
