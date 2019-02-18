#!/usr/bin/python
# -*- coding: utf-8 -*-

import statistics as stat
import datetime
import random


def replace_price(prelist):
    magic = 14

    if prelist[magic] is None:
        return prelist

    preprice = str(prelist[magic])
    price = ''
    for i, ch in enumerate(preprice):
        if not (len(preprice) - i) % 3:
            price += ','
        price += ch
    price = price.strip(',')
    out = list(prelist)
    out[magic] = price

    return out


def perc(presale, prerent):

    if presale and prerent:
        sale = []
        rent = []

        for i in presale:
            if i[14]:
                sale.append(int(str(i[14]).replace(',', '')))

        for i in prerent:
            if i[14]:
                rent.append(int(str(i[14]).replace(',', '')))

        max_perc = str(round(max(rent) / min(sale) * 100, 1))
        min_perc = str(round(min(rent) / max(sale) * 100, 1))
        avsale = stat.mean(sale)
        avrent = stat.mean(rent)

        median_sale = stat.median(sale)
        median_rent = stat.median(rent)

        average = str(round(avrent / avsale * 100, 1))
        median = str(round(median_rent / median_sale * 100, 1))
    else:
        return ['0', '0', '0', '0']

    return min_perc, average, median, max_perc


def sale_rent_aggregation(listings, zeros=0, minperc=0, maxperc=9999):
    if minperc == 'All':
        minperc = 0
    if maxperc == 'All':
        maxperc = 9999

    outdict = {}
    for ls in listings:

        if ls[16] not in outdict.keys():
            outdict[ls[16]] = {ls[12]: [ls, ]}
        else:
            if ls[12] not in outdict[ls[16]].keys():
                outdict[ls[16]][ls[12]] = [ls, ]
            else:
                outdict[ls[16]][ls[12]].append(ls)
    del_keys = set()

    for i in outdict.keys():
        newperc = perc(outdict[i].get('Sale'), outdict[i].get('Rent'))
        outdict[i]['result'] = perc(outdict[i].get('Sale'), outdict[i].get('Rent'))

        if zeros == 'on':
            if newperc[0] == '0':
                del_keys.add(i)

        print(zeros, newperc[0])
        if int(minperc) > float(newperc[2]) or float(newperc[2]) > int(maxperc):
            del_keys.add(i)

    for key in del_keys:
        del outdict[key]

    return outdict


def drop_double(data):
    doubles = set()
    out = []
    for pre in data[::-1]:
        if str(pre[14]) + pre[4].strip().lower() + pre[-4] not in doubles:
            doubles.add(str(pre[14]) + pre[4].strip().lower() + pre[-4])
            out.append(pre)
    return out[::-1]


def clean_arg(arg):
    out = arg.replace("'", '')
    out = out.replace("+", '&')
    return out


def separate_date(period):
    startdate = ''
    enddate = ''
    if period == 'All':
        return startdate, enddate

    today = datetime.datetime.today()

    if period in ('0', '7', '15', '30', '60', '90'):
        startdate = (today - datetime.timedelta(days=int(period)+1)).strftime('%Y-%m-%d')
    else:
        startdate = (today - datetime.timedelta(days=15)).strftime('%Y-%m-%d')

    return startdate, enddate


def separate_cat(precat):
    cat, subcat = 'All', 'All'
    precat = clean_arg(precat)
    if len(precat.split('-')) == 2:
        cat, subcat = precat.split('-')
    return cat, subcat


def group_place(places):
    out = []
    n = 0
    tmp = []
    for place in places:
        if n < 4:
            tmp.append(place)
            n += 1
        else:
            n = 0
            out.append(tmp)
            tmp = []
    if len(tmp) == 4:
        out.append(tmp)
    return out


def choise_top(prelist):
    prelist = list(prelist)
    out = []
    for _ in range(6):
        poz = random.randint(0, len(prelist)-1)
        print(poz, len(prelist))
        tmp = list(prelist.pop(poz)[2:])
        out.append(replace_price(tmp))
    return out


def clean_city(citylist):
    #print(citylist)
    dellist = ['الشارقة', 'الفجيرة', 'دبي', 'عجمان',
               'Uaq', 'Ras Al Khaimah Villas', 'Al Ain', 'Abu Dhabi Buildings', 'cvrfbsfbvaeywfbb']
    out = []
    for city in citylist:
        #print(city)
        if city[0] not in dellist:
            out.append(city)
    return out


def clean_args(preargs, targargs):
    out_args = {}
    for key in targargs:
        prevar = preargs.get(key)
        if prevar:
            out_args[key] = prevar.replace("'", '"')
        else:
            out_args[key] = ''
        print(key, out_args[key])
    return out_args
