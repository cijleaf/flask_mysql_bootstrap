#!/usr/bin/python3
# -*- coding: utf-8 -*-

import propertyfinder
import bayut
import dubizzlecom
import justpropertycom
from settings import DEBUG
import time


def runner(parser):
    if DEBUG:
        parser.run()
    else:
        try:
            parser.run()
        except:
            pass


parsers = (justpropertycom, propertyfinder, bayut, dubizzlecom)

start = time.time()

for parser in parsers[:]:
    runner(parser)
print(time.time() - start)
