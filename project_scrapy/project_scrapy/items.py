# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
from scrapy.item import Item, Field

import scrapy

def serialize_price(value):
    return '$ %s' % str(value)

class ProjectScrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = Field()
    #data = Field()
    price = Field()
    date = Field()
    pieces = Field()
    surface = Field()
    ville = Field()
    type_ = Field()
    gesc =Field()
    energie=Field()
    description = Field()

