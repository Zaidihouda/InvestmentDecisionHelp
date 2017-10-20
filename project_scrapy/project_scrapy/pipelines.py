# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import Spider
from scrapy.exceptions import DropItem
from scrapy import settings
from scrapy.conf import settings
import json
import csv
import codecs
from scrapy.xlib.pydispatch import dispatcher

from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter
from scrapy.exporters import CsvItemExporter

from datetime import datetime


class JsonLinesExportPipeline(object):
    nbLines = 0
    nbFiles = 0

    def __init__(self):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.files = {}

    def spider_opened(self, spider):
        i = datetime.now()
        file = codecs.open('%s_items_%s_%s.json' % (spider.name, self.nbFiles, i.strftime('%Y-%m-%dT%H-%M-%S')), 'w+b')
        self.files[spider] = file
        self.exporter = JsonLinesItemExporter(file, ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        if self.nbLines >= 10000:
            self.nbFiles = self.nbFiles + 1
            self.nbLines = 0
            i = datetime.now()
            file = codecs.open('%s_items_%s_%s.json' % (spider.name, self.nbFiles, i.strftime('%Y-%m-%dT%H-%M-%S')), 'w+b')
            self.files[spider] = file
            self.exporter = JsonLinesItemExporter(file, ensure_ascii=False)
        else:
            self.nbLines = self.nbLines + 1
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()

class CsvExportPipeline(object):
    nbLines = 0
    nbFiles = 0

    def __init__(self):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        #self.fields_to_export = [
        #    'evo'
        #]
        self.files = {}

    def spider_opened(self, spider):
        file = codecs.open('%s_items_%s_%s.csv' % (spider.name, self.nbFiles, datetime.now().strftime('%Y-%m-%dT%H-%M-%S')), 'w+b')
        self.files[spider] = file
        self.csv_exporter = CsvItemExporter(file, quoting=csv.QUOTE_ALL)
        #self.exporter.fields_to_export = ['names','stars','subjects','reviews']
        self.csv_exporter.start_exporting()

    def process_item(self, item, spider):
        if self.nbLines >= 10000:
            self.nbFiles = self.nbFiles + 1
            self.nbLines = 0
            file = codecs.open('%s_items_%s_%s.csv' % (spider.name, self.nbFiles, datetime.now().strftime('%Y-%m-%dT%H-%M-%S')), 'w+b')
            self.files[spider] = file
            self.csv_exporter = CsvItemExporter(file, quoting=csv.QUOTE_ALL)
        else:
            self.nbLines = self.nbLines + 1
        self.csv_exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.csv_exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()


"""
class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['url'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['url'])
            return item
"""
