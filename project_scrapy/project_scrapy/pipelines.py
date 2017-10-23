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
import pandas as pd
from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter
from scrapy.exporters import CsvItemExporter

from datetime import datetime
import re

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

# Cassandra Pipeline
from cassandra.cluster import Cluster

class CassandraPipeline(object):

    def __init__(self, cassandra_keyspace):
        self.cassandra_keyspace = cassandra_keyspace

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            cassandra_keyspace=crawler.settings.get('CASSANDRA_KEYSPACE')
        )

    def open_spider(self, spider):
        cluster = Cluster(['localhost'])
        self.session = cluster.connect(self.cassandra_keyspace)
        # create scrapy_items table
        self.session.execute("CREATE TABLE IF NOT EXISTS " + self.cassandra_keyspace + ".properties_ads ( url text , price text, date text, " +
                                                                                       "pieces text, surface text, ville text, type_ text, "+
                                                                                       "gesc text, energie text, description text, "+
                                                                                       "PRIMARY KEY (url));")


    def process_item(self, item, spider):
        # insert item
        var=pd.Series(["url", "price", "date", "pieces", "surface", "ville", "type_", "gesc", "energie", "description"]).str.cat(sep=', ')
        val= pd.Series([item["url"], item["price"], item["date"], item["pieces"],
                        item["surface"], item["ville"], item["type_"], item["gesc"]
                           ,item["energie"], item["description"]]).str.cat(sep='\' , \'')
        val = "\'"+val +"\'"

        requ= "INSERT INTO " + self.cassandra_keyspace + ".properties_ads({v}) VALUES({vv});".format(v=var, vv=val)
        print "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA      :", requ
        self.session.execute(requ)
        return item


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
