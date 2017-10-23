# -*- coding: utf-8 -*-
import scrapy
import urlparse
from django.utils.encoding import smart_str, smart_unicode

from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
import re

import project_scrapy.items as  it

class BoncoinimmoSpider(scrapy.Spider):
    name = 'boncoinimmo'
    allowed_domains = ['leboncoin.fr']
    start_urls = ['https://www.leboncoin.fr/ventes_immobilieres/offres']

    def parse(self, response):
        sel = Selector(response)

        ads_page = sel.xpath('//*[@id="listingAds"]/section/section/ul')

        all_li= ads_page.xpath('li')

        for li in all_li:
            #li=all_li[1]
            relative_ad_url=li.xpath('a/@href').extract()
            yield Request(urlparse.urljoin(response.url, relative_ad_url[0]), self.parse_item)


        relative_next_url = response.xpath('//*[@id="next"]//@href').extract_first()
        next_url = response.urljoin(relative_next_url)

        yield Request(next_url, callback=self.parse)



    def parse_item(self, response):


        item = it.ProjectScrapyItem()
        def parse_champ(search_xpath):
            x = response.xpath(search_xpath).extract()
            val = " "
            if (len(x) > 0):
                val = smart_str(x[0])
                regex = re.compile(r'[\n\r\t]')
                val = regex.sub("", val)
                val=' '.join(val.split()).decode('utf-8').encode('ascii', 'replace')
            return val


        search_xpath_price = '//*[@id="adview"]/section/section/section[2]/div[4]/h2/span[2]/text()'
        search_xpath_ville = '//*[@id="adview"]/section/section/section[2]/div[5]/h2/span[2]/text()'
        search_xpath_type = '//*[@id="adview"]/section/section/section[2]/div[6]/h2/span[2]/text()'
        search_xpath_pieces = '//*[@id="adview"]/section/section/section[2]/div[7]/h2/span[2]/text()'
        search_xpath_surface = '//*[@id="adview"]/section/section/section[2]/div[8]/h2/span[2]/text()'

        search_xpath_gesc = '//*[@id="adview"]/section/section/section[2]/div[9]/h2/span[2]/a/text()'

        search_xpath_energie = '//*[@id="adview"]/section/section/section[2]/div[10]/h2/span[2]/a/text()'

        search_xpath_description = '//*[@id="adview"]/section/section/section[2]/div[11]/p[2]/text()'

        search_xpath_date = '//*[@id="adview"]/section/section/section[2]/p/text()'

        item['url'] = response.url
        item['price'] = parse_champ( search_xpath_price)
        item['ville'] = parse_champ(  search_xpath_ville)
        item['type_'] = parse_champ(  search_xpath_type) # str(response.xpath(search_xpath_type).extract()).rstrip('\n')
        item['pieces'] = parse_champ(  search_xpath_pieces) #str(response.xpath(search_xpath_pieces).extract()).rstrip('\n')
        item['surface'] = parse_champ(  search_xpath_surface) #str(response.xpath(search_xpath_surface).extract()).rstrip('\n')
        item['gesc'] = parse_champ(  search_xpath_gesc) #str(response.xpath(search_xpath_gesc).extract()).rstrip('\n')
        item['energie'] = parse_champ(  search_xpath_energie) #str(response.xpath(search_xpath_energie).extract()).rstrip('\n')
        item['description'] = parse_champ(  search_xpath_description) #str(response.xpath(search_xpath_description).extract()).rstrip('\n')
        item['date'] = parse_champ(  search_xpath_date)

        print "item['date']   ", item['date']
        print "item['price']   ", item['price']
        print "item['ville']   ", item['ville']
        print "item['type_']   ", item['type_']
        print "item['pieces']   ", item['pieces']
        print "item['surface']   ", item['surface']
        print "item['gesc']    ", item['gesc']
        print "item['energie']    ", item['energie']
        print "item['description']    ", item['description']



        yield item



