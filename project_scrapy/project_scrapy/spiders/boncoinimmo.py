# -*- coding: utf-8 -*-
import scrapy
import urlparse

from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request

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
            relative_ad_url=li.xpath('a/@href').extract()
            yield Request(urlparse.urljoin(response.url, relative_ad_url[0]), self.parse_item)


        relative_next_url = response.xpath('//*[@id="next"]//@href').extract_first()
        print('Next page   ::::::: ', relative_next_url)
        next_url = response.urljoin(relative_next_url)

        yield Request(next_url, callback=self.parse)

    def parse_item(self, response):


        item = it.ProjectScrapyItem()



        search_xpath_price = '//*[@id="adview"]/section/section/section[2]/div[4]/h2/span[2]/text()'
        search_xpath_ville = '//*[@id="adview"]/section/section/section[2]/div[5]/h2/span[2]/text()'
        search_xpath_type = '//*[@id="adview"]/section/section/section[2]/div[6]/h2/span[2]/text()'
        search_xpath_pieces = '//*[@id="adview"]/section/section/section[2]/div[7]/h2/span[2]/text()'
        search_xpath_surface = '//*[@id="adview"]/section/section/section[2]/div[8]/h2/span[2]/text()'
        search_xpath_gesc = '//*[@id="adview"]/section/section/section[2]/div[9]/h2/span[2]/text()'
        search_xpath_energie = '//*[@id="adview"]/section/section/section[2]/div[10]/h2/span[2]/text()'
        search_xpath_description = '//*[@id="adview"]/section/section/section[2]/div[12]/p[2]/text()'
        search_xpath_date = '//*[@id="adview"]/section/section/section[2]/p/text()'

        item['url'] = response.url

        item['data'] = {}
        item['data']['date'] = response.xpath(search_xpath_date).extract()
        item['data']['price'] = response.xpath(search_xpath_price).extract()
        item['data']['ville'] = response.xpath(search_xpath_ville).extract()
        item['data']['type'] = response.xpath(search_xpath_type).extract()
        item['data']['pieces'] = response.xpath(search_xpath_pieces).extract()
        item['data']['surface'] = response.xpath(search_xpath_surface).extract()
        item['data']['gesc'] = response.xpath(search_xpath_gesc).extract()
        item['data']['energie'] = response.xpath(search_xpath_energie).extract()
        item['data']['description'] = response.xpath(search_xpath_description).extract()


        yield item
