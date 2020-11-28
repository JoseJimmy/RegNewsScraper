import scrapy
from scrapy.loader import ItemLoader
from investgate.items import StockInfoItems

class LseSpider(scrapy.Spider):


    name = 'lse'
    allowed_domains = ['www.londonstockexchange.com']
    # FirstPage= 'https://www.londonstockexchange.com/indices/ftse-350/constituents/table'
    # start_urls = [FirstPage] +['https://www.londonstockexchange.com/indices/ftse-350/constituents/table?page=%d'% (page) for page in range(2,19)]
    FirstPage= 'https://www.londonstockexchange.com/indices/ftse-all-share/constituents/table'
    start_urls = [FirstPage] +['https://www.londonstockexchange.com/indices/ftse-all-share/constituents/table?page=%d'% (page) for page in range(2,32)]
    #start_urls = ['https://www.londonstockexchange.com/indices/ftse-all-share/constituents/table?page=23']

    custom_settings = {'ITEM_PIPELINES': {'investgate.pipelines.StockDBPipeline': 300}}


    def parse(self, response):
        rows = response.xpath('//*[@id="ftse-index-table"]/table/tbody/tr')
        for row in rows:
            loader = ItemLoader(item=StockInfoItems(), selector=row)
            loader.add_xpath('Epic', './td[1]//a[@href]/text()')
             #Epic =  row.xpath('./td[1]//a[@href]/text()').get(),
             #Link = row.xpath('./td[1]//@href').get(),
            loader.add_xpath('Name','./td[2]//a[@href]/text()')
             #Name  =   row.xpath('./td[2]//a[@href]/text()').get(),

            loader.add_xpath('MktCap', './td[4]/text()')
             #MktCap =  row.xpath('./td[4]/text()').get(),
            #loader.add_value(Index)
            stock = loader.load_item()
             #Id =  row.xpath('./td[3]/text()').get()
            link = row.xpath('./td[1]//@href').get()+'/our-story'
            yield response.follow(link, callback = self.parseinfo,meta={'StockItem': stock})
            


    def parseinfo(self, response):
        StockItem = response.meta['StockItem']
        loader = ItemLoader(item=StockItem, response=response)
        
        #'Industry' : response.xpath('//*[@id="ccc-data-ftse-industry"]/div[2]/text()').get(),
        loader.add_xpath('Industry', '//*[@id="ccc-data-ftse-industry"]/div[2]/text()')

        #'SupSector' : response.xpath('//*[@id="ccc-data-ftse-supersector"]/div[2]/text()').get(),
        loader.add_xpath('SupSector','//*[@id="ccc-data-ftse-supersector"]/div[2]/text()')

        #'Sector' : response.xpath('//*[@id="ccc-data-ftse-sector"]/div[2]/text()').get(),
        loader.add_xpath('Sector', '//*[@id="ccc-data-ftse-sector"]/div[2]/text()')

        #'SubSector' : response.xpath('//*[@id="ccc-data-ftse-subsector"]/div[2]/text()').get()
        loader.add_xpath('SubSector', '//*[@id="ccc-data-ftse-subsector"]/div[2]/text()')
        yield loader.load_item()
