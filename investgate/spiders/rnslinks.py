import scrapy
from sqlalchemy.orm import sessionmaker
from investgate.models import StocksDB, db_connect, create_table
from investgate.items import EpicNewsLinkItems
from simhash import Simhash
from scrapy.loader import ItemLoader
from datetime import datetime
now = datetime.now()
import time
class investgate(scrapy.Spider):
    name = 'rnslinks'
    allowed_domains = ['www.investegate.co.uk']
    custom_settings = {'ITEM_PIPELINES': {'investgate.pipelines.rnslinksPipeline': 300}}

    def start_requests(self):
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        EpicUrl = 'https://www.investegate.co.uk/CompData.aspx?code=%s&tab=announcements&limit=-1'
        StartUrls=[]
        Epics=[]
        Elist = ['PRU']
        self.logger.info('==============================================')
        
        for instance in session.query(StocksDB):
            #if(instance.Epic in Elist):
            StartUrls.append(EpicUrl%(instance.Epic))
            Epics.append(instance.Epic)
            self.logger.info('Will scrape :'+instance.Epic)
        self.logger.info('==============================================')       
        session.close()
        time.sleep(10)
        ##Custom Epic list 
        
        self.logger.info('==============================================')
        self.logger.info('Fetch : '+StartUrls[0])
        self.logger.info('==============================================')
       
        for idx,url in  enumerate(StartUrls):
            yield scrapy.Request(url, self.parse,meta={'Tic': Epics[idx],'Ticno': idx+1})
        #yield scrapy.Request(StartUrls[2], self.parse,meta={'Tic': Epics[2],'TicNo':3})

    def parse(self, response):   
        Tic = response.request.meta['Tic']
        Ticno = response.request.meta['Ticno']*100000000
        if (response.status==200):
            f = open("rnslinks_status.txt", "a")
            #print(now.strftime(" %H:%M:%S ~  ")+"%d -  %s : Status OK\n"%(Ticno/100000000,Tic))
            f.write(now.strftime(" %H:%M:%S ~  ")+"%d -  %s : Status OK\n"%(Ticno/100000000,Tic))
            f.close()
        else:
            f = open("rnslinks_status.txt", "a")
            f.write(now.strftime(" %H:%M:%S ~  ")+"%d -  %s : Status OK\n"%(Ticno/100000000,Tic))
            f.close()

        item = EpicNewsLinkItems()
        dates=[]
        rows = response.xpath('//table[@id="announcementList"]//tr')[2:]
        for idx,row in enumerate(rows): #range(2,3):#len(response.xpath('//table[@id="announcementList"]//tr'))):
            loader = ItemLoader(item=EpicNewsLinkItems(), selector=row)
            
            #item['Date'] = response.xpath('//table[@id="announcementList"]//tr[%d]//td[1]/text()'%(i)).extract()
            loader.add_xpath('Ndate', './td[1]/text()')
            #item['Time'] = response.xpath('//table[@id="announcementList"]//tr[%d]//td[2]/text()'%(i)).extract()
            loader.add_xpath('Ntime', './td[2]/text()')
            #item['Title'] = response.xpath('//table[@id="announcementList"]//tr[%d]//td[4]//a[@href]/text()'%(i)).get()
            loader.add_xpath('Title', './td[4]//a[@href]/text()')
            #item['Link']  = response.xpath('//table[@id="announcementList"]//tr[%d]//td[4]//@href'%(i)).get()
            loader.add_xpath('Link', './td[4]//@href')
            link = row.xpath('./td[4]//@href').get()
            loader.add_value('UrlHash',Simhash(link).value )#Simhash(link).value)
            loader.add_value('Epic', Tic)
            loader.add_value('Sno', Ticno+idx+1)
            yield  loader.load_item()
