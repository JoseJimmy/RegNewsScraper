import scrapy
from sqlalchemy.orm import sessionmaker
from investgate.models import EpicInfo, db_connect, create_table
from investgate.items import EpicNewsLinkItems
from simhash import Simhash
from scrapy.loader import ItemLoader
from datetime import datetime
now = datetime.now()
import time
import zlib
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
        EpicId={}
        Epics=[]
        CustomList = []
        for instance in session.query(EpicInfo).order_by(EpicInfo.Epic.asc()):
            StartUrls.append(EpicUrl % (instance.Epic))
            Epics.append(instance.Epic)
            EpicId[instance.Epic] = instance.id

        if (len(CustomList) != 0):
            StartUrls= []
            Epics=[]
            for epic in CustomList:
                StartUrls.append(EpicUrl % (epic))
                Epics.append(epic)
        session.close()



        for idx,url in  enumerate(StartUrls):

            self.logger.info('==============================================')
            self.logger.info('Requesting  : #' +str(idx)+' '+StartUrls[idx])
            self.logger.info('==============================================')
            Tic = Epics[idx]
            yield scrapy.Request(url, self.parse,priority =idx+1, meta={'Tic':Tic,'Ticno':EpicId[Tic]})
        #yield scrapy.Request(StartUrls[2], self.parse,meta={'Tic': Epics[2],'TicNo':3})

    def parse(self, response):   
        Tic = response.request.meta['Tic']
        Ticno = response.request.meta['Ticno']*100000000


        item = EpicNewsLinkItems()
        dates=[]
        rows = response.xpath('//table[@id="announcementList"]//tr')[2:]
        if (response.status==200):
            f = open("rnslinks_status.csv", "a")
            #print(now.strftime(" %H:%M:%S ~  ")+"%d -  %s : Status OK\n"%(Ticno/100000000,Tic))
            f.write(now.strftime("%H:%M:%S")+",%d,%s,%d,Status OK\n"%(Ticno/100000000,Tic,len(rows)))
            f.close()
        else:
            f = open("rnslinks_status_error.csv", "a")
            f.write(now.strftime("%H:%M:%S")+",%d,%s,%d,Status Not OK\n"%(Ticno/100000000,Tic,len(rows)))
            f.close()
        ndates=[]
        for idx,row in enumerate(rows): #range(2,3):#len(response.xpath('//table[@id="announcementList"]//tr'))):
            loader = ItemLoader(item=EpicNewsLinkItems(), selector=row)
            ndates.append(row.xpath('./td[1]/text()').get())

            if(len(ndates[idx])<3):
                ndates[idx] = ndates[idx-1]
            loader.add_value('Ndate', ndates[idx])

            #loader.add_xpath('Ndate', './td[1]/text()')
            #item['Time'] = response.xpath('//table[@id="announcementList"]//tr[%d]//td[2]/text()'%(i)).extract()
            loader.add_xpath('Ntime', './td[2]/text()')
            loader.add_xpath('Source', './td[3]/a/div/text()')

            #item['Title'] = response.xpath('//table[@id="announcementList"]//tr[%d]//td[4]//a[@href]/text()'%(i)).get()
            loader.add_xpath('Title', './td[4]//a[@href]/text()')
            #item['Link']  = response.xpath('//table[@id="announcementList"]//tr[%d]//td[4]//@href'%(i)).get()
            loader.add_xpath('Link', './td[4]//@href')
            link = row.xpath('./td[4]//@href').get()
            hashstr=Tic+'-'+link.strip();
            loader.add_value('UrlHash',zlib.crc32(str.encode(hashstr)))
            loader.add_value('Epic', Tic)
            loader.add_value('Sno', Ticno+idx+1)
            yield  loader.load_item()



