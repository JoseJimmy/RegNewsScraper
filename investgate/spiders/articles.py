import scrapy
from sqlalchemy.orm import sessionmaker
from investgate.models import StocksDB, db_connect, create_table
from investgate.items import EpicNewsLinkItems,NewsItems
from simhash import Simhash
from scrapy.loader import ItemLoader
from sqlalchemy.sql import select,desc
from investgate.models import StocksDB, db_connect,NewsLinkDB
from investgate.models import StocksDB, db_connect,NewsLinkDB,NewsItemsDB
import time

class articles(scrapy.Spider):
    name = 'articles'
    allowed_domains = ['www.investegate.co.uk']
    custom_settings = {'ITEM_PIPELINES': {'investgate.pipelines.ArticlesPipeline': 300}}

    def start_requests(self):
        SessionLimit=260000
        ScrapeList = self.GetScrapeList(SessionLimit)
        for idx,i in enumerate(ScrapeList[0:-1]):
            Epic = i[0]
            Title = i[1]
            Uhash = i[2]
            Link = i[3]
            udate = i[4]
            print('\n'+'#~#'*25)
            print('Requesting <Item#%d of %d> Epic %s- %s : %s'%(idx,len(ScrapeList),Epic,udate,Title))
            print('#~#*'*25+'\n')
            yield scrapy.Request(Link, self.parse,meta={'Tic': Epic,'Uhash': Uhash,'Title':Title})


    def parse(self, response):
        if(response.status !=200):
            print('WRONG RESPONSE : Closing Spider !>>>>>'+str(response.status))
            
        Tic = response.request.meta['Tic']
        Uhash = int(response.request.meta['Uhash'])
        Title = response.request.meta['Title']
        loader = ItemLoader(item=NewsItems(), response=response)
        print('User Agent :', response.request.headers['User-Agent'])

        # loader.add_xpath('Article', '//*[@id="ArticleContent"]')
        loader.add_xpath('Article', '//*[@id="ArticleContent"]')
        loader.add_value('UrlHash',Uhash)#Simhash(link).value)
        loader.add_value('Epic', Tic)
        loader.add_value('Title', Title)
        yield  loader.load_item()
        

    # def GetScrapeList(self,SessionLimit):
    #     engine = db_connect()
    #     connection = engine.connect()
    #     SessionLinksCount = 0
    #     s = select([StocksDB.Epic])
    #     s = s.order_by(StocksDB.Epic)
    #     rp = connection.execute(s)
    #     Rows = rp.fetchall()
    #     #connection.close()
    #     Tics = [Tic[0] for Tic in Rows]
    #     #connection = engine.connect()
    #     ScrapeList=[]
    #     for Eidx,Row in enumerate(Tics):
    #         LimitReached = False
    #         Tic=Row
    #     #   Tic= 'TSCO'
    #             # Fetch Urls 
            
    #         s = select([NewsLinkDB.Link,NewsLinkDB.UrlHash,NewsLinkDB.Title,NewsLinkDB.Ndate,NewsLinkDB.Link]).where(NewsLinkDB.Epic == Tic)
    #         s = s.order_by(desc(NewsLinkDB.Ndate))
    #         Rp = connection.execute(s)
    #         Urls = Rp.fetchall()
    #         BaseUrl = 'https://www.investegate.co.uk'
            
    #         #[['Epic','Title','UrlHash','Url']]
    #         SessionLinksPerEpic = SessionLinksCount
    #         for idx,Url in enumerate(Urls):
                
    #             s = select([NewsItemsDB.UrlHash]).where(NewsItemsDB.UrlHash == int(Url.UrlHash))
    #             Rp = connection.execute(s)
    #             item = Rp.fetchall()
    #             if (item != []):
    #                 pass
    #                 #item=item[0]
    #                 #print('%s : %s %s exists , is not scraped'%(Tic,item.Title,item.UrlHash))

    #             else:
    #                 #print('Adding %s : %s - %s  is to be scraped'%(Tic,str(Url.Ndate),Url.Title))
    #                 ScrapeList.append([Tic,Url.Title,Url.UrlHash,BaseUrl+Url.Link,str(Url.Ndate)])
    #                 SessionLinksCount+=1
    #             if(SessionLinksCount>SessionLimit):
    #                 LimitReached = True
    #                 break
    #         SessionLinksPerEpic = SessionLinksCount - SessionLinksPerEpic 
    #         print('%d Links from %s to be Scraped <Total #%d>'%(SessionLinksPerEpic,Tic,SessionLinksCount))
    #         if(LimitReached):
    #             break

    #     connection.close()
    #     print('\n'+'+~*'*25)
    #     print('#'*25)
    #     print('%s : Total of %d items to be Scraped '%(Tic,len(ScrapeList)))
    #     print('#'*25)
    #     print('\n'+'+~*'*25)
    #     time.sleep(5)
    #     # Scrape_list=[['Epic','Title','UrlHash','Url','date']]
    #     return ScrapeList

    def GetScrapeList(self,SessionLimit):
        engine = db_connect()
        connection = engine.connect()
        SessionLinksCount = 0
        s = select([StocksDB.Epic])
        s = s.order_by(StocksDB.Epic)
        rp = connection.execute(s)
        Rows = rp.fetchall()
        # connection.close()
        Tics = [Tic[0] for Tic in Rows]
        # connection = engine.connect()

        ### First get all hashes from articles
        s = select([NewsItemsDB.UrlHash])
        Rp = connection.execute(s)
        Rp = Rp.fetchall()
        ScrapedUHashlist = []
        for item in Rp:
            ScrapedUHashlist.append(int(item.UrlHash))
        ###  get all hashes from source

        s = select([NewsLinkDB.UrlHash])
        Rp = connection.execute(s)
        results = Rp.fetchall()
        SourceUHashlist = []
        SourceEpiclist = []
        for item in results:
            SourceUHashlist.append(int(item.UrlHash))

        SessionLinksCount = 0

        ToScrapeUHashList = list(set(SourceUHashlist) - set(ScrapedUHashlist))

        # for idx, uhash in enumerate(SourceUHashlist):
        #     if (uhash not in ScrapedUHashlist):
        #         ToScrapeUHashList.append([uhash, SourceEpiclist[idx]])
        #         SessionLinksCount += 1
        #     if (SessionLinksCount > SessionLimit):
        #         break

        ScrapeList = []
        SessionLinksCount = 0
        EpicLinkCount = {}
        print('Links added :', end="")
        print(str(SessionLinksCount).rjust(10), end="")
        for item in (ToScrapeUHashList):
            print('\b' * 10, end='')
            s = select(
                [NewsLinkDB.Epic, NewsLinkDB.UrlHash, NewsLinkDB.Title, NewsLinkDB.Ndate, NewsLinkDB.Link]).where(
                NewsLinkDB.UrlHash == (item))
            Rp = connection.execute(s)
            results = Rp.first()
            url = results
            BaseUrl = 'https://www.investegate.co.uk'
            # [['Epic','Title','UrlHash','Url']]
            ScrapeList.append([url.Epic, url.Title, url.UrlHash, BaseUrl + url.Link, str(url.Ndate)])
            tic = url.Epic
            if (tic in EpicLinkCount):
                EpicLinkCount[url.Epic] += 1
            else:
                EpicLinkCount[url.Epic] = 1
            SessionLinksCount += 1
            print(str(SessionLinksCount).rjust(10), end="")
            if (SessionLinksCount > SessionLimit):
                break

        connection.close()
        print('\n' + '+~*' * 25)
        print('==' * 25)
        for tic in EpicLinkCount:
            print('<      %d Links from %s to be Scraped >' % (EpicLinkCount[tic], tic))
        print('==' * 25)
        print('%d Links to be Scraped >' % (SessionLinksCount))
        print('==' * 25)
        print('\n' + '+~*' * 25)
        time.sleep(5)
        # Scrape_list=[['Epic','Title','UrlHash','Url','date']]
        return ScrapeList