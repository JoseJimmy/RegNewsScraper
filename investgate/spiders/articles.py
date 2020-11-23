import scrapy
from investgate.items import NewsItems
from scrapy.loader import ItemLoader
from sqlalchemy.sql import select,desc
from investgate.models import EpicInfo, db_connect,NewsLinkDB,NewsItemsDB
import time

class articles(scrapy.Spider):
    name = 'articles'
    allowed_domains = ['www.investegate.co.uk']
    custom_settings = {'ITEM_PIPELINES': {'investgate.pipelines.ArticlesPipeline': 300}}

    def start_requests(self):
        SessionLimit=260000
        ScrapeList = self.GetScrapeList(SessionLimit)
        for idx,i in enumerate(ScrapeList[1:-1]):
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
        return ScrapeList