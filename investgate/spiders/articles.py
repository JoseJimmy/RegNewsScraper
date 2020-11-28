import scrapy
from investgate.items import NewsItems
from scrapy.loader import ItemLoader
from sqlalchemy.sql import select,desc
from investgate.models import StockInfoTable, db_connect,ArticlesTable,NewsLinksTable
import time

class articles(scrapy.Spider):
    name = 'articles'
    allowed_domains = ['www.investegate.co.uk']
    custom_settings = {'ITEM_PIPELINES': {'investgate.pipelines.ArticlesPipeline': 300}}

    def start_requests(self):
        SessionLimit=200000
        ScrapeList = self.GetScrapeList(SessionLimit)
        BaseUrl = 'https://www.investegate.co.uk'
        for idx,i in enumerate(ScrapeList[1:-1]):
            Epic = i[0]
            Title = i[1]
            Uhash = i[2]
            Link = i[3]
            udate = i[4]
            print('\n'+'#~#'*25)
            print('Requesting <Item#%d of %d> Epic %s- %s : %s'%(idx,len(ScrapeList),Epic,udate,Title))
            print('#~#*'*25+'\n')
            yield scrapy.Request(BaseUrl+Link, self.parse,meta={'Tic': Epic,'Uhash': Uhash,'Link':Link})


    def parse(self, response):
        if(response.status !=200):
            print('WRONG RESPONSE : Closing Spider !>>>>>'+str(response.status))
            
        Tic = response.request.meta['Tic']
        Uhash = int(response.request.meta['Uhash'])
        Link = response.request.meta['Link']
        loader = ItemLoader(item=NewsItems(), response=response)
        print('User Agent :', response.request.headers['User-Agent'])

        # loader.add_xpath('Article', '//*[@id="ArticleContent"]')
        loader.add_xpath('Article', '//*[@id="ArticleContent"]')
        loader.add_value('UrlHash',Uhash)#Simhash(link).value)
        loader.add_value('Epic', Tic)
        loader.add_value('Link', Link)
        loader.add_value('SHash', 0)
        loader.add_value('Artlen', 0)
        yield  loader.load_item()
        


    def GetScrapeList(self,SessionLimit):
        engine = db_connect()
        connection = engine.connect()
        SessionLinksCount = 0
        s = select([ArticlesTable.Epic])
        s = s.order_by(ArticlesTable.Epic)
        rp = connection.execute(s)
        Rows = rp.fetchall()
        # connection.close()
        Tics = [Tic[0] for Tic in Rows]
        # connection = engine.connect()

        ### First get all hashes from articles
        print('Get all hashes from articles  DB....', end='')
        s = select([ArticlesTable.UrlHash])
        Rp = connection.execute(s)
        Rp = Rp.fetchall()
        ScrapedUHashlist = []
        print('Done')
        for item in Rp:
            ScrapedUHashlist.append(int(item.UrlHash))
        ###  get all hashes from source
        print('Get all hashes from article.... DB', end='')

        s = select([NewsLinksTable.UrlHash])
        Rp = connection.execute(s)
        results = Rp.fetchall()
        SourceUHashlist = []
        SourceEpiclist = []
        for item in results:
            SourceUHashlist.append(int(item.UrlHash))
        print('Done')


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
                [NewsLinksTable.Epic, NewsLinksTable.UrlHash, NewsLinksTable.Title, NewsLinksTable.Ndate, NewsLinksTable.Link]).where(
                NewsLinksTable.UrlHash == (item))
            Rp = connection.execute(s)
            results = Rp.first()
            url = results

            # [['Epic','Title','UrlHash','Url']]
            ScrapeList.append([url.Epic, url.Title, url.UrlHash,  url.Link, str(url.Ndate)])
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