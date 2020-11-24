# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# https://stackoverflow.com/questions/54494343/scrapy-pipeline-sqlalchemy-check-if-item-exists-before-entering-to-db

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy.orm import sessionmaker
from investgate.models import EpicInfo, db_connect, create_table,NewsLinkDB,NewsItemsDB
from time import sleep
from simhash import Simhash
from datetime import datetime


class StockDBPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def close_spider(self, spider):
        # We commit and save all items to DB when spider finished scraping.
        try:
            print('*'*50)
            print('Starting commit ')
            self.session.commit()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()

    def process_item(self, item, spider):
        """Save deals in the database.

        This method is called for every item pipeline component.
        """
        item_exists = self.session.query(EpicInfo).filter_by(Epic=item['Epic']).first()
        if item_exists:
            item_exists.MktCap = item["MktCap"]
            print('UPDATED Item {} - MarketCao {}'.format(item['Epic'],item['MktCap']))
        # if not - we insert new item to DB
        else:
            new_item = EpicInfo(**item)
            self.session.add(new_item)
            print('ADDING Item {} - {} - {}'.format(item['Epic'],item['Name'],item['Industry']))
        return item


class rnslinksPipeline(object):


    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.ItemsAddedCount=0


    def close_spider(self, spider):
        # We commit and save all items to DB when spider finished scraping.
        try:
            self.session.commit()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()
        now = datetime.now()
        f = open("rnslinks_log.csv", "a")
        f.write((now.strftime("%H:%M:%S,") + " ,Items Added ,%d\n")%(self.ItemsAddedCount))
        f.close()


    def process_item(self, item, spider):

        item_exists = self.session.query(NewsLinkDB).filter_by(UrlHash=item['UrlHash']).first()
        if item_exists:

            #item_exists.Ndate = NewsLinkDB(**item)
            # # item_exists.Ntime = item["Ntime"]
            # # item_exists.Title = item["Title"]
            item_exists.Sno = item["Sno"]
            # item_exists.Source = item["Source"]
            #print('UPDATED Item {} : {} - {} into {}  '.format(item['Epic'],item['Sno'],item['UrlHash'],item_exists.UrlHash))
            self.session.commit()
        else:
            new_item = NewsLinkDB(**item)
            self.session.add(new_item)
            print('ADDING Item {} : {} - {} - {}'.format(item['Epic'], item['Sno'], item['UrlHash'], item['Ntime']))
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise
            self.ItemsAddedCount+=1
        return item

        #merged_x = self.session.merge(new_item)
        # item_exists = self.session.query(NewsLinkDB).filter_by(UrlHash=item['UrlHash']).first()
        # if item_exists:
        #     # item_exists.Ndate = item["Ndate"]
        #     # item_exists.Ntime = item["Ntime"]
        #     # item_exists.Title = item["Title"]
        #     # item_exists.Sno = item["Sno"]
        #     # item_exists.Source = item["Source"]
        #     print('UPDATED Item {} : {} - {} into {}  '.format(item['Epic'],item['Sno'],item['UrlHash'],item_exists.UrlHash))
        #     new_item = NewsLinkDB(**item)
        #     self.session.update(new_item)
        #     print('UPDATED Item {} : {} - {} into {}  '.format(new_item['Epic'], new_item['Sno'], new_item['UrlHash'],item_exists.UrlHash))
        # # if not - we insert new item to DB
        # else:
        #     new_item = NewsLinkDB(**item)
        #     self.session.add(new_item)
        #     print('ADDING Item {} : {} - {} - {}'.format(item['Epic'],item['Sno'],item['UrlHash'],item['Ntime']))


#################################################################################
#################################################################################

class ArticlesPipeline(object):

    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.Count=0



    def close_spider(self, spider):
        # We commit and save all items to DB when spider finished scraping.
        self.session.close()
        
    def process_item(self, item, spider):
        item_exists = self.session.query(NewsItemsDB).filter_by(UrlHash=item['UrlHash']).first()
        # if item exists in DB - we just update 'date' and 'subs' columns.
        if item_exists:
            print('Item exists , not UPDATED Item ')
            print('#'*50)
            return item  
        # if not - we insert new item to DB
        else:     
            new_item = NewsItemsDB(**item)
            self.session.add(new_item)
            print('-'*25)
            print('ADDING Item no {} - {} -  '.format(self.Count,item['Epic']))

        try:
            
#            print('Starting commit of #'+str(self.Count))
            self.session.commit()
        except:
            self.session.rollback()
            raise
        print('Finished commit of item #'+str(self.Count))
        print('-'*25)
        self.Count=self.Count+1
        return item   