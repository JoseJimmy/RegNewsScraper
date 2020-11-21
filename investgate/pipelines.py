# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# https://stackoverflow.com/questions/54494343/scrapy-pipeline-sqlalchemy-check-if-item-exists-before-entering-to-db

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy.orm import sessionmaker
from investgate.models import StocksDB, db_connect, create_table,NewsLinkDB,NewsItemsDB
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
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        """Save deals in the database.

        This method is called for every item pipeline component.
        """
        session = self.Session()
        stockdb = StocksDB()
        stockdb.Epic = item["Epic"]
        stockdb.Link = item["Link"]
        stockdb.Name = item["Name"]
        stockdb.MktCap = item["MktCap"]
        stockdb.SupSector = item["SupSector"]
        stockdb.SubSector = item["SubSector"]
        stockdb.Sector = item["Sector"]
        stockdb.Industry = item["Industry"]
        try:
            session.add(stockdb)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item



class rnslinksPipeline(object):
    #prev_date=datetime.strptime('01 Jan 1901', '%d %b %Y').date()

    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.prev_date=datetime.strptime('01 Jan 1999', '%d %b %Y').date()



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
        if(item["Ndate"] < datetime.strptime('01 Jan 1940', '%d %b %Y').date()):
            #print('^'*50)
            #print('ChanginG {} to  {} !!!!!!!!!.'.format(item["Ndate"] ,self.prev_date))
            #print('^'*50)

            item["Ndate"] = self.prev_date
            

        else:
            self.prev_date = item["Ndate"]
        #item['UrlHash'] = str(Simhash(item['Link'] ).value  )
        
        item_exists = self.session.query(NewsLinkDB).filter_by(UrlHash=item['UrlHash']).first()
        # if item exists in DB - we just update 'date' and 'subs' columns.
        if item_exists:
            item_exists.Ndate = item["Ndate"]
            item_exists.Ntime = item["Ntime"]
            item_exists.Title = item["Title"]
            item_exists.Sno = item["Sno"]
            #print('#'*50)
            print('UPDATED Item {} - {} - {}'.format(item['Sno'],item['Ndate'],item['Title']))
            #print('#'*50)
        # if not - we insert new item to DB
        else:     
            new_item = NewsLinkDB(**item)
            self.session.add(new_item)
            #print('@'*50)
            #print('ADDING Item {} - {} - {}'.format(item['Sno'],item['Ndate'],item['Ntime']))
            #print('@'*50)
        
        return item    
    
#################################################################################
#################################################################################

class ArticlesPipeline(object):

    # newsdb = NewsLinkDB()
    # newsdb.UHash = item["UrlHash"]
    # newsdb.Epic = item["Epic"]
    # newsdb.Link = item["Link"]
    # newsdb.Date = item["Date"]
    # newsdb.Time = item["Time"]
    # newsdb.Title = item["Title"]
    # check if item with this title exists in DB
    
        #prev_date=datetime.strptime('01 Jan 1901', '%d %b %Y').date()

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
            print('ADDING Item no {} - {} - {} '.format(self.Count,item['Epic'],item['Title']))

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