# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import scrapy
from scrapy.item import Item, Field
from itemloaders.processors import MapCompose , TakeFirst
from simhash import Simhash

from datetime import datetime

def MktCapClean(text):
    # strip the unicode quotes
    text=text.strip().replace('-', '0')
    return float(text.replace(',',''))




def convert_date(day):
    # convert string March 14, 1879 to Python date
    if (len(day.strip())<7):
        return datetime.strptime('01 Jan 1901', '%d %b %Y').date()
    else:
        return datetime.strptime(day, '%d %b %Y').date()
    
    
    
    
def convert_time(t):
    # convert string March 14, 1879 to Python date
    return datetime.strptime(t, '%H:%M %p').time()

def u2int(h):
    return int(h)
    
def u2inthash(h):
    return (int(h))

def StrProcess(art):
    art =  art.strip().replace('\\xF4\\x80\\x83\\xA4','')
    return art.encode()

class StockItems(scrapy.Item):
    Epic = scrapy.Field(input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Name = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    MktCap = scrapy.Field( input_processor=MapCompose(MktCapClean),output_processor=TakeFirst())
    Industry = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    SupSector = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    SubSector = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Sector = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    def __str__(self):
        return ""



class EpicNewsLinkItems(scrapy.Item):
    UrlHash = scrapy.Field( output_processor=TakeFirst())
    Epic = scrapy.Field(input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Link = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Title = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Sno = scrapy.Field( output_processor=TakeFirst())
    Ndate = scrapy.Field( input_processor=MapCompose(convert_date),output_processor=TakeFirst())
    Ntime = scrapy.Field( input_processor=MapCompose(convert_time),output_processor=TakeFirst())
    Source = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())

    def __str__(self):
        return ""



class NewsItems(scrapy.Item):
    UrlHash = scrapy.Field(input_processor=MapCompose(u2int),output_processor=TakeFirst())
    Link = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Epic = scrapy.Field( input_processor=MapCompose(str.strip),output_processor=TakeFirst())
    Article = scrapy.Field(input_processor=MapCompose(StrProcess),output_processor=TakeFirst())
    
    def __str__(self):
        return ""

    # def __repr__(self):
    #     """only print out attr1 after exiting the Pipeline"""
    #     return repr({"Title": self.Title})