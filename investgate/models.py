from sqlalchemy import create_engine, Column, Table, ForeignKey, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (BigInteger,Integer, String, Date, DateTime,Numeric, Float, Boolean,Time,Text)
from scrapy.utils.project import get_project_settings
import scrapy 
from sqlalchemy.dialects.mysql import LONGTEXT
import sqlalchemy


DeclarativeBase = declarative_base()
def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(get_project_settings().get("CONNECTION_STRING"))

def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)


class EpicInfo(DeclarativeBase):
    __tablename__ = "EpicInfo"
    id = Column(Integer, primary_key=True , autoincrement=True)
    Epic = Column(String(6),primary_key=True)
    Name = Column(String(300))
    MktCap = Column(Float)
    Industry = Column(String(100))
    SupSector = Column(String(100))
    SubSector = Column(String(100))
    Sector = Column(String(100))

class NewsLinkDB(DeclarativeBase):
    __tablename__ = "NewsLinks"
    Epic = Column('Epic',String(6))
    Link = Column('Link',String(400))
    Ndate = Column('Ndate',Date)
    Ntime = Column('Ntime',Time)
    Title = Column('Title',String(400))
    UrlHash = Column('UrlHash',Numeric(12,0),primary_key=True)
    Sno = Column('Sno',BigInteger)
    Source = Column('Source',String(10))

    
class NewsItemsDB(DeclarativeBase):
    __tablename__ = "Articles"
    UrlHash = Column('UrlHash',Numeric(12,0),primary_key=True)
    Title = Column('Title',String(400))
    Epic = Column('Epic',String(6))
    Article = Column('Article', LONGTEXT)
    Link = Column('Link',String(400))
