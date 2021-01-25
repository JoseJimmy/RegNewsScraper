# RegNewsScraper
Scrapy spiders to collect regulatory news released by all public companies in UK which are constituent of FTSE All Share index (INDEXFTSE: ASX). The Stock information (ticker code,sector etc) is collected from lse.co.uk and used to fetch regulatory news from a RegNews portal(investgate). 

The stock info, news links and collected news articles  are all stored in seperate table in a local MySQL database(RNS). The figure below illustrates how
scrapers work in three stages for data collection and storage. See code files more details. 

![img](https://raw.githubusercontent.com/JoseJimmy/RegNewsScraper/master/investgate/doc/RegNewsScaper.png)
