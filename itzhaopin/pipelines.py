# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi
import sys
import re
import os

if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')
class ItzhaopinPipeline(object):
    def __init__(self):
        self.dbpool = adbapi.ConnectionPool("MySQLdb",
                                            host="localhost",
                                            db="meipin",
                                            user="root",
                                            passwd="123",
                                            cursorclass=MySQLdb.cursors.DictCursor,
                                            charset="utf8",
                                            use_unicode=True
        )
    def process_item(self, item, spider):
        print spider.name
        query = self.dbpool.runInteraction(self.do_insert, item)
        return item
    def do_insert(self,db,item):
        #id = self.check_exist(db,item)
        for i in xrange(0, len(item['name'])):
            #print i 
            sql = "insert ignore into itzhao(`name`,`catalog`,`workLocation`,`recuritNumber`,`publishTime`) values ('%s','%s','%s','%s','%s')" %(item['name'][i],item['catalog'][i],item['workLocation'][i],item['recuritNumber'][i],item['publishTime'][i])
            #print sql
            db.execute(sql)
    def check_exist(self,db,item):
        db.execute("select `id` from meipin where `name` = '%s'" % item['name'])
        return db.fetchone()
    def handle_error(self,e):   
        sys.stderr.write(str(e))


