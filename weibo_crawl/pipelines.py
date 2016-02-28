# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from user_utils.mysqlmgr import MysqlMgr
from scrapy.log import logger
from weibo_crawl.items import WeiboItem
from weibo_crawl.items import WeiboSocialConnection
from weibo_crawl.items import WeiboUserInfoItem
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class WeiboCrawlPipeline(object):
    def __init__(self):
        self.__conn = MysqlMgr.get_default_mysql_conn(logger)

    def process_item(self, item, spider):
        if isinstance(item, WeiboSocialConnection):
            self.process_social_data(item)
        elif isinstance(item, WeiboUserInfoItem):
            self.process_user_info(item)
        elif isinstance(item, WeiboItem):
            self.process_weibo_context(item)
        return item

    def process_social_data(self, item):
        sql = '''
            INSERT INTO tab_social_network (user_id, weibo_count, follows_count, fans_count)
             VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE weibo_count = %s,
             follows_count = %s, fans_count = %s
        '''
        cursor = self.__conn.cursor()
        try:
            cursor.execute(sql, (item["user_id"], item["weibo"], item["follow"],
                                 item["fans"], item["weibo"], item["follow"], item["fans"],))
        except Exception, e:
            logger.error("social data insert error %s" % e)
        finally:
            cursor.close()

    def process_user_info(self, item):
        sql = '''
            INSERT INTO tab_base_info (user_id, user_name, sex, province, city, birthday, abstract)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
            user_name = %s, sex = %s, province = %s, city = %s, birthday = %s, abstract = %s
        '''
        cursor = self.__conn.cursor()
        try:
            province = item.get("province", None)
            city = item.get("city", None)
            birthday = item.get("birthday", None)
            abstract = item.get("abstract", None)
            sex = item.get("sex", "-1")
            cursor.execute("SET NAMES utf8mb4")
            cursor.execute(sql, (item["user_id"], item["user_name"], sex, province, city,
                birthday, abstract, item["user_name"], sex, province,
                city, birthday, abstract))
        except Exception, e:
            logger.error("user info data insert error %s" % e)
        finally:
            cursor.close()

    def process_weibo_context(self, item):
        sql = '''
            INSERT INTO tab_context_info (user_id, issue_time, get_time, context, like_count,
            relay_count, comment_count, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor = self.__conn.cursor()
        try:
            cursor.execute("SET NAMES utf8mb4")
            cursor.execute(sql, (item["user_id"], item["issue_time"], item["get_time"],
                             item["context"], item["like_count"], item["relay_count"],
                             item["comment_count"], item["device"]))
        except Exception, e:
            logger.error("weibo context data insert error %s" % e)
        finally:
            cursor.close()