# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WeiboUserInfoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    user_id = scrapy.Field()
    user_name = scrapy.Field()
    sex = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    birthday = scrapy.Field()
    abstract = scrapy.Field()


class WeiboSocialConnection(scrapy.Item):
    user_id = scrapy.Field()
    fans = scrapy.Field()
    follow = scrapy.Field()
    weibo = scrapy.Field()


class WeiboItem(scrapy.Item):
    context_id = scrapy.Field()
    user_id = scrapy.Field()
    sex = scrapy.Field()
    issue_time = scrapy.Field()
    get_time = scrapy.Field()
    context = scrapy.Field()
    like_count = scrapy.Field()
    relay_count = scrapy.Field()
    comment_count = scrapy.Field()
    device = scrapy.Field()

