#!/usr/bin/env python
# coding:utf-8
"""
__title__ = ''
__author__ = 'asus'
__mtime__ = '2016/1/24'
__purpose__ = 
"""
import datetime
import json
import os
import re

import requests
from bs4 import BeautifulSoup
from scrapy.log import logger
from scrapy.spiders import Spider, Request

from weibo_crawl.items import WeiboItem
from weibo_crawl.items import WeiboSocialConnection
from weibo_crawl.items import WeiboUserInfoItem
from weibo_login import WeiboLogin

import sys
reload(sys)
sys.setdefaultencoding('utf8')


class UserInfoCrawl(Spider):
    name = "weibo_user_info"
    # allowed_domains = ["weibo.cn"]

    def __init__(self, name="adwking@sina.com", password="AOdiW880721", uid="1709818975", *args, **kwargs):
        super(UserInfoCrawl, self).__init__(*args, **kwargs)
        self.uid = uid
        self.start_urls = ["http://weibo.com"]
        self.allowed_domains = ["weibo.com", "weibo.cn"]
        self.url_base = "http://weibo.cn"
        self.first_flag_info = True  # 不爬取自己的微博
        self.first_flag_home = True  # 处理自己资料的时候和其他账户有所不一

        if os.path.exists("weibocookie.json"):
            with open("weibocookie.json", "r") as f:
                self.cookie = json.load(f)
        else:
            self.weibo = WeiboLogin()
            self.session = self.weibo.login(name, password)
            cookiejar = requests.utils.dict_from_cookiejar(self.session.cookies)

            # Set sina weibo cookie
            self.cookie = {'ALF': cookiejar['ALF'],
                           'sso_info': cookiejar['sso_info'],
                           'SUB': cookiejar['SUB'],
                           'SUBP': cookiejar['SUBP'],
                           'SUE': cookiejar['SUE'],
                           'SUHB': cookiejar['SUHB'],
                           'SUP': cookiejar['SUP'],
                           'SUS': cookiejar['SUS']}
            with open("weibocookie.json", "w") as f:
                json.dump(self.cookie, f)

    def start_requests(self):
        # Parse weibo homepage
        home_url = "http://weibo.cn/u/%s" % self.uid
        yield Request(url=home_url, cookies=self.cookie, callback=self._parse_homepage, errback=self.parse_error)

    def _parse_homepage(self, response):
        html = response.body
        soup = BeautifulSoup(html, "lxml")
        # 粉丝数
        fans_count, uid = self.get_fans_count(soup)

        # 微博数量
        weibo_count = self.get_weibo_count(soup)

        # 关注
        follow_count, follow_url = self.get_follows(soup)

        # 微博，只爬第一条
        weibo_item = self.parse_weibo_context(soup, uid)
        if weibo_item is not None:
            yield weibo_item

        weibo_social = WeiboSocialConnection()
        weibo_social["user_id"] = uid
        weibo_social["weibo"] = weibo_count
        weibo_social["fans"] = fans_count
        weibo_social["follow"] = follow_count
        if weibo_count > 10:
            yield weibo_social

        # 个人资料
        detail_url_ele = soup.find("a", text=u"资料")
        if detail_url_ele:
            detail_url = self.url_base + detail_url_ele["href"]
            yield Request(url=detail_url, cookies=self.cookie,
                          callback=self.parse_info, errback=self.parse_error,
                          priority=1)

        if follow_url:
            yield Request(url=follow_url, cookies=self.cookie, callback=self.parse_follow, errback=self.parse_error)

    def parse_error(self, response):
        logger.error("post:%s" % response.url)

    def parse_info(self, response):
        html = response.body
        soup = BeautifulSoup(html, "lxml")
        info_tip_ele = soup.find("div", text=u"基本信息")
        uid = self.get_uid_from_response(response)
        info = {}
        if info_tip_ele:
            info_ele = info_tip_ele.next_sibling
            if self.first_flag_info:
                self.first_flag_info = False
                # info_eles = info_ele.find_all("a")
                # for ele in info_eles:
                #     if ele.text in [u"昵称", u"性别", u"地区", u"生日", u"简介"]:
                #         info[ele.text.encode("utf-8")] = ele.next_sibling.encode("utf-8")
                #         print ele.text, ele.next_sibling
            else:
                info_eles = info_ele.strings
                user_info = WeiboUserInfoItem()
                user_info["user_id"] = uid
                for ele in info_eles:
                    el = ele.split(":")
                    if len(el) == 2 and el[0] in [u"昵称", u"性别", u"地区", u"生日", u"简介"]:
                        info[el[0]] = el[1]
                        info_item = el[1].encode("utf-8")
                        if el[0] == u"昵称":
                            user_info["user_name"] = info_item
                        elif el[0] == u"性别":
                            user_info["sex"] = info_item
                        elif el[0] == u"地区":
                            region = info_item.split(" ")
                            if len(region) == 1:
                                user_info["province"] = ""
                                user_info["city"] = region[0]
                            else:
                                user_info["province"] = region[0]
                                user_info["city"] = region[1]
                        elif el[0] == u"生日":
                            if len(info_item.split("-")) < 3:
                                user_info["birthday"] = "2050-" + info_item
                            else:
                                user_info["birthday"] = info_item
                            p = re.compile(r"^\d{4}-\d{2}-\d{2}$")
                            if not p.findall(user_info["birthday"]):
                                user_info["birthday"] = None
                        elif el[0] == u"简介":
                            user_info["abstract"] = info_item.encode("utf-8", "ignore").replace(" ", "").\
                        replace("\n", "").replace("\xc2\xa0", "").replace("\xF0\x9F\x91\x8A", "").\
                                replace("\xF0\x9F\x91\xBC", "").replace("\xF0\x9F\x8C\xB8\xF0\x9F", "")
                yield user_info

    def parse_follow(self, response):
        html = response.body
        soup = BeautifulSoup(html, "lxml")
        table_eles = soup.find_all("table")
        for ele in table_eles:
            follower_url = ele.find("a")["href"]
            yield Request(url=follower_url, cookies=self.cookie, callback=self._parse_homepage, errback=self.parse_error)

    def get_uid_from_response(self, response):
        if isinstance(response, str):
            url = response
        else:
            url = response.url
        pattern = re.compile(r'/(\d+)/?')
        res = re.findall(pattern, url)
        id = 0
        if res:
            id = int(res[0])
            # print "id:", id
        return id

    def parse_weibo_context(self, soup, uid):
        weibo_info = WeiboItem()
        if self.first_flag_home:
            self.first_flag_home = False
            return None
        else:
            contexts = soup.find_all("div", class_="c")
            for item in contexts:
                try:
                    context = item.find("span", class_="ctt")
                    if not context:
                        continue
                    weibo_text = context.text.encode("utf-8", "ignore").replace(" ", "").\
                        replace("\n", "").replace("\xc2\xa0", "").replace("\xF0\x9F\x91\x8A", "").\
                        replace("\xF0\x9F\x91\xBC", "").replace("\xF0\x9F\x8C\xB8\xF0\x9F", "")
                    parent_ele = context.parent.parent
                    like_ele = parent_ele.find(text=re.compile(u"^赞\[\d*\]$"))
                    relay_ele = parent_ele.find(text=re.compile(u"^转发\[\d*\]$"))
                    comment_ele = parent_ele.find(text=re.compile(u"^评论\[\d*\]$"))
                    issue_time_ele = parent_ele.find("span", class_="ct")
                    issue_time = issue_time_ele.text
                    issue_time = issue_time.encode("utf-8")

                    issue = issue_time.split("来自")
                    issue_datetime = ""
                    if len(issue) > 0:
                        if "分钟" in issue[0]:
                            min = filter(str.isdigit, issue[0])
                            t = datetime.datetime.now() - datetime.timedelta(minutes=int(min))
                            issue_datetime = t.strftime("%Y-%m-%d %H:%M:%S")
                        elif "今天" in issue[0]:
                            time = issue[0].replace("今天 ", "").replace("\xc2\xa0", "")
                            issue_datetime = datetime.datetime.now().strftime("%Y-%m-%d ") + time
                        else:
                            issue_datetime = issue[0].replace("月", "-").replace("日", "").replace("\xc2\xa0", "")
                            if issue[0].count("-") < 2:
                                issue_datetime =datetime.datetime.now().strftime("%Y-") + issue_datetime
                    issue_device = issue[1] if len(issue) > 1 else None

                    weibo_info["context"] = weibo_text
                    weibo_info["user_id"] = uid
                    weibo_info["issue_time"] = issue_datetime.strip()
                    weibo_info["get_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    weibo_info["like_count"] = filter(str.isdigit, like_ele.encode("utf-8"))
                    weibo_info["relay_count"] = filter(str.isdigit, relay_ele.encode("utf-8"))
                    weibo_info["comment_count"] = filter(str.isdigit, comment_ele.encode("utf-8"))
                    weibo_info["device"] = issue_device


                    # print issue_datetime, issue_device, weibo_text
                    # print like_ele.encode("utf-8"), relay_ele.encode("utf-8"), comment_ele.encode("utf-8")
                    return weibo_info
                    # 只爬去第一条微博
                except Exception, e:
                    logger.error(e)

    def get_weibo_count(self, soup):
        # weibo_count_ele = soup.select("[href$=profile]")
        weibo_count_ele = soup.find(text=re.compile(u"^微博\[\d*\]$"))
        weibo_count = 0
        if weibo_count_ele:
            weibo_count_str = weibo_count_ele
            pattern = re.compile(r'\[*?(\d+)\]')
            res = re.findall(pattern, weibo_count_str)
            if res:
                weibo_count = int(res[0])
                # print "weibo_count", weibo_count
        return weibo_count

    def get_follows(self, soup):
        follow_url_ele = soup.select("[href$=follow]")
        follow_count = 0
        follow_url = ""
        if follow_url_ele:
            follow_url = self.url_base + follow_url_ele[0]["href"]
            follow_count_str = follow_url_ele[0].text
            pattern = re.compile(r'\[*?(\d+)\]')
            res = re.findall(pattern, follow_count_str)
            if res:
                follow_count = int(res[0])
                # print "follow_count", follow_count

        return follow_count, follow_url

    def get_fans_count(self, soup):
        fans_url_ele = soup.select("[href$=fans]")
        fans_count = 0
        uid = 0
        if fans_url_ele:
            fans_url = fans_url_ele[0]["href"]
            uid = self.get_uid_from_response(fans_url)
            fans_count_str = fans_url_ele[0].text
            pattern = re.compile(r'\[*?(\d+)\]')
            res = re.findall(pattern, fans_count_str)
            if res:
                fans_count = int(res[0])
                # print "fans_count", fans_count
        return fans_count, uid
