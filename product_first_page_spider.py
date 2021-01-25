"""
descrption: 官网产品抽取请求第一页(只采集产品名字)
url：
author: jtx
date: 2021_01_02
"""
import sys,os
sys.path.append('/home/liangzhi/xjt/')

import re
from lxml import etree
from bs4 import BeautifulSoup
import logging
import pymongo
import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG2, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
import chardet



class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'yyf_db'
        self.proj = proj
        # self.host = "www.ofweek.com"  # 网站域名
        # self.host2 = "solar.ofweek.com"
        self.host_name = "企业官网"  # 网站中文名
        # self.api_url = "https://www.ofweek.com/CATList-8100-CHANGYIEXINWE-"  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.mongo_client.admin.authenticate("data_factory", "data_factory_sjzn01")
        self.save_coll_name = "res_kb_product"  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]
        self.mongo_coll = self.mongo_db[self.save_coll_name]
        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        configure_logging("/home/liangzhi/xjt/etl/data_gather/product_crawler/PD.log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
        }
        self.headers2 = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
        }
        # 链接mongodb

    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)



    def save_record(self, record, coll_name, pk):
        my_coll = self.mongo_db[coll_name]
        tmp = []
        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # print tmp
        show = "  ".join(tmp)
        # print show
        r_in_db = my_coll.find_one(pk)  # 唯一标识字段来去重
        if not r_in_db:
            my_coll.insert(record)
            self.logger.info("成功插入(%s)  %s" % (record['company_name'], show))
        else:
            self.logger.info("重复的数据(%s)  %s" % (record['company_name'], show))  # 重复数据打印到日志

    def run(self, start_page=1, max_page=-1):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================
        with open('/home/liangzhi/xjt/etl/data_gather/product_crawler/company_product_website.txt', 'r', encoding='utf-8') as f:
            for line in f.readlines():
                try:
                    record = {}
                    product_title_list = []
                    name = line.split('\t')[0]
                    url = line.split('\t')[1].strip()
                    # print(url)
                    resp = self.downloader.crawl_data(url, None, self.headers, "get")
                    if resp:
                        # 编码判断
                        bianma = self.judge_charset(url)
                        resp.encoding = bianma
                        content = resp.text
                        html =content

                        etree_html = etree.HTML(str(content), etree.HTMLParser())
                        info = etree_html.xpath('//*')  # //代表获取子孙节点，*代表获取所有
                        for i in info:
                            i_element_text_list = i.xpath('text()')
                            if i_element_text_list:
                                i_clean = (i_element_text_list[0]).replace('\n', '').replace('\r', '').strip()
                                if 8 > len(i_clean) > 2:
                                    title_dict = {}

                                    if ('产品' in i_clean):
                                        # print(i_clean)
                                        detail_url_linshi, detail_title_linshi = self.find_detial_page_href(i)
                                        # print(detail_title_linshi)
                                        title_dict['name'] = i_clean
                                        title_dict['product_name'] = detail_title_linshi
                                        product_title_list.append(title_dict)
                    else:
                        continue

                    record['company_name'] = name
                    record['url'] = url
                    record['html'] = html
                    record['source'] = '企业官网_产品'
                    record['crawl_time'] = datetime.datetime.now()
                    record['product'] = product_title_list

                    if record:
                        self.save_record(record, self.save_coll_name, {'company_name':name})

                except Exception as E:
                    self.logger.info("E: %s" % E)



        self.logger.info("Finish Run")

    def find_detial_page_href(self, i):
        detail_url_linshi = ''
        detail_title_linshi = ''

        # # print('i_tag:', i.tag)
        ancestor_list = i.xpath('./ancestor-or-self::*')
        ancestor_list_clean = []
        for ii in ancestor_list:
            tag_name = ii.tag
            ancestor_list_clean.append(tag_name + '/')


        i_children_list = i.xpath('./*')  # 获取i的子标签 list类型
        # # print('子标签:', i.xpath('./*'))
        if i_children_list:
            for num, i_children_list_each in enumerate(i_children_list):  # 获取i的子标签的兄弟标签
                # print('第' + str(num + 1) + '次url标签搜索, 遍历i的子级兄弟标签')
                detail_url_linshi = i_children_list_each.xpath('.//a/@href')
                detail_title_linshi = i_children_list_each.xpath('.//a/text()')
                if detail_url_linshi:
                    # print('找到detail_url_linshi')
                    # print('i_children_list_each.tag', i_children_list_each.tag)
                    # print(i_children_list_each.xpath('.//a/text()'))
                    # print(i_children_list_each.xpath('.//a/@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi, detail_title_linshi
                else:
                    # print('第' + str(num + 1) + '次url标签搜索, 遍历i的子级兄弟标签'+'没有找到链接', i_children_list_each.tag)
                    pass

        # i_father1_list = i.xpath('./..')  # 获取i的父标签
        # # print('父标签:', i_father1_list)
        # # # print(type(i_father1_list))
        # if i_father1_list:
        #     i_father1 = i_father1_list[0]
        #     i_father1_brother_list_self = i_father1.xpath('./self::*')  # 获取i的父标签
        #     if i_father1_brother_list_self:
        #         for num, i_father1_brother_list_each in enumerate(i_father1_brother_list_self):
        #             # print('第' + str(num + 1) + '次url标签搜索, 遍历i的父级自身标签')
        #             detail_url_linshi = i_father1_brother_list_each.xpath('.//@href')
        #             detail_title_linshi = i_father1_brother_list_each.xpath('.//text()')
        #             if detail_url_linshi:
        #                 # print('找到detail_url_linshi')
        #                 # print('i_father1_brother_list_each.tag', i_father1_brother_list_each.tag)
        #                 # print(i_father1_brother_list_each.xpath('.//text()'))
        #                 # print(i_father1_brother_list_each.xpath('.//@href'))
        #                 # print(ancestor_list_clean, '\n')
        #                 return detail_url_linshi, detail_title_linshi



        # print(ancestor_list_clean, '\n')
        return detail_url_linshi, detail_title_linshi

    # 自动编码判断
    def judge_charset(self, charset_judge_url):
        TestData = self.openlink(charset_judge_url)
        if TestData:
            bianma = chardet.detect(TestData)
            # print("编码-----------: {} \t detail_url: {} \t ".format(bianma, charset_judge_url))
            # print(bianma['encoding'])
            result_bianma = bianma['encoding']
            return result_bianma
        else:
            result_bianma = 'utf-8'
            return result_bianma

    # urllib timeout次数
    def openlink(self, charset_judge_url):
        maxTryNum = 5
        TestData_kong = ''
        for tries in range(maxTryNum):
            try:
                TestData = urllib.request.urlopen(charset_judge_url).read()
                return TestData
            except:
                if tries < (maxTryNum - 1):
                    continue
                else:
                    self.logger.info("Has tried %d times to access url %s, all failed!", maxTryNum, charset_judge_url)
                    return TestData_kong
        return None


if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1)