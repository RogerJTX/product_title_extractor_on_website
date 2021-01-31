"""
descrption: 官网产品抽取请求第一页, 清洗程序, 提取干净的title
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
        config["db"] = '....'
        self.proj = proj
        # self.host = "www.ofweek.com"  # 网站域名
        # self.host2 = "solar.ofweek.com"
        self.host_name = "企业官网"  # 网站中文名
        # self.api_url = "https://www.ofweek.com/CATList-8100-CHANGYIEXINWE-"  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.mongo_client.admin.authenticate("...", "...")
        self.save_coll_name = "...."  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]
        self.mongo_coll = self.mongo_db[self.save_coll_name]
        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        configure_logging("PD_cleaning.log")  # 日志文件名
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

        for num, i in enumerate(self.mongo_coll.find()):
            _id = i['_id']
            url = i['url']
            product = i['product']
            content = i['html']
            product_title_list_clean = []
            print(str(num), url)
            for i2 in product:
                if type(i2) == dict:
                    product_name_linshi = ''
                    for i3_key, i3_value in i2.items():
                        if i3_key == 'name':
                            product_name_linshi = i3_value
                            print(product_name_linshi)
                        if type(i3_value) == list:
                            if len(i3_value) > 1:
                                for product_num, each_product_name in enumerate(i3_value):
                                    each_product_name_clean = each_product_name.replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').strip()
                                    if 2 < len(each_product_name_clean) < 15:
                                        if each_product_name_clean != product_name_linshi:
                                            product_title_list_clean.append(each_product_name_clean)
                                    else:
                                        print('title长度过短, 长度过长, pass')

                            else:
                                print('长度过短, 不是真正的产品列表, pass')
            self.mongo_coll.update_one({'_id': _id}, {'$set':{'product_clean':product_title_list_clean}})
            print('succeed')

        self.logger.info("Finish Run")



if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1)
