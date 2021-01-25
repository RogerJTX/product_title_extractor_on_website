"""
descrption: 官网产品抽取请求第一页, 清洗程序, 提取干净的title, 删除无关的字符和产品
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
            product_clean = i['product_clean']
            content = i['html']
            product_title_list_clean = []
            print(str(num), url)

            product_clean_new = []
            delete_word_list = ['Home', 'Nav', '联系', '动态', '我们', '企业', '竞争', '优势', '概况', '物业', '管理'
                                , '更多', '简介', '支持', '服务', '留言', '在线', '招聘', '园区', '公示', '栏目', '公司', '加盟', '招商'
                                , '了解', '198.00', '查看', '商城', 'PRODUCT', 'product', '购买', '荣誉', '新闻', '关于']

            for i2 in product_clean:
                flag_continue = 0
                for each_need_delete in delete_word_list:
                    if each_need_delete in i2:
                        flag_continue = 1
                        break
                if flag_continue == 0:
                    if i2 not in product_clean_new:
                        product_clean_new.append(i2)

            self.mongo_coll.update_one({'_id': _id}, {'$set':{'product_clean':product_clean_new}})
            print('succeed')

        self.logger.info("Finish Run")



if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1)