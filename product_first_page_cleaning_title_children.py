"""
descrption: 官网产品抽取请求第一页(清洗程序)
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
        self.mongo_client.admin.authenticate("....", "....")
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
            product = i['product']
            content = i['html']
            product_title_list = []
            print(str(num), url)

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
            self.mongo_coll.update_one({'_id':_id}, {'$set': {'product_children': product_title_list}})
            print('succeed')
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
        return detail_url_linshi, detail_title_linshi



if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1)
