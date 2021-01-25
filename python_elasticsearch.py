from elasticsearch import Elasticsearch
from elasticsearch import helpers
# elasticsearch集群服务器的地址
ES = [

]

# 创建elasticsearch客户端
es = Elasticsearch(
    ES




    # 启动前嗅探es集群服务器
    #sniff_on_start=True,
    # es集群服务器结点连接异常时是否刷新es节点信息
    #sniff_on_connection_fail=True
    # 每60秒刷新节点信息
    #sniffer_timeout=60
)
print(es)

# ret = es.search(index='industry_center_company')
# print(ret)

# res = es.get(index='industry_center_company', doc_type='company', id='5f853bac2514970234ee9a46')
# print(res)

# class fetch_index():
#     def __init__(self,index,type):
#         self.index = index
#         self.type = type
#
#     def set_search_optional(self):
#         # 检索选项
#         es_search_options = {
#             "query": {
#                 "match_all": {}
#             }
#         }
#         return es_search_options
#
#     def get_search_result(self, scroll='1m',timeout="1m"):
#         es_result = helpers.scan(
#             client=es,
#             query=self.set_search_optional(),
#             scroll=scroll,
#             index=self.index,
#             doc_type=self.type,
#             timeout=timeout
#         )
#         return es_result
#
#     def get_result_list(self):
#         final_result = []
#         for item in self.get_search_result():
#             final_result.append(item)
#         print(len(final_result))
#         return final_result
#
#     def search(self):
#         final_result = self.get_result_list()
#         return final_result
# # 从es中获取数据
# def get_esdata(index, type_file):
#     try:
#         res = fetch_index(index=index,type=type_file)
#         return res.get_result_list()
#     except Exception as e:
#         # logger.error("连接不上elasticsearch,错误为：%s" % str(e))
#         print(e, '连不上')
#
#
# if __name__== '__main__':
#
#     res = get_esdata('industry_center_company', 'company')
#     print(res)


query = {"query":{"match":{"industrys.name":"人工智能"}}}
c = 0
for form_num in range(1, 304):
    ret = es.search(index='industry_center_company', doc_type='company', body=query, from_=(form_num-1)*50, size=50)
    # print(ret)
    print(type(ret))

    total = ret['hits']['total']
    print(total)

    hits = ret['hits']['hits']
    for num, i in enumerate(hits):

        # print(i)
        name = i['_source']['name']
        website = i['_source']['website']
        industrys = i['_source']['industrys']
        print(name)
        print(website)
        # print(industrys)
        flag = 0
        for i2 in industrys:
            if i2['name'] == '人工智能' and website:
                flag = 1
        if flag == 1:
            with open('company_product_website.txt', 'a+', encoding='utf-8') as f1:
                f1.write(name + '\t' + website + '\n')
            c += 1
            print('c:', str(c))
