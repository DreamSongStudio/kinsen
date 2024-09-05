import datetime
import traceback

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from config import *


class ElasticService(object):
    config = read_yaml_all()
    if config.__getitem__("elastic"):
        es = Elasticsearch(
            hosts=os.getenv("esHost", config.__getitem__("elastic").__getitem__("host")).split(","),
            timeout=int(os.getenv("esTimeout", config.__getitem__("elastic").__getitem__("timeout"))),
            http_auth=(os.getenv("elasticName", config.__getitem__("elastic").__getitem__("elasticName")),
                       os.getenv("elasticPassWord", config.__getitem__("elastic").__getitem__("elasticPassWord")))
        )
    else:
        es = Elasticsearch(hosts=['http://127.0.0.1:9201'], timeout=30000,)

    @classmethod
    def scrollData(cls, index, body={}, process=None, pageSize=1000):
        startTime = datetime.datetime.now()
        query = cls.es.search(index=index, body=body, scroll='50m', size=pageSize)
        total = query['hits']['total']  # es查询出的结果总量
        scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果
        results = query['hits']['hits']
        for i in range(0, int(total / pageSize) + 1):
            # 异步保存数据
            loopStart = datetime.datetime.now()
            if process is not None:
                process(results)
            query = cls.es.scroll(scroll_id=scroll_id, scroll='50m')
            results = query['hits']['hits']
            scroll_id = query['_scroll_id']
            print(
                "load page={} process={}/{} duration={} totalDuration={}".format(
                    i,
                    i *
                    pageSize,
                    total,
                    datetime.datetime.now() -
                    loopStart,
                    datetime.datetime.now() -
                    startTime))

    @classmethod
    def scrollData_yield(cls, index, body=None, pageSize=1000, scroll='5m', total=None):
        if body is None:
            body = {}
        startTime = datetime.datetime.now()
        query = cls.es.search(index=index, body=body, scroll=scroll, size=pageSize)
        if not isinstance(total, (int, float)):
            total = query['hits']['total']['value']  # es查询出的结果总量
        scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果
        results = query['hits']['hits']
        sid_list = [scroll_id]
        for i in range(0, total, pageSize):
            # 异步保存数据
            loopStart = datetime.datetime.now()

            if i >= total - pageSize:
                print("finish page={} process={}/{} duration={} totalDuration={}".format(
                    total // pageSize,
                    total,
                    total,
                    datetime.datetime.now() - loopStart,
                    datetime.datetime.now() - startTime))
                yield results[:pageSize - (i - (total - pageSize))]
                break
            else:
                yield results
            query = cls.es.scroll(scroll_id=scroll_id, scroll=scroll)
            results = query['hits']['hits']
            scroll_id = query['_scroll_id']
            sid_list.append(scroll_id)
            print("load page={} process={}/{} duration={} totalDuration={}".format(
                i // pageSize,
                i,
                total,
                datetime.datetime.now() -
                loopStart,
                datetime.datetime.now() -
                startTime))
        for sid_del in sid_list:
            try:
                cls.es.clear_scroll(scroll_id=sid_del)
            except BaseException:
                pass

    @classmethod
    def search_scroll(cls, scroll_id):
        return cls.es.scroll(scroll_id=scroll_id, scroll='50m')

    @classmethod
    def search_scroll_data(cls, index, body, size, scroll):
        return cls.es.search(index=index, body=body, scroll=scroll, size=size)

    @classmethod
    def search(cls, index, body={}):
        return cls.es.search(index=index, body=body)

    @classmethod
    def search_total(cls, index, body={}):
        start_time = datetime.datetime.now()
        query = cls.es.search(index=index, body=body, scroll='50m', size=10000)
        total = query['hits']['total']["value"]
        has = 0
        while len(query['hits']['hits']) > 0:
            scroll_id = query['_scroll_id']
            yield query['hits']['hits']
            has = has + len(query['hits']['hits'])
            query = cls.es.scroll(scroll_id=scroll_id, scroll='50m')
            print("Load index={} process={}/{} duration={}".format(index, has, total,
                                                                   datetime.datetime.now() - start_time))

    @classmethod
    def msearch(cls, body={}):
        return cls.es.msearch(body=body)

    @classmethod
    def get(cls, index, id, source_filed=None):
        if source_filed:
            return cls.es.get(index=index, id=id, _source=source_filed)
        return cls.es.get(index=index, id=id)

    @classmethod
    def delete_by_id(cls, index, id):
        return cls.es.delete(index=index, id=id)

    @classmethod
    def delete_by_query(cls, index, query):
        return cls.es.delete_by_query(index=index, body=query)

    @classmethod
    def get_index_mapping(cls, index):
        return cls.es.indices.get_mapping(index=index)

    @classmethod
    def update_query(cls, index, body):
        return cls.es.update_by_query(index=index, body=body)

    @classmethod
    def reindex(cls, body, params):
        return cls.es.reindex(body=body, params=params)

    @classmethod
    def update_by_id(cls, index, id, doc, params=None):
        try:
            result = cls.es.update(
                index=index, id=id, body={"doc": doc}, params=params, retry_on_conflict=4
            )
        except Exception as e:
            # log_info("update_by_id error，index：{},id：{}，doc:{}, exception：{}".format(index, id, str(doc), e))
            result = None
        cls.es.indices.refresh(index=index)
        return result

    @classmethod
    def update(cls, index, id, doc):
        result = helpers.bulk(cls.es, [{
            "_index": index,
            "_type": "_doc",
            "_id": id,
            "_op_type": "update",
            "doc": doc
        }])
        cls.es.indices.refresh(index=index)
        return result

    @classmethod
    def save(cls, data, stats_only=True, raise_on_error=True):
        index_name = ""
        result = None
        if data and len(data) > 0:
            index_name = data[0].get("_index", "")
        try:
            start_time = datetime.datetime.now()
            result = helpers.bulk(cls.es, data, stats_only=True, raise_on_error=raise_on_error, chunk_size=300)
            end_time = datetime.datetime.now()
            cls.es.indices.refresh(index=index_name)
            # log_info("保存数据耗时: {}秒，索引名：{}，数据大小：{}".format((end_time - start_time).seconds, index_name, result))
        except Exception as e:
            # log_error("保存数据日志记录异常：".format(e))
            traceback.print_exc()
        return result

    @classmethod
    def refresh(cls, index):
        return cls.es.indices.refresh(index=index)

    @classmethod
    def save_one(cls, index, id, source):
        helpers.bulk(cls.es, [{
            "_index": index,
            "_type": "_doc",
            "_id": id,
            "_source": source
        }])
        cls.es.indices.refresh(index=index)

    @classmethod
    def create_index(cls, index, body={}):
        cls.es.indices.create(index, body)

    @classmethod
    def get_indices_create_index(cls, index, user_index, number_of_shards=1, number_of_replicas=0, tag="node-2"):
        """
        获取 索引结构, 创建索引
        :param index: 基础索引
        :type index:
        :param user_index: 用户索引
        :type user_index:
        :return:
        :rtype:
        """
        # 判断索引是否存在
        if not cls.isExists(index=user_index):
            mapping = cls.es.indices.get(index)
            del mapping[index]["settings"]["index"]["provided_name"]
            del mapping[index]["settings"]["index"]["creation_date"]
            del mapping[index]["settings"]["index"]["uuid"]
            del mapping[index]["settings"]["index"]["version"]
            # mapping[index]["settings"]["index"]["routing"]["allocation"]["include"]["tag"] = tag
            mapping[index]["settings"]["index"]["number_of_shards"] = number_of_shards
            mapping[index]["settings"]["index"]["refresh_interval"] = "1s"
            mapping[index]["settings"]["index"]["number_of_replicas"] = number_of_replicas
            cls.create_index(index=user_index, body=mapping[index])
        return {"index": user_index, "message": "创建成功"}

    @classmethod
    def isExists(cls, index):
        if cls.es.indices.exists(index):
            return True
        else:
            return False

    @classmethod
    def count(cls, index, body):
        return cls.es.count(index=index, body=body)

    @classmethod
    def search_with_limit(cls, index, body, size, scroll="5m", maximum=None):
        query = cls.es.search(index=index, body=body, scroll=scroll, size=size, request_timeout=100)
        current_size = 0
        while len(query['hits']['hits']) > 0:
            offset = None
            if maximum:
                offset = maximum - current_size
            scroll_id = query['_scroll_id']
            yield query['hits']['hits'][:offset]
            current_size += len(query['hits']['hits'])
            if maximum and current_size >= maximum:
                break
            query = cls.es.scroll(scroll_id=scroll_id, scroll=scroll, request_timeout=100)
        scroll_id = query['_scroll_id']
        cls.es.clear_scroll(scroll_id=scroll_id)


es = ElasticService()

