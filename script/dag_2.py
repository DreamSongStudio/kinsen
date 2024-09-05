import time
import random
import argparse
from elasticsearch import Elasticsearch, helpers

_SCRIPT_NAME = "init_data"
_SCRIPT_DESCRIPTION = "用于计算数据"

data_input_index = ''   # 数据来源表
data_output_index = ''  # 数据保存表
output_source = input_source = ''  # 数据源
pre_node_name = ''  # 节点名
node_name = ''  # 节点名
task_name = ''  # 任务名


def get_data():
    body = {
        "query": {
            "term": {
                "_id": f'{task_name}-{pre_node_name}'
            }
        }
    }
    data = input_source.search(body, index=data_input_index)['hits']['hits'][0]
    return data.get('_source', {}).get('data', '')


def deal_data(data):
    for i in range(len(data)):
        data[i] = i

    return data


def save_data(data):
    action = {
        "_id": f"{task_name}-{node_name}",
        "_index": data_output_index,
        "_op_type": "index",
        "_source": {
            'data': data,
            'node_name': node_name,
            'task_name': task_name,
            'create_time': time.time()
        }
    }
    helpers.bulk(output_source, [action])


def run():
    data = get_data()
    deal_data(data)
    save_data(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='参数描述')
    parser.add_argument('-i', '--input_key', type=str, default='', help='data_input_key')
    parser.add_argument('-is', '--input_source', type=str, default='', help='data_input_key')
    parser.add_argument('-o', '--output_key', type=str, default='', help='data_output_key')
    parser.add_argument('-os', '--output_source', type=str, default='', help='data_output_key')
    parser.add_argument('-nn', '--node_name', type=str, default='', help='node_name')
    parser.add_argument('-pn', '--pre_node_name', type=str, default='', help='pre_node_name')
    parser.add_argument('-tn', '--task_name', type=str, default='', help='task_name')
    args = parser.parse_args()
    data_input_index = args.input_key
    data_output_index = args.output_key
    node_name = args.node_name
    pre_node_name = args.pre_node_name
    task_name = args.task_name
    input_source = Elasticsearch(hosts=[args.input_source], timeout=30000) if args.input_source else None
    output_source = Elasticsearch(hosts=[args.output_source], timeout=30000) if args.output_source else None
    print(args)
    run()

