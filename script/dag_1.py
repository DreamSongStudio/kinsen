import time
import random
import argparse
from elasticsearch import Elasticsearch, helpers

_SCRIPT_NAME = "init_data"
_SCRIPT_DESCRIPTION = "从目标获取数据，并传递给下一个节点"
data_input_index = ''   # 数据来源表
data_output_index = ''  # 数据保存表
output_source = input_source = ''  # 数据源
node_name = ''  # 节点名
task_name = ''  # 任务名


def get_data():
    return [i for i in range(random.randint(1, 100))]


def deal_data(data):
    for i in range(len(data)):
        data[i] = data[i] * random.random()

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
    time.sleep(1)


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
    parser.add_argument('-tn', '--task_name', type=str, default='', help='task_name')
    args = parser.parse_args()
    data_input_index = args.input_key
    data_output_index = args.output_key
    node_name = args.node_name
    task_name = args.task_name
    input_source = Elasticsearch(hosts=[args.input_source], timeout=30000) if args.input_source else None
    output_source = Elasticsearch(hosts=[args.output_source], timeout=30000) if args.output_source else None
    print(args)
    run()

