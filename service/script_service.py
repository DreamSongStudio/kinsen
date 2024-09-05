import os
import re
import hashlib
import subprocess

from dto.script_node_option_dto import ScriptNodeOptionDTO
from config import DATA_SOURCE_CONFIG
from utils.custom_exception import InternalException


def get_md5(string):
    md5 = hashlib.md5()
    md5.update(string.encode('utf-8'))
    return md5.hexdigest()


def execute_cmd(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # 获取命令的返回值
    stdout = result.stdout  # 命令执行成功的输出
    stderr = result.stderr  # 命令执行失败的错误信息
    return_code = result.returncode
    if stderr:
        raise Exception(stderr)

    return stdout.split('\n')[:-1]


def construct_node_cmd(node: dict, script_info: dict):
    cmd = f'python {script_info["path"]}'
    if node.get('input_key', '') != '':
        cmd = f'{cmd} -i {node["input_key"]}'
    if node.get('input_source', '') != '':
        cmd = f'{cmd} -is {DATA_SOURCE_CONFIG[node["input_source"]]}'
    if node.get('output_key', '') != '':
        cmd = f'{cmd} -o {node["output_key"]}'
    if node.get('output_source', '') != '':
        cmd = f'{cmd} -os {DATA_SOURCE_CONFIG[node["output_source"]]}'
    if node.get('node_name', '') != '':
        cmd = f'{cmd} -nn {node["node_name"]}'
    if node.get('pre_node_name', '') != '':
        cmd = f'{cmd} -pn {node["pre_node_name"]}'
    if node.get('task_name', '') != '':
        cmd = f'{cmd} -tn {node["task_name"]}'
    return cmd


class ScriptService:
    def __init__(self, es):
        self.es = es

    def refresh_script_list(self, script_dir):
        """
        刷新当前所有脚本信息
        :param script_dir:
        :return:
        """
        # 遍历根目录及其所有子目录
        total_script = []
        for dir_path, dir_names, filenames in os.walk(script_dir):
            for filename in filenames:
                if filename == '__init__.py':
                    continue
                file_path = os.path.join(dir_path, filename)
                # 读取文件内容
                with open(file_path, 'r', encoding='utf-8') as file:
                    text_line = file.readline()
                    cur_script = {}
                    while text_line:
                        if '_SCRIPT_NAME' in text_line:
                            cur_script['name'] = re.findall(r'_SCRIPT_NAME = ["|\'](.*?)["|\']', text_line)[0]
                            cur_script['path'] = file_path
                            cur_script['filename'] = filename
                            total_script.append(cur_script)
                        if '_SCRIPT_DESCRIPTION' in text_line:
                            cur_script['description'] = re.findall(r'_SCRIPT_DESCRIPTION = ["|\'](.*?)["|\']', text_line)[0]
                        if 'def ' in text_line:
                            break
                        text_line = file.readline()
        actions = []
        total_script_name = []
        for i in total_script:
            total_script_name.append(i['name'])
            _id = get_md5(i['name'])
            i['id'] = _id
            actions.append({
                "_id": _id,
                "_index": "test_dag",
                "_op_type": "index",
                "_source": i
            })

        # 脚本名重复检测
        if len(total_script_name) != len(set(total_script_name)):
            raise InternalException("有重复的脚本名称，刷新失败")

        self.es.delete_by_query("test_dag", {"query":{"bool":{"must":[]}}})
        # 持久化存储
        self.es.save(actions)

        return total_script

    def script_add_link(self, node_list: list[ScriptNodeOptionDTO]):
        """
        创建脚本链路
        :return:
        """
        # todo 重名任务覆盖
        # todo 一个任务内，不可有多个node_name相同
        # todo 暂不支持多个根节点
        actions = []
        for i in node_list:
            data = i.dict()
            _id = get_md5(f'{data["task_name"]}-{data["node_name"]}')
            task_id = get_md5(data["task_name"])
            data['id'] = _id
            data['task_id'] = task_id
            actions.append({
                "_id": _id,
                "_index": "test_dag_link",
                "_op_type": "index",
                "_source": data
            })
            print(data)

        self.es.save(actions)

    def start_script_link(self, task_id):
        body = {
            "query": {
                "term": {
                    "task_id": task_id
                }
            },
            "sort": {
                "index":
                    {
                        "order": "asc"
                    }
            }
        }
        node_list = []
        script_ids = []
        for hits in self.es.search_total('test_dag_link', body):
            node_list.extend([hit['_source'] for hit in hits])
        script_ids = [i['script_id'] for i in node_list]

        body = {
            "query": {
                "terms": {
                    "id": script_ids
                }
            }
        }
        script_file = []
        script_file_map = {}
        for hits in self.es.search_total('test_dag', body):
            for hit in hits:
                script_file.append(hit['_source'])
                script_file_map[hit['_id']] = hit['_source']

        print(len(node_list))
        for node in node_list:
            cmd = construct_node_cmd(node, script_file_map[node['script_id']])
            msg = execute_cmd(cmd)
            print(msg)
        return node_list
