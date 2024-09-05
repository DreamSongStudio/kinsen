import os
import re

import yaml
import pathlib

default_yaml = r"conf/config.yaml"


def read_yaml_all(config=None):
    if config is None:
        yaml_path = pathlib.Path(pathlib.Path(__file__).parent).joinpath(default_yaml)
        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            return config
    return {}


# 数据源配置
DATA_SOURCE_CONFIG = {
    "dev": "http://10.0.0.104:9200"
}

