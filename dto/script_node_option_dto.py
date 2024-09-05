from pydantic import BaseModel


class ScriptNodeOptionDTO(BaseModel):
    """
    链路节点
    """
    script_id: str
    index: int
    node_name: str
    input_key: str
    input_source: str
    output_key: str
    output_source: str
    pre_node_name: str
    task_name: str


