from dto.script_node_option_dto import ScriptNodeOptionDTO
from service.script_service import ScriptService
from service.elastic import es
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from utils.custom_exception import InternalException

SCRIPT_PATH = './script'

app = FastAPI()
scriptService = ScriptService(es)


@app.exception_handler(InternalException)
async def internal_exception_handler(request: Request, exc: InternalException):
    return JSONResponse(
        status_code=500,
        content={"message": f"{exc.name}"},
    )


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/script/refresh")
async def refresh_script_list():
    """
    获取当前最新的所有脚本信息
    :return:
    """
    return {"message": 'done', "data": scriptService.refresh_script_list(SCRIPT_PATH)}


@app.post("/script/addLink")
async def script_add_link(node_list: list[ScriptNodeOptionDTO]):
    """
    创建链路任务
    :param node_list:
    :return:
    """
    return {"message": 'done', "data": scriptService.script_add_link(node_list)}


@app.post("/task/start/{task_id}")
async def script_add_link(task_id: str):
    """
    开始执行任务
    :param task_id:
    :return:
    """
    return {"message": 'done', "data": scriptService.start_script_link(task_id)}

