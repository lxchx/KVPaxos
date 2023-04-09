
import logging
import traceback
from typing import Any, Callable, List, Tuple
import asyncio
import grpc

from src.proto import KVService_pb2
from src.proto import KVService_pb2_grpc

logger = logging.getLogger(__name__)

def collect_all_ok_responses_sync(stub_method_with_requests, requirement_func, timeout: int = 60):
    loop = asyncio.get_event_loop()
    responses = loop.run_until_complete(collect_all_ok_responses(stub_method_with_requests, requirement_func, timeout, loop=loop))
    return responses

async def collect_all_ok_responses(stub_method_with_requests, requirement_func, timeout: int = 60, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()

    tasks = []

    # 创建异步任务
    for stub_method, request in stub_method_with_requests:
        task = process_request_with_timeout(
            stub_method, request, requirement_func, timeout)
        tasks.append(task)

    # 等待异步任务完成并返回响应结果
    done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    responses = [task.result() for task in done if task.result() is not None]

    return responses

async def process_request(stub_method, request, requirement_func):
    try:
        response_event = asyncio.Event()
        response = await stub_method(request)
        if requirement_func(response):
            response_event.set()
            return response
    except Exception as e:
        logger.info(f"Error when processing request:\n{traceback.format_exc()}")
    return None

async def process_request_with_timeout(stub_method, request, requirement_func, timeout):
    try:
        response = await asyncio.wait_for(process_request(stub_method, request, requirement_func), timeout)
    except asyncio.TimeoutError:
        response = None
    return response

async def collect_quorum_responses(stub_method_with_requests, quorum, resp_callback, timeout: int = 60, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()

    responses = []
    tasks = []
    for stub_method, request in stub_method_with_requests:
        task = loop.create_task(process_request_with_timeout(stub_method, request, lambda x: True, timeout))
        tasks.append(task)

    for coro in asyncio.as_completed(tasks):
        response = await coro
        if response is not None:
            resp_callback(response)
            responses.append(response)
        if len(responses) == quorum:
            break

    # 取消尚未完成的任务
    for task in tasks:
        if not task.done():
            task.cancel()

    return responses[:quorum]

def collect_quorum_responses_sync(stub_method_with_requests, quorum, resp_callback, timeout: int = 60):
    loop = asyncio.get_event_loop()
    responses = loop.run_until_complete(collect_quorum_responses(stub_method_with_requests, quorum, resp_callback, timeout, loop=loop))
    return responses
