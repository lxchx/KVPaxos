import argparse
import asyncio
from concurrent import futures
import math
import multiprocessing
import os
import random
import time
from typing import List, Union
import uuid
import logging
from collections import deque
from queue import PriorityQueue
from google.protobuf.json_format import MessageToJson, Parse

import grpc
from src.proto import KVService_pb2
from src.proto import KVService_pb2_grpc
from src.client.service_utils import collect_all_ok_responses_sync, collect_quorum_responses_sync
from src.client.bal_num_util import Snowflake
from src.utils.log_manager import LogManager

logger = logging.getLogger(__name__)

def majority(total: int) -> int:
    return int(total / 2 + 1)

class KVClient:
    __members_info = KVService_pb2.MembersInfo()

    def __init__(self, members_info: KVService_pb2.MembersInfo, ):
        self.__members_info.CopyFrom(members_info)

    def __client_feature_code():
        mac = uuid.getnode()  # 获取 MAC 地址
        pid = os.getpid()  # 获取进程 ID
        # 将 MAC 地址和 PID 转化为字符串并进行拼接
        code = f"KVCLIENT_MAC_{str(mac)}_PID_{str(pid)}"
        return code

    # 主动式的锁，需要主动查询，利用KV存储本身实现，带租期（单位是分钟）返回值代表上锁有没有成功
    def _try_lock(self, lock_object: str, lease_period: int = 30, lock_prifix: str = 'PRIVATE_LOCK') -> bool:
        lock_key = f'{lock_prifix}.{lock_object}'
        _, lock_value_json_str = self._GetInternal(
            KVService_pb2.Key.Type.Meta, lock_key, 5)
        if lock_value_json_str:
            lock_value = KVService_pb2.LockValue()
            Parse(lock_value_json_str, lock_value)
            logger.info(f'got lock value: {lock_value_json_str}')
            if lock_value.owner != KVClient.__client_feature_code() and time.time() < lock_value.expiration_timestamp:
                # 如果有锁，且锁的主人不是自己且锁没有过期，上锁失败
                return False
        # 否则继续往下
        lock_value = KVService_pb2.LockValue(owner=KVClient.__client_feature_code(
        ), expiration_timestamp=int(time.time()) + lease_period * 60)
        # 如果这里返回False说明锁被另一个Client抢到了
        return self._SetInternal(KVService_pb2.Key.Type.Meta, lock_key, MessageToJson(lock_value), 5)
    
    # TODO 不成功时应该返回锁的剩余期限
    def TryLock(self, obj: str, lease_period: int = 30) -> bool:
        return self._try_lock(obj, lease_period, 'USER_LOCK')

    def Set(self, key: str, value: str, timeout: int = 60):
        random.seed(time.time())
        while not self._SetInternal(KVService_pb2.Key.Type.User, key, value, timeout):
            # 反复执行直到写入成功
            # TODO 指数退避，重试次数过多报错
            logger.info('写入失败，是修复或version已被commit，重试')
            time.sleep(random.randint(1, 200) / 1000.0)  # 随机等待1~200毫秒，尽量避免活锁
        logger.info(f'设置({key} -> {value})成功')

    # 返回有没有设置成功，因为paxos写入跑下来一轮可能跑的是修复，也可能选到的version已经被commit了
    def _SetInternal(self, key_type: KVService_pb2.Key.Type, key_content: str, value: str, timeout: int = 60) -> bool:
        # 用step配合循环，得到一个状态机，方便重试
        # 需要跨step记忆的变量都要在这初始化
        step = 0
        curr_version = -1
        set_addrs = set()
        quorum = 0
        bal_num_generator = Snowflake()
        ballot_num = 0
        value_should_set = str()
        phase1_no_quorum_ok_count = 0
        commit_no_quorum_ok_count = 0
        while True:
            if step == 0:
                # 每次重来的时候刷新的变量放这
                set_addrs.clear()
                phase1_no_quorum_ok_count = 0
                commit_no_quorum_ok_count = 0
                # next step
                step = 1
                continue
            if step == 1:
                # 获取key对应的最高Version
                curr_version, _ = self._GetInternal(key_type, key_content, 10)
                if curr_version:
                    curr_version += 1
                else:
                    curr_version = 1
                logger.info(f'设定本次Key Version: {curr_version}')
                # next step
                step = 2
                continue
            elif step == 2:
                # Paxos phase-1
                value_should_set = value
                ballot_num = bal_num_generator.generate_id()
                set_addrs = set(
                    [member.addr for member in self.__members_info.members])
                set_addrs = set_addrs | set(
                    [member.addr for member in self.__members_info.addingMembers])
                quorum = majority(len(set_addrs))
                # 构造request
                firstReq = KVService_pb2.FirstKVServiceReq(
                    membersInfoVersion=self.__members_info.version)
                key_with_column = KVService_pb2.Key(
                    content=key_content, type=key_type)
                request = KVService_pb2.PaxosPhase1Req(
                    firstReq=firstReq, key=key_with_column, version=curr_version, ballotNum=ballot_num)

                channels = [grpc.aio.insecure_channel(
                    addr) for addr in set_addrs]
                stubs = [KVService_pb2_grpc.KVServiceStub(
                    channel) for channel in channels]
                stubmethod_with_requests = [
                    (stub.PaxosPhase1, request) for stub in stubs]

                is_member_info_expired = False
                need_higher_value_version = False
                existed_vbal_and_value = []
                # 需要超过quorum个resp.lastBal < ballot_num

                def resp_ok(resp):
                    nonlocal need_higher_value_version, is_member_info_expired
                    if resp.firstKVServiceResp and resp.firstKVServiceResp.membersInfo.version > self.__members_info.version:
                        # 如果返回的成员信息版本号大于当前版本号，则更新当前的成员信息
                        self.__members_info.CopyFrom(
                            resp.firstKVServiceResp.membersInfo)
                        is_member_info_expired = True
                        return False
                    if resp.err and resp.err != KVService_pb2.PaxosPhase1Resp.Err.OK:
                        # KeyVersionHaveCommited
                        need_higher_value_version = True
                        return False
                    if resp.vBal != 0:
                        # 之前存在v，要记录一下，但是此时并不能判断这个Acceptor是不是accept了，所以不改变返回值
                        existed_vbal_and_value.append((resp.vBal, resp.value))
                    if resp.lastBal > ballot_num:
                        return False
                    return True
                ok_resps = collect_all_ok_responses_sync(
                    stubmethod_with_requests, resp_ok, 5)

                if need_higher_value_version:
                    logger.info(
                        f'Paxos phase-1 need_higher_value_version，这个version已经commit了，失败')
                    return False

                if is_member_info_expired:
                    logger.info(
                        f'Paxos phase-1 is_member_info_expired，重新开始Set')
                    step = 0  # 整个phase-1重来
                    continue

                if len(ok_resps) < quorum:
                    phase1_no_quorum_ok_count += 1
                    if phase1_no_quorum_ok_count > 5:
                        logger.info(f'超过5次没在phase-1凑够quorum')
                        raise Exception(
                            f'too many times "no quorum ok in phase-1"')
                    step = 2
                    continue
                if len(existed_vbal_and_value) > 0:
                    # 之前有v，变成修复模式，取vbal最大的v
                    value_should_set = max(existed_vbal_and_value)[1]

                # next step
                step = 3
                continue
            elif step == 3:
                # Paxos phase-2
                # 构造request
                firstReq = KVService_pb2.FirstKVServiceReq(
                    membersInfoVersion=self.__members_info.version)
                key_with_column = KVService_pb2.Key(
                    content=key_content, type=key_type)
                # 这里value设置成value_should_set
                request = KVService_pb2.PaxosPhase2Req(
                    firstReq=firstReq, key=key_with_column, version=curr_version, value=value_should_set, ballotNum=ballot_num)

                channels = [grpc.aio.insecure_channel(
                    addr) for addr in set_addrs]
                stubs = [KVService_pb2_grpc.KVServiceStub(
                    channel) for channel in channels]
                stubmethod_with_requests = [
                    (stub.PaxosPhase2, request) for stub in stubs]

                is_member_info_expired = False
                need_higher_value_version = False
                existed_vbal_and_value = []

                def resp_ok(resp):
                    nonlocal need_higher_value_version, is_member_info_expired
                    if resp.firstKVServiceResp and resp.firstKVServiceResp.membersInfo.version > self.__members_info.version:
                        # 如果返回的成员信息版本号大于当前版本号，则更新当前的成员信息
                        self.__members_info.CopyFrom(
                            resp.firstKVServiceResp.membersInfo)
                        is_member_info_expired = True  # 这么写是帮助python理解is_member_info_expired是外部变量
                        return False
                    if resp.err and resp.err != KVService_pb2.PaxosPhase2Resp.Err.OK:
                        if resp.err == KVService_pb2.PaxosPhase2Resp.Err.KeyVersionHaveCommited:
                            need_higher_value_version = True
                        elif resp.err == KVService_pb2.PaxosPhase2Resp.Err.BallotNumTooLow:
                            pass  # 什么也不用做
                        return False
                    return True
                ok_resps = collect_all_ok_responses_sync(
                    stubmethod_with_requests, resp_ok, 5)

                if need_higher_value_version:
                    logger.info(
                        f'Paxos phase-2 need_higher_value_version，这个version已经commit了，失败')
                    return False

                if is_member_info_expired:
                    logger.info(
                        f'Paxos phase-2 is_member_info_expired，重新开始Set')
                    step = 0
                    continue

                if len(ok_resps) < quorum:
                    # 被抢占了
                    # TODO 实现指数退避
                    # 随机等待1~200毫秒，尽量避免活锁
                    logger.info(
                        f'phase-2 没成功让至少quorum个Acceptor接受，说明被抢占了，睡一会然后重试')
                    time.sleep(random.randint(1, 200) / 1000.0)
                    step = 2  # 从phase-1重新开始，会自动换更高的bal_num
                    continue
                # next step
                step = 4
                continue
            elif step == 4:
                # commit
                firstReq = KVService_pb2.FirstKVServiceReq(
                    membersInfoVersion=self.__members_info.version)
                key_with_column = KVService_pb2.Key(
                    content=key_content, type=key_type)
                # 这里value设置成value_should_set
                # TODO 理论上之前phase-2 ok的acceptor就不需要传value了，所以后面有空再实现一下，标记特别acceptor需要重构一下service_utils
                request = KVService_pb2.CommitReq(
                    firstReq=firstReq, key=key_with_column, version=curr_version, value=value_should_set)

                channels = [grpc.aio.insecure_channel(
                    addr) for addr in set_addrs]
                stubs = [KVService_pb2_grpc.KVServiceStub(
                    channel) for channel in channels]
                stubmethod_with_requests = [
                    (stub.CommitKey, request) for stub in stubs]

                is_member_info_expired = False
                need_higher_value_version = False
                existed_vbal_and_value = []

                def resp_ok(resp):
                    nonlocal need_higher_value_version, is_member_info_expired
                    if resp.firstKVServiceResp and resp.firstKVServiceResp.membersInfo.version > self.__members_info.version:
                        # 如果返回的成员信息版本号大于当前版本号，则更新当前的成员信息
                        self.__members_info.CopyFrom(
                            resp.firstKVServiceResp.membersInfo)
                        is_member_info_expired = True
                        return False
                    if resp.err and resp.err != KVService_pb2.CommitResp.Err.OK:
                        # KeyVersionHaveCommited
                        if resp.err == KVService_pb2.CommitResp.Err.AlreadyCommitHigherVersion:
                            need_higher_value_version = True
                        elif resp.err == KVService_pb2.CommitResp.Err.NotFoundVersionValue:
                            # 暂时不会发生
                            # TODO 理论上之前phase-2 ok的acceptor就不需要传value了，这里是可能发生的错误
                            pass
                        return False
                    return True
                ok_resps = collect_all_ok_responses_sync(
                    stubmethod_with_requests, resp_ok, 5)

                if need_higher_value_version:
                    logger.info(
                        f'Commit need_higher_value_version，这个version已经commit了，失败')
                    return False

                if is_member_info_expired:
                    logger.info(f'Commit is_member_info_expired，重新开始Set')
                    step = 0
                    continue

                if len(ok_resps) < quorum:
                    commit_no_quorum_ok_count += 1
                    if commit_no_quorum_ok_count > 5:
                        raise Exception(
                            f'too many times "no quorum ok in commit"')
                    step = 4
                    continue
                # finish
                step = -1
                continue
            elif step == -1:
                break
        if value_should_set == value:
            logger.info(f'值设置成功')
        else:
            logger.info(f'本轮Paxso是修复，修复的值为{value_should_set}')
        return value_should_set == value

    def Get(self, key: str, timeout: int = 60) -> str:
        _, value = self._GetInternal(KVService_pb2.Key.Type.User, key, timeout)
        return value

    def _GetInternal(self, key_type: KVService_pb2.Key.Type, key_content: str, timeout: int = 60) -> Union[int, str]:
        read_addrs = {member.addr for member in self.__members_info.members} | \
                    {member.addr for member in self.__members_info.deletingMembers}

        quorum = majority(len(read_addrs))

        # 构造request
        firstReq = KVService_pb2.FirstKVServiceReq(
            membersInfoVersion=self.__members_info.version)
        key_with_column = KVService_pb2.Key(content=key_content, type=key_type)
        request = KVService_pb2.GetValueReq(
            key=key_with_column, firstReq=firstReq)

        channels = [grpc.aio.insecure_channel(addr) for addr in read_addrs]
        stubs = [KVService_pb2_grpc.KVServiceStub(
            channel) for channel in channels]
        stubmethod_with_requests = [(stub.GetValue, request) for stub in stubs]

        is_member_info_expired = False
        err_count = multiprocessing.Value('i', 0)
        errs = []
        values = []

        def resp_callback(resp):
            nonlocal is_member_info_expired
            if resp.firstKVServiceResp and resp.firstKVServiceResp.membersInfo.version > self.__members_info.version:
                # 如果返回的成员信息版本号大于当前版本号，则更新当前的成员信息
                self.__members_info.CopyFrom(
                    resp.firstKVServiceResp.membersInfo)
                is_member_info_expired = True
                return
            if resp.err and resp.err != KVService_pb2.GetValueResp.Err.OK:
                with err_count.get_lock():
                    err_count.value += 1
                errs.append(resp.err)
                return
            if resp.value is not None:
                values.append((resp.commitedVersion, resp.value))
                return

        retry_count = -1

        while True:
            retry_count += 1
            err_count.value = 0
            is_member_info_expired = False
            errs = []
            values.clear()
            collect_quorum_responses_sync(
                stubmethod_with_requests, quorum, resp_callback, timeout)
            if is_member_info_expired:
                continue
            if retry_count < 5 and len(values) + err_count.value < quorum:
                continue
            break

        if len(values) + err_count.value >= quorum:
            if len(values) > 0:
                return max(values)
            else:
                return (None, None)
        else:
            raise Exception(
                f'Only {len(values) + err_count.value} services responded, which is less than the quorum required. Execution failed!')

    # 如果check_func返回True说明不需要继续执行了，直接返回失败
    def _copy_datas(self, key_type: KVService_pb2.Key.Type, source_addrs, target_addrs, source_expect_count: int, target_expect_count: int, check_func=lambda: False) -> bool:
        assert len(source_addrs) >= source_expect_count
        assert len(target_expect_count) >= target_expect_count
        class DataIter:
            _column = KVService_pb2.Key.Type.Meta
            _cache_datas = deque()
            _last_key = ""
            _addr = str()

            def __init__(self, addr: str, column: KVService_pb2.Key.Type):
                self._addr = addr
                self._column = column
                self._last_key = ""
                self._cache_datas = deque()
                channel = grpc.aio.insecure_channel(addr)
                self._stub = KVService_pb2_grpc.KVServiceStub(channel)

            def _pull_datas(self):
                request = KVService_pb2.GetItemsReq(prev_last_key=KVService_pb2.Key(
                    type=self._column, content=self._last_key), expect_count=100)
                loop = asyncio.get_event_loop()
                try:
                    response = loop.run_until_complete(asyncio.wait_for(self._stub.GetItems(request), timeout=3))
                except futures.TimeoutError as e:
                    logger.info(f"Timeout error occurred")
                    raise e
                self._cache_datas.extend(response.kvs)

            def awake(self, last_key: str = "") -> bool:
                self._last_key = last_key
                try:
                    self._pull_datas()
                except Exception as e:
                    return False
                return True

            # 如果抛出异常说明已经到end了
            def get(self):
                item = self._cache_datas[0]
                self._last_key = item.key.content
                return item.key.content, item.value.commitedVersion, item.value.value

            # 如果抛出异常说明拉取时出错了
            def next(self):
                item = self._cache_datas.popleft()
                if len(self._cache_datas) == 0:
                    self._pull_datas()
                return item.key.content, item.value.commitedVersion, item.value.value

            def __lt__(self, other):
                item_a, item_b = None, None
                a_err = b_err = False

                try:
                    item_a = self.get()
                except:
                    a_err = True

                try:
                    item_b = other.get()
                except:
                    b_err = True

                if a_err and b_err:
                    return False
                if a_err and not b_err:
                    return False
                if not a_err and b_err:
                    return True
                # Compare the two items based on their content and version value
                return (item_a[0], -item_a[1]) < (item_b[0], -item_b[1])
        
        iters = [DataIter(addr, key_type) for addr in source_addrs]
        usable_iters = PriorityQueue()
        usable_iters_len = 0  # PriorityQueue居然不维护长度...
        offline_iters = deque() # 网络请求出错的iter、如果usable_iters内iter总数低于source_expect_count要从里面唤醒
        for iter in iters:
            if iter.awake():
                usable_iters.put(iter)
                usable_iters_len += 1
            else:
                offline_iters.append(iter)

        last_cur_key = ""  # awake offline key用
        cur_key = ""
        cur_max_version = -1
        cur_value = ""
        
        while True:
            # 先检查usable_iters是否够source_expect_count个
            retry_times1 = 2
            while usable_iters_len < source_expect_count and retry_times1 > 0:
                # 要召回offline的iter
                iter = offline_iters.popleft()
                retry_times2 = 3
                while retry_times2 > 0:
                    if iter.awake(last_cur_key):
                        usable_iters.put(iter)
                        usable_iters_len += 1
                        break
                    else:
                        retry_times2 -= 1
                if retry_times2 == 0:
                    # 唤醒失败，塞回offline_iters尾部，下一个循环从头部拿新的
                    offline_iters.append(iter)
                    retry_times1 -= 1
            assert retry_times1 > 0

            iter = usable_iters.get(block=False)
            logger.info(f'iter from {iter._addr}')
            try:
                key, version, value = iter.get()
            except:
                # 所有iter都迭代完了
                break
            logger.info(f'got key: {key}, version: {version}, value: {value}')
            if key != cur_key:
                # 新一轮key
                # 提交key到target addrs
                retry = 3
                while cur_key != '' and retry > 0:
                    firstReq = KVService_pb2.FirstKVServiceReq(
                        membersInfoVersion=self.__members_info.version)
                    key_with_column = KVService_pb2.Key(
                        content=cur_key, type=key_type)
                    request = KVService_pb2.CommitReq(
                        firstReq=firstReq, key=key_with_column, version=cur_max_version, value=cur_value)

                    channels = [grpc.aio.insecure_channel(
                        addr) for addr in target_addrs]
                    stubs = [KVService_pb2_grpc.KVServiceStub(
                        channel) for channel in channels]
                    stubmethod_with_requests = [
                        (stub.CommitKey, request) for stub in stubs]
                    if check_func():
                        return False
                    def resp_ok(resp):
                        assert not resp.firstKVServiceResp or resp.firstKVServiceResp.membersInfo.version <= self.__members_info.version
                        return True
                    ok_resps = collect_all_ok_responses_sync(
                        stubmethod_with_requests, resp_ok, 5)
                    if len(ok_resps) >= target_expect_count:
                        logger.info(f'commit {cur_key}->{cur_value}')
                        break
                    else:
                        retry -= 1
                assert retry > 0
                    
                # 轮到新的key
                last_cur_key = cur_key
                cur_key = key
                cur_max_version = version
                cur_value = value
            else:
                if version > cur_max_version:
                    cur_max_version = version
                    cur_value = value
            try:
                iter.next()
            except:
                offline_iters.append(iter)
                usable_iters_len -= 1
            else:
                usable_iters.put(iter)

        return True
    
    # 注意在AddMember前应该先在新member上启动相应的service，上面的MembersInfo随意，其version不要比当前的大就行
    def AddMember(self, adding_member_addrs: List[str]) -> bool:
        lock_lease = 2  # 单位：分钟
        if not self._try_lock("MEMBER.members_info", lock_lease):
            # 没拿到锁
            return False
        # NOTICE 时刻检查锁是否过期了，起码应该在耗时操作后和写入操作前检查
        cur_time = time.time()

        def is_lock_expired() -> bool:
            return time.time() - cur_time >= lock_lease * 60 - 10   # 保守一点，10秒内还没过期也不要继续了

        _, members_info_json_str = self._GetInternal(
            KVService_pb2.Key.Type.Meta, "MEMBER.member_info", 10)
        if is_lock_expired():
            return False
        new_members_infos = KVService_pb2.MembersInfo()
        Parse(members_info_json_str, new_members_infos)
        max_member_id = max([m.id for m in list(new_members_infos.members) +
                            list(new_members_infos.addingMembers) + list(new_members_infos.deletingMembers)], default=0)

        # 把原来的预备成员都清除，现在同一时间只能有一个client进行成员变更，后续可以再考虑允许多个client同时做成员变更
        # 现在全程要持有成员锁，导致数据数量过大的时候容易在复制阶段因为耗时过大而丢锁，不是特别好的设计
        del new_members_infos.addingMembers[:]
        del new_members_infos.deletingMembers[:]

        adding_members = []

        # 为new_members_infos AddMember
        for addr in adding_member_addrs:
            max_member_id += 1
            adding_members.append(KVService_pb2.Member(addr=addr, id=max_member_id))
        new_members_infos.adding_members.extend(adding_members)
        new_members_infos.version += 1
        self.__members_info = new_members_infos  # 顺带更新一下自己的members_info

        # step 1 设置全局Member_info
        while not self._SetInternal(KVService_pb2.Key.Type.Meta, "MEMBER.member_info", MessageToJson(new_members_infos), 5):
            # 因为有锁的存在，理论上只会因为修复而失败，理论上应该最多只会重试一次就能成功
            if is_lock_expired():
                return False
            pass
        if is_lock_expired():
            return False

        # step 2 将旧数据复制到新成员里
        read_addrs = {member.addr for member in self.__members_info.members} | \
            {member.addr for member in self.__members_info.deletingMembers}
        target_expect = majority(len(read_addrs)+len(adding_member_addrs)) - majority(len(read_addrs))
        # 先Meta再User
        ok = self._copy_datas(KVService_pb2.Key.Type.User, read_addrs, adding_member_addrs, majority(len(read_addrs)), target_expect, is_lock_expired)
        if not ok:
            return False
        ok = self._copy_datas(KVService_pb2.Key.Type.Meta, read_addrs, adding_member_addrs, majority(len(read_addrs)), target_expect, is_lock_expired)
        if not ok:
            return False
        
        # step 3 将新成员写入到全局Member里
        new_members_infos.version += 1
        
        new_member_infos_adding_members = [member for member in new_members_infos.addingMembers if member not in adding_members]
        del new_members_infos.addingMembers[:]
        new_members_infos.addingMembers.extend(new_member_infos_adding_members)
        new_members_infos.members.extend(adding_members)
        self.__members_info = new_members_infos  # 顺带更新一下自己的members_info
        while not self._SetInternal(KVService_pb2.Key.Type.Meta, "MEMBER.member_info", MessageToJson(new_members_infos), 5):
            # 因为有锁的存在，理论上只会因为修复而失败，理论上应该最多只会重试一次就能成功
            if is_lock_expired():
                return False
            pass
        if is_lock_expired():
            return False

        return True

    def DeleteMember(self, deleting_member_ids: List[int]) -> bool:
        lock_lease = 2  # 单位：分钟
        if not self._try_lock("MEMBER.members_info", lock_lease):
            # 没拿到锁
            return False
        # NOTICE 时刻检查锁是否过期了，起码应该在耗时操作后和写入操作前检查
        cur_time = time.time()

        def is_lock_expired() -> bool:
            return time.time() - cur_time >= lock_lease * 60 - 10   # 保守一点，10秒内还没过期也不要继续了

        _, members_info_json_str = self._GetInternal(
            KVService_pb2.Key.Type.Meta, "MEMBER.member_info", 10)
        if is_lock_expired():
            return False
        new_members_infos = KVService_pb2.MembersInfo()
        Parse(members_info_json_str, new_members_infos)

        # 把原来的预备成员都清除，现在同一时间只能有一个client进行成员变更，后续可以再考虑允许多个client同时做成员变更
        # 现在全程要持有成员锁，导致数据数量过大的时候容易在复制阶段因为耗时过大而丢锁，不是特别好的设计
        del new_members_infos.addingMembers[:]
        del new_members_infos.deletingMembers[:]

        deleting_members = [member for member in new_members_infos.members if member.id in deleting_member_ids]
        remaining_members = [member for member in new_members_infos.members if member.id not in deleting_member_ids]

        del new_members_infos.members[:]
        new_members_infos.members.extend(remaining_members)
        new_members_infos.version += 1
        self.__members_info = new_members_infos  # 顺带更新一下自己的members_info

        # step 1 设置全局Member_info
        while not self._SetInternal(KVService_pb2.Key.Type.Meta, "MEMBER.member_info", MessageToJson(new_members_infos), 5):
            # 因为有锁的存在，理论上只会因为修复而失败，理论上应该最多只会重试一次就能成功
            if is_lock_expired():
                return False
            pass
        if is_lock_expired():
            return False

        # step 2 将待删除成员数据复制到剩下的成员里
        source_expect = math.ceil(len(deleting_members)/2)
        target_expect = majority(len(remaining_members))
        # 先Meta再User
        ok = self._copy_datas(KVService_pb2.Key.Type.Meta, [m.addr for m in deleting_members], [m.addr for m in remaining_members], source_expect, target_expect, is_lock_expired)
        if not ok:
            return False
        ok = self._copy_datas(KVService_pb2.Key.Type.User, [m.addr for m in deleting_members], [m.addr for m in remaining_members], source_expect, target_expect, is_lock_expired)
        if not ok:
            return False
        
        # step 3 将待删除成员全局从deletingMembers里全局删除
        new_members_infos.version += 1

        del new_members_infos.members[:]
        del new_members_infos.deletingMembers[:]
        new_members_infos.members.extend(remaining_members)
        self.__members_info = new_members_infos  # 顺带更新一下自己的members_info
        while not self._SetInternal(KVService_pb2.Key.Type.Meta, "MEMBER.member_info", MessageToJson(new_members_infos), 5):
            # 因为有锁的存在，理论上只会因为修复而失败，理论上应该最多只会重试一次就能成功
            if is_lock_expired():
                return False
            pass
        if is_lock_expired():
            return False

        return True


#if __name__ == '__main__':
#    parser = argparse.ArgumentParser()
#    parser.add_argument('--key', required=True, help='the key to set')
#    parser.add_argument('--value', required=True, help='the value to set')
#    parser.add_argument('--log_name', required=False, help='the value to set')
#
#    args = parser.parse_args()
#
#    if args.log_name:
#        log_manager = LogManager(__name__, f'{args.log_name}.log')
#    else:
#        log_manager = LogManager(__name__)
#    logger = log_manager.get_logger()
#
#    members = ['127.0.0.1:8001', '127.0.0.1:8002', '127.0.0.1:8003']
#    members = [(m, i+1) for i, m in enumerate(members)]
#    members_info = KVService_pb2.MembersInfo()
#    for addr, id in members:
#        members_info.members.append(KVService_pb2.Member(addr=addr, id=id))
#    members_info.version = 3
#
#    kv_client = KVClient(members_info)
#
#    key = args.key
#    value = args.value
#    kv_client.Set(key, value, timeout=1)
#
#    # 测试 Get 方法
#    value = kv_client.Get(key, timeout=1)
#    print(f'value: "{value}"')


if __name__ == '__main__':
    log_manager = LogManager(__name__)
    logger = log_manager.get_logger()

    members = ['127.0.0.1:8001', '127.0.0.1:8002', '127.0.0.1:8003']
    members = [(m, i+1) for i, m in enumerate(members)]
    members_info = KVService_pb2.MembersInfo()
    for addr, id in members:
        members_info.members.append(KVService_pb2.Member(addr=addr, id=id))
    members_info.version = 3

    kv_client = KVClient(members_info)

    kv_client.AddMember(["127.0.0.1:8004", "127.0.0.1:8005"])
