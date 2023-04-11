import argparse
from concurrent import futures
import copy
import time
from typing import List, Optional, Union
import grpc
import logging
from google.protobuf.json_format import MessageToJson, Parse

from src.utils.log_manager import LogManager

from src.proto import KVService_pb2
from src.proto import KVService_pb2_grpc

from src.server.storage.storage import *
from src.server.lock_manager import LockManager

logger = logging.getLogger(__name__)

def key2str(key: KVService_pb2.Key):
    return f'{{type: {key.type}, content: {key.content}}}'

def next_iter_unless_end(iter):
    try:
        next(iter)
    except StopIteration:
        pass

def iter_storage_str(iter_index: int):
    return f'ITERATOR.{iter_index}'

class KVService(KVService_pb2_grpc.KVServiceServicer):
    def __recovery(self):
        # 暂时什么也不用做
        pass

    _user_lock_manager = LockManager()
    _iter_lock_manager = LockManager()
    def __init__(self, store_path: str, need_recovery: bool, members: List[Union[str, int]] = None):
        self._storage = Storage(store_path)

        if not need_recovery:
            members_info = KVService_pb2.MembersInfo()
            for addr, id in members:
                members_info.members.append(
                    KVService_pb2.Member(addr=addr, id=id))
            members_info.version = 1
            commited_members_info = KVService_pb2.CommitedValue(value=MessageToJson(members_info), commitedVersion=1)
            self._storage.Set(DBColumn.MetaCommit, "MEMBER.member_info",
                               commited_members_info.SerializeToString())
        else:
            self.__recovery()

    def __check_member_version(self, member_version: int) -> Optional[KVService_pb2.MembersInfo]:
        commited_members_info = KVService_pb2.CommitedValue()
        commited_members_info.ParseFromString(self._storage.Get(
            DBColumn.MetaCommit, "MEMBER.member_info"))

        member_info = KVService_pb2.MembersInfo()
        Parse(commited_members_info.value, member_info)
        if member_info.version <= member_version:
            return None
        else:
            return member_info

    def __key_with_version(key: str, version: int) -> str:
        return f'{key}_{version}'

    def CommitedVersion(self, request: KVService_pb2.CommitedVersionReq, context: grpc.ServicerContext) -> KVService_pb2.CommitedVersionResp:
        with self._user_lock_manager.get_lock(request.key.SerializeToString()):
            logger.info(f'CommitedVersion request from {context.peer()}, key: {key2str(request.key)}')
            if request.key.type == KVService_pb2.Key.Type.User:
                    member_info = self.__check_member_version(
                        request.firstReq.membersInfoVersion)
                    if member_info:
                        # 如果不是最新的，直接返回最新的 member_info
                        logger.info(f'member_info_version({request.firstReq.membersInfoVersion}) too old, newest is: \n {member_info}')
                        return KVService_pb2.CommitedVersionResp(firstKVServiceResp=KVService_pb2.FirstKVServiceResp(membersInfo=member_info))

            column = DBColumn.MetaCommit if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
            commited_value_str = self._storage.Get(column, request.key.content)
            if commited_value_str is None:
                return KVService_pb2.CommitedVersionResp(key=request.key, err=KVService_pb2.CommitedVersionResp.Err.NotCommited)
            commited_value = KVService_pb2.CommitedValue()
            commited_value.ParseFromString(commited_value_str)
            logger.info(f'return {commited_value}')
            return KVService_pb2.CommitedVersionResp(err=KVService_pb2.CommitedVersionResp.Err.OK, key=request.key, version=commited_value.commitedVersion)

    def PaxosPhase1(self, request: KVService_pb2.PaxosPhase1Req, context: grpc.ServicerContext) -> KVService_pb2.PaxosPhase1Resp:
        with self._user_lock_manager.get_lock(request.key.SerializeToString()):
            logger.info(f'PaxosPhase1 request from {context.peer()}, key: {key2str(request.key)}')
            if request.key.type == KVService_pb2.Key.Type.User:
                member_info = self.__check_member_version(
                    request.firstReq.membersInfoVersion)
                if member_info:
                    # 如果不是最新的，直接返回最新的 member_info
                    logger.info(f'member_info_version({request.firstReq.membersInfoVersion}) too old, newest is: \n {member_info}')
                    return KVService_pb2.CommitedVersionResp(firstKVServiceResp=KVService_pb2.FirstKVServiceResp(membersInfo=member_info))

            # 如果已经commit就直接返回
            column = DBColumn.MetaCommit if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
            commited_value_str = self._storage.Get(column, request.key.content)
            if commited_value_str:
                commited_value = KVService_pb2.CommitedValue()
                commited_value.ParseFromString(commited_value_str)
                if commited_value.commitedVersion >= request.version:
                    return KVService_pb2.PaxosPhase1Resp(err=KVService_pb2.PaxosPhase1Resp.Err.KeyVersionHaveCommited)

            key = request.key
            version = request.version
            proposal_bal = request.ballotNum

            ret = KVService_pb2.PaxosPhase1Resp(
                err=KVService_pb2.PaxosPhase1Resp.Err.OK) 

            column = DBColumn.MetaLog if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserLog
            log_value_str = self._storage.Get(
                column, KVService.__key_with_version(key.content, version))
            log_value = KVService_pb2.LogValue()
            if log_value_str:
                log_value.ParseFromString(log_value_str)
            else:
                log_value.last_visit_bal = 0

            ret.lastBal = copy.copy(log_value.last_visit_bal)  # 未更新的last_visit_bal

            if log_value.last_visit_bal < proposal_bal:
                log_value.last_visit_bal = proposal_bal
            
            ret.key.CopyFrom(key)
            ret.version = version
            ret.value = log_value.value
            ret.vBal = log_value.value_bal
            logger.info(f'return {str(ret)}')
            return ret

    def PaxosPhase2(self, request: KVService_pb2.PaxosPhase2Req, context: grpc.ServicerContext) -> KVService_pb2.PaxosPhase2Resp:
        with self._user_lock_manager.get_lock(request.key.SerializeToString()):
            logger.info(f'PaxosPhase2 request from {context.peer()}, key: {key2str(request.key)}')
            if request.key.type == KVService_pb2.Key.Type.User:
                member_info = self.__check_member_version(
                    request.firstReq.membersInfoVersion)
                if member_info:
                    # 如果不是最新的，直接返回最新的 member_info
                    logger.info(f'member_info_version({request.firstReq.membersInfoVersion}) too old, newest is: \n {member_info}')
                    return KVService_pb2.CommitedVersionResp(firstKVServiceResp=KVService_pb2.FirstKVServiceResp(membersInfo=member_info))

            # 如果已经commit就直接返回
            column = DBColumn.MetaCommit if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
            commited_value_str = self._storage.Get(column, request.key.content)
            if commited_value_str:
                commited_value = KVService_pb2.CommitedValue()
                commited_value.ParseFromString(commited_value_str)
                if commited_value.commitedVersion >= request.version:
                    return KVService_pb2.PaxosPhase1Resp(err=KVService_pb2.PaxosPhase1Resp.Err.KeyVersionHaveCommited)

            key = request.key
            value = request.value
            version = request.version
            proposal_bal = request.ballotNum

            column = DBColumn.MetaLog if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserLog
            log_value_str = self._storage.Get(
                column, KVService.__key_with_version(key.content, version))
            log_value = KVService_pb2.LogValue()
            if log_value_str:
                log_value.ParseFromString(log_value_str)

            if log_value.last_visit_bal <= proposal_bal:
                log_value.last_visit_bal = proposal_bal
                log_value.value = value
                log_value.value_bal = proposal_bal
                self._storage.Set(column, KVService.__key_with_version(
                    key, version), log_value.SerializeToString())

            ret = KVService_pb2.PaxosPhase2Resp(
                err=KVService_pb2.PaxosPhase2Resp.Err.OK)
            ret.key.CopyFrom(key)
            ret.version = version
            logger.info(f'return {ret}')
            return ret

    def CommitKey(self, request: KVService_pb2.CommitReq, context: grpc.ServicerContext) -> KVService_pb2.CommitResp:
        with self._user_lock_manager.get_lock(request.key.SerializeToString()):
            logger.info(f'CommitKey request from {context.peer()}, key: {key2str(request.key)}')
            if request.key.type == KVService_pb2.Key.Type.User:
                member_info = self.__check_member_version(
                    request.firstReq.membersInfoVersion)
                if member_info:
                    # 如果不是最新的，直接返回最新的 member_info
                    logger.info(f'member_info_version({request.firstReq.membersInfoVersion}) too old, newest is: \n {member_info}')
                    return KVService_pb2.CommitedVersionResp(firstKVServiceResp=KVService_pb2.FirstKVServiceResp(membersInfo=member_info))

            # 如果已经commit就直接返回
            commit_column = DBColumn.MetaCommit if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
            commited_value_str = self._storage.Get(
                commit_column, request.key.content)
            commited_value = KVService_pb2.CommitedValue()
            if commited_value_str:
                commited_value.ParseFromString(commited_value_str)
                if commited_value.commitedVersion >= request.version:
                    return KVService_pb2.CommitResp(err=KVService_pb2.CommitResp.Err.AlreadyCommitHigherVersion)

            log_column = DBColumn.MetaLog if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserLog
            if hasattr(request, 'value'):
                commited_value.value = request.value
            else:
                log_value_str = self._storage.Get(
                    log_column, KVService.__key_with_version(request.key.content, request.version))
                if log_value_str == '':
                    return KVService_pb2.CommitResp(err=KVService_pb2.CommitResp.Err.NotFoundVersionValue)
                log_value = KVService_pb2.LogValue()
                commited_value.value = log_value.value
            commited_value.commitedVersion = request.version
            self._storage.Set(commit_column, request.key.content,
                            commited_value.SerializeToString())

            # 删除低于该CommitVersion的Log
            first_key_version_key = KVService.__key_with_version(
                request.key.content, 0)
            iter = self._storage.NewKeyIterator(log_column, request.key.content)
            for key in iter:
                if not key.startswith(request.key.content):
                    break
                version = int(key.split('_')[-1])
                if version <= request.version:
                    self._storage.Delete(log_column, key)
                    break

            ret = KVService_pb2.CommitResp(err=KVService_pb2.CommitResp.Err.OK)
            ret.key.CopyFrom(request.key)
            ret.version = request.version
            logger.info(f'return {ret}')
            return ret

    def GetValue(self, request: KVService_pb2.GetValueReq, context: grpc.ServicerContext) -> KVService_pb2.GetValueResp:
        with self._user_lock_manager.get_lock(request.key.SerializeToString()):
            logger.info(f'GetValue request from {context.peer()}, key: {key2str(request.key)}')
            if request.key.type == KVService_pb2.Key.Type.User:
                member_info = self.__check_member_version(
                    request.firstReq.membersInfoVersion)
                if member_info:
                    # 如果不是最新的，直接返回最新的 member_info
                    logger.info(f'member_info_version({request.firstReq.membersInfoVersion}) too old, newest is: \n {member_info}')
                    return KVService_pb2.GetValueResp(firstKVServiceResp=KVService_pb2.FirstKVServiceResp(membersInfo=member_info))

            commit_column = DBColumn.MetaCommit if request.key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
            commited_value_str = self._storage.Get(
                commit_column, request.key.content)
            commited_value = KVService_pb2.CommitedValue()
            if commited_value_str:
                commited_value.ParseFromString(commited_value_str)
                logger.info(f'return {KVService_pb2.GetValueResp(key=request.key, value=commited_value.value, commitedVersion=commited_value.commitedVersion)}')
                return KVService_pb2.GetValueResp(key=request.key, value=commited_value.value, commitedVersion=commited_value.commitedVersion)
            else:
                logger.info(f'return {KVService_pb2.GetValueResp(err=KVService_pb2.GetValueResp.Err.NotFound)}')
                return KVService_pb2.GetValueResp(err=KVService_pb2.GetValueResp.Err.NotFound)
            
    def GetItems(self, request: KVService_pb2.GetItemsReq, context: grpc.ServicerContext) -> KVService_pb2.GetItemsResp:
        logger.info(f'GetItems request from {context.peer()}')
        commit_column = DBColumn.MetaCommit if request.prev_last_key.type == KVService_pb2.Key.Type.Meta else DBColumn.UserCommit
        prev_last_key = request.prev_last_key.content if request.prev_last_key else ""
        iter = self._storage.NewItemIterator(
                commit_column, prev_last_key)
        count = 0
        ret = KVService_pb2.GetItemsResp(err=KVService_pb2.GetItemsResp.Err.OK)
        for key, value in iter:
            if prev_last_key == key:
                # 跳过prev_last_key
                continue

            key_type = KVService_pb2.Key.Type.User if commit_column == DBColumn.UserCommit else KVService_pb2.Key.Type.Meta
            commited_value = KVService_pb2.CommitedValue()
            commited_value.ParseFromString(value)                
            
            ret.kvs.append(KVService_pb2.KV(key=KVService_pb2.Key(type=key_type, content=key), value=commited_value))
            count+=1
            if count == request.expect_count:
                break
        
        if count == 0:
            return KVService_pb2.NextItemResp(err=KVService_pb2.GetItemsResp.Err.NO_MORE_ITEMS)
        return ret


if __name__ == '__main__':
    # python3 server.py --port 8001 --members 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 --store_path test_store1.db
    log_manager = LogManager(__name__)
    logger = log_manager.get_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default='8001',
                        help='Port number for server to listen on.')
    parser.add_argument('--need_recovery', type=bool, default=False,
                        help='read members_info and snapshot from storage')
    parser.add_argument('--members', nargs='+', default=[
                        '127.0.0.1:8001', '127.0.0.1:8002', '127.0.0.1:8003'], help='List of members in the Raft cluster.')
    parser.add_argument('--store_path', type=str, default='test_store.db',
                        help='Path to the directory to store KV log')
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_path = 'test_store.db'
    members = [(m, i+1) for i, m in enumerate(args.members)]
    if args.need_recovery:
        print(
            f"Starting server on port {args.port} with recovery and store_path {args.store_path}")
    else:
        print(
            f"Starting server on port {args.port} with members {args.members} and store_path {args.store_path}")
    KVService_pb2_grpc.add_KVServiceServicer_to_server(
        KVService(args.store_path, args.need_recovery, members), server)
    server.add_insecure_port(f'[::]:{args.port}')
    server.start()
    server.wait_for_termination()
