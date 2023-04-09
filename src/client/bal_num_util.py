import datetime
import hashlib
import os
import socket
import threading
import uuid

# 时间戳的起始时间（'2020-01-01 08:00:00.000'）
TWEPOCH = 1577808000000

class Snowflake:
    def __init__(self):
        self.worker_id = self._get_worker_id()
        self.sequence = 0
        self.last_timestamp = -1

    def _get_worker_id(self):
        # 使用MAC地址、PID和线程ID计算worker id，确保全球唯一
        mac_addr = hex(uuid.getnode())[2:].zfill(12)
        pid = str(os.getpid())
        tid = str(threading.current_thread().ident)

        h = hashlib.sha1()
        h.update(mac_addr.encode('utf-8'))
        h.update(pid.encode('utf-8'))
        h.update(tid.encode('utf-8'))
        return int(h.hexdigest(), 16) % (2 ** 10)

    def _get_timestamp(self):
        # 获取当前时间戳，以毫秒为单位
        delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(TWEPOCH / 1000.0)
        return int(delta.total_seconds() * 1000)

    def generate_id(self):
        timestamp = self._get_timestamp()

        if timestamp < self.last_timestamp:
            raise Exception('Clock moved backwards, Refusing to generate id')

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) % (2 ** 12)
            if self.sequence == 0:
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp
        id = (timestamp << 22) | (self.worker_id << 12) | self.sequence
        return id

    def _til_next_millis(self, last_timestamp):
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._get_timestamp()
        return timestamp
