# -*- coding: utf-8 -*-
# © 2016 QYT Technology
# Authored by: Liu tianlong (tlzmkm@gmail.com)
import json
import logging

import arrow
from redis import Redis, ConnectionPool

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)
logger = logging.getLogger(__name__)

from settings import REDIS_URL, REDIS_PORT


class GatewayRedisInfo(object):
    redis_client = None
    timeout = 60

    device_list = 'device.list'

    device_info_prefix = 'device.info'
    device_log_prefix = 'device.log'

    gateway_online_sensor = 'gateway.online'
    gateway_white_list = 'gateway.white'
    gateway_permit_join_status = 'gateway.permit_join'

    # 更新时间
    update_white = 'last.update.white.time'
    update_device = 'last.update.device.time'
    update_check_sensor = 'last.update.check.time'

    def __init__(self, pool):
        self.redis_client = Redis(connection_pool=pool)

    def set_white_list(self, gateway, sensor):
        # 设置白名单 list 转换成xx.xx 存入redis
        v = self.redis_client.get(self.gateway_white_list)
        if v:
            temp = json.loads(v)
            temp.update({gateway: sensor})

        else:
            temp = {gateway: sensor}
        self.redis_client.set(self.gateway_white_list, json.dumps(temp), self.timeout * 2)

    def get_white_list(self):
        # 获取白名单
        v = self.redis_client.get(self.gateway_white_list)

        if v:
            return json.loads(v)
        else:
            return {}

    def set_device_info(self, sensor, device_info):
        # 设置设备 get device info
        v = self.redis_client.get(self.device_info_prefix)

        if v and v != 'null':
            temp = json.loads(v)
            temp[sensor] = device_info
        else:
            temp = {sensor: device_info}
        self.redis_client.set(self.device_info_prefix, json.dumps(temp), self.timeout)

    def delete_device_info(self, sensor):
        v = self.redis_client.get(self.device_info_prefix)
        if v and v != 'null':
            temp = json.loads(v)
            del temp[sensor]
            self.redis_client.set(self.device_info_prefix, json.dumps(temp), self.timeout)

    def get_device_info(self):
        # 获取设备 获取设备信息
        v = self.redis_client.get(self.device_info_prefix)
        if v:
            return json.loads(v)
        else:
            return {}

    def set_device_log(self, sensor, log):
        # 设置设备 get device log
        v = self.redis_client.get(self.device_log_prefix)

        if v and v != 'null':
            temp = json.loads(v)
            temp[sensor] = log
        else:
            temp = {sensor: log}
        self.redis_client.set(self.device_log_prefix, json.dumps(temp), self.timeout)

    def delete_device_log(self, sensor):
        v = self.redis_client.get(self.device_log_prefix)
        if v and v != 'null':
            temp = json.loads(v)
            del temp[sensor]
            self.redis_client.set(self.device_log_prefix, json.dumps(temp), self.timeout)

    def get_device_log(self):
        log = self.redis_client.get(self.device_log_prefix)
        if log:
            return json.loads(log)
        else:
            return None

    def set_gateway_online_sensor(self, gateway, sensors):
        # 设置设备 实际上的在线设备
        v = self.redis_client.get(self.gateway_online_sensor)

        if v and v != 'null':
            temp = json.loads(v)
            temp[gateway] = sensors
        else:
            temp = {gateway: sensors}
        self.redis_client.set(self.gateway_online_sensor, json.dumps(temp), self.timeout)

    def get_gateway_online_sensor(self):
        sensor = self.redis_client.get(self.gateway_online_sensor)
        if sensor:
            return json.loads(sensor)
        else:
            return {}

    def set_gateway_permit_join_status(self, gateway, status):
        # 缓存自动入网状态
        gateway_status = self.redis_client.get(self.gateway_permit_join_status)
        if gateway_status and gateway_status != 'null':
            temp = json.loads(gateway_status)
            temp[gateway] = status
        else:
            temp = {gateway: status}
        self.redis_client.set(self.gateway_permit_join_status, json.dumps(temp), self.timeout)

    def get_gateway_permit_join_status(self):
        status = self.redis_client.get(self.gateway_permit_join_status)
        if status:
            return json.loads(status)
        else:
            return {}

    def set_update_white(self):
        self.redis_client.set(self.update_white, arrow.now().timestamp)

    def get_update_white(self):
        t = self.redis_client.get(self.update_white)

        if t:
            return t
        else:
            return arrow.now().timestamp

    def set_update_device(self):
        self.redis_client.set(self.update_device, arrow.now().timestamp)

    def get_update_device(self):
        t = self.redis_client.get(self.update_device)

        if t:
            return t
        else:
            return arrow.now().timestamp

    def set_update_check_sensor(self):
        self.redis_client.set(self.update_check_sensor, arrow.now().timestamp)

    def get_update_check_sensor(self):
        t = self.redis_client.get(self.update_check_sensor)

        if t:
            return t
        else:
            return arrow.now().timestamp
