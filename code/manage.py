# -*- coding: utf-8 -*-
# © 2016 QYT Technology
# Authored by: Liu tianlong (tlzmkm@gmail.com)
import string
import paho.mqtt.client as mqtt
import logging
import random
import json
import time
import paho.mqtt.subscribe as subscribe
import arrow
import pymysql
from threading import Thread

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
logger = logging.getLogger(__name__)

max_sensor = 10


def async_call(fn):
    def wrapper(*args, **kwargs):
        Thread(target=fn, args=args, kwargs=kwargs).start()

    return wrapper


class MQTTClientHandle(object):
    """
    host: ndp.nlecloud.com
    prot: 1883
    device_tag: 网关 设备标识
    project_id: 项目ID
    secret_key: SecretKey

    """
    _client = None

    write_mysql = False
    # 全局的log
    device_log = {}

    sensor_max_quantity = 12
    sensor_min_quantity = 10
    # 全局的 device 信息
    device_info = {}

    gateway_list = {}

    # 给实际的设备
    gateway_online_sensor = {}
    # 白名单 给传感器使用
    gateway_white_list = {}  # 白名单

    # 传感器permit_join 状态
    gateway_permit_join_status = {}

    def __init__(self, host, port, secret_key):
        self.host = host
        self.port = port
        self.secret_key = secret_key

    def initialization(self):
        # 初始化
        self._client = mqtt.Client(client_id=self.generate_number())
        self._client.connect(host=self.host, port=self.port, keepalive=60)
        logger.debug("client connect host:{host} ,port:{port}".format(host=self.host, port=self.port, ))

    def generate_number(self):
        return ''.join(random.sample(string.ascii_letters + string.digits, 16))

    @async_call
    def input_mysql(self, message):

        try:
            if self.write_mysql:
                connect = pymysql.connect(host='192.168.67.34', user='root', passwd='123456', db='mqtt_demo',
                                          charset='utf8', port=3306)
                cursor = connect.cursor()

                payload = message.payload.decode('utf-8')
                if isinstance(payload, bytes):
                    payload = payload.decode('utf-8')
                topic = message.topic
                whitelist = json.dumps(self.gateway_white_list)
                device_info = json.dumps(self.device_info)
                device_log = json.dumps(self.device_log)
                t = arrow.now().format('YYYY-MM-DD HH:mm:ss')
                sql = """INSERT INTO zigbee2mqtt_to_zigbeeserver (payload, whitelist, time,topic,device_info,device_log) VALUES ( '%s', '%s', '%s','%s','%s','%s' )""" % (
                    payload, whitelist, t, topic, device_info, device_log)
                cursor.execute(sql)
                connect.commit()
            else:
                pass
        except Exception as e:
            logger.debug('请求信息持久化的时候出错')
            logger.debug(sql)
            logger.debug(e)

    @async_call
    def output_mysql(self, topic, payload):
        try:
            if self.write_mysql:
                connect = pymysql.connect(host='192.168.67.34', user='root', passwd='123456', db='mqtt_demo',
                                          charset='utf8', port=3306)
                cursor = connect.cursor()

                whitelist = json.dumps(self.gateway_white_list)
                device_info = json.dumps(self.device_info)
                device_log = json.dumps(self.device_log)
                t = arrow.now().format('YYYY-MM-DD HH:mm:ss')
                sql = """INSERT INTO zigbeeserver_to_zigbee2mqtt (payload, whitelist, time,topic,device_info,device_log) VALUES ( '%s', '%s', '%s','%s','%s','%s' )""" % (
                    payload, whitelist, t, topic, device_info, device_log)
                cursor.execute(sql)
                connect.commit()
            else:
                pass
        except Exception as e:
            logger.debug('请求信息持久化的时候出错')
            logger.debug(sql)
            logger.debug(e)

    @async_call
    def get_gateway_white_list(self):
        while True:
            logger.debug('get_gateway_white_list')
            time.sleep(6)
            self.publish_data(topic='zigbeeserver/read/whitelist', payload=None)
            self.output_mysql(topic='zigbeeserver/read/whitelist', payload='')

    @async_call
    def set_devices(self):
        topic = 'zigbeeserver/zigbee2mqtt/{device_tag}/zigbee2mqtt/bridge/config/devices/get'
        while True:
            time.sleep(5)
            for device_tag in self.gateway_white_list.keys():
                logger.debug('update devices %s' % device_tag)
                self.publish_data(topic=topic.format(device_tag=device_tag), payload='')

    def publish_data(self, topic, payload, qos=1):
        logger.debug('publish_data')
        self._client.publish(topic=topic, payload=payload, qos=1)
        logger.debug('publish topic:%s  ,payload:%s' % (topic, payload))
        self.output_mysql(topic=topic, payload=payload)

    def get_gateway_by_sensor(self, sensor_tag):
        # 通过传感器获取网关串号
        gateway = []
        for k, v in self.gateway_white_list.items():
            if sensor_tag in v:
                gateway.append(k)
        return gateway

    @async_call
    def subscribe_forward_log(self):
        logger.debug('>>> subscribe forward_log>>>>>>')
        # 设备日志
        subscribe.callback(self.forward_log, "zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/log",
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_forward_devices(self):
        logger.debug('>>> subscribe forward_devices>>>>>>')
        # 在线设备
        subscribe.callback(self.forward_devices, 'nleconfig/zigbeeserver/+/zigbee2mqtt/bridge/config/devices/get',
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60
                           )

    @async_call
    def subscribe_forward_data(self):
        logger.debug('>>> subscribe forward_data>>>>>>')
        # 转发数据
        subscribe.callback(self.forward_data, 'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/#',
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_forward_config(self):
        logger.debug('>>> subscribe forward_config>>>>>>')
        # 配置
        subscribe.callback(self.forward_config, 'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/config',
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_forward_state(self):
        logger.debug('>>> subscribe forward_state>>>>>>')
        # 状态
        subscribe.callback(self.forward_state, 'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/state',
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subcribe_whitelist(self):
        logger.debug('>>> subscribe set_white_list>>>>>>')
        #  获取设备白名单
        subscribe.callback(self.set_white_list, 'zigbeeserver/configure/whitelist', hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_config_force_remove(self):
        subscribe.callback(self.set_config_force_remove,
                           'nleconfig/zigbeeserver/+/zigbee2mqtt/bridge/config/force_remove',
                           hostname=self.host, port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60
                           )

    def set_subscribe(self):
        self.subcribe_whitelist()
        self.subscribe_forward_log()
        self.subscribe_forward_data()
        self.subscribe_forward_devices()
        self.subscribe_forward_config()
        # self.subscribe_forward_state()
        self.subscribe_forward_action()
        self.subscribe_config_force_remove()
        self.subscribe_forward_get_device()

    @async_call
    def subscribe_forward_action(self):
        subscribe.callback(self.set_forward_action, 'connector/zigbeeserver/+/zigbee2mqtt/+/set/state',
                           hostname=self.host,
                           port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_forward_get_device(self):
        subscribe.callback(self.set_get_device,
                           'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/config/devices',
                           hostname=self.host,
                           port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    @async_call
    def subscribe_forward_nleconfig(self):
        #   上层获取配置信息
        subscribe.callback(self.set_forward_nleconfig, 'nleconfig/zigbeeserver/+/zigbee2mqtt/bridge/config/devices/get',
                           hostname=self.host,
                           port=self.port,
                           client_id=self.generate_number(),
                           keepalive=60)

    def force_remove(self, remove_sensor):
        # 删除传感器
        topic = 'zigbeeserver/zigbee2mqtt/{device}/zigbee2mqtt/bridge/config/force_remove'
        for i in remove_sensor:
            logger.info(topic.format(device=i.get('device')))
            logger.info(i.get('sensor'))
            self.publish_data(topic=topic.format(device=i.get('device')), payload=i.get('sensor'))

    def sensor_quantity_rule(self):
        # 监测传感器数量是否超出某个值
        topic = 'zigbeeserver/nleconfig/{device}/zigbee2mqtt/bridge/config/permit_join'
        for k, v in self.gateway_online_sensor.items():
            # 超过最大传感器数量  就把自动入网给禁止掉
            logger.info('gateway:%s , sensor_quantity:%s ' % (k, len(v)))
            if len(v) >= self.sensor_max_quantity and self.gateway_permit_join_status.get(k, True):
                logger.info('topic:%s , payload :false' % topic.format(device=k))
                self.publish_data(topic=topic.format(device=k), payload='false')

            elif len(v) < self.sensor_min_quantity and not self.gateway_permit_join_status.get(k, False):
                logger.info('topic:%s , payload :true' % topic.format(device=k))
                self.publish_data(topic=topic.format(device=k), payload='true')
            else:
                pass

    def gate_sensor_repeat_rule(self):
        # 一个设备多次入网
        repeat = []
        # 通过交集的方式 查询出 重复入网的传感器
        for k, v in self.gateway_online_sensor.items():
            for k2, v2 in self.gateway_online_sensor.items():
                if k != k2:
                    repeat += list(set(v2).intersection(set(v)))

        if not repeat:
            force_remove_list = []
            for sensor in repeat:
                gateways = self.get_gateway_by_sensor(sensor)
                max_sensor_quantity = 0
                temp = None
                for gateway in gateways:
                    # 如果传感器数量最多 并且不在白名单中就把他force remove
                    sensor_quantity = len(self.gateway_online_sensor.get(gateway))
                    if max_sensor_quantity < sensor_quantity and sensor not in self.gateway_white_list.get(gateway):
                        max_sensor_quantity = sensor_quantity
                        temp = gateway

                force_remove_list.append({"device": temp, "sensor": sensor})
            self.force_remove(force_remove_list)

    @async_call
    def check_sensor_repeat(self):
        # 监测一个传感器重复入网
        logger.info('>>>>>>check_sensor_repeat>>>>>')
        while True:
            time.sleep(60)
            self.gate_sensor_repeat_rule()

    @async_call
    def check_sensor_quantity(self):
        while True:
            time.sleep(60)
            self.sensor_quantity_rule()

    @async_call
    def set_forward_action(self, client, userdata, message):
        sensor = message.topic.split('/')[-3]
        topic = 'zigbeeserver/zigbee2mqtt/{devicetag}/zigbee2mqtt/{sensor}/set/state'
        for k, v in self.gateway_online_sensor.items():
            if sensor in v:
                self.publish_data(topic.format(devicetag=k, sensor=sensor), payload=message.payload)

    @async_call
    def set_get_device(self, client, userdata, message):
        logger.debug('set_get_device')
        device_tag = message.topic.split('/')[2]
        # 网关下的 传感器list 和 传感器类型
        online_sensor_list = []

        # 用来判断是否过期
        utime = {'utime': time.time()}
        for item in json.loads(message.payload.decode('utf8')):
            if 'modelID' in item.keys() and len(item.get('modelID', '')) > 2:
                #  通过model ID 来判断 是否完全注册成功
                item.update(utime)
                self.device_info.update({item.get('ieeeAddr'): item})
                online_sensor_list.append(item.get('ieeeAddr'))
        self.gateway_online_sensor.update({device_tag: online_sensor_list})

    @async_call
    def check(self):
        self.check_sensor_repeat()
        self.check_sensor_quantity()

    @async_call
    def set_forward_nleconfig(self, client, userdata, message):
        topic = 'zigbeeserver/nleconfig/{devicetag}/zigbee2mqtt/bridge/config/devices'
        device_tag = message.topic.split('/')[2]
        white_list = self.gateway_white_list.get(device_tag, [])

        des_devices = []
        for i in white_list:
            if i in self.device_info.keys():
                des_devices.append(self.device_info.get(i))

        if device_tag in self.device_info.keys():
            des_devices.append(self.device_info.get(device_tag))
        self.publish_data(topic.format(devicetag=device_tag), payload=json.dumps(des_devices))

    @async_call
    def set_config_force_remove(self, client, userdata, message):
        payload = message.payload.decode('utf8')
        if isinstance(payload, bytes):
            sensor = payload.decode('utf-8')
        else:
            sensor = payload
        gatewaylist = []

        for k, v in self.gateway_online_sensor.items():
            if sensor in v:
                gatewaylist.append(k)
        logger.info(gatewaylist)
        remove_device = []
        for s in gatewaylist:
            remove_device.append({"device": s, "sensor": payload})
        self.force_remove(remove_sensor=remove_device)

    @async_call
    def set_white_list(self, client, userdata, message):
        logger.debug('start set_white_list')
        paylaod = message.payload.decode('utf8')
        payload = json.loads(paylaod)
        self.gateway_white_list.update({payload.get('devicetag'): payload.get('sensorlist')})
        logger.debug(self.gateway_white_list)
        self.input_mysql(message=message)
        logger.debug('finish set_white_list')

    @async_call
    def forward_state(self, client, userdata, message):
        #  转发 zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/state -> zigbeeserver/zigbee2mqtt/{devicetag}/zigbee2mqtt/bridge/state
        #
        topic = 'zigbeeserver/zigbee2mqtt/{devicetag}/zigbee2mqtt/bridge/state'
        device_tag = message.topic.split('/')[2]
        self.gateway_list.update({device_tag: time.time()})
        self.publish_data(topic.format(devicetag=device_tag), payload=message.payload)

        self.input_mysql(message=message)

    @async_call
    def forward_config(self, client, userdata, message):
        # 转发 'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/config' -> zigbeeserver/nleconfig/{devicetag}/zigbee2mqtt/bridge/config
        # topic = 'zigbeeserver/nleconfig/{devicetag}/zigbee2mqtt/bridge/config'
        device_tag = message.topic.split('/')[2]
        payload = message.payload.decode('utf-8')
        logger.info('topic:%s, permit_join:%s' % (device_tag, json.loads(payload).get('permit_join')))
        self.gateway_permit_join_status.update({device_tag: json.loads(payload).get('permit_join')})
        logger.info(self.gateway_permit_join_status)
        self.gateway_list.update({device_tag: time.time()})
        # self.publish_data(topic.format(devicetag=device_tag), payload=message.payload)
        # self.input_mysql(message=message)

    @async_call
    def forward_data(self, client, userdata, message):
        # 转发 zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/# -> zigbeeserver/zigbee2mqtt/{devicetag}/zigbee2mqtt/{sensor_tag}
        # 根据白名单去转发数据
        topic = 'zigbeeserver/zigbee2mqtt/{devicetag}/zigbee2mqtt/{sensor_tag}'
        sensor_tag = message.topic.split('/')[-1]
        gatewaylist = self.get_gateway_by_sensor(sensor_tag)
        logger.debug('forward_data')
        logger.debug(message.topic)
        logger.debug(self.gateway_white_list)
        for device_tag in gatewaylist:
            self.publish_data(topic.format(devicetag=device_tag, sensor_tag=sensor_tag), payload=message.payload)
        self.input_mysql(message=message)

    @async_call
    def forward_log(self, client, userdata, message):
        # 转发 "zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/log"

        # 获取设备标识
        topic = 'zigbeeserver/nleconfig/{devicetag}/zigbee2mqtt/bridge/log'
        is_exchange = True
        device_tag = message.topic.split('/')[2]

        # 保存设备信息
        payload = message.payload.decode('utf8')
        if json.loads(payload).get('type') == 'devices':
            for item in json.loads(payload).get('message', []):
                if item and hasattr(item, 'get'):
                    self.device_log.update({item.get('ieeeAddr'): item})

            logger.debug(self.device_log)
            white_list = self.gateway_white_list.get(device_tag, [])

            des_log = []
            for i in white_list:
                if i in self.device_log.keys():
                    des_log.append(self.device_log.get(i))

            if device_tag in self.device_log.keys():
                des_log.append(self.device_log.get(device_tag))
            t = json.loads(message.payload.decode('utf-8'))
            logger.debug(t)
            t['message'] = des_log
            des_payload = json.dumps(t)
            logger.debug(payload)
        elif json.loads(payload).get('type') == 'pairing':
            logger.info(payload)
            friendly_name = json.loads(payload).get('meta', {}).get('friendly_name')
            for i in self.get_gateway_by_sensor(friendly_name):
                device_tag = i

            des_payload = payload
        else:
            is_exchange = False
            des_payload = None
            logger.debug('需要添加新的type 类型')
            logger.debug(payload)

        try:
            if is_exchange:
                self.publish_data(topic.format(devicetag=device_tag), payload=des_payload)
        except Exception as e:
            logger.debug('>>' * 50)
            logger.debug('这个是错误')
            logger.debug(message.topic)
            logger.debug(message.payload)
            logger.debug('<<' * 50)
        self.input_mysql(message=message)

    @async_call
    def forward_devices(self, client, userdata, message):
        # 转发 'zigbee2mqtt/zigbeeserver/+/zigbee2mqtt/bridge/config/devices'
        # 获取设备标识
        topic = 'zigbeeserver/nleconfig/{devicetag}/zigbee2mqtt/bridge/config/devices'
        device_tag = message.topic.split('/')[2]
        white_list = self.gateway_white_list.get(device_tag, [])

        des_devices = []
        # 白名单中的设备信息  设备信息超过10 认为就是掉线了
        for i in white_list:
            if i in self.device_info.keys() and abs(self.device_info.get(i, {}).get('utime', 0) - time.time()) < 11:
                des_devices.append(self.device_info.get(i))
        # zegebee 设备的信息
        if device_tag in self.device_info.keys() and abs(
                self.device_info.get(device_tag, {}).get('utime', 0) - time.time()) < 11:
            des_devices.append(self.device_info.get(device_tag))

        self.publish_data(topic.format(devicetag=device_tag), payload=json.dumps(des_devices))

        self.input_mysql(message=message)

    def run(self):
        # 建立连接
        self.initialization()

        # 定时获取白名单
        logger.debug('定时获取白名单')
        self.get_gateway_white_list()

        # 定时获取device
        self.set_devices()

        # 查询实际连接数量是否超过 或者 一个sensor 同时连接多台设备
        #  # 传感器管理（比如 限制个数， 去重，删除无效节点）
        self.check()

        # 订阅转发规则
        self.set_subscribe()

        self._client.loop_forever()
        # 建立连接
        # 订阅 -> 转发规则 ()
        # 心跳


if __name__ == '__main__':
    mq = MQTTClientHandle(host='192.168.67.111', port=1883, secret_key=None)
    mq.run()
