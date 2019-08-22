# -*- coding: utf-8 -*-

import redis, time, datetime, threading
from redis.sentinel import Sentinel
from wechat import *

sentinels = [
    ('10.56.50.102', 7800),
    ('10.56.50.102', 7801),
    ('10.56.50.102', 7802),
]

'''
    1、sentinal 连接不上
        重连、报警
        sentinal错误信息示例:
            2 Error 111 connecting to 10.56.50.102:7803. Connection refused.
    2、报警批量发送不逐条发送
        单独线程检查报警信息
    3、节点信息从dbop取，提供接口给dbop查看各节点状态
        cluster_obj加字段subscribe_url
    
'''

class RdbClusterSubscribe:
    def __init__(self, sentinels):
        self.sentinels      = sentinels
        self.state          = True
        self.msg            = []
        self.pubsub_dict    = {}
        self.lock           = threading.Lock()

    def __update_pubsub__(self, name, err):
        self.lock.acquire()
        self.pubsub_dict[name]['msg'] = err
        self.lock.release()

    def __update_message__(self, new):
        self.lock.acquire()
        if new:
            self.msg.append(new)
        else:
            self.msg = []
        self.lock.release()

    def get_pubsub(self):
        return self.pubsub_dict

    def send_message(self):
        msg_list = self.msg
        self.__update_message__(False)
        header = "Redis Sentinel Monitor\n"
        if msg_list and len(msg_list) > 5:
            for index in range(0, len(msg_list), 5):
                send_weixin_message(header + '\n'.join(msg_list[index: index+5]), ['jiangxu', ])
        elif msg_list:
            send_weixin_message(header + '\n'.join(msg_list), ['jiangxu', ])
        return len(msg_list)

    def start(self):
        cluster_obj = Sentinel(self.sentinels, socket_timeout = 0.5)
        for sentinel_obj in cluster_obj.sentinels:
            conn_dict = sentinel_obj.connection_pool.connection_kwargs
            name = conn_dict['host'] + ':' + str(conn_dict['port'])
            try:
                err, pubsub = None, sentinel_obj.pubsub()
                pubsub.psubscribe('*')
            except Exception, e:
                err, pubsub = str(e), None
            self.pubsub_dict[name] = {
                'sentinel': sentinel_obj,
                'pubsub': pubsub,
                'msg': err
            }
            if err == None:
                thr_obj = threading.Thread(
                    target  = self.__pubsub_thread__,
                    name    = name,
                    args    = (name, )
                )
                thr_obj.setDaemon(True)
                thr_obj.start()

    def __pubsub_thread__(self, name):
        while self.pubsub_dict[name]['msg'] == None:
            try:
                info = self.pubsub_dict[name]['pubsub'].get_message()
                if info and type(info) == dict and info['pattern']:
                    warning = "%s %s [%s] %s" % (
                        str(datetime.datetime.now()),
                        name,
                        info['channel'],
                        info['data']
                    )
                    self.__update_message__(warning)
                time.sleep(0.5)
            except Exception, e:
                self.__update_pubsub__(name, str(e))
        return None


if __name__ == '__main__':
    s = RdbClusterSubscribe(sentinels)
    s.start()
    n = 1
    while n < 120:
        msg_list = s.send_message()
        time.sleep(10) 
        n += 1
    exit(1)
    print 'End'
