# -*- coding: utf-8 -*-

import redis, time, datetime, threading, BaseHTTPServer
from redis.sentinel import Sentinel
from BaseHTTPServer import BaseHTTPRequestHandler

from wechat import *

sentinels = [
    ('10.56.50.102', 7800),
    ('10.56.50.102', 7801),
    ('10.56.50.102', 7802),
]

'''
    1、提供接口检查各Sentinel运行状态
        Sentinel端口不通不走企业微信报警，使用其他途径报警
    2、订阅到状态变动发送企业微信
        判断消息数量，每次不发送超过5条，保证企业微信消息不被阶段
'''

HTTP_HOST = '10.56.50.107'
HTTP_PORT = 7803
HTTP_DATA = {}


class TodoHandler(BaseHTTPRequestHandler):
    def __get_sentinel_data__(self):
        global HTTP_DATA
        r = [ {i: HTTP_DATA[i]['msg'] } for i in HTTP_DATA ]
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        return {'data': r, 'version': int(time.time())}

    def do_GET(self):
        self.wfile.write(json.dumps(self.__get_sentinel_data__()))

    def do_POST(self):
        self.wfile.write(json.dumps(self.__get_sentinel_data__()))


def start_httpd(httpd):
    httpd.serve_forever()


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

    def __pubsub_thread__(self, name):
        while self.pubsub_dict[name]['msg'] == None and self.state == True:
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
            except Exception, e:
                self.__update_pubsub__(name, str(e))
            time.sleep(0.5)
        return None

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

    def stop(self):
        self.state = False


if __name__ == '__main__':
    httpd   = BaseHTTPServer.HTTPServer((HTTP_HOST, HTTP_PORT), TodoHandler)
    api     = threading.Thread(target = start_httpd, name = 'HTTPServer', args=(httpd,))
    api.start()
    s = RdbClusterSubscribe(sentinels)
    s.start()
    try:
        while True:
            msg_list    = s.send_message()
            HTTP_DATA   = s.get_pubsub()
            time.sleep(10) 
    except KeyboardInterrupt, e:
        for f in [s.stop, httpd.shutdown, api._Thread__stop ]:
            f()
    print str(datetime.datetime.now()), " END"
    exit(1)
