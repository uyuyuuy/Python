#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'qaulau'

import sys
if __name__ == '__main__':
    print 'Access Denied.'
    sys.exit()

import os    
import datetime
import logging
from logging.handlers import RotatingFileHandler

APP_ROOT     = getattr(sys,'__APP_ROOT__',os.path.split(os.path.realpath(__file__))[0])
APP_PATH     = getattr(sys,'__APP_PATH__',os.path.join(APP_ROOT,'packages'))
APP_PATH and sys.path.insert(0,APP_PATH)


'''
数据库相关
'''
DATABASES = {
    'mysql':({
        'user' : 'kaoqing',
        'passwd' : 'RzrtUUNvsfPNnscY',
        'host': '127.0.0.1',
        'port': 3306,
        'charset' : 'utf8',
        'db' : 'kaoqing',
        'tablepre':'hqjf_',
        'db_fields_cache':False,
    },),
    'mssql':({
        'user':'kaoqing',
        'password':'kaoqing123456',
        'host':'172.168.16.105',
        'port': '1433',
        'database':'simple',
    },)
}

'''
导出目录相关
'''
EXPORT_PATH = os.path.join(APP_ROOT,'data/xlsx')


'''
日志配置
'''
APP_LOG = getattr(sys,'__APP_LOG__',True)
level = logging.DEBUG
#仅应用日志
if APP_LOG:
    LOGDIR = os.path.join(APP_ROOT, "logs")
    _handler = RotatingFileHandler(filename = os.path.join(LOGDIR, datetime.datetime.now().strftime("%Y-%m-%d")
                                                           + ".log"),mode = 'a+')
    _handler.setFormatter(logging.Formatter(fmt = '>>> %(asctime)-10s %(name)-12s %(levelname)-8s %(message)s',
                                            datefmt ='%Y-%m-%d %H:%M:%S'))
    LOG = logging.getLogger('hqjf_punch')
    LOG.setLevel(level)
    LOG.addHandler(_handler)
    #在控制台打印
    _console = logging.StreamHandler()
    LOG.addHandler(_console)

'''
邮件配置
'''
EMAIL = {
    'SMTP_HOST': 'smtp.163.com',
    'SMTP_PORT': 25,
    'SMTP_USER': 'hqchip@163.com',
    'SMTP_PASSWORD': 'hq123456',
    'SMTP_DEBUG': True,
    'SMTP_FROM': 'hqchip@163.com',
}

EMAIL_NOTICE = {
    #接收人员邮箱地址列表
    'accept_list':(
        '842276675@qq.com',
    ),
}

