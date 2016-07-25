#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'qaulau'


import sys
reload(sys).setdefaultencoding('utf-8')
import logging
import time
import calendar
import math
import re
import atexit
import shutil
try:
    import argparse
except ImportError:
    print '没有发现 argparse 模块，对于python版本小于2.7的请安装 argparse 模块!'
    sys.exit()

try:
    import xlsxwriter
except ImportError:
    print '请安装 XlsxWriter 模块'
    sys.exit()

try:
    import xlrd
except ImportError:
    print 'No module named xlrd, Please use `easy_install xlrd` or `pip install xlrd` install xlrd module!'
    sys.exit(0)

try:
    import json
except ImportError:
    import smiplejson as json

import pymssql
from config import *
import util
import database


_logger = logging.getLogger('hqjf_punch')


class SyncCron(object):
    '''
    同步任务
    '''

    def __init__(self):
        if not self.connect_mysql():
            return
        atexit.register(self.exit)

    def connect_mysql(self,num = 0,tablepre = None):
        '''
        连接mysql
        '''
        try:
            mysql = database.db_mysql(DATABASES['mysql'][0],tablepre = tablepre)
            self.mysql = mysql
            return True
        except Exception,e:
            num += 1
            print '连接mysql异常，异常信息：%s' % util.traceback_info(e)
            print '正在重试连接，终止请按 Ctrl + c'
            if num == 3:
                print '系统已进行3次重试连接操作，请根据异常信息检查配置'
                return False
            self.connect_mysql(num = num,tablepre = None)

    def connect_mssql(self,num = 0):
        '''
        连接microsoft sql server
        :return:
        '''
        try:
            conn = pymssql.connect(**DATABASES['mssql'][0])
            self.conn = conn
            self.mssql = conn.cursor()
            return True
        except Exception,e:
            num += 1
            print '连接SQL Server异常，异常信息：%s' % util.traceback_info(e)
            print '正在重试连接，终止请按 Ctrl + c'
            if num == 3:
                print '系统已进行3次重试连接操作，请根据异常信息检查配置'
                return False
            self.connect_mssql(num = num)

    def sync_employee(self):
        '''
        同步员工数据
        :return:
        '''
        last_id = util.number_format(util.file('punch/last_sync_employee_id'),0)
        _total_num = 0

        while True:
            self.mssql.execute("SELECT TOP 100 EmployeeID,ChineseName,EnglishName,ClassGroupID,Photo,Sex,Department,"
                               "Principalship,ApprovalTime,ClassName,EmployeeCode,sysID,ISDimission,IfOT,IfOfficial "
                               "FROM EmployeeMsg WHERE EmployeeID > %s ORDER BY EmployeeID ASC",(last_id,))
            res = self.mssql.fetchall()
            if not res:
                break
            for row in res:
                try:
                    self.mysql.insert('employee',data={
                        'employee_id':row[0],
                        'chinese_name':row[1],
                        'english_name':row[2] if row[2] else '',
                        'class_group_id':row[3],
                        'photo':'',
                        'sex':row[5] if row[5] else 0,
                        'department':row[6] if row[6] else '',
                        'principalship': row[7] if row[7] else '',
                        'approval_time':util.strtotime(str(row[8]).strip(),format='%Y-%m-%d %H:%M:%S'),
                        'class_name':row[9],
                        'employee_code':row[10],
                        'sys_id': row[11] if row[11] else 0,
                        'is_dimission':row[12],
                        'is_ot':row[13],
                        'is_official':row[14],
                    })
                    # 检测创建用户
                    self.check_account(row[1], row[0])
                    _logger.info('成功同步员工数据：%s' % row[0])
                    _total_num += 1
                except Exception,e:
                    if 'Duplicate entry' in str(e):
                         _logger.warn('员工ID：%s 数据已存在' % row[0])
                    else:
                        _logger.exception('员工数据同步异常')
            last_id = row[0]
        util.file('punch/last_sync_employee_id',last_id)
        _logger.info('成功同步员工数据 %s 条' % _total_num)
        return _total_num

    def sync_machine(self):
        '''
        同步设备数据
        :return:
        '''
        last_id = util.number_format(util.file('punch/last_sync_machine_id'),0)
        _total_num = 0
        self.mssql.execute("SELECT MachineID,MachineNo,MachineName FROM CheckMachine WHERE MachineID> %s ORDER BY "
                           "MachineID ASC",(last_id,))
        res = self.mssql.fetchall()
        if not res:
            return
        for row in res:
            try:
                self.mysql.insert('check_machine',data={
                    'mid':row[0],
                    'machine_no':row[1],
                    'machine_name':row[2],
                })
                _logger.info('成功同步设备数据：%s' % row[0])
                _total_num += 1
            except Exception,e:
                if 'Duplicate entry' in str(e):
                    _logger.warn('设备ID：%s 数据已存在' % row[0])
                else:
                    _logger.exception('设备数据同步异常')
            last_id = row[0]
        util.file('punch/last_sync_machine_id',last_id)
        _logger.info('成功同步设备数据 %s 条' % _total_num)
        return _total_num


    def sync_punch_data(self):
        '''
        同步打卡数据
        :return:
        '''
        #获取上次同步的ID
        last_id = util.number_format(util.file('punch/last_sync_data_id'),0)
        _total_num = 0
        _min_data_id = 0
        _max_data_id = 0
        _user_dict = {}
        while True:
            self.mssql.execute("SELECT TOP 1000 OriginalDataID,EmployeeID,OccurTime,EpNo,Remark FROM OriginalCheckData"
                               " WHERE OriginalDataID > %s ORDER BY OriginalDataID ASC",(last_id,))
            res = self.mssql.fetchall()
            if not res:
                break
            for row in res:
                if _min_data_id == 0:
                    _min_data_id = row[0]
                _time = util.strtotime(str(row[2]).strip(),format='%Y-%m-%d %H:%M:%S')
                if row[1] in _user_dict:
                    uid = _user_dict[row[1]]
                else:
                    ret = self.mysql.select('user',condition={'employee_id':row[1]},fields=('id'),limit=1)
                    if ret:
                        _user_dict[row[1]] = ret[0]
                        uid = ret[0]
                    else:
                        _user_dict[row[1]] = 0
                        uid = 0
                try:
                    self.mysql.insert('punch_data',data={
                        'data_id':row[0],
                        'uid':uid,
                        'employee_id':row[1],
                        'occur_time':_time,
                        'ep_no':row[3],
                        'remark':row[4] if row[4] else '',
                    })
                    _logger.info('成功同步打卡记录数据：%s' % row[0])
                    _total_num += 1
                except Exception,e:
                    if 'Duplicate entry' in str(e):
                         _logger.warn('数据ID：%s 记录已存在' % row[0])
                    else:
                        _logger.exception('打卡数据同步异常')
            last_id = row[0]
        _max_data_id = last_id
        util.file('punch/last_sync_data_id',last_id)

        if _total_num > 0:
            _logger.info('正在标记有效打卡数据...')
            self.mysql.execute("SELECT MIN(`occur_time`),MAX(`occur_time`) FROM `hqjf_punch_data` WHERE "
                               "`data_id` >= %s AND `data_id` <= %s;",(_min_data_id,_max_data_id))
            res = self.mysql.fetchone()
            _min_time = util.strtotime(util.date(res[0],format='%Y-%m-%d 00:00'))
            _max_time = util.strtotime(util.date(res[1],format='%Y-%m-%d 23:59')) + 60
            _num = int(math.ceil((_max_time - _min_time) / 86400.0))
            for i in xrange(0,_num):
                min_time = _min_time+i*86400
                max_time = _min_time+(i+1)*86400
                self.mysql.execute('UPDATE `hqjf_punch_data` SET `is_valid` = 0 WHERE `occur_time` >= %s AND'
                                   ' `occur_time` < %s ;',(min_time,max_time))
                self.mysql.commit()
                self.mysql.execute('UPDATE `hqjf_punch_data` AS  t1 INNER  JOIN (SELECT _t1.pid FROM `hqjf_punch_data` '
                                   'AS _t1 INNER JOIN (SELECT MIN(`occur_time`) occur_time, `employee_id` FROM '
                                   '`hqjf_punch_data` WHERE `occur_time` >= %s AND `occur_time` < %s GROUP BY `employee_id`) '
                                   'AS _t2 ON _t1.employee_id = _t2.employee_id AND _t1.occur_time = _t2.occur_time) '
                                   'AS t2 ON t1.pid = t2.pid SET t1.`is_valid` = 1;',(min_time,max_time))
                self.mysql.commit()
                self.mysql.execute('UPDATE `hqjf_punch_data` AS t1 INNER  JOIN (SELECT _t1.pid FROM `hqjf_punch_data` '
                                   'AS _t1 INNER JOIN (SELECT MAX(`occur_time`) occur_time, `employee_id` FROM '
                                   '`hqjf_punch_data` WHERE `occur_time` >= %s AND `occur_time` < %s GROUP BY `employee_id`) '
                                   'AS _t2 ON _t1.employee_id = _t2.employee_id AND _t1.occur_time = _t2.occur_time) '
                                   'AS t2 ON t1.pid = t2.pid SET t1.`is_valid` = 1;',(min_time,max_time))
                self.mysql.commit()
            _logger.info('正在统计打卡数据...')
            _min_gmtime = time.gmtime(_min_time)
            _max_gmtime = time.gmtime(_max_time)
            if _min_gmtime.tm_year != _max_gmtime.tm_year:
                _num = 13 - _min_gmtime.tm_mon + _max_gmtime.tm_mon + (_max_gmtime.tm_year - _min_gmtime.tm_year - 1) * 12
            else:
                _num = _max_gmtime.tm_mon - _min_gmtime.tm_mon + 1
            for i in xrange(0,_num):
                if _min_gmtime.tm_year != _max_gmtime.tm_year:
                    _y = int(math.floor((_min_gmtime.tm_mon + i) / 12.0))
                    year = _min_gmtime.tm_year + _y
                    month = _min_gmtime.tm_mon + i - _y * 12
                else:
                    year = _min_gmtime.tm_year
                    month = _min_gmtime.tm_mon + i
                self.stat_check_data(year = year,month = month)

        _logger.info('成功同步并处理打卡记录 %s 条' % _total_num)
        return _total_num


    def stat_check_data(self,year = None,month = None,day = None,flag = False):
        '''
        统计打卡数据
        :param year: 指定统计数据年份
        :param month: 指定统计数据月份
        :param day: 指定统计数据最大日期（可选）
        :return:
        '''
        if not month  and not year:
            format = None
            self.mysql.execute("SELECT MIN(`occur_time`),MAX(`occur_time`) FROM `hqjf_punch_data`")
            ret = self.mysql.fetchone()
            _min_gmtime = time.gmtime(ret[0])
            _max_gmtime = time.gmtime(ret[1])
            if _min_gmtime.tm_year != _max_gmtime.tm_year:
                _num = 13 - _min_gmtime.tm_mon + _max_gmtime.tm_mon + (_max_gmtime.tm_year - _min_gmtime.tm_year - 1) * 12
            else:
                _num = _max_gmtime.tm_mon - _min_gmtime.tm_mon + 1
            self.mysql.execute('TRUNCATE  TABLE `hqjf_check_stat`;')
            self.mysql.execute('TRUNCATE  TABLE `hqjf_overtime`;')
            for i in xrange(0,_num):
                if _min_gmtime.tm_year != _max_gmtime.tm_year:
                    _y = int(math.floor((_min_gmtime.tm_mon + i - 1) / 12.0))
                    year = _min_gmtime.tm_year + _y
                    month = _min_gmtime.tm_mon + i - _y * 12
                else:
                    year = _min_gmtime.tm_year
                    month = _min_gmtime.tm_mon + i
                self.stat_check_data(year = year,month = month,flag = flag)
            return True
        elif not month:
            format = '%s-%%m' % (year,)
        elif not year:
            format = '%%Y-%02d' % (month,)
        else:
            format = None
            ym = '%s-%02d' % (year,month)
        if format is not None:
            ym = util.date(format=format)
        match = re.search('([\d]{4}\-[0-1][0-9])',ym)
        if not match:
            _logger.error('年月日格式错误 %s' % (ym,))
            return
        ym = match.group(0)
        #self.mysql.delete('check_stat',condition={'ym':ym})
        res = self.mysql.select('user',condition={'employee_id':('<>',0)},fields = ('id','employee_id','class_id',
                                                                                    'email','email_notice'),order='id ASC')
        if not res:
            return
        _unix_time = int(time.time())
        _max_day = calendar.monthrange(int(ym[0:4]),int(ym[5:7]))[1]
        if not day or day > _max_day:
            day = _max_day
        _min_time = util.strtotime('%s-1 00:00' % ym)
        _max_time = util.strtotime('%s-%s 23:59' % (ym,day)) + 60
        if flag:
            _num = int(math.ceil((_max_time - _min_time) / 86400.0))
            _logger.info('正在标记有效打卡数据...')
            for i in xrange(0,_num):
                min_time = _min_time + i*86400
                max_time = _min_time + (i+1)*86400
                self.mysql.execute('UPDATE `hqjf_punch_data` SET `is_valid` = 0 WHERE `occur_time` >= %s AND'
                                   ' `occur_time` < %s ;',(min_time,max_time))
                self.mysql.commit()
                self.mysql.execute('UPDATE `hqjf_punch_data` AS  t1 INNER  JOIN (SELECT _t1.pid FROM `hqjf_punch_data` '
                                   'AS _t1 INNER JOIN (SELECT MIN(`occur_time`) occur_time, `employee_id` FROM '
                                   '`hqjf_punch_data` WHERE `occur_time` >= %s AND `occur_time` < %s GROUP BY `employee_id`) '
                                   'AS _t2 ON _t1.employee_id = _t2.employee_id AND _t1.occur_time = _t2.occur_time) '
                                   'AS t2 ON t1.pid = t2.pid SET t1.`is_valid` = 1;',(min_time,max_time))
                self.mysql.commit()
                self.mysql.execute('UPDATE `hqjf_punch_data` AS t1 INNER  JOIN (SELECT _t1.pid FROM `hqjf_punch_data` '
                                   'AS _t1 INNER JOIN (SELECT MAX(`occur_time`) occur_time, `employee_id` FROM '
                                   '`hqjf_punch_data` WHERE `occur_time` >= %s AND `occur_time` < %s GROUP BY `employee_id`) '
                                   'AS _t2 ON _t1.employee_id = _t2.employee_id AND _t1.occur_time = _t2.occur_time) '
                                   'AS t2 ON t1.pid = t2.pid SET t1.`is_valid` = 1;',(min_time,max_time))
                self.mysql.commit()

        ret = self.mysql.select('punch_data',condition=[('occur_time','>=',_min_time),('occur_time','<=',_max_time)],
                                fields=('occur_time'),order='occur_time DESC',limit=1)
        if ret:
            _day = util.number_format(util.date(ret[0],format="%d"),0)
            #只统计有效数据的结果
            if _day < day:
                day = _day

        _default_data = {}
        if _max_day < 31:
            for i in xrange(_max_day + 1,32):
                _default_data['day%s' % i] = -1
        _total_count = 0
        for row in res:
            try:
                stat_data = {
                    'uid':row[0],
                    'employee_id':row[1],
                    'ym':ym,
                }
                stat_data.update(_default_data)
                self.mysql.insert('check_stat',data= stat_data)
            except Exception as e:
                if 'Duplicate entry' not in str(e):
                    _logger.exception('统计打卡数据保存数据异常')

            data = {}
            for i in xrange(1,day + 1):
                data['day%s' % i] = 0
                _start_time = _min_time + (i - 1) * 86400
                _end_time = _min_time + i * 86400
                _worktime = self.get_worktime(row[2],occur_time = _start_time + 1000)
                if _worktime is None:	#判断是否为假期
                    data['day%s' % i] = 9 #周末或者假期，没有工作时间
                    continue
                ret = self.mysql.select('punch_data',condition=[('employee_id',row[1]),('is_valid',1),
                                                                ('occur_time','>=',_start_time),
                                                                ('occur_time','<',_end_time)],fields = ('occur_time','pid'),
                                        limit=2,order='occur_time ASC')
                _start_work_time = util.strtotime('%s-%02d %s' % (ym,i,_worktime[0]),format="%Y-%m-%d %H:%M:%S")
                _end_work_time = util.strtotime('%s-%02d %s' % (ym,i,_worktime[1]),format="%Y-%m-%d %H:%M:%S")
                is_work = _worktime[7]
                if not ret:  #没打卡
                    if not is_work:
                        data['day%s' % i] = 9
                    else:
                        data['day%s' % i] = 8
                    continue

                if len(ret) == 1:
                    #下班未打卡
                    if _start_work_time >= ret[0][0]:
                        data['day%s' % i] = 5
                    #上班未打卡
                    elif ret[0][0] >= _end_work_time:
                        data['day%s' % i] = 4
                        #可能存在加班
                        self.check_overtime(uid=row[0],occur_time=ret[0][0],clock_time= 0,class_id = row[2])
                    else:
                        data['day%s' % i] = 6

                    if not is_work:
                        data['day%s' % i] = 9
                else:
                    if not is_work:
                        data['day%s' % i] = 9
                        #假期加班
                        self.check_overtime(uid=row[0],occur_time=ret[1][0],clock_time=ret[0][0],class_id = row[2])
                    #上班未打卡
                    elif ret[0][0] >= _end_work_time:
                        data['day%s' % i] = 4
                        #可能存在加班
                        self.check_overtime(uid=row[0],occur_time=ret[1][0],clock_time= 0,class_id = row[2])
                        #self.mysql.update('punch_data',condition={'pid':ret[0][1]},data = {'is_valid':0})
                    elif _start_work_time >= ret[0][0] and _end_work_time <= ret[1][0]:
                        data['day%s' % i] = 1
                        #可能存在加班
                        self.check_overtime(uid=row[0],occur_time=ret[1][0],clock_time=ret[0][0],class_id = row[2])
                    elif _start_work_time >= ret[0][0] and _end_work_time > ret[1][0]:
                        # 早退
                        # 此处可能存在上班9:00 打了一次卡，中午进来打卡，然后下午出去未打卡，造成显示为早退（故折中处理此种情况备注要详尽）
                        # 这种情况不清楚是否为早退，所以需要人工处理，在下班前3小时内打卡均属于早退，否则为异常
                        if (_end_work_time - ret[1][0]) <= 10800:
                            data['day%s' % i] = 3
                        else:
                            data['day%s' % i] = 12 # 异常
                    elif _end_work_time <= ret[1][0] and _start_work_time < ret[0][0]:
                        #迟到
                        data['day%s' % i] = 2
                        _absent_time = util.strtotime('%s-%02d %s' % (ym,i,_worktime[4]),format="%Y-%m-%d %H:%M:%S") \
                            if _worktime[4] else 0
                        if _absent_time > _start_work_time and _absent_time < ret[0][0]:
                            data['day%s' % i] = 11 # 旷工
                        #可能存在加班
                        self.check_overtime(uid=row[0],occur_time=ret[1][0],clock_time=ret[0][0],class_id = row[2])
                    else:
                        #迟到并且早退
                        data['day%s' % i] = 10
                        _absent_time = util.strtotime('%s-%02d %s' % (ym,i,_worktime[4]),format="%Y-%m-%d %H:%M:%S") \
                            if _worktime[4] else 0
                        if _absent_time > _start_work_time and _absent_time < ret[0][0]:
                            data['day%s' % i] = 11 # 旷工

            for i in xrange(1,day + 1):
                status = data.get('day%s' % i,0)
                if status > 11 or status <= 1:
                    continue
                ymd = '%s-%s' % (ym,i)
                #self.save_notification(uid=row[0],status = status,ymd=ymd,email=row[3] if row[4] else None)

            if data:
                self.mysql.update('check_stat',condition={'ym':ym,'uid':row[0]},data = data)
                _logger.info('成功统计 %s 用户ID：%s 打卡及加班时间' % (ym,row[0]))
                _total_count += 1
            self.overtime_stat(row[0])
        _logger.info('成功统计 %s 打卡数据记录 %s 条' % (ym,_total_count))
        return _total_count


    def get_worktime(self,class_id = 0,occur_time = None):
        '''
        获取工作时间
        :param class_id: 分类ID
        :param week_day: 星期日期
        :return:
        '''
        if not class_id or not occur_time:
            return None
        week_day = int(util.date(occur_time,format="%w"))
        if not hasattr(self,'_worktime'):
            self._worktime = {}
        holiday = self.mysql.select('holiday',condition = [('starttime','<=',occur_time),('endtime','>=',occur_time)],
                          fields = ('workday'),limit = 1)
        if class_id in self._worktime:
            if not holiday:
                return self._worktime[class_id][week_day]
            else:
                return self._worktime[class_id][holiday[0]]
        res = self.mysql.select('work_class',condition={'id':class_id},fields=('day0','day1','day2','day3','day4','day5',
                                                                          'day6'),limit=1)
        if not res:
            return None
        ctids = list(set(res))
        ret = self.mysql.select('check_time',condition=[('id','in',ctids)],fields=('id','start_time','end_time',
                                            'over_start_time','over_end_time','absent_time','midday_time',
                                            'after_time','is_work'))
        if not ret:
            return None
        check_time = {}
        for row in ret:
            check_time[row[0]] = (row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])

        self._worktime[class_id] = []
        for i in xrange(0,7):
            self._worktime[class_id].insert(i,check_time.get(res[i],None))
        if not holiday:
            return self._worktime[class_id][week_day]
        else:
            return self._worktime[class_id][holiday[0]]


    def check_overtime(self,uid = 0,occur_time = 0,clock_time = 0,class_id = 0):
        '''
        检测加班时间
        :param uid:         用户id（user id）
        :param occur_time:  下班打卡时间
        :param clock_time:  上班打卡时间
        :param over_start_time: 加班开始时间
        :param over_end_time:   加班结束时间
        :return: overtime 加班时间
        '''
        if not uid or not occur_time:
            return 0
        _worktime = self.get_worktime(class_id,occur_time = occur_time)
        if _worktime is None:
            return 0
        over_start_time = _worktime[2]
        over_end_time = _worktime[3]
        is_work = _worktime[7]
        if over_start_time is None:
            over_start_time = '19:30:00'
        if over_end_time is None:
            over_end_time = '23:59:00'
        ymd = util.date(occur_time,format="%Y-%m-%d")
        _start_time = util.strtotime('%s %s' % (ymd,over_start_time),format="%Y-%m-%d %H:%M:%S")
        _end_time = util.strtotime('%s %s' % (ymd,over_end_time),format="%Y-%m-%d %H:%M:%S")
        _clock_time = clock_time
        _occur_time = occur_time
        if not is_work:
            if not clock_time:
                return 0
            _start_work_time = util.strtotime('%s %s' % (ymd,_worktime[0]),format="%Y-%m-%d %H:%M:%S")
            _end_work_time = util.strtotime('%s %s' % (ymd,_worktime[1]),format="%Y-%m-%d %H:%M:%S")
            _midday_time = util.strtotime('%s %s' % (ymd,_worktime[5]),format="%Y-%m-%d %H:%M:%S")
            _after_time = util.strtotime('%s %s' % (ymd,_worktime[6]),format="%Y-%m-%d %H:%M:%S")
            if clock_time < _start_work_time:
                clock_time = _start_work_time
            if occur_time > _end_time:
                occur_time = _end_time
            _reset_time = 0
            if clock_time <= _midday_time:
                if occur_time >= _midday_time and occur_time < _after_time:
                    _reset_time += occur_time - _midday_time
                elif occur_time >= _after_time and occur_time < _end_work_time:
                    _reset_time += _after_time - _midday_time
                elif occur_time >= _end_work_time and occur_time < _start_time:
                    _reset_time += occur_time - _end_work_time + _after_time - _midday_time
                elif occur_time >= _start_time:
                    _reset_time += _start_time - _end_work_time + _after_time - _midday_time
            elif clock_time > _midday_time and clock_time <= _after_time:
                _reset_time = _after_time - clock_time
                if occur_time > _after_time and occur_time <= _start_time:
                    _reset_time += occur_time - _end_work_time
                elif occur_time > _start_time:
                    _reset_time += _start_time - _end_work_time
            elif clock_time > _after_time and clock_time <= _end_work_time:
                if occur_time > _end_work_time and occur_time <= _start_time:
                    _reset_time += occur_time - _end_work_time
                elif occur_time > _start_time:
                    _reset_time += _start_time - _end_work_time
            elif clock_time > _end_work_time and clock_time <= _start_time:
                _reset_time = _start_time - clock_time
            overtime = int(math.floor((occur_time - clock_time - _reset_time) / 3600.0))
        else:
            if _start_time >= occur_time:
                return 0
            _max_overtime = int(math.floor((_end_time - _start_time) / 3600.0))
            overtime = int(math.floor((occur_time - _start_time) / 3600.0))
            if overtime > _max_overtime:
                overtime = _max_overtime
        if overtime <= 0:
            return 0
        res = self.mysql.select('overtime',condition={'ymd':ymd,'uid':uid},fields=('oid'),limit=1)
        if res:
            self.mysql.update('overtime',condition={'oid':res[0]},data={
                'clock_time':_clock_time,
                'occur_time':_occur_time,
                'overtime':overtime,
                'remark':'',
            })
        else:
            self.mysql.insert('overtime',data={
                'uid':uid,
                'ymd':ymd,
                'clock_time':_clock_time,
                'occur_time':_occur_time,
                'overtime':overtime,
                'remark':'',
            })
        return overtime

    def overtime_stat(self,uid):
        '''
        统计某个用户的加班时间
        :param uid:
        :return:
        '''
        if not uid:
            return 0
        res = self.mysql.query('SELECT SUM(`overtime`) as t FROM `hqjf_overtime` WHERE `uid` = %s AND `status` = 1;',(uid,))
        if not res or not res[0][0]:
            return 0
        overtime = util.number_format(res[0][0],0)
        if self.mysql.select('user_profile',condition={'uid':uid},fields=('uid'),limit=1):
            self.mysql.update('user_profile',data={
                'overtime':overtime,
            },condition={'uid':uid})
        else:
            self.mysql.insert('user_profile',data={
                'uid':uid,
                'overtime':overtime,
            })
        return overtime


    def _init_hqjf_tmp_user_table(self):
        '''
        初始化hqjf_tmp_user表
        :return:
        '''
        self.mysql.execute('''
          CREATE TABLE `hqjf_tmp_user` (
            `user_id` smallint(6) unsigned NOT NULL AUTO_INCREMENT,
            `user_name` varchar(24) NOT NULL DEFAULT '',
            `pinyin` varchar(48) NOT NULL DEFAULT '',
            `induction_time` int(10) unsigned NOT NULL DEFAULT '0',
            `work_code` smallint(6) unsigned NOT NULL DEFAULT '0',
            PRIMARY KEY (`user_id`),
            UNIQUE KEY `work_code` (`work_code`),
            KEY `induction_time` (`induction_time`)
          ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
        ''')
        _logger.info('成功创建数据库表 hqjf_tmp_user!')

        return True

    def import_jf_user_data(self,fname = None,column = 3,rownum = 3):
        '''
        导入华强聚丰用户数据
        :return:
        '''
        import pypinyin
        if not fname or not os.path.isfile(fname):
            _logger.warn('数据文件 %s 不存在' % fname)
            return
        column = util.number_format(column,0)
        rownum = util.number_format(rownum,0)
        if column <= 0 or rownum <= 0:
            _logger.warn('column 或者 rownum 参数无效')
            return
        s = self.ask('您输入的起始列数为 %s , 起始行数为 %s，请确认:' % (column,rownum),ask_list=('y','n'))
        if s == 'n':
            return

        #检测是否存在hqjf_tmp_user 表
        self.mysql.execute("SHOW TABLES LIKE 'hqjf_tmp_user';")
        self.mysql.commit()
        result = util.number_format(self.mysql.affected_rows(),0)
        if result <= 0:
            s = self.ask('系统检测到数据库不存在表 hqjf_tmp_user , 请初始化临时用户表，请确认操作 !',ask_list=('y','n'))
            if s == 'n':
                return
            elif s == 'y':
                self._init_hqjf_tmp_user_table()

        workbook = xlrd.open_workbook(fname,encoding_override = 'utf-8')
        sheet = workbook.sheet_by_index(0)
        if sheet.ncols < (column + 1) or sheet.nrows < (rownum - 1):
            _logger.warn('文件数据格式错误，不支持该格式数据')
            return

        col_index = column - 1
        for row in xrange(rownum - 1,sheet.nrows):
            user_name = str(sheet.cell(row,col_index).value).strip()
            work_code = util.number_format(sheet.cell(row, col_index + 1).value,0)
            _time = xlrd.xldate_as_tuple(sheet.cell(row, col_index + 2).value,0)
            induction_time = util.strtotime('%s-%02d-%s' % (_time[0],_time[1],_time[2]),format="%Y-%m-%d")
            pinyin = ''.join(pypinyin.lazy_pinyin(user_name.decode('utf-8')))
            try:
                self.mysql.insert('tmp_user',data = {
                    'user_name':user_name,
                    'pinyin':pinyin,
                    'induction_time':induction_time,
                    'work_code':work_code,
                })
            except Exception as e:
                if 'Duplicate entry' in str(e):
                    _logger.warn('用户工号：%s 记录已存在' % work_code)
                else:
                    _logger.exception('保存数据异常')

        res = self.mysql.select('tmp_user',order='induction_time ASC',fields=('user_name','pinyin','induction_time',
                                                                              'work_code'))
        if not res:
            return
        _total_count = 0
        for row in res:
            try:
                uid = self.mysql.insert('user',data={
                    'account':row[1],
                    'nickname':row[0],
                    'password':'e10adc3949ba59abbe56e057f20f883e',
                    'create_time':int(time.time()),
                    'status':1,
                    'type':1,
                    'work_code':row[3],
                    'induction_time':row[2],
                    'info':'',
                    'bind_account': '',
                    'email': '',
                    'remark':'',
                    'update_time':0,
                },return_insert_id=True)
                try:
                    self.mysql.insert('user_profile',data = {'uid':uid})
                except:
                    pass
                self.mysql.update('user',condition={'id':uid},data={'user_code':uid})
                _total_count += 1
                _logger.info('成功导入华强聚丰用户：%s' % row[0])
            except Exception as e:
                if 'Duplicate entry' in str(e):
                    _logger.warn('用户：%s 记录已存在' % row[1])
                else:
                    _logger.exception('保存数据异常')
        _logger.info('成功导入华强聚丰用户数据 %s 条' % _total_count)
        return _total_count

    def check_account(self,nickname, employee_id = 0, account=None):
        '''
        检测账号是否存在，存在则绑定，不存在新建
        '''
        import pypinyin

        if not account:
            account = ''.join(pypinyin.lazy_pinyin(nickname.decode('utf-8')))

        ret = self.mysql.select('user',condition={'account': account},fields=('id','employee_id'),limit=1)
        if ret:
            if ret[1] == 0:
                self.mysql.update('user',condition={'id':ret[0]},data={
                    'employee_id':employee_id,
                    'update_time':int(time.time())
                })
                self.mysql.update('punch_data', condition={'employee_id':row[0]}, data={'uid':ret[0]})
                _logger.info('成功更新用户打卡数据并绑定用户账号：%s ' % account)
            else:
                _end = util.number_format(account[-1],0) + 1
                account = '%s%s' % (account if _end <= 1 else account[0:-1],_end)
                self.check_account(nickname, employee_id=employee_id, account=account)
        else:
            uid = self.mysql.insert('user',data={
                'account':account,
                'nickname':nickname,
                'password':'e10adc3949ba59abbe56e057f20f883e', #123456
                'create_time':int(time.time()),
                'employee_id':employee_id,
                'status':0,
                'type':0,
                'work_code':0,
                'info':'',
                'bind_account': '',
                'email': '',
                'remark':'',
                'update_time':0,
            },return_insert_id=True)
            self.mysql.update('punch_data', condition={'employee_id':employee_id}, data={'uid':uid})
            try:
                self.mysql.insert('user_profile',data = {'uid':uid,})
            except:
                pass
            _logger.info('成功更新用户打卡数据并添加用户账号：%s' % account)
        return True

    def bind_employee_info(self):
        '''
        绑定员工信息
        :return:
        '''
        _total_count = 0
        last_id = util.number_format(util.file('punch/last_bind_employee_id'),0)
        while True:
            res = self.mysql.select('employee',condition=[('employee_id','>',last_id)],
                                    fields = ('employee_id','chinese_name'),limit=100,order= 'employee_id ASC')
            if not res:
                break
            for row in res:
                if not row[0]:
                    continue
                ret = self.mysql.select('user',condition={'employee_id':row[0]},fields=('id'),limit=1)
                if ret:
                    continue
                self.check_account(row[1],employee_id=row[0])
                _total_count += 1
            last_id = row[0]
        util.file('punch/last_bind_employee_id',last_id)
        _logger.info('成功绑定和添加员工信息 %s 条' % _total_count)
        return _total_count

    def export_stat(self,ym = None,department_id = 0):
        '''
        导出考勤统计
        :param department_id:   部门ID
        :return:
        '''
        if not ym:
            ym = util.date(format='%Y-%m')
        if department_id:
            res = self.mysql.select('department',condition={'department_id':department_id},fields=('department_id',
                                                                        'department_company','department_name'))
        else:
            res = self.mysql.select('department',fields=('department_id','department_company','department_name'))


        filepath= os.path.join(APP_ROOT,'data/export')
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        data_dict = {}
        for row in res:
            ret = self.mysql.query("SELECT t2.`nickname`,t1.`uid`,t1.`day1`,t1.`day2`,t1.`day3`,t1.`day4`,t1.`day5`,t1.`day6`,"
                             "t1.`day7`,t1.`day8`,t1.`day9`,t1.`day10`,t1.`day11`,t1.`day12`,t1.`day13`,t1.`day14`,"
                             "t1.`day15`,t1.`day16`,t1.`day17`,t1.`day18`,t1.`day19`,t1.`day20`,t1.`day21`,t1.`day22`,"
                             "t1.`day23`,t1.`day24`,t1.`day25`,t1.`day26`,t1.`day27`,t1.`day28`,t1.`day29`,t1.`day30`,"
                             "t1.`day31` FROM hqjf_check_stat AS t1 INNER JOIN hqjf_user AS t2 ON t1.uid = t2.id WHERE "
                             "t2.`department` = %s AND t1.`ym` = %s ORDER BY t1.uid ASC",(row[0],ym))
            if not ret:
                continue
            filename = os.path.join(filepath,'%s_%s_%s.xlsx' % (row[1],row[2],ym))
            wb = xlsxwriter.Workbook(filename)
            ws = wb.add_worksheet('考勤')
            merge_format = wb.add_format({'align': 'center','valign':'vcenter','bold':True,'size':16,'font':u'宋体'})
            total_num = len(ret)
            _max_day = calendar.monthrange(int(ym[0:4]),int(ym[5:7]))[1]
            ws.set_row(0,35)
            ws.set_column(0,_max_day + 3,5)
            ws.set_default_row(19)
            ws.merge_range(0,2,0,_max_day + 3,'%s月 周考勤表' % (ym.replace('-','年'),),merge_format)
            format = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1})
            ws.merge_range('A2:B2','部门',format)
            ws.write(1,2,'序号',format)
            ws.write(1,3,'姓名',format)
            for k in xrange(0,_max_day):
                ws.write(1,4 + k,k + 1,format)
            # 冻结
            ws.freeze_panes(2,4)
            format0 = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1,
                                     'bg_color':'#C0C0C0'})
            format1 = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1,
                                     'bg_color':'#CCFFCC'})
            # 事假
            format2 = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1,
                                     'bg_color':'#FFFF00'})
            # 需要人工处理
            format3 = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1,
                                     'bg_color':'#ffcc00'})
            i = 2
            j = 0
            for item in ret:
                j += 1
                if i == 2:
                    _format = wb.add_format({'rotation':90,'align': 'center','valign':'vcenter','bold':True,
                                             'size':12,'font':u'黑体','border':1})
                    ws.merge_range(2,0,total_num + 1,0,row[1],_format)
                    ws.merge_range(2,1,total_num + 1,1,row[2],_format)
                ws.write(i,2,j,format)
                ws.write(i,3,item[0],format)
                for k in xrange(0,_max_day):
                    status = item[k+2]
                    if status == 1:
                        ws.write(i,4 + k,'',format1)
                    elif  status == 2 or status == 3:
                        _time = self.get_late_time(uid=item[1],ymd='%s-%s' % (ym,k + 1),status=status)
                        if _time:
                            ws.write_comment(i,4+k,"system:\n实际打卡时间 %s" % util.date(_time,format="%H:%M:%S"))
                        ws.write(i,4 + k,'迟到' if status == 2 else '早退',format1)
                    elif status in (4,5,6,7,8):
                        if status == 4:
                            comment = "system:\n上班未打卡"
                        elif status == 5:
                            comment = "system:\n下班未打卡"
                        elif status == 8:
                            comment = "system:\n上下班均未打卡"
                        else:
                            _time = self.get_late_time(uid=item[1],ymd='%s-%s' % (ym,k + 1),status=status)
                            comment = "system:\n%s未打卡" % ('迟到下班' if status == 6 else '早退上班',)
                            if _time:
                                comment += ",实际打卡时间 %s" % util.date(_time,format="%H:%M:%S")
                        ws.write_comment(i,4+k,comment)
                        ws.write(i,4 + k,'未打卡',format)
                    elif status == 9:
                        ws.write(i,4 + k,'',format0)
                    elif status == 10:
                        _time = self.get_late_time(uid=item[1],ymd='%s-%s' % (ym,k + 1))
                        if _time:
                            ws.write_comment(i,4+k,"system:\n迟到并早退，实际打卡时间\n上班 %s\n下班 %s" %
                                             (util.date(_time[0],format="%H:%M:%S"),util.date(_time[1],format="%H:%M:%S")))
                        ws.write(i,4 + k,'迟到早退',format1)
                    elif status == 11:
                        _time = self.get_late_time(uid=item[1],ymd='%s-%s' % (ym,k + 1))
                        if _time:
                            ws.write_comment(i,4+k,"system:\n实际打卡时间%s\n折旷工一天" %
                                             (util.date(_time[0],format="%H:%M:%S"),))
                        ws.write(i,4 + k,'旷工',format)
                    elif status == 12:
                        _time = self.get_late_time(uid=item[1],ymd='%s-%s' % (ym,k + 1))
                        if _time:
                            ws.write_comment(i,4+k,"system:\n异常考勤，实际打卡时间\n上班 %s\n最后打卡时间 %s" %
                                             (util.date(_time[0],format="%H:%M:%S"),util.date(_time[1],format="%H:%M:%S")))
                        ws.write(i,4 + k,'?',format3)    
                    else:
                        ws.write(i,4 + k,'',format)
                i += 1
            ws.merge_range(i,0,i,_max_day + 3,'备注：白色为异常考勤 , 灰色为假期，黄色为需要审核考勤（可能为早退或下班未打卡）')
            wb.close()
            data_dict[row[0]] = filename
            print 'success export : %s' % filename
        return data_dict

    def export_overtime(self,uids = None,start_time = 0,end_time = None):
        '''
        导出用户的加班时间
        :param uid:
        :return:
        '''
        if isinstance(uids,basestring):
            uids = [uids]
        if not uids:
            res = self.mysql.select('user',condition={'employee_id':('<>',0)},fields = ('id','nickname'),
                                order='id ASC')
        elif len(uids) == 1:
            res = self.mysql.select('user',condition=[('id',uids[0])],fields = ('id','nickname'),
                                order='id ASC')
        else:
            res = self.mysql.select('user',condition=[('id','in',uids)],fields = ('id','nickname'),
                                order='id ASC')
        if not res:
            print '请选择有效用户'
            return None

        filepath= os.path.join(APP_ROOT,'data/export')
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        if end_time is None:
            end_time = int(time.time())

        filename = os.path.join(filepath,'overtime_%s.xlsx' % (int(time.time()),))
        wb = xlsxwriter.Workbook(filename)
        merge_format = wb.add_format({'align': 'center','valign':'vcenter','bold':True,'size':12,'font':u'宋体'})
        format = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1})
        for row in res:
            ret = self.mysql.select('overtime',condition=[('uid',row[0]),('status',1),('occur_time','>=',start_time),
                                                          ('occur_time','<=',end_time)],fields=('ymd','clock_time',
                                                            'occur_time','overtime'),order='occur_time DESC')
            try:
                ws = wb.add_worksheet(row[1])
            except Exception as e:
                if 'already in use' in str(e):
                    ws = wb.add_worksheet('%s_%s' % (row[1],row[0]))
            ws.set_row(0,35)
            ws.set_column(0,4,10)
            ws.set_default_row(19)
            ws.merge_range(0,0,0,4,'%s 加班明细表' % (row[1],),merge_format)
            ws.write(1,0,'序号',format)
            ws.write(1,1,'日期',format)
            ws.write(1,2,'上班打卡时间',format)
            ws.write(1,3,'下班打卡时间',format)
            ws.write(1,4,'加班时间(H)',format)
            if not ret:
                continue
            i = 2
            j = 0
            for item in ret:
                j += 1
                ws.write(i,0,j,format)
                ws.write(i,1,item[0],format)
                ws.write(i,2,util.date(item[1],format = '%H:%M:%S') if item[1] else '-',format)
                ws.write(i,3,util.date(item[2],format = '%H:%M:%S'),format)
                ws.write(i,4,item[3],format)
                i += 1
        wb.close()
        print 'success export : %s' % filename
        return filename

    def export_holiday_check(self,uids = None,ym = None):
        '''
        导出节假日打卡数据
        :param uids:
        :return:
        '''
        if not uids:
            return

        if not ym:
            res = self.mysql.select('user',condition=[('id',uids[0])],fields = ('id','nickname','employee_id','class_id'),
                                limit=1)
            if not res:
                print '请选择有效用户'
                return
            self.mysql.execute("SELECT MIN(`occur_time`),MAX(`occur_time`) FROM `hqjf_punch_data` WHERE "
                               "`uid` = %s",(res[0],))
            ret = self.mysql.fetchone()
            _min_gmtime = time.gmtime(ret[0])
            _max_gmtime = time.gmtime(ret[1])
            if _min_gmtime.tm_year != _max_gmtime.tm_year:
                _num = 13 - _min_gmtime.tm_mon + _max_gmtime.tm_mon + (_max_gmtime.tm_year - _min_gmtime.tm_year - 1) * 12
            else:
                _num = _max_gmtime.tm_mon - _min_gmtime.tm_mon + 1
            for i in xrange(0,_num):
                if _min_gmtime.tm_year != _max_gmtime.tm_year:
                    _y = int(math.floor((_min_gmtime.tm_mon + i - 1) / 12.0))
                    year = _min_gmtime.tm_year + _y
                    month = _min_gmtime.tm_mon + i - _y * 12
                else:
                    year = _min_gmtime.tm_year
                    month = _min_gmtime.tm_mon + i
                self.export_holiday_check(uids = uids,ym='%02d-%02d' % (year,month))
            return True
        res = self.mysql.select('user',condition=[('id',uids[0])],fields = ('id','nickname','employee_id','class_id'),
                                limit=1)
        day = calendar.monthrange(int(ym[0:4]),int(ym[5:7]))[1]
        _min_time = util.strtotime('%s-1 00:00' % ym)
        _max_time = util.strtotime('%s-%s 23:59' % (ym,day)) + 60
        ret = self.mysql.select('punch_data',condition=[('employee_id',res[2]),('occur_time','>=',_min_time),('occur_time','<=',_max_time)],
                                fields=('occur_time'),order='occur_time DESC',limit=1)
        if ret:
            _day = util.number_format(util.date(ret[0],format="%d"),0)
            #只统计有效数据的结果
            if _day < day:
                day = _day

        filepath= os.path.join(APP_ROOT,'data/export')
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        filename = os.path.join(filepath,'holiday_%s_%s.xlsx' % (res[0],ym))
        wb = xlsxwriter.Workbook(filename)
        ws = wb.add_worksheet(res[1])
        merge_format = wb.add_format({'align': 'center','valign':'vcenter','bold':True,'size':12,'font':u'宋体'})
        format = wb.add_format({'align': 'center','valign':'vcenter','size':9,'font':u'宋体','border':1})
        ws.set_row(0,35)
        ws.set_column(0,4,15)
        ws.set_default_row(19)
        ws.merge_range(0,0,0,3,'%s 节假日打卡明细表' % (res[1],),merge_format)
        ws.write(1,0,'序号',format)
        ws.write(1,1,'日期',format)
        ws.write(1,2,'上班打卡时间',format)
        ws.write(1,3,'下班打卡时间',format)
        i = 2
        j = 0
        for d in xrange(1,day + 1):
            _start_time = _min_time + (d - 1) * 86400
            _end_time = _min_time + d * 86400
            ret = self.mysql.select('punch_data',condition=[('employee_id',res[2]),('is_valid',1),
                                                                ('occur_time','>=',_start_time),
                                                                ('occur_time','<',_end_time)],fields = ('occur_time'),
                                        limit=2,order='occur_time ASC')
            if not ret:
                continue
            _worktime = self.get_worktime(res[3],occur_time = _start_time + 1000)
            if _worktime is not None and _worktime[7]:
                continue
            j += 1
            _num = len(ret)
            ws.write(i,0,j,format)
            ws.write(i,1,'%s-%s' % (ym,d),format)
            ws.write(i,2,util.date(ret[0][0],format = '%H:%M:%S'),format)
            ws.write(i,3,util.date(ret[1][0],format = '%H:%M:%S') if _num > 1 else '-',format)
            i += 1
        wb.close()
        print 'success export : %s' % filename
        return filename


    def get_late_time(self,uid = 0,ymd = None,status = 0):
        '''
        获取迟到时间
        :param ym:
        :param status:
        :param uid:
        :return:
        '''
        if not uid:
            return None
        if ymd is None:
            ymd = util.date(format="%Y-%m-%d")
        _min_time = util.strtotime('%s 00:00' % ymd)
        _max_time = _min_time + 86400
        ret = self.mysql.query("SELECT min(`occur_time`),max(`occur_time`) FROM `hqjf_punch_data` WHERE `occur_time` >= %s AND "
                               "`occur_time` < %s AND `uid` = %s LIMIT 1",(_min_time,_max_time,uid))
        if not ret:
            return None
        if status == 0:
            return ret[0]
        try:
            return ret[0][status - 2] if status <= 3 else ret[0][status - 6]
        except:
            return ret[0]

    def get_department(self,department_id = 0):
        '''
        获取部门信息
        :return:
        '''
        if department_id:
            res = self.mysql.select('department',condition={'department_id':department_id},fields=('department_id',
                                                                        'department_company','department_name'),limit=1)
        else:
            res = self.mysql.select('department',fields=('department_id','department_company','department_name'))
        return res

    def get_user_status_desc(self,uid = 0,status = 0,ymd = None):
        '''
        获取考勤状态描述
        :param status:
        :return:
        '''
        if status == 2 or status == 3:
            _time = self.get_late_time(uid=uid,ymd=ymd,status=status)
            comment = '迟到' if status == 2 else '早退'
            if _time:
                comment += "，实际打卡时间 %s" % util.date(_time,format="%H:%M:%S")
            return comment
        elif status in (4,5,6,7,8):
            if status == 4:
                comment = "上班未打卡"
            elif status == 5:
                comment = "下班未打卡"
            elif status == 8:
                comment = "上下班均未打卡"
            else:
                _time = self.get_late_time(uid=uid,ymd=ymd,status=status)
                comment = "%s未打卡" % ('迟到下班' if status == 6 else '早退上班',)
                if _time:
                    comment += ",实际打卡时间 %s" % util.date(_time,format="%H:%M:%S")
            return comment
        elif status == 10:
            _time = self.get_late_time(uid=uid,ymd=ymd)
            comment = "迟到并早退"
            if _time:
                comment += "，实际打卡时间\n上班 %s\n下班 %s" % (util.date(_time[0],format="%H:%M:%S"),
                                                          util.date(_time[1],format="%H:%M:%S"))
            return comment
        elif status == 11:
            _time = self.get_late_time(uid=uid,ymd=ymd)
            comment = '旷工'
            if _time:
                comment += "，实际打卡时间%s\n折旷工一天" % (util.date(_time[0],format="%H:%M:%S"),)
            return comment
        return None


    def save_notification(self,uid = 0,status = 0,ymd = None,email = None):
        '''
        保存考勤异常提醒信息
        :return:
        '''
        if ymd is None:
            ymd = util.date(format="%Y-%m-%d")

        _end_time =  util.strtotime('%s 00:00' % ymd) + 86400 * 7
        _unix_time = int(time.time())
        if _unix_time >= _end_time:
            return

        _fromkey = util.md5('%s_%s_%s' % (uid,ymd,status))
        ret = self.mysql.select('notification',condition={'fromkey':_fromkey},fields=('id'))
        if ret:
            return

        _desc = self.get_user_status_desc(uid=uid,status = status,ymd=ymd)
        if not _desc:
            return

        _end_time = util.date(_end_time,format="%Y-%m-%d")
        _title = '考勤异常提醒'
        _note = '您好：<br/>&nbsp;&nbsp;&nbsp;&nbsp;您的异常考勤情况如下: %s %s,请于 %s ' \
                '下班前补单,过期无效。' % (ymd,_desc,_end_time)

        self.mysql.insert('notification',data={
            'uid':uid,
            'type':'system',
            'note':_note,
            'title':_title,
            'create_time':_unix_time,
            'fromkey':_fromkey,
        })

        if not email:
            return

        self.mysql.insert('mailqueue',data={
            'touid':uid,
            'tomail':email,
            'frommail':'',
            'subject':_title,
            'message':_note,
            'create_time':_unix_time,
        })
        return True


    def send_email_notification(self):
        '''
        发送email提醒
        :return:
        '''
        res = self.mysql.select('mailqueue',fields=('mailid','tomail','subject','message','failnum'),limit=50)
        if not res:
            print '暂无需要发送的email提醒'
            return
        for row in res:
            if row[4] >= 6:
                self.mysql.delete('mailqueue',condition={'mailid':row[0]})
                continue
            ret = util.sendmail(row[1],subject=row[2],body=row[3])
            if ret:
                self.mysql.delete('mailqueue',condition={'mailid':row[0]})
                print '发送email提醒成功: %s' % (row[1],)
            else:
                self.mysql.update('mailqueue',condition={'mailid':row[0]},data={'failnum':row[4] + 1})
                print '发送email提醒失败: %s' % (row[1],)
        return True

    def ask(self,message = None,ask_list = ('y','n','q')):
        '''
        询问
        :param code:
        :param message:
        :return:
        '''
        if message is None:
            message = '请选择操作'
        message = '%s (%s) :' % (message,'/'.join(ask_list))
        while True:
            _s = raw_input(message)
            if _s in ask_list:
                return _s

    def exit(self):
        '''
        退出处理
        :return:
        '''
        self.mysql.close()
        try:
            self.conn.close()
        except:
            pass


class WebCron(SyncCron):
    '''
    站点任务
    '''

    def __init__(self):
        if not self.connect_mysql():
            return
        res = self.mysql.select('export',condition={'status':0},fields=('id','args','type','key'),limit=10)
        if not res:
            print '导出任务为空'
        else:
            for row in res:
                try:
                    args = json.loads(row[1])
                except:
                    self.mysql.update('export',condition={'id':row[0]},data={'status':3})
                    continue
                filepath = os.path.join(EXPORT_PATH,row[3][0:2])
                if not os.path.exists(filepath):
                    os.makedirs(filepath)
                filename = os.path.join(filepath,'%s.xlsx' % row[3])
                if row[2] == 'stat':
                    self.export_stat(row[0],filename,ym = args.get('ym',None),department_id = args.get('department',0))
                elif row[2] == 'overtime':
                    self.export_overtime(row[0],filename,uids=args.get('uids',0),start_time = args.get('start_time',0),
                                         end_time = args.get('end_time',None))


    def export_stat(self,cronid,filename,ym = None,department_id = 0):
        _unix_time = int(time.time())
        if ym is None or not department_id:
            self.mysql.update('export',condition={'id':cronid},data={'status':3,'update_time':_unix_time})
            return
        self.mysql.update('export',condition={'id':cronid},data={'status':2,'update_time':_unix_time})
        res = super(WebCron, self).export_stat(ym=ym,department_id=department_id)
        _unix_time = int(time.time())
        if isinstance(res,dict):
            ret = res.get(department_id,None)
            if ret is None:
                self.mysql.update('export',condition={'id':cronid},data={'status':4,'update_time':_unix_time})
            else:
                print '转移文件至 %s 中' % filename
                shutil.move(ret,filename)
                self.mysql.update('export',condition={'id':cronid},data={'status':1,'update_time':_unix_time,
                                                                         'process':100})
        else:
            self.mysql.update('export',condition={'id':cronid},data={'status':3,'update_time':_unix_time})

    def export_overtime(self,cronid,filename,uids = None,start_time = 0,end_time = None):
        _unix_time = int(time.time())
        self.mysql.update('export',condition={'id':cronid},data={'status':2,'update_time':_unix_time})
        ret = super(WebCron, self).export_overtime(uids = uids,start_time=start_time,end_time=end_time)
        _unix_time = int(time.time())
        try:
            print '转移文件至 %s 中' % filename
            shutil.move(ret,filename)
            self.mysql.update('export',condition={'id':cronid},data={'status':1,'update_time':_unix_time,
                                                                         'process':100})
        except:
            self.mysql.update('export',condition={'id':cronid},data={'status':3,'update_time':_unix_time})



def main():
    parser = argparse.ArgumentParser(description = u"华强聚丰电子考勤数据处理",add_help=False)

    sys_group = parser.add_argument_group(title = u'运行可选参数')
    sys_group.add_argument('-h','--help',dest='help',help=u'获取帮助信息',action = 'store_true',default = False)
    sys_group.add_argument('-v','--version',dest='version',help=u'获取版本信息',action = 'store_true',default = False)

    import_group = parser.add_argument_group(title = u'导入华强聚丰用户数据')
    import_group.add_argument('-i','--import',dest='action',help=u'导入华强聚丰用户数据',action = 'store_const',
                        const = 'import')
    import_group.add_argument('-f','--file',dest='file',help=u'指定导入的文件，指定格式的excel文件')
    import_group.add_argument('--column',dest='column',help=u'指定excel文件起始列数（默认为3 即 第3列开始读取数据，'
                                                            u'请根据实际excel文件填写）',type=int,default=3)
    import_group.add_argument('--row',dest='row',help=u'指定excel文件起始行数（默认为3 即 第3行开始读取数据，'
                                                            u'请根据实际excel文件填写）',type=int,default=3)

    bind_group = parser.add_argument_group(title = u'绑定员工信息')
    bind_group.add_argument('-b','--bind',dest='action',help=u'将user绑定华强员工信息',action = 'store_const',
                        const = 'bind_user')

    sync_group = parser.add_argument_group(title = u'同步打卡数据操作')
    sync_group.add_argument('-s','--sync',dest='action',help = u'同步打卡数据及员工数据',action = 'store_const',
                        const = 'sync')
    sync_group.add_argument('--employee',dest='sync',help = u'同步员工数据', action='append_const',
                        const='employee')
    sync_group.add_argument('--kaoqing',dest='sync',help = u'同步考勤数据', action='append_const',
                        const='kaoqing')

    stat_group = parser.add_argument_group(title = u'统计用户打卡考勤数据')
    stat_group.add_argument('--stat',dest='action',help=u'统计考勤数据',action = 'store_const',
                        const = 'stat')
    stat_group.add_argument('--year',dest='year',help=u'指定统计数据的年份（默认为当前年份）',type=int)
    stat_group.add_argument('--month',dest = 'month',help = u'指定统计数据的月份（默认为当前月份）',type=int)
    stat_group.add_argument('--day',dest = 'day',help = u'指定统计数据的最大日期（默认为有效数据的最大日期）',type=int)
    stat_group.add_argument('--flag',dest = 'flag',help = u'是否重新标记有效数据（如果数据中途终止过建议设置为）',
                            action = 'store_true',default = False)

    export_group = parser.add_argument_group(title = u'导出Excel数据')
    export_group.add_argument('-e','--export',dest='action',help=u'导出Excel数据',action = 'store_const',
                        const = 'export')
    export_group.add_argument('--ym',dest='ym',help=u'指定需要导出的考勤年月(格式YYYY-mm默认为当前年月)',default = None)
    export_group.add_argument('-d','--department-id',dest='department_id',help=u'指定需要导出数据的部门ID（0为所有）',
                              default = 0,type = int)
    export_group.add_argument('-u','--uid',dest='uid',help=u'指定需要导出数据的用户（0为所有）',nargs='+')
    export_group.add_argument('-p','--print-department',dest = 'print_department',help=u'打印出所有部门及对应的ID',
                              action = 'store_true',default = False)

    export_group.add_argument('--overtime',dest='overtime',help=u'导出加班数据',action = 'store_const',
                        const = 'overtime')

    export_group.add_argument('--holiday',dest='holiday',help=u'导出假期打卡数据(对比)',action = 'store_const',
                        const = 'holiday')

    notice_group = parser.add_argument_group(title = u'邮件提醒')
    notice_group.add_argument('-n','--notice',dest='action',help = u'发送邮件提醒',action = 'store_const',
                        const = 'notice')
    notice_group.add_argument('-t','--time',help = u'指定多长时间执行一次，即间隔时间(默认0s为仅执行一次)',
                              default = 0,type = int,dest = 'sleep_time')

    web_group = parser.add_argument_group(title = u'站点导出任务')
    web_group.add_argument('-w','--web',dest='action',help = u'站点导出任务',action = 'store_const',
                        const = 'web')

    args = parser.parse_args()
    if args.help:
        parser.print_help()
        print "\n帮助示例\n"
        print " 导入华强聚丰用户数据      %s -i --file=data/hqjf.xlsx" % sys.argv[0]
        print " 指定起始行列导入用户数据  %s -i --file=data/hqjf.xlsx --column=1 --row=1" % sys.argv[0]
        print " 将user绑定华强员工信息    %s -b" % sys.argv[0]
        print " 同步员工并统计打卡数据    %s -s" % sys.argv[0]
        print " 仅同步员工数据            %s -s --employee" % sys.argv[0]
        print " 统计所有考勤数据          %s --stat" % sys.argv[0]
        print " 统计所有考勤数据(重新标记有效数据)  %s --stat --flag" % sys.argv[0]
        print " 统计指定年月考勤数据      %s --stat --year=2015 --month=1" % sys.argv[0]
        print " 运行考勤异常通知          %s -n -t 60" % sys.argv[0]
        print " 导出考勤统计              %s -e -d 0" % sys.argv[0]
        print " 导出考勤统计(指定时间)    %s -e --ym=2015-01 -d 0" % sys.argv[0]
        print " 导出考勤统计(指定部门)    %s -e --ym=2015-01 -d 1" % sys.argv[0]
        print " 导出考勤统计(打印部门)    %s -e -p" % sys.argv[0]
        print " 导出加班详情表            %s -e --overtime" % sys.argv[0]
        print " 导出加班详情表(指定用户)  %s -e --overtime -u 1 2 3" % sys.argv[0]
        print " 导出指定用户假期打卡详情  %s -e --holiday -u 9" % sys.argv[0]
        print " 导出指定用户假期打卡详情(指定时间)  %s -e --holiday -u 9 --ym=2015-01" % sys.argv[0]
        print " 站点导出任务              %s -w -t 60" % sys.argv[0]
        print
    elif args.action:
        if args.action == 'import':
            SyncCron().import_jf_user_data(fname=args.file,column=args.column,rownum=args.row)
        elif args.action == 'bind_user':
            SyncCron().bind_employee_info()
        elif args.action == 'sync':
            s = SyncCron()
            if not s.connect_mssql():
                return
            if not args.sync:
                args.sync = ['employee', 'kaoqing']
            s.sync_machine()
            if 'employee' in args.sync:
                s.sync_employee()
                s.bind_employee_info()
            if 'kaoqing' in args.sync:
                s.sync_punch_data()
        elif args.action == 'stat':
            SyncCron().stat_check_data(year=args.year,month=args.month,day=args.day,flag = args.flag)
        elif args.action == 'export':
            s = SyncCron()
            departments = s.get_department()
            uids = args.uid and [util.number_format(i,0) for i in args.uid if util.number_format(i,0) > 0 ]
            if not departments:
                print '暂无部门，请先添加部门'
                return
            if args.print_department:
                th = (u'部门ID', u'部门名称', u'所属公司')
                print(u'%s\t%s\t%s' % (th[0].ljust(16),
                                    th[1].ljust(16),
                                    th[2].ljust(16)))
                print '-' * 66
                for row in departments:
                     print(u'%s\t%s\t%s' % (str(row[0]).ljust(16),
                                                    row[2].decode('utf-8', 'ignore').ljust(16),
                                                    row[1].decode('utf-8', 'ignore').ljust(16)))
                print
            elif args.holiday:
                s.export_holiday_check(uids,ym=args.ym)
            elif args.overtime:
                s.export_overtime(uids)
            else:
                departments_list = [k[0] for k in departments]
                if args.department_id and args.department_id not in departments_list:
                    print '请选择一个有效的部门ID'
                    return
                s.export_stat(ym=args.ym,department_id=args.department_id)
        elif args.action == 'notice':
            s = SyncCron()
            if args.sleep_time > 0:
                while True:
                    s.send_email_notification()
                    print '---- sleep %s s ----' % args.sleep_time
                    time.sleep(args.sleep_time)
            else:
                s.send_email_notification()
        elif args.action == 'web':
            if args.sleep_time > 0:
                while True:
                    WebCron()
                    print '---- sleep %s s ----' % args.sleep_time
                    time.sleep(args.sleep_time)
            else:
                WebCron()
    else:
        parser.print_usage()

if __name__ == '__main__':
    main()