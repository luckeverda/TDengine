###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import taos
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        
        self.tag_dict = {
            't1': 'int',
            't2': f'binary({self.binary_length})'
        }
        
        self.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        
        self.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        
        self.tbnum = 4

    def create_user(self):
        user_name = 'test'        
        tdSql.execute(f'create user {user_name} pass "test"')
        tdSql.execute(f'grant read on db.stb with t2 = "Beijing" to {user_name}')
                
    def prepare_data(self):
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
    
    def user_privilege_check(self):
        testconn = taos.connect(user='test', password='test')        
        expectErrNotOccured = False
        
        try:
            sql = "select count(*) from db.stb where t2 = 'Beijing'"
            res = testconn.query(sql)
            data = res.fetch_all()
            count = data[0][0]            
        except BaseException:
            expectErrNotOccured = True
        
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        elif count != 1:
            tdLog.exit(f"{sql}, expect result doesn't match")
        pass
    
    def user_privilege_error_check(self):
        testconn = taos.connect(user='test', password='test')        
        expectErrNotOccured = False
        
        sql_list = ["alter talbe db.stb_1 set t2 = 'Wuhan'", "drop table db.stb_1"]
        
        for sql in sql_list:
            try:
                res = testconn.execute(sql)                        
            except BaseException:
                expectErrNotOccured = True
            
            if expectErrNotOccured:
                pass
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured")
        pass

    def run(self):
        tdSql.prepare()        
        self.prepare_data()
        self.create_user()
        self.user_privilege_check()
        self.user_privilege_error_check()
                
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())