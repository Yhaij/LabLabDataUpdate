#!/user/bin/python
# -*- coding: utf-8 -*-
from math import ceil
import os
import codecs


class BaseClass(object):
    debug = True

    def __init__(self, conn, table, debug=True):
        self.__conn = conn
        self.__cur = conn.cursor()
        self.__table = table
        self.debug = debug

    def set_debug(self, debug):
        self.debug = debug

    def show_debug(self):
        if self.debug is True:
            print "【调试】模式"
        else:
            print "【执行】模式"

    def sql_commit(self):
        if self.debug is False:
            self.__conn.commit()

    def get_total_count(self):
        select_sql = self.__select_sql("count(*)")
        self.__cur.execute(select_sql)
        total_count = self.__cur.fetchall()[0][0]
        print "%s表共有%d条记录" % (self.__table, total_count)
        return total_count

    def get_loop_times(self, step):
        total_count = self.get_total_count()
        loop_time = int(ceil(total_count / float(step)))
        return loop_time

    def select_all(self):
        return self.select_term()

    def select_step(self, *args, **kwargs):
        if 'step' in kwargs.keys():
            step = int(kwargs.pop('step'))
        else:
            step = 100000
        for i in range(self.get_loop_times(step)):
            limit_str = '%d, %d' % (i*step, step)
            kwargs['limit'] = limit_str
            yield self.select_term(*args, **kwargs)

    def select_term(self, *args, **kwargs):
        select_sql = self.__select_sql(*args, **kwargs)
        self.__cur.execute(select_sql)
        return self.__cur.fetchall()

    def insert(self,  **kwargs):
        insert_sql = self.__insert_sql(**kwargs)
        if self.debug is False:
            return self.__cur.execute(insert_sql)
        else:
            return None

    def update(self, where='', **kwargs):
        update_sql = self.__update_sql(where, **kwargs)
        if self.debug is False:
            return self.__cur.execute(update_sql)
        else:
            return None

    def __select_sql(self, *args, **kwargs):
        """
        数据库查询
        :param args: 查询的字段
        :param kwargs: where=where语句  limit=limit语句  group by=group by语句
        :return: 
        :example: 
        """
        where_str = ''
        limit_str = ''
        group_str = ''
        for key in kwargs:
            if key == 'where':
                where_str = 'where %s' % kwargs[key]
            if key == 'limit':
                limit_str = 'limit %s' % kwargs[key]
            if key == 'group_by':
                group_str = 'group by %s' % kwargs[key]
        column_total = ''
        if args:
            for column in args:
                column_total = column_total + '%s,' % column
            column_total = column_total[:-1]
        else:
            column_total = '*'
        sql_str = "select %s from %s %s %s %s" % (column_total, self.__table, where_str, group_str, limit_str)
        print '[SQL]  %s' % sql_str
        return sql_str

    def __insert_sql(self, **kwargs):
        """
        数据库插入
        :param kwargs: 插入的字段名 = 插入的值
        :return: 
        example: add_sql(conn.cursor(), 'paper_total_04_24', id=1, unit='浙江', organization='杭电')
        """
        columns = ''
        values = ''
        for key in kwargs:
            columns = columns + '%s,' % key
            if isinstance(kwargs[key], str) or isinstance(kwargs[key], unicode):
                values = values + "'%s'," % kwargs[key]
            else:
                values = values + '%s,' % kwargs[key]
        columns = columns[:-1]
        values = values[:-1]
        sql_str = 'insert into %s (%s) values (%s)' % (self.__table, columns, values)
        print '[SQL]  %s' % sql_str
        return sql_str

    def __update_sql(self, where='', **kwargs):
        """
        数据库更新字段（输入参数感觉有点别扭）
        :param where: where=where语句
        :param kwargs: column=update value
        :return:
        :example: a = {'id': 2, 'he': 's'};update_sql(conn.cursor(), 'paper_total_04_24', where="id = '1'", **a)
        """
        if where:
            where_str = 'where %s' % where
        else:
            where_str = ''
        if kwargs:
            set_str = ''
        else:
            print '没有需要更新的的字段'
            return
        for key in kwargs:
            if isinstance(kwargs[key], str) or isinstance(kwargs[key], unicode):
                set_str = set_str + "%s='%s'," % (key, kwargs[key])
            else:
                set_str = set_str + '%s=%s,' % (key, kwargs[key])
        set_str = set_str[:-1]
        sql_str = "update %s set %s %s" % (self.__table, set_str, where_str)
        print '[SQL]  %s' % sql_str
        return sql_str

    @staticmethod
    def create_dir(path):
        current_dir = os.getcwd()  # 得到的路径是程序执行的入口路径
        path = current_dir + '\\' + path
        if not os.path.isdir(path):
            os.makedirs(path)  # 创建的文件夹是在main函数下的文件同级目录

    def export_data(self, file_name, *args, **kwargs):
        """
        从数据库导出需要的字段到文本
        :param file_name:文本名字 
        :param args: 字段
        :param kwargs: 
        :return: 
        """
        print '===开始导出%s表的%s数据===' % (self.__table, ','.join(args))
        if self.debug is False:
            print "【run】模式"
        else:
            print "【debug】模式"
        path = r'result\%s' % self.__table
        self.create_dir(path)
        file_path = path + '\\' + file_name
        fp = codecs.open(file_path, 'wb', encoding='utf8')
        for infos in self.select_step(*args, **kwargs):
            for info in infos:
                line = ' '.join([str(w).replace(" ", "") for w in info])
                line = line.replace('\n', '').replace('\r\n', '')
                print '--->%s' % line
                if self.debug is False:
                    fp.write('%s\r\n' % line)
        fp.close()
        print '===导出%s表的%s数据完成===' % (self.__table, ','.join(args))

