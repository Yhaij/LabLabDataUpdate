#!/user/bin/python
# -*- coding: utf-8 -*-

from public.time_printer import print_time
from Base import base_operate
from base import BaseClass
import codecs


class PatentOperate(BaseClass):
    def __init__(self, conn, table, debug=True):
        super(PatentOperate, self).__init__(conn, table, debug)
        self.__conn = conn
        self.__cur = conn.cursor()
        self.__table = table
        self.debug = debug

    @print_time
    def extract_unit_to_map(self, map_table, step=100000):
        print "===开始从%s中抽取单位===" % self.__table
        colunms = ["id", "applicant", "inventors", "area_code"]
        for infos in self.select_step(*colunms, step=step):
            for info in infos:
                id = info[0]
                applicant = info[1].split(';')[0]
                inventors = info[2].split(';')
                area_code = info[3]
                print id, applicant, area_code,
                if applicant not in inventors:
                    print 'unit is ok'
                    add_dict = {'unit': applicant, 'area_code': area_code}
                    insert_sql = base_operate.add_sql(self.__cur, map_table, **add_dict)
                    if self.debug is False:
                        self.__cur.execute(insert_sql)
                else:
                    print 'unit is not ok'
            self.sql_commit()

        print "===从%s抽取单位结束===" % self.__table

    @print_time
    def pick_each_inventor(self, step=100000):
        print "===开始抽取%s中的inventor成员===" % self.__table
        columns = ["id", "inventors"]
        for infos in self.select_step(*columns, step=step):
            for info in infos:
                id, inventor = info
                print id, inventor
                second_inventor = ''
                third_inventor = ''
                fourth_inventor = ''
                if ';' in inventor:
                    inventors = inventor.split(';')
                    inventor_count = len(inventors)
                    if inventor_count == 2:
                        second_inventor = inventors[1]
                    elif inventor_count == 3:
                        second_inventor = inventors[1]
                        third_inventor = inventors[2]
                    elif inventor_count > 3:
                        second_inventor = inventors[1]
                        third_inventor = inventors[2]
                        fourth_inventor = inventors[3]
                update_dict = {'second_inventor': second_inventor, 'third_inventor': third_inventor, 'fourth_inventor': fourth_inventor}
                where_str = 'id = %s' % id
                self.update(where=where_str, **update_dict)

            self.sql_commit()

        print "===从%s抽取inventor成员结束===" % self.__table

    @print_time
    def export_corpus(self, file_name, step=100000):
        print '===开始导出%s表语料===' % self.__table
        if self.debug is False:
            print "【run】模式"
        else:
            print "【debug】模式"
        path = r'result/%s' % self.__table
        self.create_dir(path)  # 判断路径是否存在
        file_path = r'%s/%s' % (path, file_name)
        print "打开文件 %s 准备存储数据" % file_path
        fp = codecs.open(file_path, 'wb', encoding='utf8')
        columns = ["id", "PATENT_ID", "concat_ws(' ', name, abstract)"]
        for infos in self.select_step(*columns, step=step):
            for info in infos:
                line = str(info[1]) + ',' + info[2].replace('\n', '').replace('\r\n', '')
                print '--->%s' % line
                if self.debug is False:
                    fp.write('%s\r\n' % line.decode('utf-8'))
        fp.close()
        print '===导出%s表语料完成===' % self.__table