#!/user/bin/python
# -*- coding: utf-8 -*-

from base import BaseClass
from public.time_printer import print_time
import re
import codecs


class PaperOperate(BaseClass):
    def __init__(self, conn, table, debug=True):
        # 初始化其父类，否则继承父类的方法中的私有变量会不一致
        super(PaperOperate, self).__init__(conn, table, debug)
        self.__conn = conn
        self.__cur = conn.cursor()
        self.__table = table
        self.debug = debug

    def get_debug(self):
        print self.debug

    @print_time
    def update_unit_from_area(self, step=100000):
        """
        根据unit_areacode_map_new表来更新paper的unit和area_code字段
        :return: 
        """
        print "===开始根据具体单位匹配来更新%s表的unit===" % self.__table
        self.show_debug()
        unit_table = BaseClass(self.__conn, 'unit_areacode_map_new')
        columns = ['unit', 'area_code']
        citys = unit_table.select_term(*columns) # 得到数据库中的unit和area_code
        columns = ['id', 'first_organization', 'unit', 'area_code']
        for infos in self.select_step(*columns, step=step):
            for info in infos:
                id, first_organization, old_unit, old_area_code = info
                print id, first_organization, old_unit, old_area_code
                if old_area_code is None or old_unit is None or old_unit == '':
                    if old_unit is not None and old_unit is not '':
                        first_organization = old_unit
                    if first_organization is None:
                        continue
                    for unit, areacode in citys:
                        if first_organization.startswith(unit):
                            new_unit = unit
                            new_areacode = areacode
                            a = {'unit': new_unit, 'area_code': new_areacode}
                            where_str = "id = %s" % id
                            self.update(where=where_str, **a)
                            break
            self.sql_commit()

        print "===根据具体单位匹配来更新%s表的unit完成===" % self.__table

    @print_time
    def update_unit_from_city(self, step=100000):
        """
        根据unit_citycode_map表省，市字段匹配来更新单位
        :return: 
        """
        print "===开始根据省，市字段匹配来更新%s表的unit===" % self.__table
        if self.debug is False:
            print "【run】模式"
        else:
            print "【debug】模式"
        unit_table = BaseClass(self.__conn, 'unit_citycode_map')
        columns = ['unit', 'area_code']
        citys = unit_table.select_term(*columns)  # 得到数据库中的unit和area_code
        columns = ['id', 'first_organization', 'unit', 'area_code']
        for infos in self.select_step(*columns, step=step):
            for info in infos:  # 逐个论文查看
                id, first_organization, old_unit, old_area_code = info
                print id, first_organization, old_unit, old_area_code
                if old_unit is None or old_unit is '':
                    for j in range(len(citys)):  # 逐条匹配省，市，县
                        standard_unit = citys[j][0]
                        standard_area_code = citys[j][1]
                        if first_organization.find(standard_unit) >= 0:
                            new_unit = re.sub('\(.*?\)', '', first_organization)
                            new_unit = re.search(u'[^\d\s\(\)（）].*$', new_unit)
                            if new_unit:
                                new_unit = re.split("!| |;|/", new_unit.group(0))[0]
                                new_area_code = standard_area_code
                                where_str = "id = %s" % id
                                a = {'unit': new_unit, 'area_code': new_area_code}
                                self.update(where=where_str, **a)
                            break
            self.sql_commit()

        print "===根据省，市字段匹配来更新%s表的unit完成===" % self.__table

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
        columns = ["id", "PAPER_ID", "concat_ws(' ', name, abstract, keywords)"]
        for infos in self.select_step(*columns, step=step):
            for info in infos:
                line = str(info[1]) + ',' + info[2].replace('\n', '').replace('\r\n', '')
                print '--->%s' % line
                if self.debug is False:
                    fp.write('%s\r\n' % line.decode('utf-8'))
        fp.close()
        print '===导出%s表语料完成===' % self.__table

