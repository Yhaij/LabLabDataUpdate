#!/user/bin/python
# -*- coding: utf-8 -*-
from base import BaseClass
from public.time_printer import print_time
import codecs


class ProjectOperate(BaseClass):
    def __init__(self, conn, table, debug=True):
        super(ProjectOperate, self).__init__(conn, table, debug)
        self.__conn = conn
        self.__cur = conn.cursor()
        self.__table = table
        self.debug = debug

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
        columns = ["id", "PROJECT_ID", "concat_ws(' ', name, keywords_ch, abstract_ch)"]
        for infos in self.select_step(*columns, step=step):
            for info in infos:
                line = str(info[1]) + ',' + info[2].replace('\n', '').replace('\r\n', '')
                print '--->%s' % line
                if self.debug is False:
                    fp.write('%s\r\n' % line.decode('utf-8'))
        fp.close()
        print '===导出%s表语料完成===' % self.__table