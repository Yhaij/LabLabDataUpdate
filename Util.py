#!/user/bin/python
# -*- coding: utf-8 -*-
import codecs
import datetime


class Util(object):
    @staticmethod
    def get_last_year():
        """
        得到最近一次更新的论文的年份
        :return: 
        """
        fp_year = codecs.open(r"./journalInfo/start_time.txt", "rb", encoding='utf-8')
        oldYear = str()
        for n in fp_year:
            oldYear = n
        oldYear = int(oldYear.strip())
        fp_year.close()
        return oldYear

    def update_year(self):
        """
        跟新年份
        :return: 
        """
        fp_year = codecs.open(r"./journalInfo/start_time.txt", "a+", encoding='utf-8')
        fp_year.write("%s\n" % datetime.datetime.now().year)
        fp_year.close()
