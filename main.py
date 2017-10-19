#!/user/bin/python
# -*- coding: utf-8 -*-
from operate import base_operate
from operate.base import BaseClass
import codecs
from Util import Util
from redis_operate import RecisClusterOperate
from redis_operate.RedisStruct import RedisHash
from DispatchControl import PaperJournalReptileDispatch, PaperReptileDispatch
from PaperReptile import PaperReptile
import time
import re
import Util
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

year = Util.get_last_year()


def sql_get_paper_data():
    print "开始从数据库读入相关信息"
    sqlConn = base_operate.connection_sql("techpooldata")
    papertable = BaseClass(sqlConn, "paper")
    columns = ["PAPER_ID", "url"]
    fp_w = codecs.open(r"result\paper%s" % year, "wb", encoding="utf-8")
    for infos in papertable.select_term(*columns, where="year = '%s' and url is not NULL" % year):
        uuid, url = infos
        fp_w.write("%s\t%s\n" % (uuid, url))
    fp_w.close()
    print "读取信息成功"


def get_fileName_from_url(url):
    """
    从url得到fileName(论文在知网中的唯一标志)
    :param url: 
    :return: 
    """
    pattern = re.compile("filename=(.*?)&")
    match = pattern.search(url)
    if match:
        return match.group(1)
    else:
        return None


def export_paper_data_to_redis(redisHash):
    print "开始导入数据到redis"
    fp_r = codecs.open(r"result\paper%s" % year, "rb", encoding="utf-8")
    for line in fp_r:
        uuid, url = line.split('\t')
        fileName = get_fileName_from_url(url)
        if fileName:
            redisHash.set(fileName, uuid)
    fp_r.close()
    print "导入redis成功"


def get_paper_url(paperReptileDispatch, redis):
    print "开始获取paper url 从paper journal 目录"
    fp = codecs.open(r"./journalInfo/journalNameCodes.txt", "rb", encoding='gb2312')
    fw_paper_url = codecs.open(r"./journalInfo/update_paper_url.txt", "wb", encoding='utf-8')
    pjrd = PaperJournalReptileDispatch(redis)
    for line in fp:
        infos = line.strip().split(",")
        pjrd.put(infos[0], ",".join(infos[1: -1]), infos[-1])
    for journalInfo, dbCode, fileName in pjrd.dispatch():
        fw_paper_url.write("%s\t%s\n" % (
        "\t".join(journalInfo), paperReptileDispatch.put(journalInfo, PaperReptile.url_create(dbCode, fileName))))
    fw_paper_url.close()
    fp.close()
    print "获取完成"


def get_paper_url_from_file(paperReptileDispatch):
    """
    从文件中读取论文队列，为了恢复
    :param paperReptileDispatch: 
    :return: 
    """
    print "开始从文件中读取paper url"
    fp = codecs.open(r"./journalInfo/update_paper_url.txt", "rb", encoding='utf-8')
    for line in fp:
        infos = line.strip().split("\t")
        # print infos[0:5], infos[-1]
        paperReptileDispatch.put(infos[0:5], infos[-1])
    fp.close()
    print "读取完成"


def output_result(paperReptileDispatch):
    print "开始输出增量爬取的论文信息"
    fp_w = codecs.open(r"result\add_paper%s.txt" % time.strftime('%Y-%m-%d', time.localtime(time.time())), "wb",
                       encoding='utf-8')
    outFormat = lambda s: s.strip().replace("\t", "") if s else "null"
    for dInfo in paperReptileDispatch.dispatch():
        name = outFormat(dInfo['name'])
        authors = outFormat(dInfo['authors'])
        authors_code = outFormat(dInfo['authors_code'])
        organization = outFormat(dInfo['organization'])
        first_organization = organization.split(";")[0]
        keywords = outFormat(dInfo['keywords'])
        abstract = outFormat(dInfo['abstract'])
        classification = outFormat(dInfo['classification'])
        zw_subject_code = outFormat(dInfo['zw_subject_code'])
        journal_name = outFormat(dInfo['journal_name'])
        journal_no = outFormat(dInfo['journal_no'])
        journal_url = outFormat(dInfo['journal_url'])
        url = outFormat(dInfo['url'])
        first_author, second_author, third_author, fourth_author = authors_part(authors.split(";"))
        first_author_code, second_author_code, third_author_code, fourth_author_code = authors_part(
            authors_code.split(";"))
        result = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
                 % (name, authors, authors_code, first_organization, organization, first_author, first_author_code,
                    second_author, second_author_code, third_author, third_author_code, fourth_author,
                    fourth_author_code,
                    keywords, abstract, classification, zw_subject_code, journal_name, journal_no, journal_url, url)
        print result,
        fp_w.write(result)
    fp_w.close()
    print "输出结束，增量爬取论文结束"


def authors_part(authors):
    a1 = "null"
    a2 = "null"
    a3 = "null"
    a4 = "null"
    if len(authors) == 1:
        a1 = authors[0]
    elif len(authors) == 2:
        a1, a2 = authors
    elif len(authors) == 3:
        a1, a2, a3 = authors
    elif len(authors) == 4:
        a1, a2, a3, a4 = authors
    return a1, a2, a3, a4


if __name__ == '__main__':
    sql_get_paper_data()  # 从数据库读入该年的相关数据
    redis = RecisClusterOperate.connection_redis_cluster()  # 连接redis
    redisHash = RedisHash(redis, "paper%s" % year)  # 生成redis中的hash对象
    export_paper_data_to_redis(redisHash)  # 将数据库的数据导入redis，去重
    prd = PaperReptileDispatch()
    get_paper_url(prd, redisHash)  # 爬取从paper期刊页面爬取paper的url
    # get_paper_url_from_file(prd)
    output_result(prd)  # 输出爬取结果到控制台和文件中
    Util.Util.update_year()  # 更新最近更新时间
