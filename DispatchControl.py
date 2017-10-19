#!/user/bin/python
# -*- coding: utf-8 -*-
import Queue
from PaperReptile import PaperReptile
from BaseReptile import BaseReptile
from PaperJournalReptile import PaperJournalReptile
import codecs
import time
import datetime
from Util import Util


class ReptileDispatch(object):
    """
    爬取的调度类（有待修改和完善）,以后可以加入ip代理
    """
    def __init__(self, queue=None):
        if queue:
            self.queue = queue
        else:
            self.queue = Queue.Queue()

    def put_url(self, url):
        self.queue.put(url)

    def thread_reptile(self):
        pass

    def dispatch(self, reptileType, model="get", params=None, **kwargs):
        while not self.queue.empty():
            url = self.queue.get()
            baseReptile = Factory.get_reptile(url, reptileType)
            while baseReptile.requestCode == -1:  # 可能是网站限制了爬虫，后续可能需要在这加入ip代理
                baseReptile.init(model, params, **kwargs)
                time.sleep(5)
            time.sleep(2)
            yield baseReptile.parse_html()


class PaperReptileDispatch(ReptileDispatch):
    """
    论文爬取的调度类
    """
    def __init__(self):
        super(PaperReptileDispatch, self).__init__()

    def put(self, journalInfo, url):
        # url = PaperReptile.url_create(dbCode, fileName)
        self.queue.put([journalInfo, url])
        return url

    def dispatch(self, model="get", params=None, **kwargs):
        fp_error = codecs.open(r"./result/request_paper_error.txt", "a+", encoding='utf-8')
        while not self.queue.empty():
            journalInfo, url = self.queue.get()
            baseReptile = PaperReptile(url)
            while baseReptile.requestCode == -1:  # 可能是网站限制了爬虫，后续可能需要在这加入ip代理
                baseReptile.init(model, params, **kwargs)
                time.sleep(5)
            time.sleep(1)
            paperDict = baseReptile.parse_html()
            if paperDict is None:
                fp_error.write("%s\t%s\n" % ("\t".join(journalInfo), baseReptile.url))
                fp_error.flush()
            else:
                subjectCode, journalName, pykm, year, issue = journalInfo
                paperDict["zw_subject_code"] = subjectCode
                paperDict["journal_name"] = journalName
                paperDict["journal_no"] = "%s年%s期" % (year, issue)
                paperDict["journal_url"] = "http://navi.cnki.net/KNavi/JournalDetail?pcode=CJFD&pykm=%s" % pykm
                paperDict["url"] = url
                yield paperDict
        fp_error.close()


class PaperJournalReptileDispatch(ReptileDispatch):
    """
    期刊爬取的调度类
    """
    def __init__(self, redis=None):
        super(PaperJournalReptileDispatch, self).__init__()
        self.redis = redis

    def put(self, subjectCode, journalName, pykm):
        self.queue.put([subjectCode, journalName, pykm])

    def dispatch(self, model="get", params=None, **kwargs):
        while not self.queue.empty():
            subjectCode, journalName, pykm = self.queue.get()
            print subjectCode, journalName, pykm
            for year in range(Util.get_last_year(), int(datetime.datetime.now().year) + 1):
                flag = 0
                for issue in range(1, 37):
                    if flag >= 3:  # 缺少三期及其以上不在遍历
                        break
                    paperJournalReptile = PaperJournalReptile(PaperJournalReptile.url_create(year, issue, pykm))  # 请求得到固定期刊的固定一期中的论文
                    while paperJournalReptile.requestCode == -1:  # 可能是网站限制了爬虫，后续可能需要在这加入ip代理
                        paperJournalReptile.init(model, params, **kwargs)
                        time.sleep(5)
                    paperInofs = paperJournalReptile.parse_html()
                    if paperInofs is None:
                        flag = flag + 1
                        print journalName, "不存在%d年%02d期" % (year, issue)
                    else:
                        for paperInfo in paperInofs:
                            # 将论文url存入调度类中，并写入文本  还需要一个redis判断，防止重复
                            dbCode, fileName = paperInfo
                            if not self.redis or not self.redis.exists_key(fileName): # 判读redis是否存在
                                journalInfo = [subjectCode, journalName, pykm, "%d" % year, "%02d" % issue]
                                yield journalInfo, dbCode, fileName
                            # else:
                            #     print "redis 中存在，去除"


class Factory(object):
    @staticmethod
    def get_reptile(url, reptileType):
        if reptileType == 'paper':
            return PaperReptile(url)
        elif reptileType == 'paperJournal':
            return PaperJournalReptile(url)
