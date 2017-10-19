#!/user/bin/python
# -*- coding: utf-8 -*-
from BaseReptile import BaseReptile
from PaperReptile import PaperReptile
import re


class PaperJournalReptile(BaseReptile):
    """
    论文期刊解析
    """
    def __init__(self, url, model="get", params=None, **kwargs):
        super(PaperJournalReptile, self).__init__(url)
        self.get_etree(model, params, **kwargs)

    def parse_html(self):
        if self.tree is None or self.requestCode != 1:
            return
        if self.html.encode('utf-8').find("暂无目录信息") == -1: # 找到该目录信息
            urlList = list()
            hrefs = self.get_elements_info('//span[@class="name"]/a[@target="_blank"]/@href')
            if hrefs:
                for href in hrefs:
                    pattern = re.compile("dbCode=(.*?)&filename=(.*?)&")
                    match = pattern.search(href)
                    if match and len(match.groups()) == 2:
                        urlList.append([match.group(1), match.group(2)])
            return urlList
        else:
            return None

    @staticmethod
    def url_create(year, issue, pykm):
        url = "http://navi.cnki.net/knavi/JournalDetail/GetArticleList?year=%d&issue=%02d&pykm=%s&pageIdx=0" \
               % (year, issue, pykm)
        print url
        return url

if __name__ == '__main__':
    # pattern = re.compile("dbCode=(.*?)&filename=(.*?)&")
    # match = pattern.search("Common/RedirectPage?sfield=FN&dbCode=CJFD&filename=GJGC201704001&tableName=CJFDTEMP&url=")
    # if match and len(match.groups()) == 2:
    #     print match.group(1), match.group(2)
    p = PaperJournalReptile("http://navi.cnki.net/knavi/JournalDetail/GetArticleList?year=2017&issue=01&pykm=BZGC&pageIdx=0")
    for dbCode, fileName in p.parse_html():
        print PaperReptile.url_create(dbCode, fileName)