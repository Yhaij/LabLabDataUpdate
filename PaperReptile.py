#!/user/bin/python
# -*- coding: utf-8 -*-
from BaseReptile import BaseReptile
import re


class PaperReptile(BaseReptile):
    """
    论文的解析
    """
    def __init__(self, url, model="get", params=None, **kwargs):
        BaseReptile.__init__(self, url)
        self.paperDic = dict()
        self.get_etree(model, params, **kwargs)

    def set_url(self, url, model="get", params=None, **kwargs):
        self.url = url
        self.get_etree(model, params, **kwargs)

    def parse_html(self):
        """
        解析知网的论文网页
        :return: 
        """
        if self.tree is None or self.requestCode != 1:
            return None
        paperName = self.index_of_elements_info('//div[@id="mainArea"]//h2[@class="title"]/text()')
        authors = self.get_elements_info('//div[@class="author"]//a/text()')
        organization = self.get_elements_info('//div[@class="orgn"]//a/text()')
        authorsCode = self.pares_author_code()
        abstract = self.index_of_elements_info('//span[@id="ChDivSummary"]/text()')
        catalogs = self.get_elements_info('//div[@class="wxBaseinfo"]/p')
        keywords = None;classification = None
        if catalogs is not None:# 获取关键字和分类号
            for catalog in catalogs:
                label = catalog.xpath('./label/@id')
                label = label[0] if label else None
                if label == "catalog_KEYWORD": # 判断该p标签下是关键字还是分类号
                    keywords = catalog.xpath('./a/text()')
                    if keywords:
                        keywords = "".join([n.strip() for n in keywords])
                    else:
                        keywords = None
                elif label == "catalog_ZTCLS":
                    classification = catalog.xpath('./text()')
                    classification = classification[0] if classification else None
        self.paperDic['name'] = paperName
        self.paperDic['authors'] = self.list_to_str(authors, ";")
        self.paperDic['organization'] = self.list_to_str(organization, ";")
        self.paperDic['authors_code'] = self.list_to_str(authorsCode, ";")
        self.paperDic['keywords'] = keywords
        self.paperDic['abstract'] = abstract
        self.paperDic['classification'] = classification
        # self.show_paper_dict()
        return self.paperDic

    def show_paper_dict(self):
        for key in self.paperDic:
            print key, self.paperDic[key]

    def pares_author_code(self):
        elements = self.get_elements_info('//div[@class="author"]/span/a/@onclick')
        l = list()
        if elements:
            for element in elements:
                infos = element.strip().split(',')
                if len(infos) > 2:
                    model = re.compile(r'\d+')
                    authorCode = model.findall(infos[2])
                    if len(authorCode) > 0:
                        l.append(authorCode[0])
        return l

    @staticmethod
    def url_create(dbCOde, journalCode, year, journalNo, paperNum):
        paperCode = "%s%d%02d%03d" % (journalCode, year, journalNo, paperNum)
        return "http://kns.cnki.net/kcms/detail/detail.aspx?dbCode=%s&filename=%s" % (dbCOde, paperCode)

    @staticmethod
    def url_create(dbCOde, fileName):
        return "http://kns.cnki.net/kcms/detail/detail.aspx?dbCode=%s&filename=%s" % (dbCOde, fileName)

if __name__ == '__main__':
    reptile = PaperReptile("http://kns.cnki.net/kcms/detail/detail.aspx?dbCode=CJFD&filename=BZGC201701002")
    # reptile = PaperReptile(PaperReptile.url_create("CJFD", "GJGC", 2014, 05, 2))
    reptile.parse_html()
