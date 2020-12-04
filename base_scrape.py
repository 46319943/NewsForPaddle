import requests
import re
from gne import GeneralNewsExtractor
from scrapy.selector import Selector
import dateparser
import logging

from pprint import pprint
import traceback
import sys
from datetime import datetime, timedelta

import psycopg2
import redis

from trans_util import bd09_to_wgs84
from urllib.parse import urljoin

from ..database.news_server_mongo import MongoDBPipeline
from ..item.news_item import NewsItem
from .geocoder import geocode_region, geocode_ner, scale
from setting import POSTGRESQL_CON


class BaseScrape(object):
    # 新闻主题提取器
    extractor = GeneralNewsExtractor()
    # 持久化器
    persistor = MongoDBPipeline()
    # NER识别地址
    NER_url = 'http://localhost:8889/NER'

    NER_stop_word = ['长江网', '长江日报']
    NER_stop_punctuation = r'[\.%]'

    # 访问所需Cookie
    cookies = {}
    # 提取链接xpath
    xpath = None

    # 访问浏览器代理
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'}

    @classmethod
    def indentify_website(cls, url):
        '''
        判断是否为可爬取网站
        '''
        return True

    @classmethod
    def persist(cls, item):
        '''
        爬取结果持久化
        '''
        if cls.persistor:
            cls.persistor.process_item(item)

    @staticmethod
    def extract_url(url):
        '''
        提取纯URL
        '''
        return url.strip()

    @staticmethod
    def extract_id(url):
        '''
        提取id
        '''
        return url

    @staticmethod
    def request_to_text(r):
        '''
        获取响应文本
        处理文本编码问题
        '''
        if r.encoding == 'ISO-8859-1':
            encodings = requests.utils.get_encodings_from_content(r.text)
            if encodings:
                encoding = encodings[0]
            else:
                encoding = r.apparent_encoding
            text = r.content.decode(
                encoding, 'replace')  # 如果设置为replace，则会用?取代非法字符；
        else:
            text = r.text
        return text

    @classmethod
    def extract_base(cls, url,
                     ):
        '''
        提取基础信息：标题、时间、内容等
        构建并返回Item
        '''
        try:
            r = requests.get(url, headers=cls.headers, timeout=3)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            return None

        text = cls.request_to_text(r)

        # 有时候会访问失败，重新访问
        if r.status_code == 404:
            return 404

        # html_content = Selector(text=text)
        # if html_content is None:
        #     return None

        result = cls.extractor.extract(
            text,
            with_body_html=True,
            host=url,
        )

        item = NewsItem(result)
        item['_id'] = cls.extract_id(url)
        item['url'] = url

        item['timestamp'] = dateparser.parse(
            item['publish_time']).timestamp() * 1000

        # 摘要
        # html = Selector(text=text)
        # item['description'] = html.xpath(
        #     '//meta[@name = "Description"]/@content').extract_first()
        # item['keywords'] = html.xpath(
        #     '//meta[@name = "Keywords"]/@content').extract_first()

        return item

    @classmethod
    def NER(cls, item):
        '''
        实体识别
        '''
        content = item['content']
        r = requests.post(cls.NER_url, {'text': content})
        result = r.json()

        # 去掉停用词，去符号
        word_ls = [word for sentence in result for word in sentence
                   if word[0] not in cls.NER_stop_word
                   and len(word[0]) > 1
                   and re.search(cls.NER_stop_punctuation, word[0]) is None]
        institute_ls = [word[0]
                        for word in word_ls
                        if word[1] == 'NT']
        location_ls = [word[0]
                       for word in word_ls
                       if word[1] == 'NS']
        person_ls = [word[0]
                     for word in word_ls
                     if word[1] == 'NR']
        other_ls = [word[0] for word in word_ls
                    if (word[1] != 'NR') and (word[1] != 'NT') and (word[1] != 'NS')]

        # 避免重复
        item['institute_ner'] = list(set(institute_ls))
        item['location_ner'] = list(set(location_ls))
        item['person_ner'] = list(set(person_ls))
        item['other_ner'] = list(set(other_ls))

    @classmethod
    def geocode(cls, item, city='武汉'):
        geocode_region(item)
        geocode_ner(item)
        return item

    @classmethod
    def scale(cls, item, postgis=True):
        scale(item, postgis)
        return item

    @classmethod
    def postgre(cls, item):
        '''
        将地点添加入Postgre数据库
        '''
        conn = psycopg2.connect(
            **POSTGRESQL_CON)
        cur = conn.cursor()

        if item.get('locations', None):

            for key, value in item['locations'].items():
                # 插入或更新
                query = (
                    f'Insert into location '
                    f'(geom, name, news_id) '
                    f'values '
                    f"('SRID=4326;POINT({value['longitude']} {value['latitude']})'::geometry, '{key}', '{item['_id']}') "
                    f'on conflict(name, news_id) '
                    f'DO UPDATE '
                    f'SET geom = excluded.geom; '
                )
                # UniqueViolation，InFailedSqlTransaction
                try:
                    cur.execute(query)
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()

        cur.close()
        conn.close()

    @classmethod
    def run(cls, url, retry=0):
        if not cls.indentify_website(url):
            return None
        url = cls.extract_url(url)

        print(url)
        if retry:
            print(f'retry:{retry}')

        item = cls.extract_base(url)

        if item is None:
            return cls.run(url, retry+1)
        elif item == 404:
            return 404

        cls.NER(item)
        cls.geocode(item)
        cls.postgre(item)
        cls.scale(item)

        cls.persist(item)

        return item

    @classmethod
    def run_content(cls, item):
        '''
        只根据item的content进行NER与地理编码
        '''
        cls.NER(item)
        cls.geocode(item)
        cls.scale(item, False)
        return item

    @classmethod
    def run_url_list(cls, url_list, xpath=None):
        '''
        提取一页的链接
        '''
        if xpath is None:
            xpath = cls.xpath

        r = requests.get(url_list, headers=cls.headers, cookies=cls.cookies)
        text = cls.request_to_text(r)
        selector = Selector(text=text)
        a_list = selector.xpath(xpath).extract()

        for a_url in a_list:
            a_url = a_url.strip()
            url = urljoin(url_list, a_url)
            try:
                item = cls.run(url)
                print(item)
            except:
                logging.exception('抓取出现异常')


def main():
    pass
