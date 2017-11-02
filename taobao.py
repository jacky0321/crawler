# -*- coding: utf-8 -*-
from dbmanager import CrawlDatabaseManager

from threading import Thread
from selenium import webdriver
from lxml import etree
import re
import hashlib
import time


class CrawlBSF(Thread):
    def __init__(self, url, depth = 0, index = 1):

        self.driver = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true', '--load-images=false'])
        
        # custom header
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Charset': 'utf-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
            'Connection': 'keep-alive'
        }

        # set custom headers
        for key, value in headers.iteritems():
            webdriver.DesiredCapabilities.PHANTOMJS['phantomjs.page.customHeaders.{}'.format(key)] = value

        # another way to set custome header
        webdriver.DesiredCapabilities.PHANTOMJS['phantomjs.page.settings.userAgent'] = \
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'

        self.driver.set_window_size(1280, 2400)  # optional

        self.cur_db = CrawlDatabaseManager(5)
        self.url = url
        self.depth = depth
        self.index = index
        self.content = ''
        Thread.__init__(self)


    def run(self):
        while self.depth <= 5:
            if root_page is True:
                self.depth += 1
                self.start_crawl(self.url, self.depth, self.index)
            else:
                l = self.cur_db.dequeueUrl()
                if len(l) == 0:
                    break
                else:
                    self.url, self.depth, self.index = l[0], l[1], l[2]
                    self.depth += 1
                    self.start_crawl(self.depth, self.url, self.index)


    def start_crawl(self, url, depth, index):

        print self.getName(), 'crawling', self.url, 'depth:', self.depth, '\n'

        self.save_html()

        self.get_urls()
        
        self.get_info()


    def save_html(self):
        global root_page
        self.driver.get(self.url)
        time.sleep(5)
        self.content = self.driver.page_source

        with open('%s.html' %hashlib.md5(self.url).hexdigest() , 'w+' ) as f:
            f.write(self.content.encode('utf-8'))

        self.cur_db.finishUrl(self.index)
        root_page = False


    def get_urls(self):
        # 使用 (pattern) 进行获取匹配
        # +? 使用非贪婪模式
        # [^>\"\'\s] 匹配任意不为 > " ' 空格 制表符 的字符
        tmall_links = re.findall('href=[\"\']{1}(//detail.tmall.com/item.htm[^>\"\'\s]+?)"', self.content)
        taobao_links = re.findall('href=[\"\']{1}(//detail.taobao.com/item.htm[^>\"\'\s]+?)"', self.content)

        for href in tmall_links + taobao_links:
            href = "https:" + href
            self.cur_db.enqueueUrl(href, self.depth)


    def get_info(self):
        etr = etree.HTML(self.content)

        pid = self.index
        # get price and promote price
        item_price_list = etr.xpath('//span[@class="tm-price"]')
        if len(item_price_list) == 0:
            price = 0
            pro_price = 0
        elif len(item_price_list) == 1:
            price = item_price_list[0].text
            pro_price = price
        else:
            price = etr.xpath('//dl[@class="tm-price-panel"]//span[@class="tm-price"]')[0].text
            pro_price = etr.xpath('//dl[contains(@class, "tm-promo-cur")]//span[@class="tm-price"]')[0].text

        # get title
        title = etr.xpath('//*[@class="tb-detail-hd"]/h1')[0].text.strip()

        # get sale volume
        sale_volume = etr.xpath('//li[contains(@class, "tm-ind-sellCount")]//span[@class="tm-count"]')
        if len(sale_volume) == 0:
            sale_volume = 0
        else:
            sale_volume = sale_volume[0].text

        # get comment number 
        comment_number = etr.xpath('//li[contains(@class, "tm-ind-reviewCount")]//span[@class="tm-count"]')
        if len(comment_number) == 0:
            comment_number = 0
        else:
            comment_number = comment_number[0].text

        print '名称:', title, '价格:', price, '促销价:', pro_price, '销量:', sale_volume, '评论数:', comment_number, '\n'
        
        self.cur_db.product(pid, title, pro_price, price, int(sale_volume), int(comment_number))

        comment_list = etr.xpath('//div[@class="rate-grid"]/table/tbody/tr')
        
        for i in range(1, len(comment_list)+1):
            # get pid
            pid = self.index

            # get user
            user_1 = etr.xpath('//div[@class="rate-grid"]//tr[%d]/td[@class="col-author"]/div[@class="rate-user-info"]' %i)
            if len(user_1) == 0:
                user_1 = ''
            else:
                user_1 = user_1[0].text
            user_2 = etr.xpath('//div[@class="rate-grid"]//tr[%d]/td[@class="col-author"]/div[@class="rate-user-info"]/span' %i)
            if len(user_2) == 0:
                user_2 = ''
            else:
                user_2 = user_2[0].text
            '''
            user_3 = etr.xpath('//div[@class="rate-grid"]//tr[%d]/td[@class="col-author"]/div[@class="rate-user-info"]' %i)[1].text
            if len(user_3) == 0:
                user_3 = ''
            else:
                user_3 = user_3[1].text
            '''
            user_4 = etr.xpath('//div[@class="rate-grid"]//tr[%d]/td[@class="col-author"]/div[@class="rate-user-info"]/span' %i)
            if len(user_4) == 0:
                user_4 = ''
            else:
                user_4 = user_4[1].text
            
            user = user_1 + user_2 + user_4

            # get user level
            user_level = etr.xpath('//div[@class="rate-grid"]//tr[%d]/td[@class="col-author"]/div[@class="rate-user-grade"]/a' %i)
            if len(user_level) == 0:
                user_level = ''
            else:
                user_level = user_level[0].attrib['title']

            # get comment_content
            comment_content = etr.xpath('//div[@class="rate-grid"]//tr[%d]//div[@class="tm-rate-content"]/div[@class="tm-rate-fulltxt"]' %i)
            if len(comment_content) == 0:
                comment_content = ''
            else:
                comment_content = comment_content[0].text

            # get comment_date
            comment_date = etr.xpath('//div[@class="rate-grid"]//tr[%d]//div[@class="tm-rate-date"]' %i)
            if len(comment_date) == 0:
                comment_date == ''
            else:
                comment_date = comment_date[0].text

            # get comment_reply
            comment_reply = etr.xpath('//div[@class="rate-grid"]//tr[%d]//div[@class="tm-rate-reply"]/div[@class="tm-rate-fulltxt"]' %i)
            if len(comment_reply) == 0:
                comment_reply = ''
            else:
                comment_reply = comment_reply[0].text

            print '商品id:', pid, '用户名:', user, '用户级别:', user_level, '内容:', comment_content, '时间:', comment_date, comment_reply, '\n'
            self.cur_db.comment(pid, user, user_level, comment_content, comment_reply, comment_date)


if __name__ == '__main__':
    start_url = "https://detail.tmall.com/item.htm?id=540212526343"

    CRAWL_DELAY = 5
    threads = []
    max_num_thread = 5
    root_page = True

    # first remove all finished running threads
    for t in range(max_num_thread):
        try:
            t = CrawlBSF(start_url)
            threads.append(t)
            t.start()
            t.join()
        except Exception as err:
            print "Error: unable to start thread" + err.msg

    for t in threads:
        if not t.is_alive():
            t.driver.close()
            threads.remove(t)
        if len(threads) >= max_num_thread:                
            time.sleep(CRAWL_DELAY)
            continue
        try:
            t = CrawlBSF(start_url)
            threads.append(t)
            t.start()
            t.join()
        except Exception:
            print "Error: unable to start thread"

