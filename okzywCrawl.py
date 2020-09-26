import asyncio
import time
from lxml import etree
import aiohttp
import socket
import logging
import sys
import warnings

class okzywCrawl():
    def __init__(self):
        self.headers ={"User-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/85.0.4183.102 Safari/537.36"}
        # socket.setdefaulttimeout(20)
        logging.basicConfig(level=logging.DEBUG,
        format='%(levelname)7s: %(message)s',
        # stream=sys.stderr,
        filename="d:\\log.txt")
        self.log = logging.getLogger('')
        # 开始结束页数
        self.start = 1
        self.end = 3
        # 最大协程数，控制有多少个协程同时进行
        self.maxworks = 10
        # self.qsize = 0

    async def parse_html(self,html,xpathstr):
        # 解析html得到标签值
        dom = etree.HTML(str(html))
        part_html = dom.xpath(xpathstr) 
        self.log.info("解析值为{}".format(part_html))
        print("解析值为{}".format(part_html))
        return part_html

    async def get_html(self,url):
        # 异步请求url获得网页内容
        # self.log.info("=========={}".format(url))
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url,headers=self.headers,timeout=5) as resp:
                    assert resp.status == 200                        
                    html = await resp.text()
                    # self.log.info("========",type(html))
                    return html
        except Exception as e:
            self.log.info(e)

    async def consumer(self,task_id,q):
        # 从二级页面获得想要的内容
        await asyncio.sleep(2)
        print("任务{}开始了！队列共有{}个值".format(task_id,q.qsize()))
        self.log.info("任务{}开始了！队列共有{}个值".format(task_id,q.qsize()))
        item_name_xpath = "//div[@class='vodh']/h2/text()"
        while True:
            current_url = await q.get()
            print("当前队列里有{}个值,当前取出{}".format(q.qsize(),current_url))
            self.log.info("当前队列里有{}个值,当前取出{}".format(q.qsize(),current_url))
            if current_url is None:
                q.task_done()
                break
            else:
                url = 'http://www.okzyw.com{url}'.format(url=current_url)
                html = await self.get_html(url)
                await self.parse_html(html,item_name_xpath)
                q.task_done()          

    async def porducer(self,q):
        # 获得每个二级页面的链接，然后加入队列
        pages=['http://www.okzyw.com/?m=vod-index-pg-{}.html'.format(i) for i in range(self.start,self.end)]
        detail_links_xpath = "body//a[contains(@href,'m=vod-detail-id-')]/@href"
        for url in pages:
            # self.log.info("url is {}".format(url))
            print(url)
            html = await self.get_html(url)
            urls = await self.parse_html(html,detail_links_xpath)
            print(urls)
            [await q.put(url) for url in urls]
        for i in range(self.maxworks):
            await q.put(None)
        print("生产队列里有{}个值".format(q.qsize()))
        # 等待队列清空
        await q.join()
        print("队列已清空，全部任务结束。")
        # return "Task1 finished!"

    async def eventloop(self):
        # 异步执行fist_task，完成后再并发执行second_task
        warnings.simplefilter('always', ResourceWarning)
        loop.set_debug(True)
        loop.slow_callback_duration = 0.001
        q = asyncio.Queue()
        # tasks = [self.second_task(task_id,q) for task_id in range(self.qsize)]
        producer = loop.create_task(self.porducer(q))
        consumers = [loop.create_task(self.consumer(task_id,q)) for task_id in range(self.maxworks)]
        await asyncio.wait(consumers + [producer])
        # complete = await asyncio.gather(*tasks)
        # if asyncio.as_completed(task1):
            # result = await task1
            # print('Task ret: {}'.format(result))
            # self.log.info(tasks)
        
if __name__ == '__main__':
    start = time.time()
    loop = asyncio.get_event_loop()
    okzywCrawl = okzywCrawl()
    loop.run_until_complete(okzywCrawl.eventloop())
    loop.close()
    print('最终耗时：',time.time()-start)