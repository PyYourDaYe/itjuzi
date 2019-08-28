import requests
import random
import pymongo
import time
import hashlib
'''
爬取it桔子标签接口数据
'''
user_agent_list = [
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Mobile Safari/537.36",
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.16 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36',
]
UserAgent = random.choice(user_agent_list)

headers = {
    "User-Agent": UserAgent,
}


class Spider(object):
    def __init__(self):
        # self.db = pymongo.MongoClient('localhost', 27017).itjuzi_20190724
        self.db = pymongo.MongoClient(
            'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724


    def save(self, data):
        '''
        储存url和dataId,通过dataId来去重
        :return:
        '''
        col = self.db['extracted_data']
        # col = self.db['tag']
        res = col.find_one({'dataId': data['dataId']})
        # res=None
        if res is None:
            res1 = col.insert(data)
            print('****** 数据存储 ******')
            print(res1)

    def data(self):
        global headers
        col1 = self.db['page']
        page = int(col1.find_one({'name': 'tag'})['page'])
        for p in range(page, 488):
            print('当前页数为:{}'.format(str(p)))
            url = 'https://www.itjuzi.com/api/tag/tag_list?page={}'.format(str(p))
            json_content = requests.get(url, headers=headers).json()

            print('++++++++++  json_content  ++++++++++++')
            print(json_content)

            detail_page = json_content['data']['data']
            for d in detail_page:
                # 标签id
                tag_id = d['id']
                # 标签详情页
                detail_url = 'https://www.itjuzi.com/api/tag/tag_detail?id=' + str(d['id'])
                # 投资机构(抓取列表)
                meta=requests.get(detail_url,headers=headers).json()
                dataId = hashlib.sha256(detail_url.encode()).hexdigest()

                data = {
                    "category": 'tag',
                    'tag_id':tag_id,
                    'refer': detail_url,
                    'dataId': dataId,
                    'invTime': int(time.time() * 1000),
                    'meta':meta['data']
                }

                print(data)
                self.save(data=data)
                time.sleep(5)

            col1.update_one({'name': 'tag'}, {'$set': {'page': p + 1}})


if __name__ == '__main__':
    spider = Spider()
    spider.data()
