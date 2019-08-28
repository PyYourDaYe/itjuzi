import requests
import logging
import random
import pymongo
import time
import hashlib
'''
爬取it桔子基金列表页数据
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


# ++++++++++  json_content  ++++++++++++
# {'status': '用户未登陆', 'code': 10002}


class UpdateToken(object):
    instant = None

    def __new__(cls, *args, **kwargs):
        if cls.instant is None:
            cls.instant = super().__new__(cls)
        return cls.instant

    def __init__(self):
        self.file_path = 'token.txt'

    def get_token(self, proxies=None):
        '''
        post登录页面,获取登录后的token
        :return: Authorization
        '''
        header = {
            'User-Agent': UserAgent,
        }
        login_url = 'https://www.itjuzi.com/api/authorizations'
        # 一个IP短时间只能只能请求一次
        session = requests.session()
        data = {
            "account": '18621583401', "password": '666666'
        }
        print('******执行了get_token********')
        try:
            cookies = session.post(url=login_url, headers=header, data=data, proxies=proxies, timeout=5).json()
            token = cookies['data']['token']
            print('更新Token为')
            print(token)
            # 将token写入到本地
            logging.info('*****更新Token******')
            with open(self.file_path, 'w') as f:
                f.write(token)

            time.sleep(5)

        except Exception as e:
            print(e)

        try:
            with open(self.file_path, 'r') as f:
                Authorization = f.read()
        except Exception as e:
            Authorization = None
            print(e)

        return Authorization


file_path = 'token.txt'


def token():
    '''
    校验从本地获取的Token是否可用
    :return: 可用Token
    '''
    with open(file_path, 'r') as f:
        Authorization = f.read()

    return Authorization


base_headers = {
    'UserAgent': UserAgent
}
headers = {
    "User-Agent": UserAgent,
    "Authorization": token(),
}


class Spider(object):
    def __init__(self):
        # self.db = pymongo.MongoClient('localhost',27017).itjuzi_20190724
        self.db = pymongo.MongoClient(
            'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
        self.start_url = 'https://www.itjuzi.com/api/fund'

    def save(self, data):
        '''
        储存url和dataId,通过dataId来去重
        :return:
        '''
        col = self.db['extracted_data']
        # col = self.db['fund']
        res = col.find_one({'dataId': data['dataId']})
        # res=None
        if res is None:
            res1 = col.insert(data)
            print('****** 数据存储 ******')
            print(res1)

    def data(self):
        global headers
        col1 = self.db['page']
        page = int(col1.find_one({'name': 'fund'})['page'])
        for p in range(page, 2508):
            print('当前页数为:{}'.format(str(p)))
            post_data = {
                'page': p,
                'pagetotal': 0,
                'per_page': 20,
                'type': 3
            }
            json_content = requests.post(self.start_url, data=post_data, headers=headers).json()

            print('++++++++++  json_content  ++++++++++++')
            if json_content['status'] == '用户未登陆':
                UpdateToken().get_token()
               # time.sleep(5)
                with open(file_path, 'r') as f:
                    Authorization = f.read()
                headers = {
                    "User-Agent": UserAgent,
                    "Authorization": Authorization,
                }
                json_content = requests.post(self.start_url, data=post_data, headers=headers).json()
            try:
                if json_content['message'] == 'Token过期' or '需要开通VIP':
                    UpdateToken().get_token()
                    time.sleep(5)
                    with open(file_path, 'r') as f:
                        Authorization = f.read()
                    headers = {
                        "User-Agent": UserAgent,
                        "Authorization": Authorization,
                    }
                    json_content = requests.post(self.start_url, data=post_data, headers=headers).json()
            except Exception as e:
                print(e)
            print(json_content)

            detail_page = json_content['data']['list']
           # time.sleep(5)
            for d in detail_page:
                if d is None:
                    continue
                id = d['fund_id']
                detail_url = 'https://www.itjuzi.com/api/fund/' + str(id)

                detail_json = requests.get(detail_url, headers=headers).json()
                print(detail_json)

                fund_name = detail_json['data']['fund_name']
                fund_manage_name = detail_json['data']['fund_manage_name']
                fund_type = detail_json['data']['fund_type']
                fund_number = detail_json['data']['fund_number']
                fund_born_time = detail_json['data']['fund_born_time']
                fund_records_time = detail_json['data']['fund_records_time']
                # 机构网址 (中国证券投资基金业协会中登记信息)
                fund_status = detail_json['data']['fund_status']
                fund_currency = detail_json['data']['fund_currency']
                try:
                    invst_id = detail_json['data']['invst_id']
                except:
                    invst_id = None
                dataId = hashlib.sha256(detail_url.encode()).hexdigest()

                data = {
                    'category': '基金',
                    'fund_id': id,
                    'fund_name': fund_name,
                    'fund_manage_name': fund_manage_name,
                    'fund_type': fund_type,
                    'fund_number': fund_number,
                    'fund_born_time': fund_born_time,
                    'fund_records_time': fund_records_time,
                    'fund_status': fund_status,
                    'fund_currency': fund_currency,
                    'invst_id': invst_id,
                    'refer': detail_url,
                    'dataId': dataId,
                    'invTime': int(time.time() * 1000)
                }

                print(data)
                self.save(data=data)
                time.sleep(5)

            col1.update_one({'name': 'fund'}, {'$set': {'page': p + 1}})


if __name__ == '__main__':
    spider = Spider()
    spider.data()
