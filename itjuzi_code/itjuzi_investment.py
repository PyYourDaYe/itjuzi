import requests
import logging
import random
import pymongo
import time
import hashlib
'''
爬取it桔子投资机构接口数据
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


class Spider(object):
    def __init__(self):
        global UserAgent
        self.headers = {
            "User-Agent": UserAgent,
            "Authorization": token(),
        }
        self.start_url = 'https://www.itjuzi.com/api/investments'
        # self.db = pymongo.MongoClient(
        #     'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
        self.db = pymongo.MongoClient('localhost', 27017).itjuzi_20190724

    def save(self, data):
        '''
        储存url和dataId,通过dataId来去重
        :return:
        '''
        # col = self.db['extracted_data']
        col = self.db['investment']
        res = col.find_one({'dataId': data['dataId']})
        # res=None
        if res is None:
            res1 = col.insert(data)
            print('****** 数据存储 ******')
            print(res1)

    def data(self):
        col1 = self.db['page']
        page = int(col1.find_one({'name': 'investment'})['page'])
        for p in range(page, 388):
            print('当前页数为:{}'.format(str(p)))

            post_data = {
                'page': p,
                'per_page': 20,
                'sort': "year_count",
            }
            json_content = requests.post(self.start_url, data=post_data, headers=self.headers).json()

            print('++++++++++  json_content  ++++++++++++')
            if json_content['status'] == '用户未登陆':
                UpdateToken().get_token()
                time.sleep(5)
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

            detail_page = json_content['data']['data']
            # time.sleep(5)
            for d in detail_page:
                if d is None:
                    continue
                investment_id = d['id']
                name = d['name']
                detail_url = 'https://www.itjuzi.com/api/investments/' + str(investment_id)
                try:
                    investment_logo = d['logo']
                except:
                    investment_logo = None
                detail_json = requests.get(detail_url, headers=self.headers).json()
                print(detail_json)
                try:
                    website_url = detail_json['data']['url']
                except:
                    website_url = None
                try:
                    describe = detail_json['data']['des']
                except:
                    describe = None
                try:
                    scope = detail_json['data']['scope']
                except:
                    scope = None
                try:
                    investment_round = detail_json['data']['investment_round']
                except:
                    investment_round = None
                # 退出案例列表（公司ID，投资轮次）
                req_url = 'https://www.itjuzi.com/api/invst_news_info?id={}'.format(investment_id)
                req_json = requests.get(url=req_url, headers=self.headers).json()
                print(req_json)
                # time.sleep(1)
                try:
                    quit_case = req_json['data']['quit_case']
                except:
                    quit_case = None
                # 成员列表
                try:
                    invst_person_info = req_json['data']['invst_person_info']
                except:
                    invst_person_info = None
                # 相关新闻列表
                try:
                    invst_news_info = req_json['data']['invst_news_info']
                except:
                    invst_news_info = None
                dataId = hashlib.sha256(detail_url.encode()).hexdigest()

                data = {
                    "category": '机构',
                    'investment_id': investment_id,
                    'name': name,
                    'investment_logo': investment_logo,
                    'website_url': website_url,
                    'describe': describe,
                    # 投资领域(列表)
                    'scope': scope,
                    # 投资轮次(列表)
                    'investment_round': investment_round,
                    'quit_case': quit_case,
                    'invst_person_info': invst_person_info,
                    'invst_news_info': invst_news_info,
                    'refer': detail_url,
                    'dataId': dataId,
                    'invTime': int(time.time() * 1000)
                }

                print(data)
                self.save(data=data)
                # time.sleep(5)
                # break

            col1.update_one({'name': 'investment'}, {'$set': {'page': p + 1}})


if __name__ == '__main__':
    spider = Spider()
    spider.data()
