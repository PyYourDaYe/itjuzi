import requests
import logging
import random
import pymongo
import time
import hashlib
import redis

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

def get_proxies():
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    conn = redis.Redis(connection_pool=redis_pool)
    result = conn.keys()
    proxy_list =[]
    for x in result:
        ip = x.decode()
        proxy = {'https': ip}

        proxy_list.append(proxy)

    return proxy_list
proxies=random.choice(get_proxies())
print(proxies)

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

            time.sleep(1)

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
        global UserAgent
        self.db = pymongo.MongoClient(
            'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
        # self.db = pymongo.MongoClient('localhost', 27017).itjuzi_20190724

    def save(self, data):
        '''
        储存url和dataId,通过dataId来去重
        :return:
        '''
        col = self.db['extracted_data']
        # col = self.db['policy_api']
        res = col.find_one({'dataId': data['dataId']})
        # res=None
        if res is None:
            res1 = col.insert(data)
            print('****** 数据存储 ******')
            print(res1)

    def data(self):
        global proxies,headers
        col1 = self.db['page']
        page = int(col1.find_one({'name': 'policy_api'})['page'])
        for p in range(page, 1554):
            print('当前页数为:{}'.format(str(p)))
            list_url = 'https://www.itjuzi.com/api/policy?page={}&per_page=20&location=&begin_time=0&end_time=0&keyword='.format(
                str(p))
            try:
                json_content = requests.get(list_url, headers=headers,proxies=proxies,timeout=4).json()
            except:
                for x in range(10):
                    try:
                        proxies = random.choice(get_proxies())
                        print(proxies)
                        json_content = requests.get(list_url, headers=headers, proxies=proxies,
                                                 timeout=4).json()
                        break
                    except:
                        pass
            print('++++++++++  json_content  ++++++++++++')
            if json_content['status'] == '用户未登陆' or '该账户已在其他设备登陆':
                UpdateToken().get_token()
                # time.sleep(5)
                with open(file_path, 'r') as f:
                    Authorization = f.read()
                headers = {
                    "User-Agent": UserAgent,
                    "Authorization": Authorization,
                }
                try:
                    json_content = requests.get(list_url, headers=headers, proxies=proxies, timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            json_content = requests.get(list_url, headers=headers, proxies=proxies,
                                                        timeout=4).json()
                            break
                        except:
                            pass
            try:
                if json_content['message'] == 'Token过期' or '需要开通VIP':
                    UpdateToken().get_token()
                    # time.sleep(5)
                    with open(file_path, 'r') as f:
                        Authorization = f.read()
                    headers = {
                        "User-Agent": UserAgent,
                        "Authorization": Authorization,
                    }
                    try:
                        json_content = requests.get(list_url, headers=headers, proxies=proxies, timeout=4).json()
                    except:
                        for x in range(10):
                            try:
                                proxies = random.choice(get_proxies())
                                print(proxies)
                                json_content = requests.get(list_url, headers=headers, proxies=proxies,
                                                            timeout=4).json()
                                break
                            except:
                                pass
            except Exception as e:
                print(e)
            # print(json_content)

            detail_page = json_content['data']['data']
            time.sleep(0.2)
            for d in detail_page:
                if d is None:
                    continue
                policy_id = d['id']
                detail_url = 'https://www.itjuzi.com/api/policy/detail?id=' + str(policy_id)
                # detail_json = requests.get(detail_url, headers=headers).json()
                try:
                    detail_json = requests.get(detail_url, headers=headers, proxies=proxies, timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            detail_json = requests.get(detail_url, headers=headers, proxies=proxies,
                                                        timeout=4).json()
                            break
                        except:
                            pass
                # print(detail_json)
                policy_meta = detail_json['data']
                dataId = hashlib.sha256(detail_url.encode()).hexdigest()

                data = {
                    "category": 'policy_api',
                    'refer': detail_url,
                    'dataId': dataId,
                    'invTime': int(time.time() * 1000)
                }
                data.update(policy_meta)

                #print(data)
                self.save(data=data)
                time.sleep(1)

            col1.update_one({'name': 'policy_api'}, {'$set': {'page': p + 1}})


if __name__ == '__main__':
    spider = Spider()
    spider.data()
