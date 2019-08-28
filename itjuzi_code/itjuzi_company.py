import requests
import logging
import random
import pymongo
import time
import hashlib
import redis
import oss2
from apscheduler.schedulers.blocking import BlockingScheduler
'''
爬取it桔子公司接口数据
'''
OSS_ACCESS_KEY_ID = 'LTAIFrKBmXgK7457'
OSS_ACCESS_KEY_SECRET = '4oPvw4OMbpTJwlwYvkm2vUjiGmau1P'
OSS_ENDPOINT = 'oss-cn-shenzhen.aliyuncs.com'
OSS_INTERNAL_ENDPOINT = 'oss-cn-shenzhen.aliyuncs.com'
OSS_BUCKET = 'si-policy-test'
OSS_URL_VALIDITY = 3600 * 24 * 365 * 10  # oss文件的url有效期,单位秒
BASE_URL = 'https://www.itjuzi.com'

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

def oss(img_key, img_data):
    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    # a = bucket.put_object_from_file(img_key, img_data)
    a = bucket.put_object(img_key, img_data)
    print('status' + str(a.status))
    # 获取返回的url
    jpg_url = bucket.sign_url('GET', img_key, OSS_URL_VALIDITY)

    return jpg_url


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

        self.start_url = 'https://www.itjuzi.com/api/companys'
        self.db = pymongo.MongoClient(
            'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
        # self.db = pymongo.MongoClient('localhost', 27017).itjuzi_20190724

    def save(self, data):
        '''
        储存url和dataId,通过dataId来去重
        :return:
        '''
        # col = self.db['company']
        col = self.db['extracted_data']
        res = col.find_one({'dataId': data['dataId']})
        # res=None
        if res is None:
            res1 = col.insert(data)
            print('****** 数据存储 ******')
            print(res1)

    def data(self):
        global headers
        col1 = self.db['page']
        col1.update_one({'name': 'company'}, {'$set': {'page': 1}})
        page = int(col1.find_one({'name': 'company'})['page'])
        for p in range(page, 4):
            print('当前页数为:{}'.format(str(p)))
            post_data = {
                'page': p,
                'pagetotal': 0,
                'per_page': 20
            }
            try:
                json_content = requests.post(self.start_url, data=post_data, headers=headers,proxies=proxies,timeout=4).json()
            except:
                for x in range(10):
                    try:
                        proxies = random.choice(get_proxies())
                        print(proxies)
                        json_content = requests.post(self.start_url, data=post_data, headers=headers, proxies=proxies,
                                                 timeout=4).json()
                        break
                    except:
                        pass

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
                try:
                    json_content = requests.post(self.start_url, data=post_data, headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            json_content = requests.post(self.start_url, data=post_data, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass
            try:
                if json_content['message'] == 'Token过期' or '需要开通VIP':
                    UpdateToken().get_token()
                  #  time.sleep(5)
                    with open(file_path, 'r') as f:
                        Authorization = f.read()
                    headers = {
                        "User-Agent": UserAgent,
                        "Authorization": Authorization,
                    }
                    try:
                        json_content = requests.post(self.start_url, data=post_data, headers=headers, proxies=proxies,
                                                     timeout=4).json()
                    except:
                        for x in range(10):
                            try:
                                proxies = random.choice(get_proxies())
                                print(proxies)
                                json_content = requests.post(self.start_url, data=post_data, headers=headers,
                                                             proxies=proxies,
                                                             timeout=4).json()
                                break
                            except:
                                pass
            except Exception as e:
                print(e)
            # print(json_content)

            detail_page = json_content['data']['data']
           # time.sleep(5)
            for d in detail_page:
                if d is None:
                    continue
                id = d['id']
                detail_url = 'https://www.itjuzi.com/api/companies/{}?type=basic'.format(str(id))
                try:
                    detail_json = requests.get(detail_url,headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            detail_json = requests.post(detail_url, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass

                if detail_json['status']=="error":
                    continue
                com_name = detail_json['data']['basic']['com_name']
                com_des = detail_json['data']['basic']['com_des']
                com_logo_archive = detail_json['data']['basic']['com_logo_archive']
                img_content = requests.get(url=com_logo_archive, headers=base_headers).content
                jpg_url = oss(com_logo_archive, img_content)
                tag_info = detail_json['data']['basic']['tag_info']

                com_stage_name = detail_json['data']['basic']['com_stage_name']
                com_url = detail_json['data']['basic']['com_url']
                com_round_name = detail_json['data']['basic']['com_round_name']
                com_scope=detail_json['data']['basic']['com_scope']
                com_sub_scope=detail_json['data']['basic']['com_sub_scope']
                com_slogan=detail_json['data']['basic']['com_slogan']
                # 专辑 (boolean类型,当值为1时为加入IT桔子独角兽俱乐部)
                try:
                    unicorn = detail_json['data']['basic']['unicorn']
                except:
                    unicorn = None
                # 相关组织ID(数据列表)
                try:
                    com_organization = detail_json['data']['basic']['com_organization']
                except:
                    com_organization = None
                # 基本信息(包含一句话描述,公司名,成立时间,公司规模)
                # 公司名
                com_registered_name = detail_json['data']['basic']['com_registered_name']
                com_born_date = detail_json['data']['basic']['com_born_date']
                try:
                    com_scale_name = detail_json['data']['basic']['company_scale']['com_scale_name']
                except:
                    com_scale_name = None
                com_status_name = detail_json['data']['basic']['com_status_name']

                # 联系方式及地址
                contact_url = 'https://www.itjuzi.com/api/companies/{}?type=contact'.format(str(id))

                try:
                    contact_json = requests.get(contact_url,headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            contact_json = requests.post(contact_url, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass

                company_tel = contact_json['data']['contact']['tel']
                company_email = contact_json['data']['contact']['email']
                company_addr = contact_json['data']['contact']['addr']

                # 团队成员信息
                person_url = 'https://www.itjuzi.com/api/companies/{}?type=person'.format(str(id))
                person_json = requests.get(person_url, headers=headers).json()
                try:
                    person_json = requests.get(person_url,headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            person_json = requests.post(person_url, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass

                try:
                    person_list = person_json['data']['person']
                except:
                    person_list = None
                # 产品禅大师ID
                try:
                    products = person_json['data']['products']
                except:
                    products = None
                # 新闻动态列表（时间   url   类型）
                report_url = 'https://www.itjuzi.com/api/companies/{}?type=report'.format(str(id))

                try:
                    report_json = requests.get(report_url,headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            report_json = requests.post(report_url, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass

                try:
                    report_list = report_json['data']['report']
                except:
                    report_list = None
                # 注册资本               股东信息（构造数据集）   出资信息（构造数据集）   里程碑列表（时间，描述）
                commerce_url = 'https://www.itjuzi.com/api/companies/{}/commerce'.format(str(id))

                try:
                    commerce_json = requests.get(commerce_url,headers=headers, proxies=proxies,
                                                 timeout=4).json()
                except:
                    for x in range(10):
                        try:
                            proxies = random.choice(get_proxies())
                            print(proxies)
                            commerce_json = requests.post(commerce_url, headers=headers,
                                                         proxies=proxies,
                                                         timeout=4).json()
                            break
                        except:
                            pass
                try:
                    regcap = commerce_json['elecredit_basic']['regcap']
                except:
                    regcap = None
                # 成立时间
                try:
                    esdate = commerce_json['elecredit_basic']['esdate']
                except:
                    esdate = None
                # 公司类型
                try:
                    enttype = commerce_json['elecredit_basic']['enttype']
                except:
                    enttype = None
                # 法人代表名
                try:
                    frname = commerce_json['elecredit_basic']['frname']
                except:
                    frname = None
                # 注册地址
                try:
                    dom = commerce_json['elecredit_basic']['dom']
                except:
                    dom = None
                # 股东信息（构造数据集）
                try:
                    elecredit_shareholder = commerce_json['elecredit_shareholder']
                except:
                    elecredit_shareholder = None
                # 工商变更列表
                try:
                    elecredit_alter = commerce_json['elecredit_alter']
                except:
                    elecredit_alter = None
                dataId = hashlib.sha256(detail_url.encode()).hexdigest()

                data = {
                    "category": '公司',
                    'company_id': id,
                    'com_name': com_name,
                    'com_des': com_des,
                    'com_logo_archive': jpg_url,
                    'tag_info': tag_info,
                    'com_stage_name':com_stage_name,
                    'com_round_name': com_round_name,
                    'com_url': com_url,
                    'com_scope':com_scope,
                    'com_slogan':com_slogan,
                    'com_sub_scope':com_sub_scope,
                    'unicorn': unicorn,
                    'com_organization': com_organization,
                    'com_registered_name': com_registered_name,
                    'com_born_date': com_born_date,
                    'com_scale_name': com_scale_name,
                    'com_status_name': com_status_name,
                    'company_tel': company_tel,
                    'company_email': company_email,
                    'company_addr': company_addr,
                    'person_list': person_list,
                    'products': products,
                    'report_list': report_list,
                    'regcap': regcap,
                    'esdate': esdate,
                    'enttype': enttype,
                    'frname': frname,
                    'dom': dom,
                    'elecredit_shareholder': elecredit_shareholder,
                    'elecredit_alter': elecredit_alter,
                    'refer': detail_url,
                    'dataId': dataId,
                    'invTime': int(time.time() * 1000)
                }
                print(data)
                self.save(data=data)
                # time.sleep(5)
                # break

            col1.update_one({'name': 'company'}, {'$set': {'page': p + 1}})

if __name__ == '__main__':
    # spider = Spider()
    # spider.data()
    sched = BlockingScheduler()
    sched.add_job(Spider().data, 'interval', hours=12)
    sched.start()
