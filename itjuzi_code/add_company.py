import requests
import logging
import random
import pymongo
import time
import redis

'''
插入数据库前6万条

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


base_headers = {
    "User-Agent": UserAgent
}


def update_data():
    client = pymongo.MongoClient(
        'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
    # client = pymongo.MongoClient('localhost', 27017).itjuzi_20190724
    # col = client['company']
    col = client['extracted_data']
    col1 = client['page']
    count = int(col1.find_one({'name': 'count'})['value'])
    print(count)
    for res in col.find({'category':'公司'},no_cursor_timeout=True).skip(count).limit(58000):
        # print(res)

        id = res['company_id']
        dataId = res['dataId']
        detail_url = 'https://www.itjuzi.com/api/companies/{}?type=basic'.format(str(id))
        try:
            detail_json = requests.get(detail_url, headers=base_headers, proxies=proxies,
                                         timeout=4).json()
        except:
            for x in range(10):
                try:
                    proxies = random.choice(get_proxies())
                    print(proxies)
                    detail_json = requests.get(detail_url, headers=base_headers, proxies=proxies,
                                                 timeout=4).json()
                    break
                except:
                    pass

       # print(detail_json)

        count += 1
        col1.update({
            'name': 'count'
        }, {
            '$set': {
                'value': count
            }
        })

        if 'com_url' in res:
            continue
        com_stage_name = detail_json['data']['basic']['com_stage_name']
        com_url = detail_json['data']['basic']['com_url']
        com_round_name = detail_json['data']['basic']['com_round_name']
        com_scope = detail_json['data']['basic']['com_scope']
        com_sub_scope = detail_json['data']['basic']['com_sub_scope']

        # 团队成员信息
        person_url = 'https://www.itjuzi.com/api/companies/{}?type=person'.format(str(id))
        try:
            person_json = requests.get(person_url, headers=base_headers, proxies=proxies,
                                         timeout=4).json()
        except:
            for x in range(10):
                try:
                    proxies = random.choice(get_proxies())
                    print(proxies)
                    person_json = requests.get(person_url, headers=base_headers, proxies=proxies,
                                                 timeout=4).json()
                    break
                except:
                    pass
        # 产品禅大师ID
        try:
            products = person_json['data']['products']
        except:
            products = None

        data = {
            'com_url': com_url,
            'com_stage_name': com_stage_name,
            'com_round_name': com_round_name,
            'com_scope': com_scope,
            'com_sub_scope': com_sub_scope,
            'products': products,
        }

        r = col.update(
            {
                'dataId': dataId
            },
            {
                '$set':data
            }
        )
        print('result: ', r)
        time.sleep(1)

if __name__ == '__main__':
    update_data()

