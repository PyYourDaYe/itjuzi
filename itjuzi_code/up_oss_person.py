import os
import requests
import random
import pymongo

import oss2, uuid
'''
上传投资人的logo信息,并返回oss的链接,将itjuzi数据库中的数据替换掉
'''
OSS_ACCESS_KEY_ID = 'LTAIFrKBmXgK7457'
OSS_ACCESS_KEY_SECRET = '4oPvw4OMbpTJwlwYvkm2vUjiGmau1P'
OSS_ENDPOINT = 'oss-cn-shenzhen.aliyuncs.com'
OSS_INTERNAL_ENDPOINT = 'oss-cn-shenzhen.aliyuncs.com'
OSS_BUCKET = 'si-policy-test'
OSS_URL_VALIDITY = 3600 * 24 * 365 * 10  # oss文件的url有效期,单位秒
BASE_URL = 'https://www.itjuzi.com'
QCRAWLER_SAVE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
    'User-Agent': UserAgent,
}
# client = pymongo.MongoClient('localhost', 27017).itjuzi_20190724


client = pymongo.MongoClient(
    'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724

def oss(img_key, img_data):
    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    # a = bucket.put_object_from_file(img_key, img_data)
    a = bucket.put_object(img_key, img_data)
    print('status' + str(a.status))
    # 获取返回的url
    jpg_url = bucket.sign_url('GET', img_key, OSS_URL_VALIDITY)

    return jpg_url


def up():
    # 下载logo
    # col = client['investment']
    i = 25
    col = client['extracted_data']
    res = col.find({'category': '投资人'}).skip(25)
    for x in res:

        url = x['person_logo']
        if url == None:
            continue

        # try:
        img_content = requests.get(url=url, headers=headers).content
        # print(img_content)
        # 上传logo文件到oss
        dataId = x['dataId']
        jpg_url = oss(url, img_content)
        # 将数据库中的logo url 替换为oss中的地址
        col.update_one(
            {
                'dataId': dataId
            },
            {
                '$set': {
                    'person_logo': jpg_url
                }
            })
        i += 1
        print(i)
        print('upload success!')
        # except Exception as e:
        #     print(e)


if __name__ == "__main__":
    up()

