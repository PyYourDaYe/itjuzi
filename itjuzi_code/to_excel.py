import pymongo
import pandas as pd

'''
用pandas格式化将数据写入到表格,也可使用mongoexport直接导出数据或者mongo的图形化工具导出
'''
db = pymongo.MongoClient(
    'mongodb://admin:Ai4ever123!@dds-wz92ccd4e2618da433270.mongodb.rds.aliyuncs.com:3717/').itjuzi_20190724
col = db['extracted_data']
result = []
result2 = []

#res1=col.find({'category':'公司'}).limit(50000)
res2=col.find({'category':'公司'},no_cursor_timeout=True)

#[result.append(x) for x in res1]

#pd1 = pd.DataFrame(result)
#ipd1.to_excel('company.xlsx')

[result2.append(x) for x in res2]
pd2=pd.DataFrame(result2)
pd2.to_excel('company.xlsx')
