import pymongo

# 连接MongoDB
client = pymongo.MongoClient('localhost', 27017)
db = client['foooodata']
collection = db['constprice']

# 打印集合统计信息
count = collection.count_documents({})
print(f'集合 constprice 中的记录总数: {count}')

# 检查前5条记录，查看导入的字段
print('\n前5条记录的字段示例:')
for doc in collection.find().limit(5):
    print(f"nameid: {doc.get('nameid')}, name: {doc.get('name')}, price: {doc.get('price')}")

# 检查是否有price=0的记录
zero_price_count = collection.count_documents({'price': 0})
print(f'\nprice=0的记录数量: {zero_price_count}')

# 关闭连接
client.close()