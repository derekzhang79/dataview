import pymongo

# 连接MongoDB
client = pymongo.MongoClient('localhost', 27017)
db = client['foooodata']
collection = db['constprice']

# 打印集合统计信息
count = collection.count_documents({})
print(f'集合 constprice 中的记录总数: {count}')

# 检查必要字段
print('\n检查所有记录是否包含必要字段(name, nameid, price, spec):')
missing_name = collection.count_documents({'name': None})
missing_nameid = collection.count_documents({'nameid': None})
missing_price = collection.count_documents({'price': None})
missing_spec = collection.count_documents({'spec': None})
print(f'name为None的记录数: {missing_name}')
print(f'nameid为None的记录数: {missing_nameid}')
print(f'price为None的记录数: {missing_price}')
print(f'spec为None的记录数: {missing_spec}')

# 检查是否有记录同时缺少多个字段
records_with_issues = collection.count_documents({
    '$or': [
        {'name': None},
        {'nameid': None},
        {'price': None},
        {'spec': None}
    ]
})
print(f'有任何字段问题的记录数: {records_with_issues}')

# 显示一些完整记录的详细信息
print('\n显示3条完整记录的详细信息:')
for doc in collection.find().limit(3):
    print(f"_id: {doc.get('_id')}")
    print(f"name: {doc.get('name')}")
    print(f"nameid: {doc.get('nameid')}")
    print(f"price: {doc.get('price')}")
    print(f"spec: {doc.get('spec')}")
    print("------------------")

# 检查一些包含spec字段的记录
print('\n显示3条包含spec字段的记录示例:')
for doc in collection.find({'spec': {'$ne': 0}}).limit(3):
    print(f"nameid: {doc.get('nameid')}, name: {doc.get('name')}, spec: {doc.get('spec')}")

# 关闭连接
client.close()
print('\n检查完成！')