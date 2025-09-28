import pandas as pd

# 创建测试数据，包含要更新的name、spec和price字段
data = [
    {'name': '更新后的熟牛肉', 'nameid': '705011300', 'spec': '更新后的规格1', 'price': 199.9},
    {'name': '更新后的鳝鱼丝', 'nameid': '801042600', 'spec': '更新后的规格2', 'price': 188.8},
    {'name': '更新后的仔鸡（整）', 'nameid': '701044800', 'spec': '更新后的规格3', 'price': 177.7}
]

# 创建DataFrame
df = pd.DataFrame(data)

# 保存为Excel文件
df.to_excel('comprehensive_test.xlsx', index=False, engine='openpyxl')

print(f"已创建测试文件 'comprehensive_test.xlsx'")
print(f"包含 {len(df)} 条测试记录")
print("测试数据:")
for index, row in df.iterrows():
    print(f"name: {row['name']}, nameid: {row['nameid']}, spec: {row['spec']}, price: {row['price']}")