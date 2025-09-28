import pandas as pd

# 创建测试数据
data = {
    'name': ['测试商品1', '测试商品2', '测试商品3'],
    'nameid': ['0705011300', '0801042600', '0701044800'],
    'spec': ['规格1', '规格2', '规格3'],
    'price': [99.9, 88.8, 77.7]
}

# 创建DataFrame并导出为Excel文件
df = pd.DataFrame(data)
df.to_excel('test_update_price.xlsx', index=False, engine='openpyxl')
print("测试Excel文件已创建: test_update_price.xlsx")
print(df)