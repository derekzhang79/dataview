import pandas as pd
import re

# 读取Excel文件
df = pd.read_excel('xi10dong.xlsx')

# 查看nameid列的前几行，了解数据格式
print("原始nameid列前5行:")
print(df['nameid'].head())

# 定义函数提取数字部分
def extract_number(text):
    # 使用正则表达式匹配数字部分
    # 匹配格式为: 数字和点组成的序列，可能有多个部分
    match = re.search(r'(\d+(?:\.\d+)*)', str(text))
    if match:
        return match.group(1)
    else:
        return text  # 如果没有匹配到数字，返回原文本

# 应用函数更新nameid列
df['nameid'] = df['nameid'].apply(extract_number)

# 查看更新后的结果
print("\n更新后的nameid列前5行:")
print(df['nameid'].head())

# 显示一些示例，对比更新前后的变化
sample_data = df[['name', 'nameid']].head(10)
print("\n更新前后对比示例:")
for idx, row in sample_data.iterrows():
    original_name = row['name']
    new_id = row['nameid']
    print(f"{original_name}: {new_id}")

# 统计处理结果
total_rows = len(df)
successful_extractions = len(df[df['nameid'].str.contains(r'^\d', na=False)])
print(f"\n处理统计:")
print(f"总行数: {total_rows}")
print(f"成功提取数字的行数: {successful_extractions}")
print(f"提取成功率: {successful_extractions/total_rows*100:.2f}%")

# 保存更新后的数据（如果需要）
df.to_excel('xi10dong_updated.xlsx', index=False)