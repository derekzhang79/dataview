import pandas as pd
import pymongo
from pymongo import MongoClient
import re
import sys
import pandas as pd

def extract_number(text):
    """提取nameid列中的数字部分"""
    match = re.search(r'(\d+(?:\.\d+)*)', str(text))
    if match:
        return match.group(1)
    else:
        return text

def import_excel_to_mongodb(excel_file, db_name, collection_name):
    """
    将Excel数据导入到MongoDB
    
    参数:
    excel_file: Excel文件路径
    db_name: 数据库名称
    collection_name: 集合名称
    """
    
    try:
        # 1. 读取Excel文件
        print(f"正在读取Excel文件: {excel_file}")
        df = pd.read_excel(excel_file)
        print(f"成功读取数据，共{len(df)}行")
        
        # 2. 处理nameid列，提取数字部分
        print("正在处理nameid列...")
        df['nameid'] = df['nameid'].apply(extract_number)
        
        # 3. 连接MongoDB
        print("正在连接MongoDB...")
        client = MongoClient('localhost', 27017)  # 默认本地连接
        db = client[db_name]
        collection = db[collection_name]
        
        # 4. 清空现有集合（可选）
        print("清空现有集合...")
        collection.delete_many({})
        
        # 5. 转换数据并插入到MongoDB
        print("正在导入数据到MongoDB...")
        records = df.to_dict('records')
        
        # 批量插入数据
        result = collection.insert_many(records)
        
        # 6. 验证导入结果
        count = collection.count_documents({})
        print(f"数据导入完成! 成功导入 {len(result.inserted_ids)} 条记录")
        print(f"当前集合中共有 {count} 条记录")
        
        # 7. 显示前几条记录作为验证
        print("\n前5条记录预览:")
        for doc in collection.find().limit(5):
            print(f"ID: {doc.get('id')}, 名称: {doc.get('name')}, nameid: {doc.get('nameid')}")
        
        client.close()
        return True
        
    except FileNotFoundError:
        print(f"错误: 找不到文件 {excel_file}")
        return False
    except pymongo.errors.ServerSelectionTimeoutError:
        print("错误: 无法连接到MongoDB，请确保MongoDB服务正在运行")
        return False
    except Exception as e:
        print(f"错误: {str(e)}")
        return False

def main():
    # 配置参数
    excel_file = 'xi10dong.xlsx'  # Excel文件路径
    db_name = 'foooodata'         # 数据库名称
    collection_name = 'test' # 集合名称
    
    print("开始导入Excel数据到MongoDB")
    print("=" * 50)
    
    # 执行导入
    success = import_excel_to_mongodb(excel_file, db_name, collection_name)
    
    if success:
        print("\n" + "=" * 50)
        print("数据导入成功完成!")
        print(f"数据库: {db_name}")
        print(f"集合: {collection_name}")
        print("您可以使用以下命令在MongoDB shell中查看数据:")
        print(f"  use {db_name}")
        print(f"  db.{collection_name}.find().pretty()")
    else:
        print("\n数据导入失败!")

if __name__ == "__main__":
    main()