import pandas as pd
import pymongo
from pymongo import MongoClient
import re
import sys
import argparse
import pandas as pd
from typing import Dict, Any

def extract_number(text):
    """提取nameid列中的数字部分"""
    match = re.search(r'(\d+(?:\.\d+)*)', str(text))
    if match:
        return match.group(1)
    else:
        return text

def process_excel_data(df):
    """处理Excel数据，提取nameid的数字部分"""
    print("正在处理nameid列...")
    if 'nameid' in df.columns:
        df['nameid'] = df['nameid'].apply(extract_number)
    return df

def smart_upsert_to_mongodb(collection, data_list):
    """
    智能插入/更新数据到MongoDB
    基于nameid的唯一性，对number开头的字段执行相加操作
    """
    operations = []
    
    for data in data_list:
        # 查找是否已存在相同nameid的记录
        existing_record = collection.find_one({"nameid": data["nameid"]})
        
        if existing_record:
            # 如果记录已存在，则更新
            update_fields = {}
            
            # 处理number开头的字段 - 相加操作
            for key, value in data.items():
                if key.startswith('number') and not pd.isna(value):
                    # 获取现有值，如果不存在则为0
                    existing_value = existing_record.get(key, 0)
                    # 如果现有值是None，设为0
                    if existing_value is None:
                        existing_value = 0
                    # 相加
                    new_value = existing_value + value
                    update_fields[key] = new_value
                    print(f"更新number字段: {key}={existing_value} + {value} = {new_value}")
            
            # 添加其他新字段（非number开头）
            for key, value in data.items():
                if not key.startswith('number') and key != 'nameid':
                    # 如果字段不存在或者需要更新
                    if key not in existing_record or (not pd.isna(value) and existing_record.get(key) != value):
                        update_fields[key] = value
                        print(f"添加/更新字段: {key}={value}")
            
            if update_fields:
                operations.append(
                    pymongo.UpdateOne(
                        {"nameid": data["nameid"]},
                        {"$set": update_fields}
                    )
                )
                print(f"更新记录: nameid={data['nameid']}")
        else:
            # 如果记录不存在，则插入新记录
            # 处理NaN值为None
            clean_data = {}
            for key, value in data.items():
                if pd.isna(value):
                    clean_data[key] = 0
                else:
                    clean_data[key] = value
            
            operations.append(
                pymongo.InsertOne(clean_data)
            )
            print(f"插入新记录: nameid={data['nameid']}")
    
    # 批量执行操作
    if operations:
        result = collection.bulk_write(operations)
        return result
    return None

def import_excel_to_mongodb(excel_file, db_name, collection_name):
    """
    将多个Excel文件数据导入到MongoDB
    
    参数:
    excel_files: Excel文件路径列表
    db_name: 数据库名称
    collection_name: 集合名称
    """
    
    try:
        # 连接MongoDB
        print("正在连接MongoDB...")
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        
        # 为nameid字段创建唯一索引（如果不存在）
        collection.create_index([("nameid", pymongo.ASCENDING)], unique=True)
        print("已确保nameid字段的唯一索引")
        
        total_inserted = 0
        total_updated = 0
        
        #for excel_file in excel_files:
        print(f"\n正在处理文件: {excel_file}")
        print("=" * 50)
            
            # 读取Excel文件
        df = pd.read_excel(excel_file)
        print(f"成功读取数据，共{len(df)}行")
        print(f"列名: {list(df.columns)}")
            
            # 处理nameid列
        df = process_excel_data(df)
            
            # 转换数据格式，处理NaN值
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                # 将NaN转换为None，以便MongoDB处理
                if pd.isna(value):
                    record[col] = 0
                else:
                    record[col] = value
            records.append(record)
            
            # 智能插入/更新数据
        result = smart_upsert_to_mongodb(collection, records)
            
        if result:
            total_inserted += result.inserted_count
            total_updated += result.modified_count
            print(f"文件 {excel_file} 处理完成: 插入 {result.inserted_count} 条, 更新 {result.modified_count} 条")
        else:
            print(f"文件 {excel_file} 没有需要更新的数据")
        
        # 验证最终结果
        final_count = collection.count_documents({})
        print(f"\n导入完成!")
        print(f"总共插入: {total_inserted} 条新记录")
        print(f"总共更新: {total_updated} 条现有记录")
        print(f"集合中总记录数: {final_count}")
        
        # 显示一些示例记录
        print("\n前5条记录预览:")
        for doc in collection.find().limit(5):
            print(f"nameid: {doc.get('nameid')}, 名称: {doc.get('name')}")
            # 显示number相关的字段
            number_fields = {k: v for k, v in doc.items() if k.startswith('number')}
            if number_fields:
                print(f"  number字段: {number_fields}")
        
        client.close()
        return True
        
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 {e}")
        return False
    except pymongo.errors.ServerSelectionTimeoutError:
        print("错误: 无法连接到MongoDB，请确保MongoDB服务正在运行")
        return False
    except pymongo.errors.BulkWriteError as e:
        print(f"批量写入错误: {e.details}")
        return False
    except Exception as e:
        print(f"错误: {str(e)}")
        return False

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='智能导入Excel数据到MongoDB')
    parser.add_argument('excel_file', help='要导入的Excel文件路径')
    parser.add_argument('--db', default='foooodata', help='MongoDB数据库名称(默认: foooodata)')
    parser.add_argument('--collection', default='test', help='MongoDB集合名称(默认: test)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 配置参数
    excel_file = args.excel_file
    db_name = args.db
    collection_name = args.collection
    
    print(f"开始智能导入Excel数据到MongoDB")
    print(f"要导入的文件: {excel_file}")
    print(f"目标数据库: {db_name}")
    print(f"目标集合: {collection_name}")
    print("特殊规则:")
    print("1. 以nameid作为唯一键")
    print("2. 如果nameid不存在，插入新记录")
    print("3. 对number开头的字段执行相加操作")
    print("4. 自动添加其他新字段（如price9, bidprice9等）")
    print("=" * 60)
    
    # 执行导入
    success = import_excel_to_mongodb(excel_file, db_name, collection_name)
    
    if success:
        print("\n" + "=" * 60)
        print("数据导入成功完成!")
        print(f"数据库: {db_name}")
        print(f"集合: {collection_name}")
        print("\n您可以使用以下命令在MongoDB shell中查看数据:")
        print(f"  use {db_name}")
        print(f"  db.{collection_name}.find().pretty()")
        print(f"  db.{collection_name}.count()")
        
        # 显示特定记录的示例
        print("\n示例记录详情:")
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        
        # 查找有number10和number9的记录
        sample_record = collection.find_one({
            "$and": [
                {"nameid": {"$exists": True}},
                {"number10": {"$exists": True}},
                {"number9": {"$exists": True}}
            ]
        })
        
        if sample_record:
            print(f"示例记录 - nameid: {sample_record.get('nameid')}")
            print(f"  number10: {sample_record.get('number10')}")
            print(f"  number9: {sample_record.get('number9')}")
            print(f"  price10: {sample_record.get('price10')}")
            print(f"  price9: {sample_record.get('price9')}")
        
        client.close()
    else:
        print("\n数据导入失败!")

if __name__ == "__main__":
    main()