import pandas as pd
import pymongo
from pymongo import MongoClient
import re
import sys
import argparse
import pandas as pd
from typing import Dict, Any

def extract_number(text):
    """提取nameid列中的数字部分，优先提取末尾的长数字串"""
    # 优先提取末尾的长数字串（例如：-0701012400格式中的数字）
    match_end = re.search(r'-(\d{10,})', str(text))
    if match_end:
        return match_end.group(1)
    
    # 提取任何数字串（保留原有逻辑作为后备）
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

def clean_data_record(record):
    """清理数据记录，将NaN/None/N/A转换为0"""
    cleaned = {}
    for key, value in record.items():
        # 处理所有字段 - 将NaN、None和N/A转换为0
        if pd.isna(value) or value is None or str(value).lower() in ['n/a', 'na', 'none']:
            cleaned[key] = 0
        else:
            cleaned[key] = value
    return cleaned

def ensure_number_fields_zero(collection):
    """
    确保所有number开头的字段在数据库中不为null，而是0
    """
    print("确保number字段不为null...")
    
    # 获取所有文档
    all_docs = collection.find({})
    
    for doc in all_docs:
        update_needed = False
        update_fields = {}
        
        # 检查每个number开头的字段
        for field_name in doc.keys():
            if field_name.startswith('number') and doc[field_name] is None:
                update_fields[field_name] = 0
                update_needed = True
        
        # 如果有需要更新的字段，执行更新
        if update_needed:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": update_fields}
            )
            print(f"更新文档 {doc.get('nameid', '未知')}: 设置 {list(update_fields.keys())} 为0")

def smart_upsert_to_mongodb(collection, data_list):
    """
    智能插入/更新数据到MongoDB
    基于nameid的唯一性，对number开头的字段执行相加操作
    """
    operations = []
    inserted_count = 0
    updated_count = 0
    
    for data in data_list:
        # 清理数据，确保number字段不为null
        data = clean_data_record(data)
        
        # 确保nameid不为空
        if data.get('nameid') is None:
            print("警告: 跳过nameid为空的记录")
            continue
            
        # 查找是否已存在相同nameid的记录
        existing_record = collection.find_one({"nameid": data["nameid"]})
        
        if existing_record:
            # 如果记录已存在，则更新
            update_fields = {}
            
            # 处理number开头的字段 - 相加操作
            # 以下代码已被注释掉
            # for key, value in data.items():
            #     if key.startswith('number') and value is not None:
            #         # 获取现有值，如果不存在或为None则设为0
            #         existing_value = existing_record.get(key)
            #         if existing_value is None:
            #             existing_value = 0
            #         
            #         # 相加
            #         new_value = existing_value + value
            #         update_fields[key] = new_value
            #         print(f"更新number字段: {key}={existing_value} + {value} = {new_value}")
            
            # 添加其他新字段（非number开头）
            for key, value in data.items():
                if not key.startswith('number') and key != 'nameid':
                    # 如果字段不存在或者需要更新
                    if key not in existing_record or existing_record.get(key) != value:
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
                updated_count += 1
            else:
                print(f"无需更新: nameid={data['nameid']}")
        else:
            # 如果记录不存在，则插入新记录
            operations.append(
                pymongo.InsertOne(data)
            )
            print(f"插入新记录: nameid={data['nameid']}")
            inserted_count += 1
    
    # 批量执行操作
    if operations:
        result = collection.bulk_write(operations)
        print(f"批量操作结果: 插入 {inserted_count} 条, 更新 {updated_count} 条")
        return result
    return None

def import_excel_to_mongodb(excel_file, db_name, collection_name):
    """
    
    参数:
    excel_file: Excel文件路径列表
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
        
        
        print(f"\n正在处理文件: {excel_file}")
        print("=" * 50)
            
            # 读取Excel文件
        df = pd.read_excel(excel_file)
        print(f"成功读取数据，共{len(df)}行")
        print(f"列名: {list(df.columns)}")
            
            # 处理nameid列
        df = process_excel_data(df)
            
            # 转换数据格式，处理NaN值和N/A值
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                # 将NaN和N/A值转换为0
                if pd.isna(value) or str(value).lower() in ['n/a', 'na', 'none']:
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
        
        # 确保所有number字段不为null
        ensure_number_fields_zero(collection)
        
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
    
    print("开始智能导入Excel数据到MongoDB")
    print("特殊规则:")
    print("1. 以nameid作为唯一键")
    print("2. 如果nameid不存在，插入新记录")
    print("3. 如果nameid已存在，更新现有记录，添加新字段")
    print("4. 对number开头的字段执行相加操作")
    print("5. 自动添加其他新字段（如price9, bidprice9等）")
    print("6. 确保所有number字段不为null，而是0")
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
        sample_records = collection.find({
            "$or": [
                {"number10": {"$exists": True}},
                {"number9": {"$exists": True}}
            ]
        }).limit(3)
        
        for doc in sample_records:
            print(f"示例记录 - nameid: {doc.get('nameid')}, 名称: {doc.get('name')}")
            if 'number10' in doc:
                print(f"  number10: {doc.get('number10')}")
            if 'number9' in doc:
                print(f"  number9: {doc.get('number9')}")
            # 显示所有字段
            print(f"  所有字段: {list(doc.keys())}")
            print("---")
        
        # 检查是否有重复的nameid
        pipeline = [
            {"$group": {
                "_id": "$nameid",
                "count": {"$sum": 1}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }}
        ]
        duplicates = list(collection.aggregate(pipeline))
        if duplicates:
            print(f"\n警告: 发现 {len(duplicates)} 个重复的nameid:")
            for dup in duplicates:
                print(f"  nameid: {dup['_id']}, 出现次数: {dup['count']}")
        else:
            print("\n检查完成: 没有发现重复的nameid")
        
        client.close()
    else:
        print("\n数据导入失败!")

if __name__ == "__main__":
    main()