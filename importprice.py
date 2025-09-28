import pandas as pd
import pymongo
from pymongo import MongoClient
import re
import argparse
from typing import Dict, Any
import os



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



def smart_upsert_to_mongodb(collection, data_list):
    """
    智能插入/更新数据到MongoDB
    基于nameid的唯一性，当字段值不同时更新对应字段
    """
    operations = []
    inserted_count = 0
    updated_count = 0
    
    for data in data_list:
        # 清理数据，确保字段不为null
        data = clean_data_record(data)
        
        # 确保nameid不为空
        if data.get('nameid') is None:
            print("警告: 跳过nameid为空的记录")
            continue
            
        # 查找是否已存在相同nameid的记录
        existing_record = collection.find_one({"nameid": data["nameid"]})
        
        if existing_record:
            # 如果记录已存在，处理所有关键字段
            update_fields = {}
            updated_any = False
            
            # 处理price字段
            if 'price' in data:
                current_price = data['price']
                existing_price = existing_record.get('price')
                
                # 只有当price值不同时才进行更新
                if current_price != existing_price:
                    update_fields['price'] = current_price
                    print(f"更新price字段: nameid={data['nameid']} 从 {existing_price} 更新为 {current_price}")
                    updated_any = True
            
            # 处理name字段
            if 'name' in data:
                current_name = data['name']
                existing_name = existing_record.get('name')
                
                # 只有当name值不同时才进行更新
                if current_name != existing_name:
                    update_fields['name'] = current_name
                    print(f"更新name字段: nameid={data['nameid']} 从 {existing_name} 更新为 {current_name}")
                    updated_any = True
            
            # 处理spec字段
            if 'spec' in data:
                current_spec = data['spec']
                existing_spec = existing_record.get('spec')
                
                # 只有当spec值不同时才进行更新
                if current_spec != existing_spec:
                    update_fields['spec'] = current_spec
                    print(f"更新spec字段: nameid={data['nameid']} 从 {existing_spec} 更新为 {current_spec}")
                    updated_any = True
            
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
                print(f"无需更新: nameid={data['nameid']} 的所有字段值都没有变化")
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

def import_excel_to_mongodb(excel_files, db_name, collection_name):
    """
    从Excel文件导入数据到MongoDB
    
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
        
        # 逐个处理文件
        for excel_file in excel_files:
            print(f"\n正在处理文件: {excel_file}")
            print("=" * 50)
            
            # 读取Excel文件
            df = pd.read_excel(excel_file, engine='openpyxl')
            print(f"成功读取数据，共{len(df)}行")
            print(f"列名: {list(df.columns)}")
            
            # 检查是否包含必要的字段
            if 'name' not in df.columns or 'nameid' not in df.columns or 'spec' not in df.columns:
                print(f"警告: 文件 {excel_file} 不包含name、nameid或spec必要字段，跳过处理")
                continue
            
            # 
            
            # 检查price列是否存在
            has_price_column = 'price' in df.columns
            
            # 只保留必要的字段
            columns_to_keep = ['name', 'nameid', 'spec']
            if has_price_column:
                columns_to_keep.append('price')
                print("文件包含price字段，将正常导入")
            else:
                print("文件不包含price字段，将设置price=0")
            
            # 只选择存在的列
            df = df[columns_to_keep]
            
            # 转换数据格式，处理NaN值和N/A值
            records = []
            for _, row in df.iterrows():
                record = {}
                # 处理name和nameid字段
                for col in ['name', 'nameid']:
                    value = row[col]
                    # 将NaN和N/A值转换为0
                    if pd.isna(value) or str(value).lower() in ['n/a', 'na', 'none']:
                        record[col] = 0
                    else:
                        record[col] = value
                
               
                
                # 处理spec字段 - 与name和nameid处理逻辑一致
                spec_value = row['spec']
                if pd.isna(spec_value) or str(spec_value).lower() in ['n/a', 'na', 'none']:
                    record['spec'] = 0
                else:
                    record['spec'] = spec_value

                
                 # 处理price字段
                if has_price_column:
                    price_value = row['price']
                    if pd.isna(price_value) or str(price_value).lower() in ['n/a', 'na', 'none']:
                        record['price'] = 0
                    else:
                        record['price'] = price_value
                else:
                    # 如果没有price列，设置为0
                    record['price'] = 0
                
                records.append(record)
            
            # 智能插入/更新数据
            result = smart_upsert_to_mongodb(collection, records)
            
            if result:
                total_inserted += result.inserted_count
                total_updated += result.modified_count
                print(f"文件 {excel_file} 处理完成: 插入 {result.inserted_count} 条, 更新 {result.modified_count} 条")
        

        
        # 确保所有记录的price字段不为null，而是0
        print("确保所有记录的price字段不为null...")
        collection.update_many(
            {"price": None},
            {"$set": {"price": 0}}
        )
        
        # 确保所有记录的spec字段不为null，而是0
        print("确保所有记录的spec字段不为null...")
        collection.update_many(
            {"spec": None},
            {"$set": {"spec": 0}}
        )
        
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
    parser.add_argument('excel_file', nargs='?', default='constprice.xlsx', help='要导入的Excel文件路径（默认: constprice.xlsx）')
    parser.add_argument('--db', default='foooodata', help='MongoDB数据库名称(默认: foooodata)')
    parser.add_argument('--collection', default='constprice', help='MongoDB集合名称(默认: constprice)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 配置参数
    db_name = args.db
    collection_name = args.collection
    excel_file = args.excel_file
    
    # 确定要导入的Excel文件
    if not os.path.exists(excel_file):
        print(f"错误: 找不到文件 {excel_file}")
        return
    
    excel_files = [excel_file]
    print(f"开始导入指定文件到MongoDB")
    print(f"要导入的文件: {excel_file}")
    
    print("=" * 60)
    print("特殊规则:")
    print("1. 以nameid作为唯一键")
    print("2. 如果nameid不存在，插入新记录")
    print("3. 如果nameid已存在，当字段值不同时更新对应字段")
    print("4. 导入name、nameid、spec和price字段")
    print("5. 确保所有字段不为null，数值类型为0")
    print("=" * 60)
    
    # 执行导入
    success = import_excel_to_mongodb(excel_files, db_name, collection_name)
    
    if success:
        print("\n" + "=" * 60)
        print("数据导入成功完成!")
        print(f"数据库: {db_name}")
        print(f"集合: {collection_name}")
        print("\n您可以使用以下命令在MongoDB shell中查看数据:")
        print(f"  use {db_name}")
        print(f"  db.{collection_name}.find().pretty()")
        print(f"  db.{collection_name}.count()")
        

        
        # 检查是否有重复的nameid
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        
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