import pymongo
from pymongo import MongoClient
import argparse
import json
import os


def set_default_values_to_mongodb(db_name, collection_name, field_defaults, force_update=False):
    """
    设置MongoDB集合中指定字段的默认值
    
    参数:
    db_name: 数据库名称
    collection_name: 集合名称
    field_defaults: 字典，键为字段名，值为默认值
    
    返回:
    dict: 操作结果，包含更新的记录数和操作状态
    """
    try:
        # 连接MongoDB
        print(f"正在连接MongoDB...")
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        
        # 验证集合是否存在
        if collection_name not in db.list_collection_names():
            print(f"警告: 集合 {collection_name} 不存在于数据库 {db_name} 中")
            client.close()
            return {
                'success': False,
                'message': f"集合 {collection_name} 不存在",
                'updated_count': 0
            }
        
        # 准备更新操作
        update_result = None
        for field_name, default_value in field_defaults.items():
            # 根据是否强制更新决定过滤条件
            if force_update:
                # 强制更新：没有过滤条件，更新所有文档
                filter_query = {}
                print(f"\n正在强制为所有文档的字段 '{field_name}' 设置值 '{default_value}'")
            else:
                # 默认行为：只更新缺少该字段、字段值为None或字段值为空值的文档
                filter_query = {
                    "$or": [
                        {field_name: {'$exists': False}},
                        {field_name: None},
                        {field_name: ""},
                        {field_name: []},
                        {field_name: {}}
                    ]
                }
                print(f"\n正在为字段 '{field_name}' 设置默认值 '{default_value}'")
                print(f"过滤条件: {filter_query}")
                
            update_query = {"$set": {field_name: default_value}}
            
            print(f"\n正在为字段 '{field_name}' 设置默认值 '{default_value}'")
            print(f"过滤条件: {filter_query}")
            
            # 执行更新操作
            update_result = collection.update_many(filter_query, update_query)
            
            # 输出更新结果
            print(f"更新完成: 匹配到 {update_result.matched_count} 条记录, 更新了 {update_result.modified_count} 条记录")
        
        # 获取集合中总记录数
        total_records = collection.count_documents({})
        print(f"\n集合 {collection_name} 中总记录数: {total_records}")
        
        # 预览一些更新后的文档
        print("\n前5条记录预览:")
        for doc in collection.find().limit(5):
            # 格式化输出，只显示相关字段
            display_doc = {k: v for k, v in doc.items() if k in field_defaults or k == '_id' or k == 'nameid' or k == 'name'}
            print(f"{json.dumps(display_doc, ensure_ascii=False, default=str)}")
        
        client.close()
        return {
            'success': True,
            'message': f"成功为集合 {collection_name} 中的字段设置默认值",
            'updated_count': update_result.modified_count if update_result else 0
        }
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("错误: 无法连接到MongoDB，请确保MongoDB服务正在运行")
        return {
            'success': False,
            'message': "MongoDB连接失败",
            'updated_count': 0
        }
    except Exception as e:
        print(f"错误: {str(e)}")
        return {
            'success': False,
            'message': str(e),
            'updated_count': 0
        }


def parse_field_defaults(field_strings):
    """
    解析命令行传入的字段默认值字符串列表
    格式为: field_name=default_value
    """
    field_defaults = {}
    
    for field_string in field_strings:
        try:
            # 分割字段名和默认值
            field_name, default_value = field_string.split('=', 1)
            
            # 尝试转换默认值的类型
            # 1. 尝试转换为整数
            try:
                default_value = int(default_value)
            except ValueError:
                # 2. 尝试转换为浮点数
                try:
                    default_value = float(default_value)
                except ValueError:
                    # 3. 尝试转换为布尔值
                    if default_value.lower() == 'true':
                        default_value = True
                    elif default_value.lower() == 'false':
                        default_value = False
                    # 4. 保持为字符串
            
            field_defaults[field_name] = default_value
            
        except ValueError:
            print(f"警告: 字段格式不正确: {field_string}，应为 field_name=default_value 格式")
    
    return field_defaults


def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='为MongoDB集合中的字段设置默认值')
    parser.add_argument('--db', default='foooodata', help='MongoDB数据库名称(默认: foooodata)')
    parser.add_argument('--collection', default='constprice', help='MongoDB集合名称(默认: constprice)')
    parser.add_argument('--force', action='store_true', help='强制更新所有文档中的字段值，而不仅限于空值或缺失字段')
    parser.add_argument('fields', nargs='+', help='要设置默认值的字段，格式为: field_name=default_value，可指定多个字段')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 配置参数
    db_name = args.db
    collection_name = args.collection
    
    # 解析字段默认值
    field_defaults = parse_field_defaults(args.fields)
    
    if not field_defaults:
        print("错误: 没有有效的字段默认值设置")
        return
    
    print("=" * 60)
    print(f"开始为MongoDB集合中的字段设置默认值")
    print(f"数据库: {db_name}")
    print(f"集合: {collection_name}")
    print(f"要设置的字段及默认值: {field_defaults}")
    print("=" * 60)
    
    # 执行设置默认值操作
    result = set_default_values_to_mongodb(db_name, collection_name, field_defaults, args.force)
    
    if result['success']:
        print("\n" + "=" * 60)
        print("设置默认值成功完成!")
        print(f"总共更新了 {result['updated_count']} 条记录")
        print(f"\n您可以使用以下命令在MongoDB shell中验证结果:")
        print(f"  use {db_name}")
        print(f"  db.{collection_name}.find().pretty()")
        print(f"  db.{collection_name}.count()")
    else:
        print(f"\n设置默认值失败: {result['message']}")


if __name__ == "__main__":
    main()