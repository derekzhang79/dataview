#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo
import argparse
import sys
from datetime import datetime


def connect_to_mongodb(db_name='foooodata', host='localhost', port=27017):
    """连接到MongoDB数据库"""
    try:
        client = pymongo.MongoClient(host=host, port=port, serverSelectionTimeoutMS=5000)
        # 测试连接
        client.server_info()
        db = client[db_name]
        print(f"成功连接到MongoDB数据库: {db_name}")
        return db
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(f"MongoDB连接超时: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"MongoDB连接失败: {e}")
        sys.exit(1)


def check_columns_exist(collection, numer_name, price_name):
    """检查指定的列是否存在于数据库集合中"""
    try:
        # 获取一个文档来检查字段是否存在
        sample_doc = collection.find_one()
        if not sample_doc:
            print("数据库集合中没有数据")
            return False
        
        # 检查必要的字段是否存在
        required_fields = ['price', numer_name, price_name]
        missing_fields = [field for field in required_fields if field not in sample_doc]
        
        if missing_fields:
            print(f"以下必要字段在数据库中不存在: {', '.join(missing_fields)}")
            return False
        
        # 检查字段类型是否为数值类型
        for field in required_fields:
            if not isinstance(sample_doc[field], (int, float)) and sample_doc[field] is not None:
                print(f"字段 '{field}' 不是数值类型，无法进行计算")
                return False
        
        return True
    except Exception as e:
        print(f"检查字段时出错: {e}")
        return False


def calculate_profit(collection, number_name, price_name):
    """计算总体利润"""
    try:
        total_profit = 0
        total_revenue = 0
        count = 0
        
        # 查询所有文档
        for doc in collection.find():
            price = doc.get('price', 0)
            number_value = doc.get(number_name, 0)
            price_value = doc.get(price_name, 0)
            
            # 确保所有值都是数值类型
            if not isinstance(price, (int, float)):
                price = 0
            if not isinstance(number_value, (int, float)):
                number_value = 0
            if not isinstance(price_value, (int, float)):
                price_value = 0
            
            # 计算列盈利
            column_profit = (price_value - price) * number_value
            total_profit += column_profit

            total_revenue += price_value * number_value
            count += 1
        
        return total_profit, total_revenue, count
    except Exception as e:
        print(f"计算利润时出错: {e}")
        return None, None, 0


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='计算数据库中基于指定列的总体利润')
    parser.add_argument('--number_name', required=True, help='数量列名称')
    parser.add_argument('--price_name', required=True, help='价格列名称')
    parser.add_argument('--db', default='foooodata', help='数据库名称，默认为foooodata')
    parser.add_argument('--collection', default='constprice', help='集合名称，默认为constprice')
    parser.add_argument('--host', default='localhost', help='MongoDB主机地址，默认为localhost')
    parser.add_argument('--port', type=int, default=27017, help='MongoDB端口，默认为27017')
    
    args = parser.parse_args()
    
    # 连接到MongoDB
    db = connect_to_mongodb(args.db, args.host, args.port)
    
    # 获取集合
    collection = db[args.collection]
    
    # 检查列是否存在
    if not check_columns_exist(collection, args.number_name, args.price_name):
        print("参数无效或指定的列在数据库中不存在，无法进行计算")
        sys.exit(1)
    
    print(f"开始计算总体利润，使用列: {args.number_name} 和 {args.price_name}")
    
    # 计算总体利润
    total_profit, total_revenue,count = calculate_profit(collection, args.number_name, args.price_name)
    
    if total_profit is not None and total_revenue is not None:
        print(f"\n计算结果:")
        print(f"处理的记录总数: {count}")
        print(f"总体利润: {total_profit:.2f},营业额: {total_revenue:.2f},利润率：{total_profit/total_revenue:.2%}")
        print(f"计算时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"算法: 列盈利 = ({args.price_name} - price) * {args.number_name}，总盈利 = 所有列盈利之和")
    else:
        print("计算失败，请检查日志获取更多信息")


if __name__ == '__main__':
    main()