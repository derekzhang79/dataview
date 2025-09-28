import pandas as pd
import argparse
import pandas as pd
from pymongo import MongoClient


def export_mongodb_to_excel(db_name='foooodata', collection_name='constprice', output_file='constprice.xlsx'):
    """
    从MongoDB导出数据到Excel文件
    
    参数:
    db_name: 数据库名称，默认为'foooodata'
    collection_name: 集合名称，默认为'constprice'
    output_file: 输出Excel文件名，默认为'constprice.xlsx'
    """
    try:
        # 连接到MongoDB
        print(f"正在连接到MongoDB数据库: {db_name}")
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        
        # 查询所有文档，排除_id字段
        print(f"正在查询集合: {collection_name}")
        cursor = collection.find({}, {'_id': 0})
        
        # 转换为DataFrame
        print(f"正在处理数据...")
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            print("警告: 查询结果为空")
            return False
        
        # 导出到Excel
        print(f"正在导出到Excel文件: {output_file}")
        df.to_excel(output_file, index=False)
        
        print(f"导出成功!")
        print(f"共导出 {len(df)} 条记录")
        print(f"包含字段: {list(df.columns)}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"导出过程中发生错误: {str(e)}")
        return False


def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='从MongoDB导出数据到Excel文件')
    
    # 添加命令行参数
    parser.add_argument('--db', type=str, default='foooodata', 
                        help='MongoDB数据库名称 (默认: foooodata)')
    parser.add_argument('--collection', type=str, default='constprice', 
                        help='MongoDB集合名称 (默认: constprice)')
    parser.add_argument('--output', type=str, default='constprice.xlsx',
                        help='输出Excel文件名 (默认: constprice.xlsx)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 执行导出并检查结果
    success = export_mongodb_to_excel(args.db, args.collection, args.output)
    
    if not success:
        print("数据导出失败!")


if __name__ == '__main__':
    main()