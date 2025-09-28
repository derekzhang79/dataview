import pymongo
from pymongo import MongoClient

"""
MongoDB删除字段工具
此脚本用于从MongoDB集合中删除指定字段
"""

def remove_field_from_collection(db_name, collection_name, field_name):
    """
    从MongoDB集合中删除指定字段
    
    参数:
    db_name: 数据库名称
    collection_name: 集合名称
    field_name: 要删除的字段名
    """
    
    try:
        # 连接MongoDB
        print(f"正在连接MongoDB数据库 '{db_name}'，集合 '{collection_name}'")
        client = MongoClient('localhost', 27017)  # 默认本地连接
        db = client[db_name]
        collection = db[collection_name]
        
        # 获取集合中的文档数量
        total_docs = collection.count_documents({})
        print(f"集合中共有 {total_docs} 条文档")
        
        # 使用update_many删除字段
        # $unset操作符用于从文档中删除指定字段
        result = collection.update_many({}, {'$unset': {field_name: ''}})
        
        print(f"成功处理 {result.modified_count} 条文档")
        print(f"已从集合 '{collection_name}' 中删除字段 '{field_name}'")
        
        # 验证删除结果 - 显示前3条文档
        print("\n验证结果（前3条文档）:")
        for doc in collection.find().limit(3):
            print(f"文档内容: {doc}")
        
        client.close()
        return True
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("错误: 无法连接到MongoDB，请确保MongoDB服务正在运行")
        return False
    except Exception as e:
        print(f"错误: {str(e)}")
        return False

def main():
    # 配置参数
    db_name = 'foooodata'         # 数据库名称
    collection_name = 'xi10dong'  # 集合名称
    field_name_to_remove = 'id'   # 要删除的字段名
    
    print("开始从MongoDB集合中删除字段")
    print("=" * 50)
    
    # 执行删除操作
    success = remove_field_from_collection(db_name, collection_name, field_name_to_remove)
    
    if success:
        print("\n" + "=" * 50)
        print("字段删除成功完成!")
        print(f"数据库: {db_name}")
        print(f"集合: {collection_name}")
        print(f"已删除字段: {field_name_to_remove}")
    else:
        print("\n字段删除失败!")

if __name__ == "__main__":
    main()