from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo import MongoClient, ASCENDING, DESCENDING
import pandas as pd
import argparse
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows

def highlight_min_values_in_excel(df, output_file):
    """对Excel文件中的price9和price10、bidprice9和bidprice10比较，将较小值标记为红色字体"""
    # 使用ExcelWriter和openpyxl引擎
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 先将数据写入Excel
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        # 获取工作表
        worksheet = writer.sheets['Sheet1']
        
        # 获取列名行索引（第1行）
        header_row = 1
        
        # 获取price9、price10、bidprice9、bidprice10列的索引
        price9_col = None
        price10_col = None
        bidprice9_col = None
        bidprice10_col = None
        
        # 查找各列的索引
        for cell in worksheet[header_row]:
            if cell.value == 'price9':
                price9_col = cell.column
            elif cell.value == 'price10':
                price10_col = cell.column
            elif cell.value == 'bidprice9':
                bidprice9_col = cell.column
            elif cell.value == 'bidprice10':
                bidprice10_col = cell.column
        
        # 创建红色字体样式
        red_font = Font(color='FF0000')
        
        # 从第2行开始遍历数据行（跳过表头）
        for row in range(2, worksheet.max_row + 1):
            # 比较price9和price10
            if price9_col and price10_col:
                price9_cell = worksheet.cell(row=row, column=price9_col)
                price10_cell = worksheet.cell(row=row, column=price10_col)
                
                try:
                    # 尝试将值转换为数字进行比较
                    price9_val = float(price9_cell.value) if price9_cell.value else float('inf')
                    price10_val = float(price10_cell.value) if price10_cell.value else float('inf')
                    
                    # 将较小值标记为红色
                    if price9_val < price10_val:
                        price9_cell.font = red_font
                    elif price10_val < price9_val:
                        price10_cell.font = red_font
                except (ValueError, TypeError):
                    # 如果无法比较，跳过
                    pass
            
            # 比较bidprice9和bidprice10
            if bidprice9_col and bidprice10_col:
                bidprice9_cell = worksheet.cell(row=row, column=bidprice9_col)
                bidprice10_cell = worksheet.cell(row=row, column=bidprice10_col)
                
                try:
                    # 尝试将值转换为数字进行比较
                    bidprice9_val = float(bidprice9_cell.value) if bidprice9_cell.value else float('inf')
                    bidprice10_val = float(bidprice10_cell.value) if bidprice10_cell.value else float('inf')
                    
                    # 将较小值标记为红色
                    if bidprice9_val < bidprice10_val:
                        bidprice9_cell.font = red_font
                    elif bidprice10_val < bidprice9_val:
                        bidprice10_cell.font = red_font
                except (ValueError, TypeError):
                    # 如果无法比较，跳过
                    pass


def export_mongodb_to_excel():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='从MongoDB导出数据到Excel文件')
    parser.add_argument('--db', default='foooodata', help='MongoDB数据库名称(默认: foooodata)')
    parser.add_argument('collection', help='MongoDB集合名称(必须指定)')
    parser.add_argument('--fields', help='要导出的字段名，多个字段用逗号分隔(默认导出所有字段)')
    parser.add_argument('--sort', default='nameid', help='排序字段(默认: nameid)')
    parser.add_argument('--order', choices=['asc', 'desc'], default='asc', help='排序顺序(默认: asc升序)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 配置参数
    db_name = args.db
    collection_name = args.collection
    output_file = f'{collection_name}.xlsx'
    
    # 处理字段参数
    fields = None
    if args.fields:
        fields = {field.strip(): 1 for field in args.fields.split(',')}
        # 确保_id字段不被包含（除非明确指定）
        if '_id' not in fields:
            fields['_id'] = 0
    else:
        # 默认不导出level1, level2, level3, spec和_id字段
        fields = {'level1': 0, 'level2': 0, 'level3': 0, 'spec': 0, '_id': 0}
    
    # 处理排序参数
    sort_field = args.sort
    sort_order = ASCENDING if args.order == 'asc' else DESCENDING
    sort_criteria = [(sort_field, sort_order)]
    
    try:
        # 连接到MongoDB
        print(f"正在连接MongoDB数据库: {db_name}")
        print(f"正在访问集合: {collection_name}")
        client = MongoClient('mongodb://localhost:27017/')
        db = client[db_name]
        collection = db[collection_name]
        
        # 查询所有文档，应用字段筛选和排序
        print(f"正在查询文档...")
        print(f"  导出字段: {'所有字段(除level1,level2,level3,spec,_id)' if args.fields is None else args.fields}")
        print(f"  排序方式: {sort_field} {args.order}")
        cursor = collection.find({}, fields).sort(sort_criteria)
        
        # 将游标转换为列表，然后创建DataFrame
        print("正在处理数据...")
        df = pd.DataFrame(list(cursor))
        
        # 如果用户指定了字段，按照用户指定的顺序排列列
        if args.fields:
            column_order = [field.strip() for field in args.fields.split(',')]
            # 过滤出存在的列
            existing_columns = [col for col in column_order if col in df.columns]
            # 补充其他可能存在的列
            remaining_columns = [col for col in df.columns if col not in existing_columns]
            df = df[existing_columns + remaining_columns]
        
        # 检查是否有数据
        if df.empty:
            print("警告: 没有找到数据")
            return
        
        # 导出DataFrame到Excel文件并高亮显示较小值
        print(f"正在导出到Excel文件: {output_file}")
        print(f"  导出列: {', '.join(df.columns.tolist())}")
        print(f"  正在处理数值比较，将较小值标记为红色字体")
        highlight_min_values_in_excel(df, output_file)
        
        print(f"导出完成! 共导出{len(df)}条记录")
        print(f"输出文件: {output_file}")
        
        client.close()
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("错误: 无法连接到MongoDB，请确保MongoDB服务正在运行")
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    export_mongodb_to_excel()