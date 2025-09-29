from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo import MongoClient, ASCENDING, DESCENDING
import pandas as pd
import argparse
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows

def highlight_min_values_in_excel(df, output_file):
    """对Excel文件中如果price的数字比任何bidprice开头的列小，就把price的数字设置为红色字体"""
    # 使用ExcelWriter和openpyxl引擎
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 先将数据写入Excel
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        # 获取工作表
        worksheet = writer.sheets['Sheet1']
        
        # 获取列名行索引（第1行）
        header_row = 1
        
        # 收集price列和所有以bidprice开头的列的索引
        price_col = None
        bidprice_cols = []
        bidprice_column_names = []
        
        # 查找各列的索引
        for cell in worksheet[header_row]:
            if cell.value == 'price':
                price_col = cell.column
            elif isinstance(cell.value, str) and cell.value.startswith('bidprice'):
                bidprice_cols.append(cell.column)
                bidprice_column_names.append(cell.value)
        
        # 创建红色字体样式
        red_font = Font(color='FF0000')
        
        # 从第2行开始遍历数据行（跳过表头）
        for row in range(2, worksheet.max_row + 1):
            # 如果找到了price列和至少一个bidprice列
            if price_col and bidprice_cols:
                price_cell = worksheet.cell(row=row, column=price_col)
                
                try:
                    # 尝试获取price值
                    price_val = float(price_cell.value) if price_cell.value else float('inf')
                    
                    # 检查price是否小于所有bidprice列的值
                    is_price_smallest = True
                    
                    for col_idx in bidprice_cols:
                        bidprice_cell = worksheet.cell(row=row, column=col_idx)
                        try:
                            bidprice_val = float(bidprice_cell.value) if bidprice_cell.value else float('-inf')
                            # 如果price不小于当前bidprice，标记为False并跳出循环
                            if price_val >= bidprice_val:
                                is_price_smallest = False
                                break
                        except (ValueError, TypeError):
                            # 如果当前bidprice列的值无法转换为数字，跳过该列
                            continue
                    
                    # 如果price小于所有有效的bidprice值，设置为红色
                    if is_price_smallest:
                        price_cell.font = red_font
                except (ValueError, TypeError):
                    # 如果price值无法转换为数字，跳过
                    pass
            
            # 比较bidprice9和bidprice10（保持原有功能）
            bidprice9_col = None
            bidprice10_col = None
            for i, col_name in enumerate(bidprice_column_names):
                if col_name == 'bidprice9':
                    bidprice9_col = bidprice_cols[i]
                elif col_name == 'bidprice10':
                    bidprice10_col = bidprice_cols[i]
            
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
        
        # 从constprice集合获取price数据并添加到DataFrame
        print("正在从constprice集合获取产品价格数据...")
        # 获取constprice集合引用
        constprice_collection = db['constprice']
        
        # 创建一个新的price列，初始化为0
        df['price'] = 0
        
        # 用于统计匹配情况
        matched_count = 0
        unmatched_count = 0
        
        # 遍历DataFrame，使用nameid查询constprice中的price
        for index, row in df.iterrows():
            try:
                nameid = row['nameid']
                
                # 尝试将nameid转换为整数，因为constprice集合中的nameid是整数类型
                try:
                    # 先去除可能的前导零和空格
                    if isinstance(nameid, str):
                        nameid_str = nameid.strip()
                        # 尝试转换为整数
                        query_nameid = int(nameid_str)
                    else:
                        query_nameid = int(nameid)
                except (ValueError, TypeError):
                    # 如果转换失败，使用原始nameid继续查询
                    query_nameid = nameid
                    
                # 查询constprice集合中对应nameid的文档
                const_price_doc = constprice_collection.find_one({'nameid': query_nameid}, {'price': 1, '_id': 0})
                
                # 如果找到对应文档，处理price值
                if const_price_doc and 'price' in const_price_doc:
                    price_value = const_price_doc['price']
                    matched_count += 1
                    
                    # 处理不同类型的price值
                    try:
                        # 尝试将price值转换为浮点数
                        if isinstance(price_value, str):
                            # 处理字符串类型的price
                            price_value = price_value.strip().upper()
                            if price_value in ('N/A', 'NA', '', 'NONE', 'NULL'):
                                df.at[index, 'price'] = 0
                            else:
                                # 尝试将字符串转换为数字
                                df.at[index, 'price'] = float(price_value)
                        elif isinstance(price_value, (int, float)):
                            # 对于数字类型的price，直接使用
                            df.at[index, 'price'] = price_value
                        else:
                            # 其他类型默认为0
                            df.at[index, 'price'] = 0
                    except (ValueError, TypeError):
                        # 如果转换失败，设置为0
                        df.at[index, 'price'] = 0
                else:
                    unmatched_count += 1
                    # 如果没有找到对应文档，保持price为0
                    df.at[index, 'price'] = 0
                    # 可选：添加日志记录未匹配的nameid
                    # print(f"未找到nameid为{nameid}的产品价格数据")
            except Exception as e:
                # 安全地处理nameid可能不存在的情况
                try:
                    error_nameid = row.get('nameid', '未知')
                    print(f"获取产品ID为{error_nameid}的价格时出错: {str(e)}")
                except:
                    print(f"获取产品价格时出错: {str(e)}")
                # 即使出错，也要确保price列为0
                df.at[index, 'price'] = 0
                continue
        
        # 显示匹配统计信息
        print(f"从constprice集合获取价格数据完成：")
        print(f"  成功匹配: {matched_count} 条记录")
        print(f"  未找到匹配: {unmatched_count} 条记录")
        
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