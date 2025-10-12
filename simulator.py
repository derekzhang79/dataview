import pandas as pd
import os
import sys
import pandas as pd
import numpy as np

#改进，在原来excel表中增加两个自己填报项目，计算时候进行复制，并参与运算

def generate_price1(data, input_column='bidprice10', mark_number='number11',multiplier=1.0):
    """通过bidprice10列生成price1列数据，如果bidprice10的值为0或者不存在，则取对应行的price代替。
       如果mark_number列为0或者为空，对应的行数据返回0。"""
    data_copy = data.copy()
    
    # 检查输入列是否存在
    if input_column not in data_copy.columns:
        # 输入列不存在，尝试使用price列
        if 'price' in data_copy.columns:
            print(f"警告: 输入文件中不存在{input_column}列，price1将使用price列的值")
            result = data_copy['price'].copy()
        else:
            print(f"警告: 输入文件中不存在{input_column}列和price列，price1将使用默认值")
            result = [0] * len(data_copy)
    else:
        # 输入列存在，创建结果数组
        result = data_copy[input_column].copy()
        
        # 检查price列是否存在
        price_available = 'price' in data_copy.columns
        
        # 处理每一行
        for index, row in data_copy.iterrows():
            # 如果bidprice10为0或缺失
            if pd.isna(row[input_column]) or row[input_column] == 0:
                if price_available and not pd.isna(row['price']) and row['price'] != 0:
                    # 使用price列的值
                    result.at[index] = row['price']
                else:
                    # 如果price列不存在或price也为0，则保持原0值
                    result.at[index] = 0
    
    # 检查mark_number列是否存在，如果存在且为0或空，则对应行数据返回0
    if mark_number in data_copy.columns:
        for index, row in data_copy.iterrows():
            if pd.isna(row[mark_number]) or row[mark_number] == 0:
                result.at[index] = 0
    
    # 应用乘数并返回结果
    return result * multiplier


def generate_price2(data, input_column='bidprice9', mark_number='number11', multiplier=1.0):
    """通过bidprice9列生成price2列数据，当bidprice9不存在或为0时使用对应行的price值代替。
       如果mark_number列为0或者为空，对应的行数据返回0。"""
    # 检查price列是否存在
    price_column_exists = 'price' in data.columns
    
    # 确保输入列存在
    if input_column not in data.columns:
        if price_column_exists:
            print(f"警告: 输入文件中不存在{input_column}列，price2将使用price列的值")
            # 创建结果数组，默认使用price列的值
            result = data['price'].copy()
        else:
            print(f"警告: 输入文件中不存在{input_column}列和price列，price2将使用默认值")
            result = [0] * len(data)
    else:
        # 复制数据
        data_copy = data.copy()
        # 创建结果数组，初始使用input_column的值
        result = data_copy[input_column].copy()
        
        # 当input_column的值为0或缺失，且price列存在时，使用price列的值代替
        if price_column_exists:
            # 将缺失值标记为True
            is_missing = data_copy[input_column].isna()
            # 将0值标记为True
            is_zero = (data_copy[input_column] == 0) & ~is_missing
            # 当值为0或缺失且price列不为0时，使用price列的值代替
            should_replace = (is_missing | is_zero) & (data['price'] != 0) & ~data['price'].isna()
            result[should_replace] = data.loc[should_replace, 'price']
        
        # 处理剩余的缺失值，使用0代替
        result = result.fillna(0)
    
    # 检查mark_number列是否存在，如果存在且为0或空，则对应行数据返回0
    if mark_number in data.columns:
        # 找出mark_number列为0或空的行索引
        mark_zero_or_missing = data[mark_number].isna() | (data[mark_number] == 0)
        result[mark_zero_or_missing] = 0
    
    # 应用乘数并返回结果
    return result * multiplier


def generate_price4(data, column1='bidprice9', column2='bidprice10', price_column='price'):
    """通过price、bidprice9和bidprice10列生成price4列数据，返回三列中的最小值"""
    # 检查所有需要的列是否存在
    columns_to_check = [price_column, column1, column2]
    missing_columns = [col for col in columns_to_check if col not in data.columns]
    
    # 复制数据以避免修改原始数据
    data_copy = data.copy()
    
    # 为缺失的列创建全0的临时列
    for col in missing_columns:
        print(f"警告: 输入文件中不存在{col}列，将使用0值代替")
        data_copy[col] = 0
    
    # 处理各列中的NaN值，使用0代替
    for col in columns_to_check:
        data_copy[col] = data_copy[col].fillna(0)
    
    # 创建一个包含三列数据的DataFrame用于计算最小值
    min_data = data_copy[[price_column, column1, column2]].copy()
    
    # 计算每行的最小值
    result = min_data.min(axis=1)
    
    # 处理可能的缺失值，使用0代替
    result = result.fillna(0)
    
    return result


def generate_price3(data, input_column='price', mark_number='number11', multiplier=1.0):
    """通过price列生成price3列数据，应用乘数转换逻辑。
       如果mark_number列为0或者为空，对应的行数据返回0。"""
    # 确保输入列存在
    if input_column not in data.columns:
        print(f"警告: 输入文件中不存在{input_column}列，price3将使用默认值")
        return [0] * len(data)
    
    # 处理可能的缺失值，使用0代替
    data_copy = data.copy()
    result = data_copy[input_column].fillna(0)
    
    # 检查mark_number列是否存在，如果存在且为0或空，则对应行数据返回0
    if mark_number in data.columns:
        # 找出mark_number列为0或空的行索引
        mark_zero_or_missing = data[mark_number].isna() | (data[mark_number] == 0)
        result[mark_zero_or_missing] = 0
    
    # 应用乘数并返回结果
    return result * multiplier

def generate_price5(data, input_column='price', multiplier=0.5):
    """通过original_price列生成price5列数据"""
    # 确保输入列存在，如果不存在则尝试使用fallback_column
    if input_column not in data.columns:
        print(f"警告: 输入文件中不存在{input_column}列和{fallback_column}列，price5将使用默认值")
        return [0] * len(data)
    
    # 处理可能的缺失值，使用0代替
    data_copy = data.copy()
    data_copy[input_column] = data_copy[input_column].fillna(0)
    
    # 生成price5列，应用乘数转换逻辑
    return data_copy[input_column] * multiplier


def generate_median(data, columns=['price1', 'price2', 'price3', 'price4', 'price5']):
    """计算price1、price2、price3从小到大排列的中位数。
       如果price1的数值为0，则对应行的median值直接返回0，不需要计算中位数。"""
    # 确保所有需要的列都存在
    missing_columns = [col for col in columns if col not in data.columns]
    if missing_columns:
        print(f"警告: 输入数据中不存在{', '.join(missing_columns)}列，median将使用默认值")
        return [0] * len(data)
    
    # 创建数据副本并处理缺失值
    data_copy = data.copy()
    for col in columns:
        data_copy[col] = data_copy[col].fillna(0)
    
    # 检测price1列为0的行
    price1_zero_mask = data_copy['price1'] == 0
    
    # 计算每一行的中位数
    # 将五列数据按行排序
    sorted_values = np.sort(data_copy[columns].values, axis=1)
    # 取中间值作为中位数（由于是五列，索引为2的元素就是中位数）
    median_values = sorted_values[:, 2]
    
    # 对于price1为0的行，将中位数设为0
    median_values[price1_zero_mask] = 0
    
    return median_values

def generate_ratios(data, columns=['price1', 'price2', 'price3', 'price4', 'price5']):
    """生成s1、s2、s3、s4、s5五列数据，其中a为price1、price2、price3、price4、price5的最小值
    s1 = a/price1 * 60
    s2 = a/price2 * 60
    s3 = a/price3 * 60
    s4 = a/price4 * 60
    s5 = a/price5 * 60
    如果检测到price1为0，则s1、s2、s3、s4、s5自动设置为0返回
    """
    # 确保所有需要的列都存在
    missing_columns = [col for col in columns if col not in data.columns]
    if missing_columns:
        print(f"警告: 输入数据中不存在{', '.join(missing_columns)}列，s1、s2、s3、s4、s5将使用默认值")
        # 返回默认值为0的五列数据
        zero_values = [0] * len(data)
        return zero_values, zero_values, zero_values, zero_values, zero_values
    
    # 创建数据副本并处理缺失值
    data_copy = data.copy()
    for col in columns:
        data_copy[col] = data_copy[col].fillna(0)
    
    # 检测price1列为0的行
    price1_zero_mask = data_copy['price1'] == 0
    
    # 如果所有行的price1都为0，直接返回全0的结果
    if price1_zero_mask.all():
        zero_values = [0] * len(data)
        return zero_values, zero_values, zero_values, zero_values, zero_values
    
    # 计算每一行的最小值a
    min_values = np.min(data_copy[columns].values, axis=1)
    
    # 为了避免除以零的情况，我们将0替换为一个很小的数
    epsilon = 1e-10
    for col in columns:
        data_copy[col] = data_copy[col].replace(0, epsilon)
        
    # 计算s1、s2、s3、s4、s5
    s1 = (min_values / data_copy['price1'].values) * 60
    s2 = (min_values / data_copy['price2'].values) * 60
    s3 = (min_values / data_copy['price3'].values) * 60
    s4 = (min_values / data_copy['price4'].values) * 60
    s5 = (min_values / data_copy['price5'].values) * 60
    
    # 对于price1为0的行，将所有s值设为0
    if price1_zero_mask.any():
        s1[price1_zero_mask] = 0
        s2[price1_zero_mask] = 0
        s3[price1_zero_mask] = 0
        s4[price1_zero_mask] = 0
        s5[price1_zero_mask] = 0
    
    return s1, s2, s3, s4, s5

def generate_a_values(data):
    """生成a1、a2、a3、a4、a5五列数据
    先完成所有前面数据的生成，设b为所有median列的总和，然后：
    a1 = s1 * median / b
    a2 = s2 * median / b
    a3 = s3 * median / b
    a4 = s4 * median / b
    a5 = s5 * median / b
    """
    # 确保所有需要的列都存在
    required_columns = ['median', 's1', 's2', 's3', 's4', 's5']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"警告: 输入数据中不存在{', '.join(missing_columns)}列，a1、a2、a3、a4、a5将使用默认值")
        # 返回默认值为0的五列数据
        zero_values = [0] * len(data)
        return zero_values, zero_values, zero_values, zero_values, zero_values
    
    # 创建数据副本并处理缺失值
    data_copy = data.copy()
    for col in required_columns:
        data_copy[col] = data_copy[col].fillna(0)
    
    # 计算median列的总和b
    b = data_copy['median'].sum()
    
    # 为了避免除以零的情况，我们将0替换为一个很小的数
    epsilon = 1e-10
    if b == 0:
        b = epsilon
    
    # 计算a1、a2、a3、a4、a5
    a1 = (data_copy['s1'].values * data_copy['median'].values) / b
    a2 = (data_copy['s2'].values * data_copy['median'].values) / b
    a3 = (data_copy['s3'].values * data_copy['median'].values) / b
    a4 = (data_copy['s4'].values * data_copy['median'].values) / b
    a5 = (data_copy['s5'].values * data_copy['median'].values) / b
    
    return a1, a2, a3, a4, a5

def update_ratios(data):
    """根据规则更新s1、s2、s3、s4、s5五列数据
    规则：
    - 如果price1为0，则直接返回原始的s1、s2、s3、s4、s5值，不进行更新
    - 如果price1 < median*0.2 或 price1 > median*1.8，则s1 = min(s2, s3, s4, s5)
    - 如果price2 < median*0.2 或 price2 > median*1.8，则s2 = min(s1, s3, s4, s5)
    - 如果price3 < median*0.2 或 price3 > median*1.8，则s3 = min(s1, s2, s4, s5)
    - 如果price4 < median*0.2 或 price4 > median*1.8，则s4 = min(s1, s2, s3, s5)
    - 如果price5 < median*0.2 或 price5 > median*1.8，则s5 = min(s1, s2, s3, s4)
    ""
    
    # 修正文档字符串未终止问题，先删除多余的未终止字符串
    # 确保所有需要的列都存在
    required_columns = ['price1', 'price2', 'price3', 'price4', 'price5', 'median', 's1', 's2', 's3', 's4', 's5']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"警告: 输入数据中不存在{', '.join(missing_columns)}列，无法更新s1、s2、s3、s4、s5")
        # 返回原始数据
        return data['s1'].values, data['s2'].values, data['s3'].values, data['s4'].values, data['s5'].values
    
    # 检查price1是否为0，如果是则直接返回原始的s1-s5值
    if 'price1' in data.columns:
        data_copy_precheck = data.copy()
        data_copy_precheck['price1'] = data_copy_precheck['price1'].fillna(0)
        if (data_copy_precheck['price1'] == 0).any():
            # 如果有任何一行的price1为0，则直接返回原始值
            return data['s1'].values, data['s2'].values, data['s3'].values, data['s4'].values, data['s5'].values
    
    # 创建数据副本并处理缺失值
    data_copy = data.copy()
    for col in required_columns:
        data_copy[col] = data_copy[col].fillna(0)
    
    # 计算上下界
    lower_bound = data_copy['median'] * 0.2
    upper_bound = data_copy['median'] * 1.8
    
    # 保存原始的s1、s2、s3值
    original_s1 = data_copy['s1'].values.copy()
    original_s2 = data_copy['s2'].values.copy()
    original_s3 = data_copy['s3'].values.copy()
    original_s4 = data_copy['s4'].values.copy()
    original_s5 = data_copy['s5'].values.copy()
    
    # 创建要返回的数组（初始为原始值的副本）
    updated_s1 = original_s1.copy()
    updated_s2 = original_s2.copy()
    updated_s3 = original_s3.copy()
    updated_s4 = original_s4.copy()
    updated_s5 = original_s5.copy()
    
    # 对每一行单独处理，确保正确处理同时需要更新多列的情况
    for i in range(len(data_copy)):
        price1 = data_copy.loc[i, 'price1']
        price2 = data_copy.loc[i, 'price2']
        price3 = data_copy.loc[i, 'price3']
        price4 = data_copy.loc[i, 'price4']
        price5 = data_copy.loc[i, 'price5']
        current_lower = lower_bound.iloc[i]
        current_upper = upper_bound.iloc[i]
        
        # 检查是否需要更新s1
        if price1 < current_lower or price1 > current_upper:
            updated_s1[i] = min(original_s2[i], original_s3[i], original_s4[i], original_s5[i])
        
        # 检查是否需要更新s2
        if price2 < current_lower or price2 > current_upper:
            updated_s2[i] = min(original_s1[i], original_s3[i], original_s4[i], original_s5[i])
        
        # 检查是否需要更新s3
        if price3 < current_lower or price3 > current_upper:
            updated_s3[i] = min(original_s1[i], original_s2[i], original_s4[i], original_s5[i])
        
        # 检查是否需要更新s4
        if price4 < current_lower or price4 > current_upper:
            updated_s4[i] = min(original_s1[i], original_s2[i], original_s3[i], original_s5[i])
        
        # 检查是否需要更新s5
        if price5 < current_lower or price5 > current_upper:
            updated_s5[i] = min(original_s1[i], original_s2[i], original_s3[i], original_s4[i])
    
    return updated_s1, updated_s2, updated_s3, updated_s4, updated_s5


def main():
    # 从命令行参数获取输入文件，如果没有提供则提示用户输入
    if len(sys.argv) < 2:
        input_file = input("请输入Excel文件路径: ")
    else:
        input_file = sys.argv[1]
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 找不到输入文件 '{input_file}'")
        sys.exit(1)
    
    # 读取Excel文件
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        print(f"成功读取输入文件 '{input_file}'")
        print(f"数据形状: {df.shape}")
    except Exception as e:
        print(f"读取文件时出错: {e}")
        sys.exit(1)
    
    # 从输入文件名中提取基础名称（去掉扩展名）
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 生成输出文件名
    output_file = f"simulator{base_name}.xlsx"
    
    # 生成新的列数据
    df['price1'] = generate_price1(df)
    df['price2'] = generate_price2(df)
    df['price3'] = generate_price3(df)
    df['price4'] = generate_price4(df)
    df['price5'] = generate_price5(df)
    df['median'] = generate_median(df) #基准值
    
    # 生成s1、s2、s3、s4、s5列
    df['s1'], df['s2'], df['s3'], df['s4'], df['s5']  = generate_ratios(df)
    
    # 根据规则更新s1、s2、s3、s4、s5列
    df['s1'], df['s2'], df['s3'], df['s4'], df['s5'] = update_ratios(df)
    
    # 生成a1、a2、a3、a4、a5列（在所有前面数据生成完成后）
    df['a1'], df['a2'], df['a3'], df['a4'], df['a5'] = generate_a_values(df)
    
    # 打印生成的新列的统计信息
    print("\n生成的新列统计信息:")
    print(df[['price1', 'price2', 'price3', 'price4', 'price5', 'median', 's1', 's2', 's3', 's4', 's5', 'a1', 'a2', 'a3', 'a4', 'a5']].describe())
    
    # 计算a1-a5列的总和
    sum1 = df['a1'].sum()
    sum2 = df['a2'].sum()
    sum3 = df['a3'].sum()
    sum4 = df['a4'].sum()
    sum5 = df['a5'].sum()
    
    # 对总和进行从小到大排序
    sums = [(sum1, 'sum1'), (sum2, 'sum2'), (sum3, 'sum3'), (sum4, 'sum4'), (sum5, 'sum5')]
    sorted_sums = sorted(sums, key=lambda x: x[0])
    
    # 打印排序后的结果
    print("\n各a列总和从小到大排序:")
    for value, name in sorted_sums:
        print(f"{name}: {value}")
    
    # 将结果写入新的Excel文件
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='SimulatedData')
        print(f"\n成功生成输出文件: '{output_file}'")
    except Exception as e:
        print(f"写入文件时出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()