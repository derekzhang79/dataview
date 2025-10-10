# 设置matplotlib为非交互式后端，适合在无图形界面环境运行
import matplotlib
matplotlib.use('Agg')  # 使用Agg后端，无需图形界面

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
import sys
import os
warnings.filterwarnings('ignore')

# 设置中文字体，添加多个系统常见的中文字体作为备选
plt.rcParams['font.family'] = ['SimHei', 'WenQuanYi Micro Hei', 'Heiti TC', 'PingFang SC', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 读取数据 - 从命令行参数获取输入文件
if len(sys.argv) < 2:
    print("用法: python dataanalysis.py <输入文件名>")
    sys.exit(1)

input_file = sys.argv[1]

# 从输入文件名中提取基础名称（去掉扩展名）
base_name = os.path.splitext(os.path.basename(input_file))[0]

# 检查输入文件是否存在
if not os.path.exists(input_file):
    print(f"错误: 找不到输入文件 '{input_file}'")
    # 创建测试数据用于演示
    print("正在创建测试数据...")
    # 创建示例数据
    data = {
        'name': [f'产品{i}' for i in range(1, 51)],
        'nameid': [10000 + i for i in range(1, 51)],
        'price9': np.random.uniform(50, 200, 50),
        'bidprice9': np.random.uniform(60, 220, 50),
        'bidprice10': np.random.uniform(60, 220, 50),
        'price10': np.random.uniform(50, 200, 50),
        'price': np.random.uniform(40, 180, 50),
        'number9': np.random.randint(10, 100, 50),
        'number10': np.random.randint(10, 100, 50)
    }
    df = pd.DataFrame(data)
    print(f"已创建测试数据，共{df.shape[0]}行")
else:
    df = pd.read_excel(input_file)
    print(f"成功读取输入文件 '{input_file}'")

print("数据概览:")
print(f"数据形状: {df.shape}")
print("\n前5行数据:")
print(df.head())

print("\n数据基本信息:")
print(df.info())

print("\n数值列统计描述:")
print(df.describe())

# 数据清洗
df_clean = df.copy()

# 首先检查并报告price为0的情况
zero_price_count = (df_clean['price'] == 0).sum()
if zero_price_count > 0:
    print(f"警告: 发现{zero_price_count}条记录的price列为0")

# 对于数量列，用0填充
quantity_columns = ['number9', 'number10']
for col in quantity_columns:
    df_clean[col] = df_clean[col].fillna(0)

# 根据用户需求1：剔除那些number9不为0但number10为0的产品数据
before_count = df_clean.shape[0]
df_clean = df_clean[ (df_clean['number9'] == 0) | (df_clean['number10'] != 0) ]
removed_count = before_count - df_clean.shape[0]

print(f"\n第一步清洗后数据形状: {df_clean.shape}")
print(f"剔除了{removed_count}条number9不为0但number10为0的产品数据")

# 根据新需求：在进行数据分析和做图例时，如果遇到bidprice9、bidprice10、price9、price10为空或者0时，忽略该行数据
# 先保存原始数据形状用于报告
original_data_shape = df_clean.shape

# 创建掩码来标识bidprice9、bidprice10、price9、price10中任意一个为空或0的行
cols_to_check = ['bidprice9', 'bidprice10', 'price9', 'price10']
mask = df_clean[cols_to_check].isna().any(axis=1) | (df_clean[cols_to_check] == 0).any(axis=1)

# 过滤掉这些行
df_clean = df_clean[~mask]

# 统计被过滤掉的行数
filtered_rows = original_data_shape[0] - df_clean.shape[0]
print(f"第二步清洗后数据形状: {df_clean.shape}")
print(f"剔除了{filtered_rows}条bidprice9、bidprice10、price9、price10为空或0的产品数据")

# 对于保留的数据，对price为0的情况进行特殊处理（如果仍有需要）
remaining_zero_price_count = (df_clean['price'] == 0).sum()
if remaining_zero_price_count > 0:
    print(f"警告: 发现{remaining_zero_price_count}条记录的price列为0，将进行处理")
    
    # 创建一个掩码来标识price为0的记录
    zero_price_mask = df_clean['price'] == 0
    
    # 对于price为0的记录，尝试使用price9和price10的平均值
    # 计算price9和price10的平均值，排除0值
    df_clean.loc[zero_price_mask, 'price'] = df_clean.loc[zero_price_mask, ['price9', 'price10']].mean(axis=1)
    
    # 再次检查是否还有price为0的记录（如果price9和price10也都是0）
    remaining_zero_price = (df_clean['price'] == 0).sum()
    if remaining_zero_price > 0:
        # 使用所有price的非零平均值来填充剩余的0值
        overall_mean_price = df_clean.loc[df_clean['price'] != 0, 'price'].mean()
        df_clean.loc[df_clean['price'] == 0, 'price'] = overall_mean_price
        print(f"已将{remaining_zero_price}条记录的price列0值替换为整体平均价格: {overall_mean_price:.2f}")
    
    # 更新处理后的price为0的计数
    final_zero_price_count = (df_clean['price'] == 0).sum()
    if final_zero_price_count == 0:
        print(f"成功处理所有{remaining_zero_price_count}条price列为0的记录")

# 1. 基础统计分析
print("\n=== 基础统计分析 ===")
# 计算月度变化
df_clean['bidprice_change'] = df_clean['bidprice10'] - df_clean['bidprice9']
df_clean['bidprice_change_pct'] = (df_clean['bidprice10'] - df_clean['bidprice9']) / df_clean['bidprice9'] * 100

# 安全计算毛利率（防止除零错误）
df_clean['margin9'] = np.where(df_clean['price'] != 0, 
                              (df_clean['bidprice9'] - df_clean['price']) / df_clean['price'] * 100, 
                              0)
df_clean['margin10'] = np.where(df_clean['price'] != 0, 
                               (df_clean['bidprice10'] - df_clean['price']) / df_clean['price'] * 100, 
                               0)

# 计算利润额度（按照招标价送货的利润）
df_clean['profit9'] = df_clean['number9'] * (df_clean['bidprice9'] - df_clean['price'])
df_clean['profit10'] = df_clean['number10'] * (df_clean['bidprice10'] - df_clean['price'])

print(f"9月平均毛利率: {df_clean['margin9'].mean():.2f}%")
print(f"10月平均毛利率: {df_clean['margin10'].mean():.2f}%")
print(f"9月总利润: {df_clean['profit9'].sum():.2f}")
print(f"10月总利润: {df_clean['profit10'].sum():.2f}")
print(f"平均价格变化: {df_clean['bidprice_change'].mean():.4f}")
print(f"价格变化标准差: {df_clean['bidprice_change'].std():.4f}")

# 2. 数据可视化
print("\n=== 数据可视化 ===")
fig, axes = plt.subplots(3, 3, figsize=(24, 18))
fig.suptitle('价格数据分析与预测', fontsize=16, fontweight='bold')

# 2.1 两个月使用数量最大的前20个产品中标、成本和价格分布（9月份）
# 计算每个产品的9月和10月总使用量
usage_top_20 = df_clean.nlargest(20, ['number9', 'number10'])

# 为9月和10月分别绘制前20个使用量最大的产品
bar_width = 0.35
x_pos = range(len(usage_top_20))

# 9月使用量最大的前20个产品
axes[0,0].bar(x_pos, usage_top_20['number9'], width=bar_width, alpha=0.7, label='9月使用量', color='blue')
axes[0,0].set_ylabel('使用量', fontsize=12)
axes[0,0].legend(loc='upper left')

# 共享x轴显示9月中标价格和成本价格
axes_twin = axes[0,0].twinx()
axes_twin.bar([p + bar_width for p in x_pos], usage_top_20['bidprice9'], width=bar_width, alpha=0.7, label='9月中标价格', color='lightblue')
axes_twin.plot([p + bar_width for p in x_pos], usage_top_20['price'], marker='o', color='green', alpha=0.8, label='9月成本价格')
axes_twin.set_ylabel('价格', fontsize=12)
axes_twin.legend(loc='upper right')

axes[0,0].set_title('9月使用量最大的前20个产品 - 使用量、中标价格与成本价格')
axes[0,0].set_xlabel('产品名称')
axes[0,0].set_xticks([p + bar_width/2 for p in x_pos])
axes[0,0].set_xticklabels(usage_top_20['name'], rotation=45, ha='right')
axes[0,0].tick_params(axis='x', rotation=45)

# 2.2 两个月使用数量最大的前20个产品中标、成本和价格分布（10月份）
axes[0,1].bar(x_pos, usage_top_20['number10'], width=bar_width, alpha=0.7, label='10月使用量', color='red')
axes[0,1].set_ylabel('使用量', fontsize=12)
axes[0,1].legend(loc='upper left')

# 共享x轴显示10月中标价格和成本价格
axes_twin2 = axes[0,1].twinx()
axes_twin2.bar([p + bar_width for p in x_pos], usage_top_20['bidprice10'], width=bar_width, alpha=0.7, label='10月中标价格', color='lightcoral')
axes_twin2.plot([p + bar_width for p in x_pos], usage_top_20['price'], marker='o', color='green', alpha=0.8, label='10月成本价格')
axes_twin2.set_ylabel('价格', fontsize=12)
axes_twin2.legend(loc='upper right')

axes[0,1].set_title('10月使用量最大的前20个产品 - 使用量、中标价格与成本价格')
axes[0,1].set_xlabel('产品名称')
axes[0,1].set_xticks([p + bar_width/2 for p in x_pos])
axes[0,1].set_xticklabels(usage_top_20['name'], rotation=45, ha='right')
axes[0,1].tick_params(axis='x', rotation=45)

# 2.3 中标价格与成本价格关系
axes[0,2].scatter(df_clean['price'], df_clean['bidprice9'], alpha=0.6, label='9月', color='blue')
axes[0,2].scatter(df_clean['price'], df_clean['bidprice10'], alpha=0.6, label='10月', color='red')
axes[0,2].plot([df_clean['price'].min(), df_clean['price'].max()], 
               [df_clean['price'].min(), df_clean['price'].max()], 'k--', alpha=0.8, label='成本线')
axes[0,2].set_title('中标价格 vs 成本价格')
axes[0,2].set_xlabel('成本价格')
axes[0,2].set_ylabel('中标价格')
axes[0,2].legend()

# 2.4 每月总利润额对比
# 计算9月和10月的总利润额
september_total_profit = df_clean['profit9'].sum()
october_total_profit = df_clean['profit10'].sum()

# 创建柱状图展示总利润额对比
months = ['9月', '10月']
total_profits = [september_total_profit, october_total_profit]

axes[1,0].bar(months, total_profits, alpha=0.7, color=['blue', 'red'])
axes[1,0].set_title('每月总利润额对比')
axes[1,0].set_xlabel('月份')
axes[1,0].set_ylabel('总利润额')

# 在柱状图上显示具体数值
for i, v in enumerate(total_profits):
    axes[1,0].text(i, v + v * 0.05, f'{v:.2f}', ha='center')

# 2.5 毛利率分布
axes[1,1].hist(df_clean['margin9'].dropna(), bins=20, alpha=0.7, label='9月毛利率', color='lightblue')
axes[1,1].hist(df_clean['margin10'].dropna(), bins=20, alpha=0.7, label='10月毛利率', color='lightcoral')
axes[1,1].axvline(df_clean['margin9'].mean(), color='blue', linestyle='--', alpha=0.8, label='9月平均')
axes[1,1].axvline(df_clean['margin10'].mean(), color='red', linestyle='--', alpha=0.8, label='10月平均')
axes[1,1].set_title('毛利率分布')
axes[1,1].set_xlabel('毛利率 (%)')
axes[1,1].set_ylabel('频次')
axes[1,1].legend()

# 2.6 价格变化百分比
price_change_pct = df_clean['bidprice_change_pct'].dropna()
positive_changes = price_change_pct[price_change_pct > 0]
negative_changes = price_change_pct[price_change_pct < 0]

categories = ['价格上涨', '价格下降', '价格不变']
values = [len(positive_changes), len(negative_changes), len(price_change_pct) - len(positive_changes) - len(negative_changes)]

axes[1,2].bar(categories, values, color=['red', 'green', 'gray'])
axes[1,2].set_title('价格变化情况统计')
axes[1,2].set_ylabel('产品数量')

for i, v in enumerate(values):
    axes[1,2].text(i, v + 0.5, str(v), ha='center')

# 2.7 利润分布对比
axes[2,0].hist(df_clean['profit9'].dropna(), bins=20, alpha=0.7, label='9月利润', color='lightblue')
axes[2,0].hist(df_clean['profit10'].dropna(), bins=20, alpha=0.7, label='10月利润', color='lightcoral')
axes[2,0].axvline(df_clean['profit9'].mean(), color='blue', linestyle='--', alpha=0.8, label='9月平均')
axes[2,0].axvline(df_clean['profit10'].mean(), color='red', linestyle='--', alpha=0.8, label='10月平均')
axes[2,0].set_title('利润分布对比')
axes[2,0].set_xlabel('利润')
axes[2,0].set_ylabel('频次')
axes[2,0].legend()

# 2.8 利润额最高的20种产品分析（9月和10月对比）
# 计算每个产品的平均利润并排序（使用9月和10月的平均利润）
df_clean['avg_profit'] = (df_clean['profit9'] + df_clean['profit10']) / 2
most_profitable = df_clean.nlargest(20, 'avg_profit')
x_pos1 = range(len(most_profitable))
bar_width = 0.35

axes[2,1].bar([p - bar_width/2 for p in x_pos1], most_profitable['profit9'], width=bar_width, alpha=0.7, label='9月利润', color='blue')
axes[2,1].bar([p + bar_width/2 for p in x_pos1], most_profitable['profit10'], width=bar_width, alpha=0.7, label='10月利润', color='red')

axes[2,1].set_title('利润额最高的20种产品分析')
axes[2,1].set_xlabel('产品名称')
axes[2,1].set_ylabel('利润')
axes[2,1].set_xticks(x_pos1)
axes[2,1].set_xticklabels(most_profitable['name'], rotation=45, ha='right')
axes[2,1].legend()
axes[2,1].tick_params(axis='x', rotation=45)

# 2.9 利润额最低的20种产品分析（9月和10月对比）
least_profitable = df_clean.nsmallest(20, 'avg_profit')
x_pos2 = range(len(least_profitable))

# 创建新的子图来展示利润额最低的20种产品
fig.delaxes(axes[2,2])  # 删除原来的子图
axes[2,2] = fig.add_subplot(3, 3, 9)

axes[2,2].bar([p - bar_width/2 for p in x_pos2], least_profitable['profit9'], width=bar_width, alpha=0.7, label='9月利润', color='lightblue')
axes[2,2].bar([p + bar_width/2 for p in x_pos2], least_profitable['profit10'], width=bar_width, alpha=0.7, label='10月利润', color='lightcoral')

axes[2,2].set_title('利润额最低的20种产品分析')
axes[2,2].set_xlabel('产品名称')
axes[2,2].set_ylabel('利润')
axes[2,2].set_xticks(x_pos2)
axes[2,2].set_xticklabels(least_profitable['name'], rotation=45, ha='right')
axes[2,2].legend()
axes[2,2].tick_params(axis='x', rotation=45)

plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # 调整布局，为标题留出空间
# 保存图表为文件而不是显示
analysis_chart_file = f'{base_name}_price_analysis_charts.png'
plt.savefig(analysis_chart_file, dpi=300, bbox_inches='tight')
plt.close()  # 关闭图形，释放内存
print(f"价格分析图表已保存为: {analysis_chart_file}")

# 3. 价格预测模型
print("\n=== 价格预测模型 ===")
# 准备特征和目标变量
features = ['price9', 'bidprice9', 'number9', 'number10', 'price']
X = df_clean[features].fillna(0)  # 用0填充剩余的缺失值
y = df_clean['bidprice10']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 训练线性回归模型
lr_model = LinearRegression()
lr_model.fit(X_train, y_train)
y_pred_lr = lr_model.predict(X_test)

# 训练随机森林模型
rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)
y_pred_rf = rf_model.predict(X_test)

# 模型评估
print("模型性能评估:")
print(f"线性回归 - MAE: {mean_absolute_error(y_test, y_pred_lr):.4f}, R²: {r2_score(y_test, y_pred_lr):.4f}")
print(f"随机森林 - MAE: {mean_absolute_error(y_test, y_pred_rf):.4f}, R²: {r2_score(y_test, y_pred_rf):.4f}")

# 选择更好的模型
if r2_score(y_test, y_pred_rf) > r2_score(y_test, y_pred_lr):
    best_model = rf_model
    best_predictions = y_pred_rf
    model_name = "随机森林"
else:
    best_model = lr_model
    best_predictions = y_pred_lr
    model_name = "线性回归"

print(f"\n选择的最佳模型: {model_name}")

# 4. 预测11月中标价格
print("\n=== 11月中标价格预测 ===")
# 使用10月数据预测11月中标价格
# 假设11月的特征与10月相似，但我们可以考虑一些趋势
X_11 = df_clean[features].copy()

# 对数量特征进行简单调整（假设11月数量与10月相似）
X_11['number9'] = df_clean['number10']  # 用10月数据作为11月的"上月数据"
X_11['number10'] = df_clean['number10'] * 1.05  # 假设11月数量比10月增长5%

# 对价格特征进行简单调整
X_11['price9'] = df_clean['bidprice10']  # 用10月中标价作为11月的"上月报价"
X_11['bidprice9'] = df_clean['bidprice10']  # 用10月中标价作为11月的"上月中标价"

# 成本价格保持不变
X_11['price'] = df_clean['price']

# 预测11月中标价格
bidprice11_pred = best_model.predict(X_11.fillna(0))

# 创建预测结果DataFrame
predictions_df = df_clean[['name', 'nameid', 'bidprice9', 'bidprice10', 'price']].copy()
predictions_df['bidprice11_pred'] = bidprice11_pred
predictions_df['suggested_price11'] = bidprice11_pred * 1.02  # 建议报价比预测中标价高2%

# 计算预测变化
predictions_df['change_10_to_11'] = predictions_df['bidprice11_pred'] - predictions_df['bidprice10']
predictions_df['change_pct_10_to_11'] = (predictions_df['bidprice11_pred'] - predictions_df['bidprice10']) / predictions_df['bidprice10'] * 100

print("\n预测结果摘要:")
print(f"预测的11月中标价格范围: {predictions_df['bidprice11_pred'].min():.2f} - {predictions_df['bidprice11_pred'].max():.2f}")
print(f"平均预测变化: {predictions_df['change_10_to_11'].mean():.4f}")
print(f"预测价格上涨产品数量: {(predictions_df['change_10_to_11'] > 0).sum()}")
print(f"预测价格下降产品数量: {(predictions_df['change_10_to_11'] < 0).sum()}")

# 5. 可视化预测结果
# 5.1 创建9月份中标价与成本价差额与采购数量关系图例
fig1, ax1 = plt.subplots(figsize=(15, 25))  # 创建一个长图

# 计算9月中标价与成本价的差额
df_clean['diff9'] = df_clean['bidprice9'] - df_clean['price']
# 按差额从大到小排序
september_sorted = df_clean.sort_values('diff9', ascending=False)

# 设置产品名称的位置
y_pos1 = range(len(september_sorted))

# 绘制9月中标价与成本价的差额（条形图）
bars1 = ax1.barh(y_pos1, september_sorted['diff9'], alpha=0.7, color=['green' if x > 0 else 'red' for x in september_sorted['diff9']])

# 在条形图上叠加采购数量（散点图）
# 对数量进行标准化，使其在同一图表上可见
max_number1 = max(september_sorted['number9'].max(), september_sorted['number10'].max())
number_scale1 = max(september_sorted['diff9'].max(), abs(september_sorted['diff9'].min())) * 0.8
scatter9_1 = ax1.scatter(september_sorted['diff9'], y_pos1, s=september_sorted['number9'] * 2, alpha=0.5, color='blue', label='9月采购数量')
scatter10_1 = ax1.scatter(september_sorted['diff9'] + (number_scale1 / max_number1) * 5, y_pos1, s=september_sorted['number10'] * 2, alpha=0.5, color='red', label='10月采购数量')

# 设置图表属性
ax1.set_title('9月中标价与成本价差额与采购数量关系分析（按差额排序）')
ax1.set_xlabel('中标价 - 成本价（差额）')
ax1.set_ylabel('产品名称')
ax1.set_yticks(y_pos1)
ax1.set_yticklabels(september_sorted['name'], fontsize=8)
ax1.axvline(x=0, color='black', linestyle='-', alpha=0.3)

# 添加图例
from matplotlib.lines import Line2D
custom_lines1 = [Line2D([0], [0], color='green', lw=4, alpha=0.7),
                Line2D([0], [0], color='red', lw=4, alpha=0.7)]
ax1.legend(custom_lines1 + [scatter9_1, scatter10_1], ['正差额（盈利）', '负差额（亏损）', '9月采购数量', '10月采购数量'], loc='upper right')

# 添加网格线便于阅读
ax1.grid(axis='x', alpha=0.3)

# 调整布局以适应长标签
plt.setp(ax1.yaxis.get_majorticklabels(), ha='right')

plt.tight_layout()

# 5.2 创建10月份中标价与成本价差额与采购数量关系图例
fig2, ax2 = plt.subplots(figsize=(15, 25))  # 创建一个长图

# 计算10月中标价与成本价的差额
df_clean['diff10'] = df_clean['bidprice10'] - df_clean['price']
# 按差额从大到小排序
october_sorted = df_clean.sort_values('diff10', ascending=False)

# 设置产品名称的位置
y_pos2 = range(len(october_sorted))

# 绘制10月中标价与成本价的差额（条形图）
bars2 = ax2.barh(y_pos2, october_sorted['diff10'], alpha=0.7, color=['green' if x > 0 else 'red' for x in october_sorted['diff10']])

# 在条形图上叠加采购数量（散点图）
# 对数量进行标准化，使其在同一图表上可见
max_number2 = max(october_sorted['number9'].max(), october_sorted['number10'].max())
number_scale2 = max(october_sorted['diff10'].max(), abs(october_sorted['diff10'].min())) * 0.8
scatter9_2 = ax2.scatter(october_sorted['diff10'], y_pos2, s=october_sorted['number9'] * 2, alpha=0.5, color='blue', label='9月采购数量')
scatter10_2 = ax2.scatter(october_sorted['diff10'] + (number_scale2 / max_number2) * 5, y_pos2, s=october_sorted['number10'] * 2, alpha=0.5, color='red', label='10月采购数量')

# 设置图表属性
ax2.set_title('10月中标价与成本价差额与采购数量关系分析（按差额排序）')
ax2.set_xlabel('中标价 - 成本价（差额）')
ax2.set_ylabel('产品名称')
ax2.set_yticks(y_pos2)
ax2.set_yticklabels(october_sorted['name'], fontsize=8)
ax2.axvline(x=0, color='black', linestyle='-', alpha=0.3)

# 添加图例
custom_lines2 = [Line2D([0], [0], color='green', lw=4, alpha=0.7),
                Line2D([0], [0], color='red', lw=4, alpha=0.7)]
ax2.legend(custom_lines2 + [scatter9_2, scatter10_2], ['正差额（盈利）', '负差额（亏损）', '9月采购数量', '10月采购数量'], loc='upper right')

# 添加网格线便于阅读
ax2.grid(axis='x', alpha=0.3)

# 调整布局以适应长标签
plt.setp(ax2.yaxis.get_majorticklabels(), ha='right')

plt.tight_layout()

# 保存两个图例为一个文件
# 先保存临时文件
temp_9month_file = f'temp_{base_name}_9month.png'
temp_10month_file = f'temp_{base_name}_10month.png'
fig1.savefig(temp_9month_file, dpi=300, bbox_inches='tight')
fig2.savefig(temp_10month_file, dpi=300, bbox_inches='tight')

# 创建一个新的图形来组合两个长图
combined_fig = plt.figure(figsize=(15, 50))

# 添加9月的子图
ax_combined1 = combined_fig.add_subplot(211)
ax_combined1.imshow(plt.imread(temp_9month_file))
ax_combined1.axis('off')

# 添加10月的子图
ax_combined2 = combined_fig.add_subplot(212)
ax_combined2.imshow(plt.imread(temp_10month_file))
ax_combined2.axis('off')

plt.tight_layout()
# 保存组合后的图表
prediction_chart_file = f'{base_name}_price_prediction_charts.png'
combined_fig.savefig(prediction_chart_file, dpi=300, bbox_inches='tight')
plt.close('all')
print(f"价格预测图表已保存为: {prediction_chart_file}")

# 清理临时文件
if os.path.exists(temp_9month_file):
    os.remove(temp_9month_file)
if os.path.exists(temp_10month_file):
    os.remove(temp_10month_file)

# 6. 生成报价建议报告
print("\n=== 报价建议报告 ===")
# 根据预测结果生成分类建议
def get_pricing_strategy(row):
    predicted_change_pct = row['change_pct_10_to_11']
    
    # 安全计算当前毛利率，防止price为0导致的除零错误
    if row['price'] != 0:
        current_margin = (row['bidprice10'] - row['price']) / row['price'] * 100
    else:
        current_margin = 0  # 当price为0时，设置默认毛利率
    
    if predicted_change_pct > 5:
        return "积极报价"  # 预期价格大幅上涨
    elif predicted_change_pct > 0:
        return "稳健报价"  # 预期价格小幅上涨
    elif predicted_change_pct > -5:
        return "保守报价"  # 预期价格小幅下降
    else:
        return "防御性报价"  # 预期价格大幅下降

predictions_df['pricing_strategy'] = predictions_df.apply(get_pricing_strategy, axis=1)

# 按策略分组统计
strategy_counts = predictions_df['pricing_strategy'].value_counts()
print("\n报价策略分布:")
for strategy, count in strategy_counts.items():
    print(f"{strategy}: {count}个产品")

# 显示部分产品的详细建议
print("\n部分产品详细报价建议:")
sample_recommendations = predictions_df[['name', 'nameid', 'bidprice10', 'bidprice11_pred', 'suggested_price11', 'pricing_strategy']].head(10)
for _, row in sample_recommendations.iterrows():
    print(f"产品: {row['name']} (ID: {row['nameid']})")
    print(f"  10月中标价: {row['bidprice10']:.2f}")
    print(f"  预测11月中标价: {row['bidprice11_pred']:.2f}")
    print(f"  建议11月报价: {row['suggested_price11']:.2f}")
    print(f"  报价策略: {row['pricing_strategy']}")
    print()

# 7. 保存预测结果
output_filename = f"{base_name}_price_predictions_and_recommendations.xlsx"
predictions_df.to_excel(output_filename, index=False)
print(f"\n预测结果和报价建议已保存到: {output_filename}")

# 8. 关键洞察总结
print("\n=== 关键洞察总结 ===")
print("1. 价格趋势分析:")
print(f"   - 从9月到10月，{len(positive_changes)}个产品价格上涨，{len(negative_changes)}个产品价格下降")
print(f"   - 平均毛利率: 9月 {df_clean['margin9'].mean():.2f}%, 10月 {df_clean['margin10'].mean():.2f}%")

print("\n2. 预测洞察:")
print(f"   - 使用{model_name}模型预测11月中标价格")
print(f"   - 预测价格平均变化: {predictions_df['change_10_to_11'].mean():.4f}")
print(f"   - {strategy_counts['积极报价']}个产品建议采用积极报价策略")
print(f"   - {strategy_counts['防御性报价']}个产品建议采用防御性报价策略")

print("\n3. 行动建议:")
print("   - 对于预测价格上涨的产品，可以适当提高报价")
print("   - 对于预测价格下降的产品，需要控制成本或寻找替代产品")
print("   - 定期监控实际中标价格与预测的差异，调整预测模型")