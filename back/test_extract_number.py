import re

# 复制修改后的extract_number函数实现用于测试
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

# 测试用例
test_cases = [
    "猪肉糜（肥2瘦8）-0701012400",  # 应该提取出: 0701012400
    "牛肉-0702013500",              # 应该提取出: 0702013500
    "鸡肉123",                      # 应该提取出: 123（没有末尾长数字串）
    "蔬菜类-070301",               # 应该提取出: 070301（长度不够10位，使用后备逻辑）
    "无数字文本",                   # 应该返回原文本
]

# 执行测试
print("=== 测试extract_number函数 ===")
for test_case in test_cases:
    result = extract_number(test_case)
    print(f"输入: '{test_case}'")
    print(f"输出: '{result}'")
    print("---")