#!/bin/bash
# 快速验证脚本

echo "=========================================="
echo "推送流程优化系统验证"
echo "=========================================="
echo ""

PROJECT_DIR="/Users/variya/.openclaw/workspace/projects/a-stock-advisor"
cd "$PROJECT_DIR"

# 1. 检查文件存在性
echo "1. 检查文件存在性..."
FILES=(
    "scripts/is_trading_day.py"
    "scripts/auto_push_system.py"
    "scripts/push_monitor.py"
    "data/akshare_real_data_fixed.pkl"
    "config/feishu_config.json"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (不存在)"
    fi
done

echo ""

# 2. 检查文件权限
echo "2. 检查文件权限..."
for file in scripts/is_trading_day.py scripts/auto_push_system.py scripts/push_monitor.py; do
    if [ -x "$file" ]; then
        echo "  ✓ $file (可执行)"
    else
        echo "  ✗ $file (不可执行)"
    fi
done

echo ""

# 3. 语法检查
echo "3. Python语法检查..."
python3 -m py_compile scripts/is_trading_day.py 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ is_trading_day.py"
else
    echo "  ✗ is_trading_day.py (语法错误)"
fi

python3 -m py_compile scripts/auto_push_system.py 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ auto_push_system.py"
else
    echo "  ✗ auto_push_system.py (语法错误)"
fi

python3 -m py_compile scripts/push_monitor.py 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ push_monitor.py"
else
    echo "  ✗ push_monitor.py (语法错误)"
fi

echo ""

# 4. 模块导入验证
echo "4. 模块导入验证..."
python3 -c "from scripts.is_trading_day import TradingDayChecker; print('  ✓ is_trading_day');" 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ 模块导入成功"
else
    echo "  ✗ 模块导入失败"
fi

python3 -c "from scripts.auto_push_system import AutoPushSystem; print('  ✓ auto_push_system');" 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ 模块导入成功"
else
    echo "  ✗ 模块导入失败"
fi

python3 -c "from scripts.push_monitor import PushMonitor; print('  ✓ push_monitor');" 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ 模块导入成功"
else
    echo "  ✗ 模块导入失败"
fi

echo ""

# 5. 功能测试（非交易日）
echo "5. 交易日判断测试..."
python3 scripts/is_trading_day.py 2>&1 | grep "判断结果"

echo ""

# 6. 总结
echo "=========================================="
echo "验证完成"
echo "=========================================="
echo ""
echo "文件位置："
echo "  - is_trading_day.py:     scripts/is_trading_day.py"
echo "  - auto_push_system.py:    scripts/auto_push_system.py"
echo "  - push_monitor.py:        scripts/push_monitor.py"
echo ""
echo "使用方法："
echo "  python3 scripts/is_trading_day.py"
echo "  python3 scripts/auto_push_system.py"
echo "  python3 scripts/push_monitor.py"
echo ""
