#!/bin/bash

echo "🔍 ĐÁNH GIÁ HỆ THỐNG IOT WATER METER"
echo "===================================="

cd performance

echo "🔧 Đánh giá Functions + Performance + Benchmark..."
python3 function_evaluator.py
python3 performance_evaluator.py

echo ""
echo "✅ HOÀN THÀNH!"
echo "📋 Báo cáo:"
echo "   - performance/function_evaluation_summary.txt"
echo "   - performance/performance_summary.txt"
echo ""
echo "💡 Xem nhanh: cat performance/*_summary.txt"