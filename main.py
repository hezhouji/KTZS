import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import requests
import json
import os
from datetime import datetime, timedelta

# --- 配置从环境变量读取 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def calculate_percentile_score(current_value, history_series, reverse=False):
    """计算百分位得分 (0-100)"""
    history_series = history_series.dropna()
    if history_series.empty:
        return 50
    percentile = stats.percentileofscore(history_series, current_value)
    return 100 - percentile if reverse else percentile

def get_label(score):
    if score <= 20: return "🥶 极度恐惧 (建议贪婪)"
    elif score <= 40: return "😨 恐惧 (分批买入)"
    elif score <= 60: return "😐 中立"
    elif score <= 80: return "🤩 贪婪 (谨慎追高)"
    else: return "🔥 极度贪婪 (建议恐惧)"

def analyze_ashare():
    """A股模型：基于沪深300股债利差"""
    print(">>> 正在计算 A股 股债利差...")
    try:
        # 获取沪深300估值数据
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        df_val.set_index('日期', inplace=True)
        
        # 获取10年期国债收益率
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_bond.set_index('日期', inplace=True)
        
        # 合并数据
        merged = pd.DataFrame()
        merged['pe'] = df_val['市盈率TTM']
        merged = merged.join(df_bond['中国国债收益率10年'], how='inner')
        
        # 计算利差：1/PE - 国债收益率/100
        merged['spread'] = (1 / merged['pe']) - (merged['中国国债收益率10年'] / 100)
        
        current_spread = merged['spread'].iloc[-1]
        # 利差越大越恐惧（越便宜），所以 reverse=True 得到低分
        score = calculate_percentile_score(current_spread, merged['spread'], reverse=True)
        
        return {
            "market": "A股 (沪深 300)",
            "score": int(score),
            "label": get_label(score),
            "detail": f"PE: {merged['pe'].iloc[-1]:.2f} | 利差: {current_spread*100:.2f}%"
        }
    except Exception as e:
        print(f"A股计算失败: {e}")
        return None

def analyze_us_share():
    """美股模型：基于 VIX 与 均线偏离度"""
    print(">>> 正在计算 美股 恐贪指数...")
    try:
        vix = yf.Ticker("^VIX").history(period="1y")['Close']
        spy = yf.Ticker("^GSPC").history(period="2y")['Close']
        
        # 因子1: VIX百分位 (VIX越高越恐惧/分越低)
        score_vix = calculate_percentile_score(vix.iloc[-1], vix, reverse=True)
        
        # 因子2: 200日均线乖离率
        ma200 = spy.rolling(window=200).mean()
        bias = (spy - ma200) / ma200
        score_bias = calculate_percentile_score(bias.iloc[-1], bias, reverse=False)
        
        final_score = (score_vix * 0.5) + (score_bias * 0.5)
        
        return {
            "market": "美股 (标普 500)",
            "score": int(final_score),
            "label": get_label(final_score),
            "detail": f"VIX: {vix.iloc[-1]:.2f} | 200日乖离: {bias.iloc[-1]*100:+.2f}%"
        }
    except Exception as e:
        print(f"美股计算失败: {e}")
        return None

def send_feishu(results):
    if not FEISHU_WEBHOOK:
        print("错误：未检测到 FEISHU_WEBHOOK 环境变量")
        return

    elements = []
    for res in results:
        bar = "🔴" * (res['score'] // 10) + "⚪" * (10 - (res['score'] // 10))
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**{res['market']}**\n指数：{res['score']} {bar}\n状态：{res['label']}\n数据：{res['detail']}"}
        })
        elements.append({"tag": "hr"})

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📈 恐贪指数每日提醒"}, "template": "blue"},
            "elements": elements
        }
    }
    
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"飞书推送结果: {r.status_code}, {r.text}")

if __name__ == "__main__":
    final_results = []
    # 依次运行
    cn = analyze_ashare()
    if cn: final_results.append(cn)
    
    us = analyze_us_share()
    if us: final_results.append(us)
    
    # 只要有结果就尝试发送
    if final_results:
        send_feishu(final_results)
    else:
        print("所有市场计算均失败，检查网络或接口")