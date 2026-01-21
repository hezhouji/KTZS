import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import requests
import json
import os
from datetime import datetime, timedelta

FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def calculate_percentile_score(current_value, history_series, reverse=False):
    history_series = history_series.dropna()
    if history_series.empty: return 50
    percentile = stats.percentileofscore(history_series, current_value)
    return 100 - percentile if reverse else percentile

def get_label(score):
    if score <= 20: return "🥶 极度恐惧 (建议贪婪)"
    elif score <= 40: return "😨 恐惧 (分批买入)"
    elif score <= 60: return "😐 中立"
    elif score <= 80: return "🤩 贪婪 (谨慎追高)"
    else: return "🔥 极度贪婪 (建议恐惧)"

def analyze_ashare():
    print(">>> 正在计算 A股 (沪深300) 股债利差模型...")
    try:
        # 获取估值数据
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        
        # 兼容性修复：自动寻找包含 '市盈率' 或 'PE' 的列
        pe_col = [c for c in df_val.columns if '市盈率' in c and 'TTM' in c]
        date_col = [c for c in df_val.columns if '日期' in c or 'date' in c]
        
        if not pe_col or not date_col:
            print(f"找不到 PE 或 日期列。当前列名: {df_val.columns.tolist()}")
            return None
            
        df_val[date_col[0]] = pd.to_datetime(df_val[date_col[0]])
        df_val.set_index(date_col[0], inplace=True)
        
        # 获取10年期国债收益率
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_bond.set_index('日期', inplace=True)
        
        merged = pd.DataFrame()
        merged['pe'] = df_val[pe_col[0]]
        merged = merged.join(df_bond['中国国债收益率10年'], how='inner')
        
        # 计算利差 (ERP)
        merged['spread'] = (1 / merged['pe']) - (merged['中国国债收益率10年'] / 100)
        
        current_spread = merged['spread'].iloc[-1]
        score = calculate_percentile_score(current_spread, merged['spread'], reverse=True)
        
        return {
            "market": "A股 (沪深 300)",
            "score": int(score),
            "label": get_label(score),
            "detail": f"PE: {merged['pe'].iloc[-1]:.2f} | 利差: {current_spread*100:.2f}%"
        }
    except Exception as e:
        print(f"A股计算失败: {str(e)}")
        return None

def analyze_us_share():
    print(">>> 正在计算 美股 (标普500) 混合模型...")
    try:
        vix = yf.Ticker("^VIX").history(period="1y")['Close']
        spy = yf.Ticker("^GSPC").history(period="2y")['Close']
        
        score_vix = calculate_percentile_score(vix.iloc[-1], vix, reverse=True)
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
        print(f"美股计算失败: {str(e)}")
        return None

def send_feishu(results):
    if not FEISHU_WEBHOOK:
        print("未检测到 Webhook")
        return

    elements = []
    for res in results:
        bar_count = max(1, res['score'] // 10)
        bar = "🔴" * bar_count + "⬜" * (10 - bar_count)
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**{res['market']}**\n指数：{res['score']} {bar}\n状态：{res['label']}\n数据：{res['detail']}"}
        })
        elements.append({"tag": "hr"})

    # 这里的标题包含“恐贪”和“指数”，请确保飞书后台有其中一个关键词
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📊 恐贪指数每日提醒"}, "template": "blue"},
            "elements": elements
        }
    }
    
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"飞书推送结果: {r.status_code}, {r.text}")

if __name__ == "__main__":
    final_results = []
    res_cn = analyze_ashare()
    if res_cn: final_results.append(res_cn)
    
    res_us = analyze_us_share()
    if res_us: final_results.append(res_us)
    
    if final_results:
        send_feishu(final_results)
    else:
        print("计算全部失败，无法发送")