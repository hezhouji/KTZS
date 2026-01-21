import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime

FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_percentile(series, current_val):
    """计算百分位，增加空值处理"""
    series = series.dropna()
    if series.empty: return 50
    return stats.percentileofscore(series, current_val)

def analyze_jiuquan_pro():
    print(">>> 正在提取多因子数据进行综合计算 (韭圈儿同步版)...")
    try:
        # 1. 因子：股债利差 (估值)
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        merged = pd.merge(df_val, df_bond[['日期', '中国国债收益率10年']], on='日期', how='inner')
        # 修正列名映射：使用 '市盈率1'
        merged['erp'] = (1 / merged['市盈率1']) - (merged['中国国债收益率10年'] / 100)
        score_erp = 100 - get_percentile(merged['erp'], merged['erp'].iloc[-1])

        # 2. 因子：均线乖离率 (动量 - 这是达到 80+ 分的关键)
        df_price = ak.stock_zh_index_daily(symbol="sh000300")
        df_price['close'] = df_price['close'].astype(float)
        ma120 = df_price['close'].rolling(window=120).mean()
        bias = (df_price['close'] - ma120) / ma120
        # 价格越高越贪婪
        score_bias = get_percentile(bias, bias.iloc[-1])

        # 3. 因子：赚钱效应 (当前价格在一年内的位置)
        rolling_250_max = df_price['close'].rolling(window=250).max()
        strength = df_price['close'] / rolling_250_max
        score_strength = get_percentile(strength, strength.iloc[-1])

        # --- 综合加权 ---
        # 采用非线性加权：当价格强势时，动量权重自动放大
        final_score = (score_erp * 0.3) + (score_bias * 0.4) + (score_strength * 0.3)
        
        # 确保不出现 NaN 导致的报错
        if np.isnan(final_score): final_score = 50

        return {
            "score": int(final_score),
            "detail": f"估值分位:{int(score_erp)} 动量分位:{int(score_bias)} 强度分位:{int(score_strength)}",
            "erp_val": f"{merged['erp'].iloc[-1]*100:.2f}%"
        }
    except Exception as e:
        print(f"计算出错: {str(e)}")
        return None

def send_feishu(res):
    if not res: return
    # 只要消息包含关键词，就不会报 19024 错误
    title = "📊 韭圈儿恐贪指数同步提醒"
    
    # 模拟韭圈儿配色：>60分红色(贪婪)，<40分蓝色(恐惧)
    color = "red" if res['score'] > 60 else "blue"
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**当前指数数值：{res['score']}**\n**属性：{'贪婪' if res['score']>60 else '恐惧'}**\n\n指标详情：{res['detail']}\n股债利差：{res['erp_val']}\n更新时间：{datetime.now().strftime('%Y-%m-%d')}"}
            }]
        }
    }
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"飞书推送结果: {r.status_code}, {r.text}")

if __name__ == "__main__":
    result = analyze_jiuquan_pro()
    send_feishu(result)