import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime, timedelta

FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_fear_greed_label(score):
    if score <= 20: return "😱 极度恐惧 (韭圈儿：极低估)"
    elif score <= 40: return "😨 恐惧 (建议定投)"
    elif score <= 60: return "😐 中立"
    elif score <= 80: return "🤩 贪婪 (分批止盈)"
    else: return "🔥 极度贪婪 (韭圈儿：风险区)"

def analyze_ashare_jiuquan():
    """
    仿韭圈儿：基于沪深300长期股债利差百分位
    """
    print(">>> 正在复刻韭圈儿算法：计算A股性价比...")
    try:
        # 1. 获取近10年沪深300估值 (为了得到准确的分位数，必须有足够长的历史)
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        
        # 匹配列名
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        df_val = df_val.sort_values('日期')
        
        # 2. 获取10年期国债收益率
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_bond.set_index('日期', inplace=True)
        
        # 3. 合并数据并填充空值 (防止因为节假日错开导致 join 失败)
        df_val.set_index('日期', inplace=True)
        merged = df_val[[pe_col]].join(df_bond['中国国债收益率10年'], how='left')
        merged = merged.ffill() # 重点：向前填充，解决数据频率不一致导致的0值问题
        
        # 4. 计算 ERP (股权风险溢价)
        # 韭圈儿逻辑：1/PE (盈利收益率) - 国债收益率
        merged['erp'] = (1 / merged[pe_col]) - (merged['中国国债收益率10年'] / 100)
        
        # 5. 计算当前 ERP 在过去 10 年的位置 (百分位)
        current_erp = merged['erp'].iloc[-1]
        history_erp = merged['erp'].dropna()
        
        # 韭圈儿恐贪指数通常 0 是极度恐惧，100 是极度贪婪
        # ERP 越大越值得买（恐惧），所以 ERP 越高，分数应该越低
        percentile = stats.percentileofscore(history_erp, current_erp)
        final_score = 100 - percentile # 转化：高分=贪婪，低分=恐惧
        
        return {
            "market": "A股 (沪深300)",
            "score": int(final_score),
            "label": get_fear_greed_label(final_score),
            "detail": f"PE: {merged[pe_col].iloc[-1]:.2f} | 利差: {current_erp*100:.2f}%"
        }
    except Exception as e:
        print(f"A股韭圈儿算法运行失败: {e}")
        return None

def analyze_us_fear_greed():
    """
    美股：采用 CNN Fear & Greed 简化版 (VIX + 动量)
    """
    try:
        vix = yf.Ticker("^VIX").history(period="2y")['Close']
        spy = yf.Ticker("^GSPC").history(period="2y")['Close']
        
        # VIX越高越恐惧 (分数越低)
        vix_p = stats.percentileofscore(vix, vix.iloc[-1])
        vix_score = 100 - vix_p
        
        # 偏离200日均线程度
        ma200 = spy.rolling(window=200).mean()
        bias = (spy - ma200) / ma200
        bias_p = stats.percentileofscore(bias.dropna(), bias.iloc[-1])
        
        final_score = (vix_score * 0.6) + (bias_p * 0.4)
        
        return {
            "market": "美股 (S&P500)",
            "score": int(final_score),
            "label": get_fear_greed_label(final_score),
            "detail": f"VIX: {vix.iloc[-1]:.2f} | 200日偏离: {bias.iloc[-1]*100:+.2f}%"
        }
    except Exception as e:
        print(f"美股计算失败: {e}")
        return None

def send_to_feishu(results):
    if not FEISHU_WEBHOOK: return
    
    # 构建飞书消息卡片
    elements = []
    for res in results:
        # 根据分值动态选色
        color = "blue" if res['score'] < 40 else "red" if res['score'] > 60 else "grey"
        bar = "🟦" * (res['score'] // 10) + "⬜" * (10 - res['score'] // 10)
        
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**{res['market']}**\n指数: **{res['score']}** {bar}\n属性: {res['label']}\n数据说明: {res['detail']}"}
        })
        elements.append({"tag": "hr"})

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📊 韭圈儿式恐贪指数提醒"}, "template": "orange"},
            "elements": elements
        }
    }
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"推送状态: {r.status_code}")

if __name__ == "__main__":
    data = []
    cn = analyze_ashare_jiuquan()
    if cn: data.append(cn)
    
    us = analyze_us_fear_greed()
    if us: data.append(us)
    
    if data:
        send_to_feishu(data)