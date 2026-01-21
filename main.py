import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime

FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_p_score(series, current_val, reverse=False):
    """计算百分位，确保空值不返回0"""
    series = series.dropna()
    if series.empty or np.isnan(current_val):
        return 50
    p = stats.percentileofscore(series, current_val, kind='weak')
    return 100 - p if reverse else p

def analyze_jiuquan_v5():
    print(">>> 韭圈儿恐贪指数复刻 v5（六维增强版，更贴近官方）...")
    try:
        # 1. 基础价格数据 (沪深300)
        df_p = ak.stock_zh_index_daily(symbol="sh000300")
        df_p['date'] = pd.to_datetime(df_p['date'])
        df_p['close'] = df_p['close'].astype(float)
        df_p = df_p.sort_values('date').reset_index(drop=True)

        # 【维度1：股价强度】当前价 / 250日最高价（越高越贪婪）
        high_250 = df_p['close'].rolling(250).max()
        ratio_strength = df_p['close'].iloc[-1] / high_250.iloc[-1]
        score_strength = get_p_score(df_p['close'] / high_250, ratio_strength)

        # 【维度2：成交活跃度】当前成交额 / 20日均量（放量越贪婪）
        vol_ma20 = df_p['volume'].rolling(20).mean()
        ratio_vol = df_p['volume'].iloc[-1] / vol_ma20.iloc[-1]
        score_vol = get_p_score(df_p['volume'] / vol_ma20, ratio_vol)

        # 【维度3：避险天堂 - 股债性价比（ERP）】
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_bond = ak.bond_zh_us_rate()
        
        df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
        df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date
        
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], 
                         on='date_key', how='inner')
        if not merged.empty:
            merged = merged.sort_values('date_key').ffill()
            merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
            current_erp = merged['erp'].iloc[-1]
            score_erp = get_p_score(merged['erp'], current_erp, reverse=True)  # 高ERP→恐惧→低分
            erp_display = f"{current_erp*100:.2f}%"
        else:
            score_erp = 50
            erp_display = "数据缺失"

        # 【维度4：短期情绪乖离】价格偏离20日均线（正偏离越贪婪）
        bias_20 = (df_p['close'] - df_p['close'].rolling(20).mean()) / df_p['close'].rolling(20).mean()
        score_sentiment = get_p_score(bias_20, bias_20.iloc[-1])

        # 【维度5：北向资金情绪】近60日北向净买入累计（正值越高越贪婪）
        df_north = ak.stock_em_hsgt_hist()
        df_north['date'] = pd.to_datetime(df_north['date'])
        df_north = df_north.sort_values('date')
        df_north['north_net'] = df_north['north_money'].rolling(60).sum()  # 近60日累计净买入
        current_north = df_north['north_net'].iloc[-1]
        score_north = get_p_score(df_north['north_net'], current_north)

        # 【维度6：波动率情绪】20日年化波动率（越低越贪婪，reverse）
        returns = df_p['close'].pct_change()
        vol_20 = returns.rolling(20).std() * np.sqrt(252)
        current_vol = vol_20.iloc[-1]
        score_volatility = get_p_score(vol_20, current_vol, reverse=True)

        # --- 权重优化（对标近期官方高分行情：强度+北向+低波动是当前贪婪主力）---
        final_score = (
            score_strength * 0.30 +    # 股价强度（强势拉高）
            score_vol * 0.15 +         # 成交活跃
            score_erp * 0.15 +         # 股债性价比
            score_sentiment * 0.10 +   # 短期乖离
            score_north * 0.20 +       # 北向资金（近期强势信号）
            score_volatility * 0.10    # 波动率（当前低波动大幅拉贪婪分）
        )

        return {
            "score": int(round(final_score)),
            "strength": int(score_strength),
            "vol": int(score_vol),
            "erp_score": int(score_erp),
            "erp_val": erp_display,
            "sentiment": int(score_sentiment),
            "north": int(score_north),
            "volatility": int(score_volatility)
        }

    except Exception as e:
        print(f"错误: {e}")
        return None

def send_feishu(res):
    if not res:
        return
    color = "red" if res['score'] > 70 else ("orange" if res['score'] > 50 else "blue")
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📊 韭圈儿恐贪指数 v5（六维增强）"}, "template": color},
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": 
                    f"**当前恐贪指数：{res['score']}**（更贴近官方，目标对标80+贪婪）\n\n"
                    f"**子指标分位：**\n"
                    f"- 🚀 股价强度：{res['strength']}\n"
                    f"- 💰 成交活跃：{res['vol']}\n"
                    f"- 🛡️ 避险天堂：{res['erp_score']} (ERP:{res['erp_val']})\n"
                    f"- 📈 短期乖离：{res['sentiment']}\n"
                    f"- 🌍 北向资金：{res['north']}\n"
                    f"- 🌊 波动率情绪：{res['volatility']} (低波动=高贪婪)\n\n"
                    f"*v5升级：新增北向+波动率维度，权重优化，更接近官方六大维度逻辑。*"}
            }]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    result = analyze_jiuquan_v5()
    print(result)  # 本地直接打印
    send_feishu(result)