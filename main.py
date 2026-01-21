import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime

FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_p_score(series, current_val, reverse=False):
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50
    p = stats.percentileofscore(series, current_val)
    return 100 - p if reverse else p

def analyze_jiuquan_full():
    print(">>> 正在复刻韭圈儿六大维度模型...")
    try:
        # 1. 股价强度 & 成交量 (权重 40%)
        df_p = ak.stock_zh_index_daily(symbol="sh000300")
        df_p['close'] = df_p['close'].astype(float)
        # 强度：当前价在一年内的位置
        high_250 = df_p['close'].rolling(250).max()
        score_strength = get_p_score(df_p['close']/high_250, (df_p['close']/high_250).iloc[-1])
        # 成交量：当前成交额 vs 20日均线
        vol_ma20 = df_p['volume'].rolling(20).mean()
        score_vol = get_p_score(df_p['volume']/vol_ma20, (df_p['volume']/vol_ma20).iloc[-1])

        # 2. 避险天堂 (股债收益差 - 权重 20%) - 解决 0 分关键
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_bond = ak.bond_zh_us_rate()
        df_val['date'] = pd.to_datetime(df_val['日期']).dt.date
        df_bond['date'] = pd.to_datetime(df_bond['日期']).dt.date
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        
        merged = pd.merge(df_val[['date', pe_col]], df_bond[['date', '中国国债收益率10年']], on='date')
        merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
        # 截图显示 3.52% 在中立区间，ERP 越高越恐惧，需 reverse
        score_erp = 100 - get_p_score(merged['erp'], merged['erp'].iloc[-1])

        # 3. 升贴水率 (权重 20%)
        # 模拟：利用指数乖离度替代期货基差（正乖离大代表看多热度高）
        bias_20 = (df_p['close'] - df_p['close'].rolling(20).mean()) / df_p['close'].rolling(20).mean()
        score_basis = get_p_score(bias_20, bias_20.iloc[-1])

        # 4. 指数波动率 (权重 20%)
        # 历史波动率：波动剧增且下跌是恐惧，波动剧增且上涨是极度贪婪
        vix_sim = df_p['close'].pct_change().rolling(20).std()
        score_vix = get_p_score(vix_sim, vix_sim.iloc[-1])

        # --- 综合权重拟合 ---
        # 2026-01-20 行情：强度(95) + 成交(85) + 估值(20) + 乖离(90) + 波动(80)
        final_score = (score_strength * 0.25) + (score_vol * 0.2) + (score_erp * 0.2) + (score_basis * 0.2) + (score_vix * 0.15)
        
        return {
            "score": int(final_score),
            "strength": int(score_strength),
            "vol": int(score_vol),
            "erp": int(score_erp),
            "basis": int(score_basis)
        }
    except Exception as e:
        print(f"致命错误: {e}")
        return None

def send_feishu(res):
    if not res: return
    color = "red" if res['score'] > 60 else "blue"
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📊 韭圈儿六大维度恐贪同步"}, "template": color},
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**当前指数：{res['score']}**\n\n- 🚀 股价强度分位：{res['strength']}\n- 💰 成交活跃分位：{res['vol']}\n- 🛡️ 股债性价比：{res['erp']}\n- 📈 升贴水(乖离)：{res['basis']}"}
            }]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    result = analyze_jiuquan_full()
    send_feishu(result)