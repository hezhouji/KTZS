import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime

# 从 GitHub Secrets 读取环境变量
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_percentile(series, current_val):
    """计算当前值在历史序列中的百分位排位 (0-100)"""
    series = series.dropna()
    if series.empty: return 50
    return stats.percentileofscore(series, current_val)

def analyze_jiuquan_final():
    print(">>> 正在复刻韭圈儿 83 分逻辑模型...")
    try:
        # --- 1. 获取基础数据：沪深300指数 ---
        df_p = ak.stock_zh_index_daily(symbol="sh000300")
        df_p['close'] = df_p['close'].astype(float)
        df_p['日期'] = pd.to_datetime(df_p['date'])
        
        # --- 2. 动量因子 (影响分数的关键：热度) ---
        # 计算价格偏离 120 日线的程度 (Bias)
        ma120 = df_p['close'].rolling(window=120).mean()
        bias = (df_p['close'] - ma120) / ma120
        # 价格越高分越高（贪婪）
        score_momentum = get_percentile(bias, bias.iloc[-1])

        # --- 3. 股债性价比因子 (底层安全垫：估值) ---
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        
        # 适配列名 (处理你遇到的 '市盈率1' 变动)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        
        # 合并数据并使用前向填充，防止因节假日不同步导致的 0 分
        merged = pd.merge(df_val[['日期', pe_col]], df_bond[['日期', '中国国债收益率10年']], on='日期', how='inner')
        merged = merged.ffill().dropna()
        
        # ERP = 1/PE - 国债收益率
        merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
        
        # ERP越高越安全(低分)，所以得分 = 100 - 百分位
        score_erp = 100 - get_percentile(merged['erp'], merged['erp'].iloc[-1])

        # --- 4. 股价强度 (当前价格在过去一年中的位置) ---
        rolling_250_max = df_p['close'].rolling(window=250).max()
        strength = df_p['close'] / rolling_250_max
        score_strength = get_percentile(strength, strength.iloc[-1])

        # --- 5. 综合拟合权重 ---
        # 既然 1月20日是 83 分，说明动量和强度的权重非常高
        # 动量 45% + 强度 35% + 估值 20%
        final_score = (score_momentum * 0.45) + (score_strength * 0.35) + (score_erp * 0.20)
        
        return {
            "score": int(final_score),
            "momentum": int(score_momentum),
            "erp": int(score_erp),
            "strength": int(score_strength),
            "date": datetime.now().strftime('%Y-%m-%d')
        }
    except Exception as e:
        print(f"模型计算失败详情: {str(e)}")
        return None

def send_to_feishu(res):
    if not res: return
    
    # 标题必须包含关键词：指数
    title = f"📊 韭圈儿恐贪指数同步提醒 ({res['date']})"
    # 颜色策略：>60红(贪婪), <40蓝(恐惧)
    color = "red" if res['score'] > 60 else "blue"
    
    # 构建卡片内容
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md", 
                "content": f"**当前指数：{res['score']}**\n指数属性：**{'🔥 贪婪' if res['score']>60 else '❄️ 恐惧'}**\n\n"
                           f"**维度拆解：**\n"
                           f"- 🚀 动量热度分位：{res['momentum']} (主导)\n"
                           f"- 📈 股价强度分位：{res['strength']}\n"
                           f"- 🛡️ 股债性价比分位：{res['erp']} (底层)"
            }
        },
        {"tag": "hr"},
        {
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": "注：基于价格动能、一年股价分位及 ERP 综合拟合，对标韭圈儿 App 指标。"}]
        }
    ]

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": elements
        }
    }
    
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"飞书发送结果: {r.status_code}, {r.text}")

if __name__ == "__main__":
    result = analyze_jiuquan_final()
    if result:
        send_to_feishu(result)