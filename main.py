import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime

# 配置环境变量
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

def get_percentile(series, current_val):
    series = series.dropna()
    if series.empty: return 50
    return stats.percentileofscore(series, current_val)

def analyze_jiuquan_v3():
    """
    韭圈儿本土化模型：多因子情绪合成
    """
    print(">>> 正在模拟韭圈儿本土化算法：计算全市场情绪...")
    try:
        # 1. 股价强度/广度 (权重 30%) - 反映“龙腾股跃”的关键
        # 使用沪深300价格偏离20日和120日线的程度
        df_p = ak.stock_zh_index_daily(symbol="sh000300")
        df_p['close'] = df_p['close'].astype(float)
        bias_short = (df_p['close'] - df_p['close'].rolling(20).mean()) / df_p['close'].rolling(20).mean()
        bias_long = (df_p['close'] - df_p['close'].rolling(120).mean()) / df_p['close'].rolling(120).mean()
        score_momentum = (get_percentile(bias_short, bias_short.iloc[-1]) * 0.4 + 
                          get_percentile(bias_long, bias_long.iloc[-1]) * 0.6)

        # 2. 股债比类 (权重 20%) - 宏观性价比
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        merged = pd.merge(df_val, df_bond[['日期', '中国国债收益率10年']], on='日期', how='inner')
        erp = (1 / merged['市盈率1']) - (merged['中国国债收益率10年'] / 100)
        # ERP越高代表越便宜(恐惧)，所以得分 = 100 - 百分位
        score_erp = 100 - get_percentile(erp, erp.iloc[-1])

        # 3. 资金流入/活跃度 (权重 25%) - 成交量放大
        df_p['vol_ma'] = df_p['volume'].rolling(20).mean()
        vol_ratio = df_p['volume'] / df_p['vol_ma']
        score_vol = get_percentile(vol_ratio, vol_ratio.iloc[-1])

        # 4. 期货基差/波动率模拟 (权重 25%) 
        # 简化版：通过历史波动率的标准差分位来模拟情绪亢奋度
        std_20 = df_p['close'].pct_change().rolling(20).std()
        score_vix = get_percentile(std_20, std_20.iloc[-1])

        # --- 最终加权合成 ---
        # 逻辑：价格强度 > 股债比 > 活跃度 > 波动率
        final_score = (score_momentum * 0.4) + (score_erp * 0.25) + (score_vol * 0.2) + (score_vix * 0.15)
        
        # 针对 2026-01-20 的截图进行模型校准
        # 截图 83 分属于“贪婪”区间
        return {
            "score": int(final_score),
            "momentum": int(score_momentum),
            "erp": int(score_erp),
            "label": "🔥 贪婪" if final_score > 60 else "❄️ 恐惧" if final_score < 40 else "😐 中性"
        }
    except Exception as e:
        print(f"算法执行错误: {e}")
        return None

def send_feishu(res):
    if not res: return
    # 关键词必须包含：指数
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📈 韭圈儿恐贪指数 (本土化拟合版)"}, "template": "red" if res['score'] > 60 else "blue"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**当前指数数值：{res['score']}**\n指数属性：**{res['label']}**\n\n**子指标拆解：**\n- 股价动能分位：{res['momentum']} (核心驱动)\n- 股债性价比分位：{res['erp']} (底层安全垫)\n\n<font color='grey'>注：本指标通过量化波动率、动能、估值合成，对标韭圈儿情绪模型。</font>"}}
            ]
        }
    }
    r = requests.post(FEISHU_WEBHOOK, json=payload)
    print(f"发送状态: {r.status_code}")

if __name__ == "__main__":
    result = analyze_jiuquan_v3()
    send_feishu(result)