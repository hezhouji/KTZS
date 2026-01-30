import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime, timedelta

# 配置
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"

def get_p_score(series, current_val, reverse=False):
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50
    p = stats.percentileofscore(series, current_val, kind='weak')
    return 100 - p if reverse else p

def get_actual_val(date_str):
    """从 KTZS/YYYYMMDD.txt 获取昨日韭圈儿实际值"""
    path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return float(f.read().strip())
        except: return None
    return None

def analyze_full_factors(target_date, df_p, df_val, df_bond):
    """全因子扫描模型"""
    try:
        # 截取历史数据
        df_curr = df_p[df_p['date'] <= target_date].copy()
        
        # 1. 价格动能 (权重: 0.3)
        h250 = df_curr['close'].rolling(250).max()
        s_score = get_p_score(df_curr['close']/h250, (df_curr['close']/h250).iloc[-1])
        
        # 2. 成交量能 (权重: 0.2)
        v20 = df_curr['volume'].rolling(20).mean()
        v_score = get_p_score(df_curr['volume']/v20, (df_curr['volume']/v20).iloc[-1])
        
        # 3. 股债性价比 (权重: 0.15)
        # (复用之前的日期匹配逻辑计算 ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        e_score = 50 # 默认
        
        # 4. 模拟新增因子：全市场赚钱效应 (权重: 0.2)
        # 获取当日涨跌家数 (以此模拟截图中的'股价强度')
        try:
            df_adv = ak.stock_zh_a_spot_em()
            up_count = len(df_adv[df_adv['涨跌幅'] > 0])
            score_market = (up_count / len(df_adv)) * 100
        except: score_market = 50

        # 5. 模拟新增因子：杠杆水平/北向流向 (权重: 0.15)
        # 此处使用 Bias 乖离度作为情绪溢价的替代
        bias = (df_curr['close'] - df_curr['close'].rolling(20).mean()) / df_curr['close'].rolling(20).mean()
        score_bias = get_p_score(bias, bias.iloc[-1])

        # 原始加权总分 (Raw)
        raw = (s_score * 0.3) + (v_score * 0.2) + (score_market * 0.2) + (score_bias * 0.15) + (15) # 基础分补正
        
        return {"raw": raw, "factors": {"强度": s_score, "量能": v_score, "普涨": score_market, "情绪": score_bias}}
    except: return None

def main():
    today = datetime.now().date()
    yest_str = (today - timedelta(days=1)).strftime("%Y%m%d")
    
    # 获取数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_bond = ak.bond_zh_us_rate()
    
    # 1. 计算昨日模型值并获取实际值
    yest_model = analyze_full_factors(today - timedelta(days=1), df_p, df_val, df_bond)
    yest_actual = get_actual_val(yest_str)
    
    # 2. 动态计算今日模型
    today_model = analyze_full_factors(today, df_p, df_val, df_bond)
    
    # 3. 归因修正逻辑
    bias = 0
    reason = "继承昨日误差"
    if yest_model and yest_actual:
        bias = yest_actual - yest_model['raw']
        
        # 智能规律推测：
        # 如果今日成交量暴增 > 30%，则推测情绪有过热溢价，额外增加修正值的 10%
        vol_change = (df_p['volume'].iloc[-1] / df_p['volume'].iloc[-2]) - 1
        if vol_change > 0.3:
            bias *= 1.1
            reason = "成交量异常爆表：调高亢奋系数"
        elif vol_change < -0.2:
            bias *= 0.9
            reason = "成交急剧萎缩：情绪退潮加速"

    final = max(0, min(100, today_model['raw'] + bias))
    
    # 4. 发送推送
    send_feishu_final(final, today_model, bias, reason)

def send_feishu_final(final, model, bias, reason):
    # 构建飞书消息逻辑...
    print(f"今日预测: {final}, 修正原因: {reason}")
    # (此处省略具体的 requests 发送代码，与前几版一致)

if __name__ == "__main__":
    main()