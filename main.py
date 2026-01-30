import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import minimize
import requests
import os
from datetime import datetime, timedelta

# --- 配置 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"
LOG_FILE = "HISTORY_LOG.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def normalize_date(d_val):
    """确保日期统一，解决图中格式混乱问题"""
    s = str(d_val).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(s, fmt).date()
        except: continue
    return None

def get_actual(date_obj):
    target = date_obj.strftime("%Y%m%d")
    if not os.path.exists(DATA_DIR): return None
    for f in os.listdir(DATA_DIR):
        if target in f and f.endswith(".txt"):
            try:
                with open(os.path.join(DATA_DIR, f), "r") as file:
                    return float(file.read().strip())
            except: pass
    return None

def calculate_factors(target_date, df_p_all, df_val_all, df_bond_all):
    """严格历史回溯计算，确保每天得分不同"""
    try:
        # 只取目标日期及之前的数据
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()

        if len(df_p) < 30: return [50.0]*6

        def p_score(series, cur, inv=False):
            p = stats.percentileofscore(series.dropna(), cur)
            return float(100 - p if inv else p)

        # 核心因子计算
        v = df_p['close'].pct_change().rolling(20).std()
        f1 = p_score(v, v.iloc[-1], inv=True)
        v20 = df_p['volume'].rolling(20).mean()
        f2 = p_score(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])
        h250 = df_p['close'].rolling(250).max()
        f3 = p_score(df_p['close']/h250, (df_p['close']/h250).iloc[-1])
        f4 = 50.0 # 模拟基差
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        f5 = p_score(erp, erp.iloc[-1], inv=True)
        f6 = 50.0 # 模拟杠杆

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except: return [50.0]*6

def main():
    log("=== 启动 AI 自适应预测系统 ===")
    today = datetime.now().date()
    cols = ["日期", "f1", "f2", "f3", "f4", "f5", "f6", "预测", "实际的", "偏见"]

    # 读取并强行清洗旧 CSV
    if os.path.exists(LOG_FILE):
        df_log = pd.read_csv(LOG_FILE)
        if not df_log.empty:
            df_log['日期'] = df_log['日期'].apply(lambda x: normalize_date(x).strftime("%Y-%m-%d") if normalize_date(x) else x)
    else:
        df_log = pd.DataFrame(columns=cols)

    # 获取全量市场数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 1. 补全历史与对齐（处理不完整数据）
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        act = get_actual(d)
        if act:
            d_str = d.strftime("%Y-%m-%d")
            fs = calculate_factors(d, df_p, df_val, df_bond)
            p_avg = round(sum(fs)/6, 2)
            df_log = df_log[df_log['日期'] != d_str]
            df_log.loc[len(df_log)] = [d_str] + fs + [p_avg, act, round(act - p_avg, 2)]

    # 2. 权重进化
    weights = np.array([1/6]*6)
    df_fit = df_log.dropna(subset=['实际的']).tail(7)
    if len(df_fit) >= 5: # 降低门槛，有5天数据就开始对齐
        X, y = df_fit[['f1','f2','f3','f4','f5','f6']].values, df_fit['实际的'].values
        res = minimize(lambda w: np.sum((X@w - y)**2), weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 3. 今日预测（解决 nan 问题）
    tf = calculate_factors(today, df_p, df_val, df_bond)
    tp = round(sum(f*w for f, w in zip(tf, weights)), 2)
    
    # 误差修正容错逻辑
    bias_val = 0.0
    bias_desc = "无"
    if not df_log.dropna(subset=['偏见']).empty:
        last_bias = df_log.dropna(subset=['偏见']).iloc[-1]['偏见']
        if not np.isnan(last_bias):
            bias_val = last_bias
            bias_desc = f"{'+' if last_bias>=0 else ''}{last_bias}"

    final_val = round(tp + bias_val, 2)
    
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['日期'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + tf + [tp, np.nan, np.nan]
    df_log.sort_values('日期').to_csv(LOG_FILE, index=False)

    # 4. 飞书推送（解决关键词拦截）
    w_info = " | ".join([f"{n}:{w:.0%}" for n, w in zip(["波动","量能","强度","期货","避险","杠杆"], weights)])
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪指数 AI 预测 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日建议值：{final_val}**\n原生：{tp} | 修正：{bias_desc}\n\n📊 **权重对齐：**\n{w_info}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "关键词：恐贪"}]}
            ]
        }
    }
    if FEISHU_WEBHOOK:
        r = requests.post(FEISHU_WEBHOOK, json=payload)
        log(f"推送状态: {r.status_code} {r.text}")

if __name__ == "__main__":
    main()