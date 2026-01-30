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

def is_workday(d):
    return d.weekday() < 5

def normalize_date(d_input):
    """确保日期格式统一为 YYYY-MM-DD"""
    d_str = str(d_input).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(d_str, fmt).date()
        except: continue
    return None

def get_actual_val(date_obj):
    target = date_obj.strftime("%Y%m%d")
    if not os.path.exists(DATA_DIR): return None
    for f in os.listdir(DATA_DIR):
        if target in f and f.endswith(".txt"):
            try:
                with open(os.path.join(DATA_DIR, f), "r") as file:
                    return float(file.read().strip())
            except: pass
    return None

# --- 核心计算：带历史切片 ---
def calculate_factors(target_date, df_p_all, df_val_all, df_bond_all):
    try:
        # 【核心修复】只看目标日期之前的数据
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()

        if len(df_p) < 30: return [50.0]*6

        def p_score(series, cur, inv=False):
            p = stats.percentileofscore(series.dropna(), cur)
            return float(100 - p if inv else p)

        # 维度计算
        v = df_p['close'].pct_change().rolling(20).std()
        f1 = p_score(v, v.iloc[-1], inv=True)
        v20 = df_p['volume'].rolling(20).mean()
        f2 = p_score(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])
        h250 = df_p['close'].rolling(250).max()
        f3 = p_score(df_p['close']/h250, (df_p['close']/h250).iloc[-1])
        f4 = 50.0 # 升贴水模拟
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        f5 = p_score(erp, erp.iloc[-1], inv=True)
        f6 = 50.0 # 杠杆模拟

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except: return [50.0]*6

def main():
    log("=== 启动具备历史切片能力的分析系统 ===")
    today = datetime.now().date()
    
    # 1. 鲁棒性初始化 CSV
    cols = ["date","f1","f2","f3","f4","f5","f6","predict","actual","bias"]
    if os.path.exists(LOG_FILE):
        try:
            df_log = pd.read_csv(LOG_FILE)
            if df_log.empty: df_log = pd.DataFrame(columns=cols)
        except: df_log = pd.DataFrame(columns=cols)
    else:
        df_log = pd.DataFrame(columns=cols)

    # 2. 获取基础数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 3. 历史补全 (过去10个工作日)
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        act = get_actual_val(d)
        d_str = d.strftime("%Y-%m-%d")
        
        if act:
            fs = calculate_factors(d, df_p, df_val, df_bond)
            p_val = sum(fs)/6
            df_log = df_log[df_log['date'] != d_str] # 覆盖
            df_log.loc[len(df_log)] = [d_str] + fs + [round(p_val, 2), act, round(act-p_val, 2)]

    # 4. 权重自动对齐 (每7天)
    weights = np.array([1/6]*6)
    df_fit = df_log.dropna(subset=['actual']).tail(7)
    if len(df_fit) >= 7:
        X, y = df_fit[['f1','f2','f3','f4','f5','f6']].values, df_fit['actual'].values
        res = minimize(lambda w: np.sum((X@w - y)**2), weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 5. 今日预测
    tf = calculate_factors(today, df_p, df_val, df_bond)
    tp = sum(f*w for f, w in zip(tf, weights))
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + tf + [round(tp, 2), np.nan, np.nan]
    
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)
    
    # 6. 推送
    w_info = " | ".join([f"{n}:{w:.0%}" for n, w in zip(["波动","量能","强度","期货","避险","杠杆"], weights)])
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 预测 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日预测：{tp:.2f}**\n\n📊 **权重对齐状态：**\n{w_info}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"因子原始分: {' / '.join(map(str, tf))}"}]}
            ]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    main()