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
    """强制统一日期格式"""
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

# --- 因子计算（带严格时间切片） ---
def calculate_factors(target_date, df_p_all, df_val_all, df_bond_all):
    try:
        # 【修复核心】严格过滤：只保留目标日期及之前的数据
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()

        if len(df_p) < 30: return [50.0]*6

        def p_score(series, current, inv=False):
            p = stats.percentileofscore(series.dropna(), current)
            return float(100 - p if inv else p)

        # 1. 波动 (20日)
        v = df_p['close'].pct_change().rolling(20).std()
        f1 = p_score(v, v.iloc[-1], inv=True)
        # 2. 成交量
        v20 = df_p['volume'].rolling(20).mean()
        f2 = p_score(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])
        # 3. 强度
        h250 = df_p['close'].rolling(250).max()
        f3 = p_score(df_p['close']/h250, (df_p['close']/h250).iloc[-1])
        # 4. 升贴水 (模拟)
        f4 = 50.0
        # 5. 避险 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        f5 = p_score(erp, erp.iloc[-1], inv=True)
        # 6. 杠杆
        f6 = 50.0

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except: return [50.0]*6

def main():
    log("=== 启动数据对齐与动态微调系统 ===")
    today = datetime.now().date()
    
    # 1. 初始化 CSV（解决 EmptyDataError）
    cols = ["date","f1","f2","f3","f4","f5","f6","predict","actual","bias"]
    try:
        df_log = pd.read_csv(LOG_FILE)
        if df_log.empty: raise pd.errors.EmptyDataError
    except (pd.errors.EmptyDataError, FileNotFoundError):
        df_log = pd.DataFrame(columns=cols)

    # 2. 获取数据源
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 3. 历史补全 (过去14天)
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        act = get_actual_val(d)
        d_str = d.strftime("%Y-%m-%d")
        
        if act:
            fs = calculate_factors(d, df_p, df_val, df_bond)
            p_val = sum(fs)/6
            df_log = df_log[df_log['date'] != d_str] # 覆盖旧记录
            df_log.loc[len(df_log)] = [d_str] + fs + [round(p_val, 2), act, round(act-p_val, 2)]

    # 4. 权重对齐 (基于最近7天记录)
    weights = np.array([1/6]*6)
    df_fit = df_log.dropna(subset=['actual']).tail(7)
    if len(df_fit) >= 7:
        X, y = df_fit[['f1','f2','f3','f4','f5','f6']].values, df_fit['actual'].values
        res = minimize(lambda w: np.sum((X@w - y)**2), weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 5. 今日预测
    tf = calculate_factors(today, df_p, df_val, df_bond)
    tp = sum(f*w for f, w in zip(tf, weights))
    df_log = df_log[df_log['date'] != today.strftime("%Y-%m-%d")]
    df_log.loc[len(df_log)] = [today.strftime("%Y-%m-%d")] + tf + [round(tp, 2), np.nan, np.nan]
    
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)
    log(f"今日预测成功: {tp:.2f}")

    # 6. 推送
    names = ["波动", "量能", "强度", "期货", "避险", "杠杆"]
    w_str = " | ".join([f"{n}:{w:.0%}" for n, w in zip(names, weights)])
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 预测 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**最终预测值：{tp:.2f}**\n\n📊 **最新权重对齐：**\n{w_str}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"维度分: {' / '.join(map(str, tf))}"}]}
            ]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    main()