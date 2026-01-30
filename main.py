import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import minimize
import requests
import os
from datetime import datetime, timedelta

# --- 基础配置 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"
LOG_FILE = "HISTORY_LOG.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def is_workday(d_obj):
    return d_obj.weekday() < 5

def normalize_date(d_val):
    """强制清洗日期格式为 YYYY-MM-DD"""
    if not d_val or pd.isna(d_val): return None
    s = str(d_val).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except: continue
    return None

def get_actual_val(date_obj):
    """从 KTZS 文件夹匹配补录的真实值"""
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
    """带严格时间切片的六维度模型"""
    try:
        # 只保留目标日期及之前的数据
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()
        
        if len(df_p) < 30: return [50.0] * 6

        def get_p(series, cur, inv=False):
            series = pd.to_numeric(series, errors='coerce').dropna()
            if series.empty or np.isnan(cur): return 50.0
            p = stats.percentileofscore(series, cur, kind='weak')
            return float(100 - p if inv else p)

        # f1: 指数波动 (20日)
        vol = df_p['close'].pct_change().rolling(20).std()
        f1 = get_p(vol, vol.iloc[-1], inv=True)
        
        # f2: 总成交量 (20日比)
        v20 = df_p['volume'].rolling(20).mean()
        f2 = get_p(df_p['volume'] / v20, (df_p['volume'] / v20).iloc[-1])
        
        # f3: 股价强度 (250日高点位置)
        h250 = df_p['close'].rolling(250).max()
        f3 = get_p(df_p['close'] / h250, (df_p['close'] / h250).iloc[-1])
        
        # f4: 升贴水率 (基差模拟)
        f4 = 50.0 
        
        # f5: 避险天堂 (ERP 股债性价比) - 深度兼容性修复
        pe_val = None
        for col in ['市盈率1', '市盈率TTM', '市盈率']:
            if col in df_val.columns:
                pe_val = pd.to_numeric(df_val[col], errors='coerce')
                break
        
        bond_rate = None
        for col in ['中国国债收益率10年', 'rate', '收益率']:
            if col in df_bond.columns:
                bond_rate = pd.to_numeric(df_bond[col], errors='coerce') / 100
                break

        if pe_val is not None and bond_rate is not None:
            erp_series = (1 / pe_val) - bond_rate
            f5 = get_p(erp_series, erp_series.iloc[-1], inv=True)
        else:
            f5 = 50.0 # 兜底逻辑

        # f6: 杠杆水平
        f6 = 50.0 

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except Exception as e:
        log(f"因子计算失败: {e}")
        return [50.0] * 6

def main():
    log("=== 启动具备自动进化能力的恐贪 AI 系统 ===")
    today = datetime.now().date()
    
    # 1. 抓取全量市场数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 初始化 CSV 记录
    cols = ["date", "f1", "f2", "f3", "f4", "f5", "f6", "predict", "actual", "bias"]
    if os.path.exists(LOG_FILE):
        try:
            df_log = pd.read_csv(LOG_FILE)
            if not df_log.empty:
                df_log['date'] = df_log['date'].apply(lambda x: normalize_date(x).strftime("%Y-%m-%d") if normalize_date(x) else x)
        except: df_log = pd.DataFrame(columns=cols)
    else:
        df_log = pd.DataFrame(columns=cols)

    # 3. 补全历史与误差计算 (回溯最近 14 天)
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        act = get_actual_val(d)
        if act is not None:
            d_str = d.strftime("%Y-%m-%d")
            fs = calculate_factors(d, df_p, df_val, df_bond)
            p_raw = round(sum(fs) / 6, 2)
            df_log = df_log[df_log['date'] != d_str]
            df_log.loc[len(df_log)] = [d_str] + fs + [p_raw, act, round(act - p_raw, 2)]

    # 4. 动态权重优化 (最小二乘法)
    weights = np.array([1/6] * 6)
    df_fit = df_log.dropna(subset=['actual']).tail(7)
    if len(df_fit) >= 5:
        X = df_fit[['f1', 'f2', 'f3', 'f4', 'f5', 'f6']].values
        y = df_fit['actual'].values
        res = minimize(lambda w: np.sum((X @ w - y)**2), weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 5. 今日预测及误差对齐
    today_factors = calculate_factors(today, df_p, df_val, df_bond)
    today_raw = round(sum(f * w for f, w in zip(today_factors, weights)), 2)
    
    bias_fix = 0.0
    if not df_fit.empty:
        last_b = df_fit.iloc[-1]['bias']
        if not np.isnan(last_b): bias_fix = last_b
    
    final_predict = round(today_raw + bias_fix, 2)

    # 记录今日数据
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + today_factors + [today_raw, np.nan, np.nan]
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)

    # 6. 飞书卡片推送
    w_detail = " | ".join([f"{n}:{w:.0%}" for n, w in zip(["波动","量能","强度","期货","避险","杠杆"], weights)])
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 指数预测报告 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日建议值：{final_predict}**\n原生分：{today_raw} | 修正值：{bias_fix:+.1f}\n\n📊 **AI 权重进化详情：**\n{w_detail}"}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"维度得分: {' / '.join(map(str, today_factors))} | 搜索词: 恐贪"}]}
            ]
        }
    }
    
    if FEISHU_WEBHOOK:
        r = requests.post(FEISHU_WEBHOOK, json=payload)
        log(f"飞书推送结果: {r.status_code}, {r.text}")

if __name__ == "__main__":
    main()