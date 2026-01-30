import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import minimize
import requests
import os
from datetime import datetime, timedelta

# --- 配置区 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"
LOG_FILE = "HISTORY_LOG.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def is_workday(date):
    return date.weekday() < 5

def normalize_date(d_input):
    """统一日期处理：将字符串或对象一律转为 datetime.date"""
    if isinstance(d_input, str):
        # 兼容多种文件名格式
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y年%m月%d日"):
            try: return datetime.strptime(d_input.replace(".txt", ""), fmt).date()
            except: continue
    elif isinstance(d_input, datetime):
        return d_input.date()
    return d_input

def get_actual_val(date_obj):
    """精准匹配文件名"""
    target_str = date_obj.strftime("%Y%m%d")
    for f in os.listdir(DATA_DIR):
        if target_str in f and f.endswith(".txt"):
            try:
                with open(os.path.join(DATA_DIR, f), "r") as file:
                    return float(file.read().strip())
            except: return None
    return None

def get_p_score(series, current_val, reverse=False):
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50.0
    # 强制分位计算基于历史背景
    p = stats.percentileofscore(series, current_val, kind='weak')
    return float(100 - p if reverse else p)

# --- 核心修复：带时间切片的因子计算 ---
def calculate_six_factors(target_date, df_p_all, df_val_all, df_bond_all):
    """确保只计算 target_date 之前的数据"""
    try:
        t_date = normalize_date(target_date)
        # 严格切片：只保留目标日期及之前的数据
        df_p = df_p_all[df_p_all['date'] <= t_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= t_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= t_date].copy()
        
        if df_p.empty: return [50.0]*6

        # 1. 波动 (20日)
        vols = df_p['close'].pct_change().rolling(20).std()
        score_vol = get_p_score(vols, vols.iloc[-1], reverse=True)

        # 2. 成交量 (20日比)
        v20 = df_p['volume'].rolling(20).mean()
        score_v = get_p_score(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])

        # 3. 强度 (250日)
        h250 = df_p['close'].rolling(250).max()
        score_strength = get_p_score(df_p['close']/h250, (df_p['close']/h250).iloc[-1])

        # 4. 升贴水 (基差) - 模拟历史切片
        try:
            df_basis = ak.stock_js_index_ts(symbol="IF0")
            df_basis['date'] = pd.to_datetime(df_basis['date']).dt.date
            df_b_cut = df_basis[df_basis['date'] <= t_date]
            score_basis = get_p_score(df_b_cut['basis_rate'], df_b_cut['basis_rate'].iloc[-1])
        except: score_basis = 50.0

        # 5. 避险 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp_series = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        score_safe = get_p_score(erp_series, erp_series.iloc[-1], reverse=True)

        # 6. 杠杆
        score_margin = 50.0 # 保持默认

        return [score_vol, score_v, score_strength, score_basis, score_safe, score_margin]
    except Exception as e:
        log(f"因子计算异常: {e}")
        return [50.0]*6

def main():
    log("=== 启动数据标准化与动态微调系统 ===")
    today = datetime.now().date()
    
    # 获取全量数据（只获取一次）
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 1. 历史数据标准化写回
    if os.path.exists(LOG_FILE):
        df_log = pd.read_csv(LOG_FILE)
        # 强制日期格式统一为 YYYY-MM-DD
        df_log['date'] = df_log['date'].apply(lambda x: normalize_date(x).strftime("%Y-%m-%d"))
        # 去重，保留最后一次计算
        df_log = df_log.drop_duplicates(subset=['date'], keep='last')
        df_log.to_csv(LOG_FILE, index=False)
    else:
        with open(LOG_FILE, "w") as f:
            f.write("date,f1,f2,f3,f4,f5,f6,predict,actual,bias\n")

    # 2. 补全与重算逻辑 (检查过去14天)
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        
        act = get_actual_val(d)
        if act:
            d_str = d.strftime("%Y-%m-%d")
            df_current = pd.read_csv(LOG_FILE)
            # 只有当 actual 缺失或者日期不存在时才重算
            if d_str not in df_current['date'].values or pd.isna(df_current.loc[df_current['date']==d_str, 'actual'].values[0]):
                f_scores = calculate_six_factors(d, df_p, df_val, df_bond)
                p_val = sum(f_scores)/6
                # 构造新行
                new_row = [d_str] + [round(x, 2) for x in f_scores] + [round(p_val, 2), act, round(act-p_val, 2)]
                df_current = df_current[df_current['date'] != d_str] # 删旧
                df_current.loc[len(df_current)] = new_row # 添新
                df_current.sort_values('date').to_csv(LOG_FILE, index=False)
                log(f"已更新历史日期: {d_str}")

    # 3. 今日预测 (基于最新权重)
    # ... (此处接之前的优化权重与预测逻辑，确保日期标准化)