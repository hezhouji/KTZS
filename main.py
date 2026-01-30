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

def is_workday(d_obj):
    return d_obj.weekday() < 5

def normalize_date(d_input):
    """强制统一日期格式为 YYYY-MM-DD"""
    if not d_input: return None
    s = str(d_input).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except: continue
    return None

def get_actual_val(date_obj):
    """从 KTZS 文件夹匹配补录的韭圈实际值"""
    target = date_obj.strftime("%Y%m%d")
    if not os.path.exists(DATA_DIR): return None
    for f in os.listdir(DATA_DIR):
        if target in f and f.endswith(".txt"):
            try:
                with open(os.path.join(DATA_DIR, f), "r") as file:
                    return float(file.read().strip())
            except: pass
    return None

# --- 核心算法：带历史切片的六维度模型 ---
def calculate_factors(target_date, df_p_all, df_val_all, df_bond_all):
    try:
        # 严格过滤：只保留目标日期及之前的数据，模拟历史当天的真实视角
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()
        
        if len(df_p) < 30: return [50.0] * 6

        def get_p(series, cur, inv=False):
            p = stats.percentileofscore(series.dropna(), cur, kind='weak')
            return float(100 - p if inv else p)

        # 1. 指数波动 (20日)
        vol = df_p['close'].pct_change().rolling(20).std()
        f1 = get_p(vol, vol.iloc[-1], inv=True)
        
        # 2. 总成交量 (20日比)
        v20 = df_p['volume'].rolling(20).mean()
        f2 = get_p(df_p['volume'] / v20, (df_p['volume'] / v20).iloc[-1])
        
        # 3. 股价强度 (250日高点位置)
        h250 = df_p['close'].rolling(250).max()
        f3 = get_p(df_p['close'] / h250, (df_p['close'] / h250).iloc[-1])
        
        # 4. 升贴水率 (模拟基差分)
        f4 = 50.0 
        
        # 5. 避险天堂 (ERP 股债性价比)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp = (1 / df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float) / 100)
        f5 = get_p(erp, erp.iloc[-1], inv=True)
        
        # 6. 杠杆水平 (融资强度模拟)
        f6 = 50.0 

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except Exception as e:
        log(f"因子计算失败: {e}")
        return [50.0] * 6

def main():
    log("=== 启动 AI 自适应预测系统 ===")
    today = datetime.now().date()
    if not is_workday(today):
        log("非交易日，跳过计算。")
        return

    # 1. 数据源初始化 (仅获取一次全量)
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 加载或初始化历史记录
    cols = ["date", "f1", "f2", "f3", "f4", "f5", "f6", "predict", "actual", "bias"]
    if os.path.exists(LOG_FILE):
        try:
            df_log = pd.read_csv(LOG_FILE)
            # 标准化已存在的日期
            df_log['date'] = df_log['date'].apply(lambda x: normalize_date(x).strftime("%Y-%m-%d") if normalize_date(x) else x)
        except: df_log = pd.DataFrame(columns=cols)
    else:
        df_log = pd.DataFrame(columns=cols)

    # 3. 历史补全 (回溯最近 14 天)
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        act = get_actual_val(d)
        if act is not None:
            d_str = d.strftime("%Y-%m-%d")
            fs = calculate_factors(d, df_p, df_val, df_bond)
            p_avg = round(sum(fs) / 6, 2)
            # 覆盖更新
            df_log = df_log[df_log['date'] != d_str]
            df_log.loc[len(df_log)] = [d_str] + fs + [p_avg, act, round(act - p_avg, 2)]

    # 4. 权重进化 (基于过去 7 条记录最小二乘法对齐)
    weights = np.array([1/6] * 6)
    df_fit = df_log.dropna(subset=['actual']).tail(7)
    if len(df_fit) >= 7:
        X = df_fit[['f1', 'f2', 'f3', 'f4', 'f5', 'f6']].values
        y = df_fit['actual'].values
        def objective(w): return np.sum((X @ w - y)**2)
        res = minimize(objective, weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 5. 今日预测
    today_factors = calculate_factors(today, df_p, df_val, df_bond)
    today_predict = round(sum(f * w for f, w in zip(today_factors, weights)), 2)
    
    # 修正值计算 (取最近一个工作日的偏差进行对齐)
    final_display = today_predict
    bias_info = "无（待对齐）"
    if not df_fit.empty:
        last_bias = df_fit.iloc[-1]['bias']
        final_display = round(today_predict + last_bias, 2)
        bias_info = f"{'+' if last_bias>=0 else ''}{last_bias}"

    # 保存今日数据（actual 留空等明天补录）
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + today_factors + [today_predict, np.nan, np.nan]
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)

    # 6. 飞书卡片推送
    w_names = ["波动", "量能", "强度", "期货", "避险", "杠杆"]
    w_detail = " | ".join([f"{n}:{w:.0%}" for n, w in zip(w_names, weights)])
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪指数 AI 预测报告 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日建议值：{final_display}**\n原生得分：{today_predict} (修正:{bias_info})\n\n📊 **AI 权重对齐：**\n{w_detail}"}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"维度分: {' / '.join(map(str, today_factors))} | 关键词: 恐贪"}]}
            ]
        }
    }
    
    if FEISHU_WEBHOOK:
        res = requests.post(FEISHU_WEBHOOK, json=payload)
        log(f"推送状态: {res.status_code}, 返回: {res.text}")

if __name__ == "__main__":
    main()