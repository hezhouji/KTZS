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

def get_actual_val(date_obj):
    path = os.path.join(DATA_DIR, f"{date_obj.strftime('%Y%m%d')}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return float(f.read().strip())
        except: return None
    return None

def get_p_score(series, current_val, reverse=False):
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50.0
    p = stats.percentileofscore(series, current_val, kind='weak')
    return float(100 - p if reverse else p)

# --- 核心计算函数 ---
def calculate_six_factors(target_date, df_p, df_val, df_bond):
    """计算六大维度原始分"""
    try:
        # 1. 指数波动 (20日波动率) - 反向指标
        vol = df_p['close'].pct_change().rolling(20).std()
        score_vol = get_p_score(vol, vol.iloc[-1], reverse=True)

        # 2. 总成交量
        v20 = df_p['volume'].rolling(20).mean()
        score_v = get_p_score(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])

        # 3. 股价强度 (相对于250日高点)
        h250 = df_p['close'].rolling(250).max()
        score_strength = get_p_score(df_p['close']/h250, (df_p['close']/h250).iloc[-1])

        # 4. 升贴水率 (基差率)
        try:
            df_basis = ak.stock_js_index_ts(symbol="IF0") 
            score_basis = get_p_score(df_basis['basis_rate'], df_basis['basis_rate'].iloc[-1])
        except: score_basis = 50.0

        # 5. 避险天堂 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        erp_series = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        score_safe = get_p_score(erp_series, erp_series.iloc[-1], reverse=True)

        # 6. 杠杆水平 (融资买入占比)
        try:
            df_margin = ak.stock_margin_zh_info() # 获取全市场融资数据
            m_ratio = df_margin['融资买入额'].pct_change()
            score_margin = get_p_score(m_ratio, m_ratio.iloc[-1])
        except: score_margin = 50.0

        return [score_vol, score_v, score_strength, score_basis, score_safe, score_margin]
    except Exception as e:
        log(f"因子计算异常: {e}")
        return [50.0]*6

# --- 动态权重优化逻辑 ---
def optimize_weights(df_history):
    """基于过去7条有效记录，通过最小二乘法优化权重"""
    if len(df_history) < 7:
        return np.array([1/6]*6) # 样本不足时均分
    
    recent = df_history.tail(7)
    X = recent[['f1','f2','f3','f4','f5','f6']].values
    y = recent['actual'].values

    # 目标函数：预测值与实际值的误差平方和最小
    def objective(w):
        return np.sum((X @ w - y)**2)

    cons = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1}) # 权重和为1
    bounds = [(0.05, 0.4)] * 6 # 每个维度权重在 5%-40% 之间
    
    res = minimize(objective, [1/6]*6, method='SLSQP', bounds=bounds, constraints=cons)
    return res.x if res.success else np.array([1/6]*6)

def main():
    log("=== 启动 AI 动态权重自适应系统 ===")
    today = datetime.now().date()
    if not is_workday(today): return

    # 1. 加载数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 补全历史与持久化
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("date,f1,f2,f3,f4,f5,f6,predict,actual,bias\n")

    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        act = get_actual_val(d)
        if act:
            df_h = pd.read_csv(LOG_FILE)
            if d.strftime("%Y-%m-%d") not in df_h['date'].values:
                f_scores = calculate_six_factors(d, df_p, df_val, df_bond)
                # 补录时暂用均分权重记录 predict
                p_val = sum(f_scores)/6
                with open(LOG_FILE, "a") as f:
                    line = f"{d.strftime('%Y-%m-%d')}," + ",".join([f"{x:.2f}" for x in f_scores]) + f",{p_val:.2f},{act:.2f},{act-p_val:.2f}\n"
                    f.write(line)

    # 3. 动态计算今日权重
    df_history = pd.read_csv(LOG_FILE).dropna(subset=['actual'])
    current_weights = optimize_weights(df_history)
    
    # 4. 执行今日预测
    today_factors = calculate_six_factors(today, df_p, df_val, df_bond)
    today_predict = sum(f * w for f, w in zip(today_factors, current_weights))
    
    # 写入今日初步记录 (actual 留空，待明天补录)
    with open(LOG_FILE, "a") as f:
        line = f"{today.strftime('%Y-%m-%d')}," + ",".join([f"{x:.2f}" for x in today_factors]) + f",{today_predict:.2f},,\n"
        f.write(line)

    # 5. 每 7 天分析报告
    report_msg = ""
    if len(df_history) % 7 == 0 and len(df_history) > 0:
        names = ["指数波动", "总成交量", "股价强度", "升贴水率", "避险天堂", "杠杆水平"]
        weight_str = "\n".join([f"- {n}: {w:.1%}" for n, w in zip(names, current_weights)])
        avg_bias = df_history['bias'].tail(7).abs().mean()
        report_msg = f"\n\n📊 **本周权重贡献总结**：\n{weight_str}\n\n*本周平均误差：{avg_bias:.2f}*"

    # 6. 飞书推送
    send_content = (f"**今日最终预测：{today_predict:.2f}**\n"
                    f"模型已根据过去7日偏差自动微调权重。{report_msg}")
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪指数 AI 预测 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": send_content}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "维度分: " + "/".join([f"{x:.0f}" for x in today_factors])}]}
            ]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    main()