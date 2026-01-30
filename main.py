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
LOG_FILE = "HISTORY_LOG.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def is_workday(date):
    """简单判断是否为工作日（跳过周六日）"""
    return date.weekday() < 5

def get_target_dates(today):
    """
    逻辑：如果是周一，我们需要前一天的数据（上周五）
    如果是周二到周五，我们需要前一天（周一到周四）
    """
    yest = today - timedelta(days=1)
    while not is_workday(yest):
        yest -= timedelta(days=1)
    return yest

def get_actual_val(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return float(f.read().strip())
        except: return None
    return None

def save_to_history(date_str, raw, bias, final):
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("date,raw_score,bias,final_prediction\n")
    
    df = pd.read_csv(LOG_FILE)
    if str(date_str) not in df['date'].values.astype(str):
        with open(LOG_FILE, "a") as f:
            f.write(f"{date_str},{raw:.2f},{bias:.2f},{final:.2f}\n")
        log(f"✅ 已存证 {date_str}")

def send_feishu(title, text, color="blue"):
    if not FEISHU_WEBHOOK: return
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": text}}]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

def calculate_score(target_date, df_p, df_val, df_bond):
    """核心计算模型 - 增强容错版"""
    try:
        # 截取数据并过滤空值
        df_curr = df_p[df_p['date'] <= target_date].dropna(subset=['close', 'volume']).copy()
        if df_curr.empty: return None
        
        # 1. 动能 (处理可能的空值)
        h250 = df_curr['close'].rolling(250, min_periods=1).max()
        curr_ratio = df_curr['close'].iloc[-1] / h250.iloc[-1]
        s_score = stats.percentileofscore((df_curr['close']/h250).dropna(), curr_ratio)
        
        # 2. 量能
        v20 = df_curr['volume'].rolling(20, min_periods=1).mean()
        curr_v_ratio = df_curr['volume'].iloc[-1] / v20.iloc[-1]
        v_score = stats.percentileofscore((df_curr['volume']/v20).dropna(), curr_v_ratio)
        
        # 3. 股债 (ERP) - 容易出 nan 的重灾区
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        # 强制转换类型并处理空值
        df_val[pe_col] = pd.to_numeric(df_val[pe_col], errors='coerce')
        df_bond['中国国债收益率10年'] = pd.to_numeric(df_bond['中国国债收益率10年'], errors='coerce')
        
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key').dropna()
        merged = merged[merged['date_key'] <= target_date]
        
        if not merged.empty:
            merged['erp'] = (1 / merged[pe_col]) - (merged['中国国债收益率10年'] / 100)
            e_score = 100 - stats.percentileofscore(merged['erp'], merged['erp'].iloc[-1])
        else:
            log("ERP数据匹配为空，使用默认分50")
            e_score = 50

        # 加权计算，并使用 np.nan_to_num 兜底
        raw = (np.nan_to_num(s_score) * 0.4 + 
               np.nan_to_num(v_score) * 0.3 + 
               np.nan_to_num(e_score) * 0.3)
        
        return round(raw, 2)
    except Exception as e:
        log(f"因子计算崩裂: {e}")
        return None
def main():
    log("=== 启动具备周末感知能力的分析流程 ===")
    today = datetime.now().date()
    if not is_workday(today):
        log("今日非交易日，跳过。")
        return

    # 1. 拉取数据
    log("同步市场数据...")
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 补算与报警逻辑
    # 检查过去5个工作日是否有待补录数据
    last_bias = 0
    for i in range(5, 0, -1):
        check_day = today - timedelta(days=i)
        if not is_workday(check_day): continue
        
        actual = get_actual_val(check_day)
        raw = calculate_score(check_day, df_p, df_val, df_bond)
        
        if actual and raw:
            bias = actual - raw
            save_to_history(check_day.strftime("%Y-%m-%d"), raw, bias, actual)
            last_bias = bias # 记录最近一次的有效偏差
        elif i == 1 or (today.weekday() == 0 and (today - check_day).days <= 3):
            # 如果是“上一工作日”缺失，发飞书通知
            if not actual:
                send_feishu("⚠️ 恐贪指数补录提醒", f"缺失日期: **{check_day}**\n请在 `KTZS/` 文件夹补上传该日数值文件。", "orange")

    # 3. 今日预测
    today_raw = calculate_score(today, df_p, df_val, df_bond)
    if today_raw:
        # 如果历史记录里有最近的偏差，直接使用
        if os.path.exists(LOG_FILE):
            df_h = pd.read_csv(LOG_FILE)
            if not df_h.empty: last_bias = df_h['bias'].iloc[-1]
            
        final = round(today_raw + last_bias, 2)
        send_feishu(f"📊 恐贪指数预测 ({today})", 
                    f"**预测值：{final}**\n原生：{today_raw} | 修正：{last_bias:+.2f}\n\n*注：已自动对齐上一工作日误差。*", "blue")

if __name__ == "__main__":
    main()