import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
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
    date_str = date_obj.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                content = f.read().strip()
                return float(content) if content else None
        except: return None
    return None

def save_to_history(date_str, raw, bias, final):
    """持久化记录，自动去重并按日期排序存入"""
    if np.isnan(raw) or np.isnan(bias) or np.isnan(final):
        return

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("date,raw_score,bias,final_prediction\n")
    
    df = pd.read_csv(LOG_FILE)
    if str(date_str) not in df['date'].values.astype(str):
        with open(LOG_FILE, "a") as f:
            f.write(f"{date_str},{raw:.2f},{bias:.2f},{final:.2f}\n")
        log(f"✅ 历史存证: {date_str}")

def send_feishu(title, text, color="blue"):
    if not FEISHU_WEBHOOK: return
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": text}}]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)

def calculate_score(target_date, df_p, df_val, df_bond):
    try:
        df_curr = df_p[df_p['date'] <= target_date].dropna(subset=['close', 'volume']).copy()
        if df_curr.empty: return None
        
        h250 = df_curr['close'].rolling(250, min_periods=30).max()
        s_score = stats.percentileofscore((df_curr['close']/h250).dropna(), (df_curr['close']/h250).iloc[-1])
        
        v20 = df_curr['volume'].rolling(20, min_periods=5).mean()
        v_score = stats.percentileofscore((df_curr['volume']/v20).dropna(), (df_curr['volume']/v20).iloc[-1])
        
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        df_val[pe_col] = pd.to_numeric(df_val[pe_col], errors='coerce')
        df_bond['中国国债收益率10年'] = pd.to_numeric(df_bond['中国国债收益率10年'], errors='coerce')
        
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key').dropna()
        merged = merged[merged['date_key'] <= target_date]
        
        if not merged.empty:
            merged['erp'] = (1 / merged[pe_col]) - (merged['中国国债收益率10年'] / 100)
            e_score = 100 - stats.percentileofscore(merged['erp'], merged['erp'].iloc[-1])
        else: e_score = 50

        raw = (np.nan_to_num(s_score) * 0.4 + np.nan_to_num(v_score) * 0.3 + np.nan_to_num(e_score) * 0.3)
        return round(float(raw), 2)
    except: return None

def main():
    log("=== 启动 KTZS 智能自校准系统 ===")
    today = datetime.now().date()
    if not is_workday(today): return

    # 1. 加载数据
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 补算逻辑 (回溯过去10天，补全缺失的历史记录)
    log("开始检查历史数据完整性...")
    for i in range(10, 0, -1):
        check_day = today - timedelta(days=i)
        if not is_workday(check_day): continue
        
        actual = get_actual_val(check_day)
        if actual is not None:
            raw = calculate_score(check_day, df_p, df_val, df_bond)
            if raw is not None:
                save_to_history(check_day.strftime("%Y-%m-%d"), raw, actual - raw, actual)
    
    # 3. 核心修复：获取离今日最近的有效偏差
    last_bias = 0.0
    if os.path.exists(LOG_FILE):
        try:
            df_h = pd.read_csv(LOG_FILE)
            # 转换日期格式进行排序，确保取到的是物理时间上最接近今天的记录
            df_h['date'] = pd.to_datetime(df_h['date'])
            df_h = df_h.sort_values(by='date').dropna(subset=['bias'])
            # 过滤掉非数字的偏差
            df_h = df_h[df_h['bias'].apply(lambda x: str(x).lower() != 'nan')]
            
            if not df_h.empty:
                # 获取日期最晚的那一行
                latest_record = df_h.iloc[-1]
                last_bias = float(latest_record['bias'])
                log(f"今日修正参考日期: {latest_record['date'].date()}，偏差: {last_bias:+.2f}")
            
            # 检查昨天（或上个工作日）是否缺失实际值并报警
            yest_workday = today - timedelta(days=1)
            while not is_workday(yest_workday): yest_workday -= timedelta(days=1)
            if str(yest_workday) not in df_h['date'].dt.date.values.astype(str):
                send_feishu("⚠️ 恐贪指数缺失提醒", f"缺失上一交易日 ({yest_workday}) 数据，请及时补录。", "orange")
        except Exception as e:
            log(f"读取历史偏差失败: {e}")

    # 4. 执行今日预测
    today_raw = calculate_score(today, df_p, df_val, df_bond)
    if today_raw is not None:
        final_prediction = round(today_raw + last_bias, 2)
        send_feishu(f"📊 恐贪指数预测 ({today})", 
                    f"**今日推测值：{final_prediction}**\n"
                    f"模型原生：{today_raw:.2f}\n"
                    f"偏差修正：{last_bias:+.2f}\n\n"
                    f"*注：已自动对齐最近一个有效交易日的偏差。*", "blue")
    else:
        log("今日模型计算失败")

if __name__ == "__main__":
    main()