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

def get_last_workday(target_date):
    """获取目标日期的前一个工作日（跳过周末）"""
    dt = target_date - timedelta(days=1)
    while dt.weekday() >= 5:  # 5是周六，6是周日
        dt -= timedelta(days=1)
    return dt

def get_actual_val(date_str):
    path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return float(f.read().strip())
        except: return None
    return None

def save_to_history(date_str, raw, bias, final):
    """记录历史，确保不重复"""
    new_line = f"{date_str},{raw:.2f},{bias:.2f},{final:.2f}\n"
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("date,raw_score,bias,final_prediction\n")
    
    df = pd.read_csv(LOG_FILE)
    if str(date_str) not in df['date'].values.astype(str):
        with open(LOG_FILE, "a") as f:
            f.write(new_line)
        log(f"✅ 历史存证: {date_str} 数据已写入 CSV")

def calculate_logic(target_date, df_p, df_val, df_bond):
    """单日因子模型核心算法"""
    try:
        df_curr = df_p[df_p['date'] <= target_date].copy()
        if df_curr.empty: return None
        
        # 1. 股价强度 (250日分位)
        h250 = df_curr['close'].rolling(250).max()
        s_score = stats.percentileofscore(df_curr['close']/h250, (df_curr['close']/h250).iloc[-1], kind='weak')
        
        # 2. 成交量能 (20日均量比)
        v20 = df_curr['volume'].rolling(20).mean()
        v_score = stats.percentileofscore(df_curr['volume']/v20, (df_curr['volume']/v20).iloc[-1], kind='weak')
        
        # 3. 股债性价比 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key')
        merged = merged[merged['date_key'] <= target_date]
        merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
        # ERP越高越恐惧，所以得分 = 100 - 百分位
        e_score = 100 - stats.percentileofscore(merged['erp'], merged['erp'].iloc[-1], kind='weak')

        raw = (s_score * 0.4) + (v_score * 0.3) + (e_score * 0.3)
        return round(raw, 2)
    except Exception as e:
        log(f"日期 {target_date} 计算失败: {e}")
        return None

def main():
    today = datetime.now().date()
    # 如果今天是周末，程序不运行
    if today.weekday() >= 5:
        log("今日为周末，休市不运行。")
        return

    # 1. 预加载数据
    log("获取全量金融数据...")
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 补算逻辑：检查过去7天
    log("启动历史补算自检...")
    for i in range(7, 0, -1):
        target_day = today - timedelta(days=i)
        if target_day.weekday() >= 5: continue # 跳过周末
        
        t_str = target_day.strftime("%Y%m%d")
        actual = get_actual_val(t_str)
        if actual:
            raw = calculate_logic(target_day, df_p, df_val, df_bond)
            if raw: save_to_history(t_str, raw, actual - raw, actual)
        elif i == 1 or (today.weekday() == 0 and i == 3): # 昨天缺失 或 周一运行且上周五缺失
            # 飞书报警逻辑
            requests.post(FEISHU_WEBHOOK, json={
                "msg_type": "text", "content": {"text": f"⚠️ 缺失对标数据: {t_str}.txt，请及时补录。"}
            })

    # 3. 今日预测
    last_workday = get_last_workday(today)
    yest_actual = get_actual_val(last_workday.strftime("%Y%m%d"))
    
    today_raw = calculate_logic(today, df_p, df_val, df_bond)
    
    if today_raw:
        # 寻找最近的一个偏差值
        if os.path.exists(LOG_FILE):
            df_h = pd.read_csv(LOG_FILE)
            last_bias = df_h['bias'].iloc[-1] if not df_h.empty else 0
        else: last_bias = 0
        
        final_prediction = round(today_raw + last_bias, 2)
        log(f"今日预测: {final_prediction} (基于最近偏差 {last_bias:+.2f})")
        
        # 发送飞书
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {"title": {"tag": "plain_text", "content": f"📊 恐贪指数预测 ({today})"}, "template": "blue"},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日预测：{final_prediction}**\n原生：{today_raw} | 修正：{last_bias:+.2f}"}},
                    {"tag": "note", "elements": [{"tag": "plain_text", "content": f"已自动跳过周末，对标前一工作日：{last_workday}"}]}
                ]
            }
        }
        requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    main()