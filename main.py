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
    """判断是否为工作日（跳过周六日）"""
    return date.weekday() < 5

def get_actual_val(date_obj):
    """从文件夹读取实际值"""
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
    """持久化记录，自动过滤 nan"""
    if np.isnan(raw) or np.isnan(bias) or np.isnan(final):
        log(f"⚠️ 拒绝记录异常数据: {date_str}")
        return

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("date,raw_score,bias,final_prediction\n")
    
    df = pd.read_csv(LOG_FILE)
    if str(date_str) not in df['date'].values.astype(str):
        with open(LOG_FILE, "a") as f:
            f.write(f"{date_str},{raw:.2f},{bias:.2f},{final:.2f}\n")
        log(f"✅ 历史存档成功: {date_str}")

def send_feishu(title, text, color="blue"):
    if not FEISHU_WEBHOOK:
        log("未检测到 Webhook，跳过推送")
        return
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": text}}]
        }
    }
    try:
        res = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
        log(f"飞书推送状态: {res.status_code}")
    except Exception as e:
        log(f"推送失败: {e}")

def calculate_score(target_date, df_p, df_val, df_bond):
    """核心因子模型：股价强度 + 成交量能 + 股债性价比"""
    try:
        # 截取数据
        df_curr = df_p[df_p['date'] <= target_date].dropna(subset=['close', 'volume']).copy()
        if df_curr.empty: return None
        
        # 1. 股价强度 (250日分位)
        h250 = df_curr['close'].rolling(250, min_periods=30).max()
        s_score = stats.percentileofscore((df_curr['close']/h250).dropna(), (df_curr['close']/h250).iloc[-1])
        
        # 2. 成交量能 (20日均量比)
        v20 = df_curr['volume'].rolling(20, min_periods=5).mean()
        v_score = stats.percentileofscore((df_curr['volume']/v20).dropna(), (df_curr['volume']/v20).iloc[-1])
        
        # 3. 股债性价比 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        df_val[pe_col] = pd.to_numeric(df_val[pe_col], errors='coerce')
        df_bond['中国国债收益率10年'] = pd.to_numeric(df_bond['中国国债收益率10年'], errors='coerce')
        
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key').dropna()
        merged = merged[merged['date_key'] <= target_date]
        
        if not merged.empty:
            merged['erp'] = (1 / merged[pe_col]) - (merged['中国国债收益率10年'] / 100)
            e_score = 100 - stats.percentileofscore(merged['erp'], merged['erp'].iloc[-1])
        else:
            e_score = 50

        # 加权求和，防御性处理 nan
        raw = (np.nan_to_num(s_score) * 0.4 + 
               np.nan_to_num(v_score) * 0.3 + 
               np.nan_to_num(e_score) * 0.3)
        
        return round(float(raw), 2)
    except Exception as e:
        log(f"因子计算报错: {e}")
        return None

def main():
    log("=== 启动 KTZS 智能预测系统 ===")
    today = datetime.now().date()
    if not is_workday(today):
        log("休市日，程序退出")
        return

    # 1. 加载数据
    log("正在同步 AkShare 市场数据...")
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 2. 补算与缺失报警 (回溯5个工作日)
    last_bias = 0.0
    for i in range(5, 0, -1):
        check_day = today - timedelta(days=i)
        if not is_workday(check_day): continue
        
        actual = get_actual_val(check_day)
        raw = calculate_score(check_day, df_p, df_val, df_bond)
        
        if actual is not None and raw is not None:
            bias = actual - raw
            save_to_history(check_day.strftime("%Y-%m-%d"), raw, bias, actual)
            last_bias = float(bias)
        elif i == 1 or (today.weekday() == 0 and (today - check_day).days <= 3):
            # 只有上一工作日缺失才报警
            if actual is None:
                send_feishu("⚠️ 数据缺失补录提醒", f"缺失日期: **{check_day}**\n请尽快在 `KTZS/` 补上传文件。", "orange")

    # 3. 预测逻辑：从日志获取最新的有效偏差
    if os.path.exists(LOG_FILE):
        try:
            df_h = pd.read_csv(LOG_FILE).dropna(subset=['bias'])
            # 过滤掉存为字符串的 "nan"
            df_h = df_h[df_h['bias'].apply(lambda x: str(x).lower() != 'nan')]
            if not df_h.empty:
                last_bias = float(df_h['bias'].iloc[-1])
        except: pass

    # 4. 执行今日预测
    today_raw = calculate_score(today, df_p, df_val, df_bond)
    if today_raw is not None and not np.isnan(today_raw):
        final_prediction = round(today_raw + last_bias, 2)
        log(f"预测完成: {final_prediction}")
        
        send_feishu(f"📊 恐贪指数预测 ({today})", 
                    f"**今日推测值：{final_prediction}**\n"
                    f"模型原生：{today_raw:.2f}\n"
                    f"偏差修正：{last_bias:+.2f}\n\n"
                    f"*注：系统已自动识别并跳过周末数据干扰。*", "blue")
    else:
        log("今日模型计算失败，可能数据源未更新")

if __name__ == "__main__":
    main()