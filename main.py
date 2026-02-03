import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import minimize
import requests
import os
import time
from datetime import datetime, timedelta

# --- 基础配置 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"
LOG_FILE = "HISTORY_LOG.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def is_workday(d_obj):
    return d_obj.weekday() < 5

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

# --- 数据获取模块 (增强稳健性) ---
def fetch_data_with_retry(func, **kwargs):
    for _ in range(3):
        try:
            df = func(**kwargs)
            if df is not None and not df.empty:
                return df
        except:
            time.sleep(1)
    return pd.DataFrame()

def main():
    log("=== 启动 AI 实盘预测系统 (修复版) ===")
    
    # 0. 清理旧数据，确保重新计算
    if os.path.exists(LOG_FILE):
        try: os.remove(LOG_FILE)
        except: pass

    today = datetime.now().date()
    
    # --- 1. 获取全维度真实数据 (带异常拦截) ---
    
    # [f1, f2, f3] 现货数据
    log("1/4 获取沪深300现货数据...")
    try:
        df_p = fetch_data_with_retry(ak.stock_zh_index_daily, symbol="sh000300")
        df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    except Exception as e:
        log(f"⚠️ 现货数据获取失败: {e}")
        df_p = pd.DataFrame()

    # [f4] 期货数据
    log("2/4 获取股指期货(IF)数据...")
    try:
        df_fut = fetch_data_with_retry(ak.futures_zh_daily_sina, symbol="IF0")
        if not df_fut.empty:
            df_fut['date'] = pd.to_datetime(df_fut['date']).dt.date
    except:
        df_fut = pd.DataFrame()

    # [f5] 估值与国债
    log("3/4 获取估值与国债数据...")
    try:
        df_val = fetch_data_with_retry(ak.stock_zh_index_value_csindex, symbol="000300")
        df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
        df_bond = fetch_data_with_retry(ak.bond_zh_us_rate)
        df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date
    except:
        df_val, df_bond = pd.DataFrame(), pd.DataFrame()

    # [f6] 融资融券 (修复点：改用上交所汇总接口)
    log("4/4 获取融资融券数据 (SSE)...")
    try:
        # 使用 ak.stock_margin_sse_summary 替代不稳定的 exchange 接口
        df_margin = fetch_data_with_retry(ak.stock_margin_sse_summary)
        if not df_margin.empty:
            # 统一列名格式
            df_margin.rename(columns={"信用交易日期": "date_key", "融资余额": "rzye"}, inplace=True)
            df_margin['date_key'] = pd.to_datetime(df_margin['date_key']).dt.date
    except Exception as e:
        log(f"⚠️ 融资数据获取失败: {e}")
        df_margin = pd.DataFrame()

    # --- 2. 定义因子计算引擎 ---
    def calculate_factors(target_date, _df_p, _df_fut, _df_val, _df_bond, _df_margin):
        try:
            # 基础数据切片
            if _df_p.empty: return [50.0]*6
            cut_p = _df_p[_df_p['date'] <= target_date].copy()
            if len(cut_p) < 30: return [50.0] * 6

            def get_score(series, current_val, invert=False):
                s = pd.to_numeric(series, errors='coerce').dropna()
                if len(s) < 10 or pd.isna(current_val): return 50.0
                # 取最近3年数据作为分位参考
                s_window = s.tail(750) 
                p = stats.percentileofscore(s_window, current_val, kind='weak')
                return float(100 - p if invert else p)

            # [f1] 波动率
            vol = cut_p['close'].pct_change().rolling(20).std()
            f1 = get_score(vol, vol.iloc[-1], invert=True)

            # [f2] 成交量
            vol_ratio = cut_p['volume'] / cut_p['volume'].rolling(20).mean()
            f2 = get_score(vol_ratio, vol_ratio.iloc[-1], invert=False)

            # [f3] 价格强度
            high_250 = cut_p['close'].rolling(250).max()
            f3 = get_score(cut_p['close'] / high_250, (cut_p['close'] / high_250).iloc[-1], invert=False)

            # [f4] 升贴水
            f4 = 50.0
            if not _df_fut.empty:
                cut_f = _df_fut[_df_fut['date'] <= target_date].copy()
                merged = pd.merge(cut_p[['date','close']], cut_f[['date','close']], on='date', suffixes=('_spot', '_fut'))
                if not merged.empty:
                    basis = (merged['close_fut'] - merged['close_spot']) / merged['close_spot']
                    f4 = get_score(basis, basis.iloc[-1], invert=False)

            # [f5] 股债性价比 ERP
            f5 = 50.0
            if not _df_val.empty and not _df_bond.empty:
                pe_col = next((c for c in _df_val.columns if '市盈率' in c and 'TTM' in c), None)
                if not pe_col: pe_col = next((c for c in _df_val.columns if '市盈率' in c), None)
                rate_col = next((c for c in _df_bond.columns if '10年' in c), None)
                
                if pe_col and rate_col:
                    cut_v = _df_val[_df_val['date_key'] <= target_date].set_index('date_key')[[pe_col]]
                    cut_b = _df_bond[_df_bond['date_key'] <= target_date].set_index('date_key')[[rate_col]]
                    erp_df = cut_v.join(cut_b).dropna()
                    if not erp_df.empty:
                        erp = (1 / pd.to_numeric(erp_df[pe_col])) - (pd.to_numeric(erp_df[rate_col]) / 100)
                        f5 = get_score(erp, erp.iloc[-1], invert=False)

            # [f6] 杠杆资金 (使用上交所数据)
            f6 = 50.0
            if not _df_margin.empty:
                cut_m = _df_margin[_df_margin['date_key'] <= target_date].copy()
                if not cut_m.empty and 'rzye' in cut_m.columns:
                    m_val = pd.to_numeric(cut_m['rzye'], errors='coerce')
                    f6 = get_score(m_val, m_val.iloc[-1], invert=False)

            return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
        except Exception as e:
            log(f"因子计算出错: {e}")
            return [50.0] * 6

    # --- 3. 重建历史 ---
    log("开始计算历史与预测...")
    cols = ["date", "f1", "f2", "f3", "f4", "f5", "f6", "predict", "actual", "bias"]
    df_log = pd.DataFrame(columns=cols)

    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        
        fs = calculate_factors(d, df_p, df_fut, df_val, df_bond, df_margin)
        p_raw = round(sum(fs) / 6, 2)
        act = get_actual_val(d)
        bias = round(act - p_raw, 2) if act is not None else np.nan
        df_log.loc[len(df_log)] = [d.strftime("%Y-%m-%d")] + fs + [p_raw, act, bias]

    # --- 4. 权重优化 ---
    weights = np.array([1/6] * 6)
    df_fit = df_log.dropna(subset=['actual']).tail(10)
    if len(df_fit) >= 5:
        X = df_fit[['f1', 'f2', 'f3', 'f4', 'f5', 'f6']].values
        y = df_fit['actual'].values
        res = minimize(lambda w: np.sum((X @ w - y)**2), weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # --- 5. 今日结果 ---
    today_fs = calculate_factors(today, df_p, df_fut, df_val, df_bond, df_margin)
    today_raw = round(sum(f * w for f, w in zip(today_fs, weights)), 2)
    
    bias_fix = 0.0
    if not df_fit.empty:
        last_biases = df_fit['bias'].ewm(alpha=0.5).mean()
        bias_fix = last_biases.iloc[-1]
        if np.isnan(bias_fix): bias_fix = 0.0
    
    final_predict = round(today_raw + bias_fix, 2)

    # 保存
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + today_fs + [today_raw, np.nan, np.nan]
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)
    log(f"✅ 计算完成，今日预测: {final_predict}")

    # --- 6. 飞书推送 ---
    w_info = " | ".join([f"{n}:{w:.0%}" for n, w in zip(["波动","量能","强度","期现","股债","杠杆"], weights)])
    color_template = "red" if final_predict > 80 else ("green" if final_predict < 20 else "purple")
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 实盘预测 ({today})"}, "template": color_template},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日建议值：{final_predict}**\n原生分：{today_raw} | 修正：{bias_fix:+.1f}\n\n📊 **因子详情 (AkShare实时)**:\n🌊 波动: {today_fs[0]} | 🔋 量能: {today_fs[1]}\n💪 强度: {today_fs[2]} | ⚖️ 期现: {today_fs[3]}\n🛡️ 股债: {today_fs[4]} | 🎰 杠杆: {today_fs[5]}\n\n🧠 **AI 权重配置**:\n{w_info}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "关键词: 恐贪"}]}
            ]
        }
    }
    if FEISHU_WEBHOOK:
        try: requests.post(FEISHU_WEBHOOK, json=payload)
        except: pass

if __name__ == "__main__":
    main()