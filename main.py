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

def normalize_date(d_val):
    if not d_val or pd.isna(d_val): return None
    s = str(d_val).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(s, fmt).date()
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

# --- 数据获取模块 (增加重试机制) ---
def fetch_data_with_retry(func, **kwargs):
    for _ in range(3):
        try:
            return func(**kwargs)
        except:
            time.sleep(2)
    return pd.DataFrame()

def main():
    log("=== 启动全量真实数据 AI 系统 (去伪求真版) ===")
    
    # 0. 强力清理旧数据
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
        log("已删除旧表格，准备重新计算")

    today = datetime.now().date()
    
    # --- 1. 获取全维度真实数据 ---
    log("1/5 正在获取 沪深300 现货数据...")
    df_p = fetch_data_with_retry(ak.stock_zh_index_daily, symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    
    log("2/5 正在获取 股指期货(IF) 数据 (计算升贴水)...")
    # IF0 代表沪深300主力连续合约
    df_fut = fetch_data_with_retry(ak.futures_zh_daily_sina, symbol="IF0")
    if not df_fut.empty:
        df_fut['date'] = pd.to_datetime(df_fut['date']).dt.date
    else:
        log("⚠️ 警告: 期货数据获取失败，f4 将受影响")

    log("3/5 正在获取 估值与国债数据 (计算ERP)...")
    df_val = fetch_data_with_retry(ak.stock_zh_index_value_csindex, symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    
    df_bond = fetch_data_with_retry(ak.bond_zh_us_rate)
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    log("4/5 正在获取 融资融券数据 (计算杠杆)...")
    # 交易所融资数据通常延迟一天
    df_margin = fetch_data_with_retry(ak.stock_margin_account_exchange)
    if not df_margin.empty:
        df_margin['date_key'] = pd.to_datetime(df_margin['日期']).dt.date

    # --- 2. 定义因子计算引擎 ---
    def calculate_factors(target_date, _df_p, _df_fut, _df_val, _df_bond, _df_margin):
        try:
            # 数据切片：绝不使用未来的数据
            cut_p = _df_p[_df_p['date'] <= target_date].copy()
            
            if len(cut_p) < 30: return [50.0] * 6 # 数据不足

            # 通用分位数计算函数
            def get_score(series, current_val, invert=False):
                # 清洗数据，确保是数字
                s = pd.to_numeric(series, errors='coerce').dropna()
                if len(s) < 10 or pd.isna(current_val): return 50.0
                
                # 计算当前值在历史(过去3年/750天)中的百分位
                # 使用近3年窗口更符合市场近况
                s_window = s.tail(750) 
                p = stats.percentileofscore(s_window, current_val, kind='weak')
                return float(100 - p if invert else p)

            # [f1] 波动率 (20日) - 反向
            vol = cut_p['close'].pct_change().rolling(20).std()
            f1 = get_score(vol, vol.iloc[-1], invert=True)

            # [f2] 成交量 (20日均比) - 正向
            vol_ratio = cut_p['volume'] / cut_p['volume'].rolling(20).mean()
            f2 = get_score(vol_ratio, vol_ratio.iloc[-1], invert=False)

            # [f3] 价格强度 (250日高点比) - 正向
            high_250 = cut_p['close'].rolling(250).max()
            strength = cut_p['close'] / high_250
            f3 = get_score(strength, strength.iloc[-1], invert=False)

            # [f4] 升贴水率 (期货 - 现货) / 现货 - 正向
            # 逻辑：升水(期货>现货)代表看多，贴水代表看空
            f4 = 50.0
            if not _df_fut.empty:
                cut_f = _df_fut[_df_fut['date'] <= target_date].copy()
                # 合并现货和期货
                merged = pd.merge(cut_p[['date','close']], cut_f[['date','close']], on='date', suffixes=('_spot', '_fut'))
                if not merged.empty:
                    merged['basis_rate'] = (merged['close_fut'] - merged['close_spot']) / merged['close_spot']
                    f4 = get_score(merged['basis_rate'], merged['basis_rate'].iloc[-1], invert=False)

            # [f5] 股债性价比 ERP (1/PE - 国债) - 正向
            # 逻辑：ERP越高，股票越有吸引力，应该是贪婪(高分)
            f5 = 50.0
            # 智能列名匹配
            pe_col = next((c for c in _df_val.columns if '市盈率' in c and 'TTM' in c), None) # 优先找TTM
            if not pe_col: pe_col = next((c for c in _df_val.columns if '市盈率' in c), None)
            
            rate_col = next((c for c in _df_bond.columns if '10年' in c), None)

            if pe_col and rate_col:
                # 必须对齐日期
                cut_v = _df_val[_df_val['date_key'] <= target_date].set_index('date_key')[[pe_col]]
                cut_b = _df_bond[_df_bond['date_key'] <= target_date].set_index('date_key')[[rate_col]]
                
                # 合并
                erp_df = cut_v.join(cut_b).dropna()
                if not erp_df.empty:
                    erp_series = (1 / pd.to_numeric(erp_df[pe_col])) - (pd.to_numeric(erp_df[rate_col]) / 100)
                    # ERP 越高越值得买(贪婪)，所以不反向
                    f5 = get_score(erp_series, erp_series.iloc[-1], invert=False)

            # [f6] 杠杆资金 (融资余额) - 正向
            # 逻辑：融资余额高代表散户狂热
            f6 = 50.0
            if not _df_margin.empty:
                cut_m = _df_margin[_df_margin['date_key'] <= target_date].copy()
                if not cut_m.empty:
                    # 融资余额列名通常是 "融资余额" 或 "rzye"
                    margin_col = next((c for c in cut_m.columns if '融资余额' in c), None)
                    if margin_col:
                        m_val = pd.to_numeric(cut_m[margin_col], errors='coerce')
                        # 杠杆数据通常滞后，取最近的一个有效值
                        f6 = get_score(m_val, m_val.iloc[-1], invert=False)

            return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
        except Exception as e:
            log(f"日期 {target_date} 计算出错: {e}")
            return [50.0] * 6

    # --- 3. 重建历史与预测 ---
    log("5/5 开始回溯计算历史 (最近14天)...")
    cols = ["date", "f1", "f2", "f3", "f4", "f5", "f6", "predict", "actual", "bias"]
    df_log = pd.DataFrame(columns=cols)

    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        
        # 尝试计算每一天
        # 只要是工作日都算，为了画图好看，不仅仅是有实际值才算
        fs = calculate_factors(d, df_p, df_fut, df_val, df_bond, df_margin)
        p_raw = round(sum(fs) / 6, 2)
        
        act = get_actual_val(d)
        bias = round(act - p_raw, 2) if act is not None else np.nan
        
        df_log.loc[len(df_log)] = [d.strftime("%Y-%m-%d")] + fs + [p_raw, act, bias]

    # --- 4. 动态权重优化 ---
    weights = np.array([1/6] * 6)
    df_fit = df_log.dropna(subset=['actual']).tail(10) # 取最近10个有效数据
    if len(df_fit) >= 5:
        X = df_fit[['f1', 'f2', 'f3', 'f4', 'f5', 'f6']].values
        y = df_fit['actual'].values
        # 约束：权重和为1，单项权重在 5% 到 40% 之间
        res = minimize(lambda w: np.sum((X @ w - y)**2), weights, 
                       bounds=[(0.05, 0.4)]*6, 
                       constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # --- 5. 今日最终计算 ---
    today_fs = calculate_factors(today, df_p, df_fut, df_val, df_bond, df_margin)
    today_raw = round(sum(f * w for f, w in zip(today_fs, weights)), 2)
    
    # 误差修正
    bias_fix = 0.0
    if not df_fit.empty:
        # 使用指数移动平均来平滑误差
        last_biases = df_fit['bias'].ewm(alpha=0.5).mean()
        bias_fix = last_biases.iloc[-1]
        if np.isnan(bias_fix): bias_fix = 0.0

    final_predict = round(today_raw + bias_fix, 2)

    # 存入
    t_str = today.strftime("%Y-%m-%d")
    # 如果今天已经算过（比如重跑），先删掉旧的
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + today_fs + [today_raw, np.nan, np.nan]
    
    # 保存
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)
    log(f"计算完成。今日预测: {final_predict} (权重优化后)")

    # --- 6. 飞书推送 ---
    w_info = " | ".join([f"{n}:{w:.0%}" for n, w in zip(["波动","量能","强度","期现","股债","杠杆"], weights)])
    # 构造颜色：贪婪(>80)红，恐惧(<20)绿
    color_template = "red" if final_predict > 80 else ("green" if final_predict < 20 else "purple")
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 实盘预测 ({today})"}, 
                "template": color_template
            },
            "elements": [
                {
                    "tag": "div", 
                    "text": {
                        "tag": "lark_md", 
                        "content": f"**今日建议值：{final_predict}**\n"
                                   f"原生分：{today_raw} | 修正：{bias_fix:+.1f}\n"
                                   f"----------------\n"
                                   f"📊 **真实数据因子：**\n"
                                   f"🌊 波动: {today_fs[0]} | 🔋 量能: {today_fs[1]}\n"
                                   f"💪 强度: {today_fs[2]} | ⚖️ 期现: {today_fs[3]}\n"
                                   f"🛡️ 股债: {today_fs[4]} | 🎰 杠杆: {today_fs[5]}\n"
                                   f"----------------\n"
                                   f"🧠 **AI 权重进化：**\n{w_info}"
                    }
                },
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "注：数据源已切换为 AkShare 实时接口 | 关键词: 恐贪"}]}
            ]
        }
    }
    
    if FEISHU_WEBHOOK:
        try:
            r = requests.post(FEISHU_WEBHOOK, json=payload)
            log(f"推送状态: {r.status_code}")
        except Exception as e:
            log(f"推送失败: {e}")

if __name__ == "__main__":
    main()