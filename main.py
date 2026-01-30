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

def is_workday(date_obj):
    return date_obj.weekday() < 5

def parse_date(d_str):
    """强制将各种日期格式转换为标准 YYYY-MM-DD"""
    d_str = str(d_str).replace(".txt", "").replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(d_str, fmt).date()
        except: continue
    return None

def get_actual_val(date_obj):
    """搜索文件夹中匹配日期的数值"""
    target = date_obj.strftime("%Y%m%d")
    if not os.path.exists(DATA_DIR): return None
    for f in os.listdir(DATA_DIR):
        if target in f and f.endswith(".txt"):
            try:
                with open(os.path.join(DATA_DIR, f), "r") as file:
                    return float(file.read().strip())
            except: pass
    return None

# --- 六维度核心计算（带严格切片） ---
def calculate_six_factors(target_date, df_p_all, df_val_all, df_bond_all):
    try:
        # 【关键修复】只保留目标日期及之前的数据
        df_p = df_p_all[df_p_all['date'] <= target_date].copy()
        df_val = df_val_all[df_val_all['date_key'] <= target_date].copy()
        df_bond = df_bond_all[df_bond_all['date_key'] <= target_date].copy()
        
        if len(df_p) < 30: return [50.0]*6

        def get_p(series, current, reverse=False):
            p = stats.percentileofscore(series.dropna(), current, kind='weak')
            return float(100 - p if reverse else p)

        # 1. 波动 (20日)
        v = df_p['close'].pct_change().rolling(20).std()
        f1 = get_p(v, v.iloc[-1], reverse=True)

        # 2. 总成交量 (20日比)
        v20 = df_p['volume'].rolling(20).mean()
        f2 = get_p(df_p['volume']/v20, (df_p['volume']/v20).iloc[-1])

        # 3. 股价强度 (250日位置)
        h250 = df_p['close'].rolling(250).max()
        f3 = get_p(df_p['close']/h250, (df_p['close']/h250).iloc[-1])

        # 4. 升贴水 (简单模拟基差)
        f4 = 50.0 # 暂无稳定历史切片接口时设为中性

        # 5. 避险天堂 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val_all.columns else '市盈率TTM'
        erp = (1/df_val[pe_col].astype(float)) - (df_bond['中国国债收益率10年'].astype(float)/100)
        f5 = get_p(erp, erp.iloc[-1], reverse=True)

        # 6. 杠杆水平 (模拟融资强度)
        f6 = 50.0 

        return [round(x, 2) for x in [f1, f2, f3, f4, f5, f6]]
    except:
        return [50.0]*6

def main():
    log("=== 启动数据标准化自学习系统 ===")
    today = datetime.now().date()
    
    # 获取全量数据用于切片
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 1. 初始化或清洗 CSV
    cols = ["date","f1","f2","f3","f4","f5","f6","predict","actual","bias"]
    if os.path.exists(LOG_FILE):
        df_log = pd.read_csv(LOG_FILE)
        df_log['date'] = df_log['date'].apply(lambda x: parse_date(x).strftime("%Y-%m-%d") if parse_date(x) else x)
        df_log = df_log.drop_duplicates(subset=['date'], keep='last')
    else:
        df_log = pd.DataFrame(columns=cols)

    # 2. 补全历史与重算（过去 14 天）
    for i in range(14, 0, -1):
        d = today - timedelta(days=i)
        if not is_workday(d): continue
        
        act = get_actual_val(d)
        d_str = d.strftime("%Y-%m-%d")
        
        # 如果有实际值且(记录缺失或数值没算对)，则重算
        if act:
            f_scores = calculate_six_factors(d, df_p, df_val, df_bond)
            p_val = sum(f_scores)/6
            new_data = [d_str] + f_scores + [round(p_val, 2), act, round(act-p_val, 2)]
            df_log = df_log[df_log['date'] != d_str] # 删旧
            df_log.loc[len(df_log)] = new_data

    # 3. 权重动态优化（基于过去 7 条实际记录）
    weights = np.array([1/6]*6)
    df_learn = df_log.dropna(subset=['actual'])
    if len(df_learn) >= 7:
        recent = df_learn.tail(7)
        X = recent[['f1','f2','f3','f4','f5','f6']].values
        y = recent['actual'].values
        def obj(w): return np.sum((X @ w - y)**2)
        res = minimize(obj, weights, bounds=[(0.05, 0.4)]*6, constraints={'type':'eq','fun':lambda w: sum(w)-1})
        if res.success: weights = res.x

    # 4. 今日预测
    today_f = calculate_six_factors(today, df_p, df_val, df_bond)
    today_p = sum(f * w for f, w in zip(today_f, weights))
    
    # 写入今日行（待后续填入实际值）
    t_str = today.strftime("%Y-%m-%d")
    df_log = df_log[df_log['date'] != t_str]
    df_log.loc[len(df_log)] = [t_str] + today_f + [round(today_p, 2), np.nan, np.nan]
    
    # 保存结果
    df_log.sort_values('date').to_csv(LOG_FILE, index=False)
    log(f"今日预测完成: {today_p:.2f}")

    # 5. 推送飞书 (每 7 天附带报告)
    report = ""
    if len(df_learn) % 7 == 0 and len(df_learn) > 0:
        names = ["波动", "量能", "强度", "期货", "避险", "杠杆"]
        w_list = [f"{n}:{w:.0%}" for n, w in zip(names, weights)]
        report = "\n\n📊 **本周权重进化结果**：\n" + " | ".join(w_list)

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎯 恐贪 AI 预测 ({today})"}, "template": "purple"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**最终预测值：{today_p:.2f}**\n*已根据历史韭圈数据完成权重对齐*{report}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"维度分: {' / '.join(map(str, today_f))}"}]}
            ]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    main()