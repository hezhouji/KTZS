import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime, timedelta

# 环境变量
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

# 配置文件夹路径
DATA_DIR = "KTZS"

def get_p_score(series, current_val, reverse=False):
    """计算百分位，处理空值"""
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50
    # kind='weak' 对应 <= current_val 的比例，更符合常规百分位理解
    p = stats.percentileofscore(series, current_val, kind='weak')
    return 100 - p if reverse else p

def get_file_content(date_str):
    """尝试读取 KTZS/YYYYMMDD.txt"""
    file_path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                val = float(f.read().strip())
            return val
        except:
            return None
    return None

def calculate_raw_model(df_p, df_val, df_bond, target_date):
    """
    计算指定日期的原生模型分数
    target_date: datetime.date 对象
    """
    try:
        # 截取直到 target_date 的数据 (防止用到未来的数据)
        # 注意：这里假设数据是按时间排序的
        df_p_curr = df_p[df_p['date'] <= target_date].copy()
        
        if df_p_curr.empty: return None

        # 1. 股价强度 (Strength)
        high_250 = df_p_curr['close'].rolling(250).max()
        curr_close = df_p_curr['close'].iloc[-1]
        strength_val = curr_close / high_250.iloc[-1]
        # 计算历史序列用于百分位
        s_series = df_p_curr['close'] / high_250
        s_score = get_p_score(s_series, strength_val)

        # 2. 成交活跃 (Volume)
        vol_ma20 = df_p_curr['volume'].rolling(20).mean()
        curr_vol = df_p_curr['volume'].iloc[-1]
        vol_ratio = curr_vol / vol_ma20.iloc[-1]
        v_series = df_p_curr['volume'] / vol_ma20
        v_score = get_p_score(v_series, vol_ratio)

        # 3. 情绪乖离 (Bias)
        bias_20 = (df_p_curr['close'] - df_p_curr['close'].rolling(20).mean()) / df_p_curr['close'].rolling(20).mean()
        curr_bias = bias_20.iloc[-1]
        b_score = get_p_score(bias_20, curr_bias)

        # 4. 避险天堂 (ERP) - 需匹配日期
        # 找到 target_date 或之前最近的一天
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        
        # 筛选数据
        df_val_curr = df_val[df_val['date_key'] <= target_date]
        df_bond_curr = df_bond[df_bond['date_key'] <= target_date]
        
        if df_val_curr.empty or df_bond_curr.empty:
            e_score = 50 # 默认中性
            erp_val = 0
        else:
            # 取最后一行
            pe_val = float(df_val_curr.iloc[-1][pe_col])
            bond_val = float(df_bond_curr.iloc[-1]['中国国债收益率10年'])
            
            # 计算当天的 ERP
            erp_val = (1 / pe_val) - (bond_val / 100)
            
            # 这里的历史百分位计算比较耗时，简化处理：
            # 如果需要非常精确的历史百分位，需要 merge 所有历史。
            # 为保证速度，这里暂时用简单的 0.03-0.05 区间估算，或者复用 merge 逻辑
            # 为了准确，我们还是做一次 merge
            merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key')
            merged = merged[merged['date_key'] <= target_date]
            merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
            e_score = get_p_score(merged['erp'], erp_val, reverse=True)

        # --- 原生模型权重 ---
        # 强度40% + 成交30% + 乖离15% + 避险15%
        raw_score = (s_score * 0.4) + (v_score * 0.3) + (b_score * 0.15) + (e_score * 0.15)
        
        return {
            "score": raw_score, 
            "details": {"s": s_score, "v": v_score, "b": b_score, "e": e_score},
            "raw_vals": {"s": strength_val, "v": vol_ratio, "e": erp_val}
        }

    except Exception as e:
        print(f"计算日期 {target_date} 出错: {e}")
        return None

def main_logic():
    print(">>> 启动滚动偏差修正预测模型...")
    
    # 1. 确定日期
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y%m%d")
    
    print(f"今日: {today}, 需回溯日期: {yesterday} (文件: {yesterday_str}.txt)")

    # 2. 获取数据源 (一次性获取，避免重复请求)
    print("正在拉取全量数据...")
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['close'] = df_p['close'].astype(float)
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_bond = ak.bond_zh_us_rate()
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date

    # 3. 关键步骤：计算昨天的模型值 (Backtest)
    yesterday_model = calculate_raw_model(df_p, df_val, df_bond, yesterday)
    
    # 4. 获取昨天的实际值 (Ground Truth)
    yesterday_actual = get_file_content(yesterday_str)
    
    bias = 0
    bias_msg = "⚠️ 无昨日数据，无法修正"
    
    if yesterday_model and yesterday_actual:
        # 计算偏差：偏差 = 实际值 - 模型值
        # 如果昨天实际是 83，模型算出来 65，偏差就是 +18
        bias = yesterday_actual - yesterday_model['score']
        bias_msg = f"✅ 昨日实际 {yesterday_actual} vs 模型 {yesterday_model['score']:.2f} -> 偏差修正 {bias:+.2f}"
    elif not yesterday_actual:
        bias_msg = f"❌ 未找到文件 KTZS/{yesterday_str}.txt"

    # 5. 计算今天的模型值 (Forecast)
    today_model = calculate_raw_model(df_p, df_val, df_bond, today)
    
    if not today_model:
        print("今日数据尚未更新或计算失败")
        return

    # 6. 应用修正 (Apply Bias)
    # 今天的预测值 = 今天的模型值 + 昨天的偏差
    final_prediction = today_model['score'] + bias
    
    # 边界处理 (0-100)
    final_prediction = max(0, min(100, final_prediction))

    return {
        "date": today.strftime("%Y-%m-%d"),
        "final": round(final_prediction, 2),
        "bias": round(bias, 2),
        "bias_msg": bias_msg,
        "raw_today": round(today_model['score'], 2),
        "details": today_model['details'],
        "vals": today_model['raw_vals']
    }

def send_feishu(res):
    if not res: return
    color = "red" if res['final'] > 60 else "blue"
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🔮 韭圈儿指数预测 ({res['date']})"}, "template": color},
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**今日推测值：{res['final']}**\n(模型 {res['raw_today']} + 修正 {res['bias']})"
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**📊 修正逻辑：**\n{res['bias_msg']}"
                    }
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**今日因子详情：**\n"
                                   f"- 🚀 强度：{int(res['details']['s'])} (位置:{res['vals']['s']*100:.1f}%)\n"
                                   f"- 💰 成交：{int(res['details']['v'])} (放量:{res['vals']['v']:.2f}x)\n"
                                   f"- 🛡️ 避险：{int(res['details']['e'])} (利差:{res['vals']['e']*100:.2f}%)\n"
                                   f"- 📈 乖离：{int(res['details']['b'])}"
                    }
                },
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "注：推测值基于昨日实际误差进行滚动修正，只要市场风格不突变，准确率将极高。"}]}
            ]
        }
    }
    requests.post(FEISHU_WEBHOOK, json=payload)

if __name__ == "__main__":
    result = main_logic()
    send_feishu(result)