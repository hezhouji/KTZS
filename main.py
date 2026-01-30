import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import requests
import os
from datetime import datetime, timedelta

# 飞书配置
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")
DATA_DIR = "KTZS"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def get_actual_val(date_str):
    path = os.path.join(DATA_DIR, f"{date_str}.txt")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                val = float(f.read().strip())
                log(f"成功读取昨日实际值: {val}")
                return val
        except Exception as e:
            log(f"读取文件内容失败: {e}")
    else:
        log(f"未找到昨日校准文件: {path}")
    return None

def get_p_score(series, current_val, reverse=False):
    series = series.dropna()
    if series.empty or np.isnan(current_val): return 50
    p = stats.percentileofscore(series, current_val, kind='weak')
    return 100 - p if reverse else p

def analyze_factors(target_date, df_p, df_val, df_bond):
    log(f"正在分析日期 {target_date} 的多维度因子...")
    try:
        df_curr = df_p[df_p['date'] <= target_date].copy()
        if df_curr.empty: return None

        # 1. 动能 (250日位置)
        h250 = df_curr['close'].rolling(250).max()
        s_score = get_p_score(df_curr['close']/h250, (df_curr['close']/h250).iloc[-1])
        
        # 2. 量能 (20日均量比)
        v20 = df_curr['volume'].rolling(20).mean()
        v_score = get_p_score(df_curr['volume']/v20, (df_curr['volume']/v20).iloc[-1])
        
        # 3. 股债 (ERP)
        pe_col = '市盈率1' if '市盈率1' in df_val.columns else '市盈率TTM'
        # 简单匹配当日ERP
        merged = pd.merge(df_val[['date_key', pe_col]], df_bond[['date_key', '中国国债收益率10年']], on='date_key')
        merged = merged[merged['date_key'] <= target_date]
        if not merged.empty:
            merged['erp'] = (1 / merged[pe_col].astype(float)) - (merged['中国国债收益率10年'].astype(float) / 100)
            e_score = get_p_score(merged['erp'], merged['erp'].iloc[-1], reverse=True)
        else: e_score = 50

        # 4. 情绪乖离
        bias = (df_curr['close'] - df_curr['close'].rolling(20).mean()) / df_curr['close'].rolling(20).mean()
        b_score = get_p_score(bias, bias.iloc[-1])

        raw = (s_score * 0.4) + (v_score * 0.3) + (e_score * 0.15) + (b_score * 0.15)
        return {"score": raw, "s": s_score, "v": v_score, "e": e_score, "b": b_score}
    except Exception as e:
        log(f"因子计算异常: {e}")
        return None

def send_feishu(content):
    if not FEISHU_WEBHOOK:
        log("错误: 未配置飞书 Webhook 环境变量")
        return
    
    log("正在发送飞书通知...")
    # 注意：标题必须包含你在飞书机器人后台设置的“关键词”
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"📊 恐贪指数预测同步 ({content['date']})"}, "template": "orange"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**今日推测数值：{content['final']}**\n公式：模型({content['raw']}) + 修正({content['bias']})"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**归因逻辑：**\n{content['reason']}"}},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "注：该预测已根据昨日韭圈儿实际误差自动校准。"}]}
            ]
        }
    }
    
    try:
        res = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
        log(f"飞书返回状态码: {res.status_code}")
        log(f"飞书返回内容: {res.text}")
        if res.status_code != 200:
            log("提示: 请检查飞书机器人安全设置中的'关键词'是否包含【恐贪】或【指数】")
    except Exception as e:
        log(f"网络请求失败: {e}")

def main():
    log("=== 启动自动化分析流程 ===")
    today = datetime.now().date()
    yest = today - timedelta(days=1)
    
    log(f"今日日期: {today}, 目标对标日期: {yest}")

    # 1. 抓取数据
    log("开始拉取 akshare 数据...")
    df_p = ak.stock_zh_index_daily(symbol="sh000300")
    df_p['date'] = pd.to_datetime(df_p['date']).dt.date
    df_val = ak.stock_zh_index_value_csindex(symbol="000300")
    df_val['date_key'] = pd.to_datetime(df_val['日期']).dt.date
    df_bond = ak.bond_zh_us_rate()
    df_bond['date_key'] = pd.to_datetime(df_bond['日期']).dt.date
    log("数据拉取完毕")

    # 2. 计算昨日模型与获取实际值
    yest_model = analyze_factors(yest, df_p, df_val, df_bond)
    yest_actual = get_actual_val(yest.strftime("%Y%m%d"))
    
    # 3. 计算偏差
    bias = 0
    reason = "继承昨日误差惯性"
    if yest_model and yest_actual:
        bias = yest_actual - yest_model['score']
        log(f"计算得出偏差: {bias:+.2f}")
    else:
        log("警告: 缺少昨日对比数据，修正值为0")

    # 4. 计算今日预测
    today_model = analyze_factors(today, df_p, df_val, df_bond)
    if today_model:
        # 简单环境修正
        vol_change = (df_p['volume'].iloc[-1] / df_p['volume'].iloc[-2]) - 1
        if vol_change > 0.2: 
            bias *= 1.1
            reason = "今日放量显著，强化亢奋偏置"
        
        final_val = round(max(0, min(100, today_model['score'] + bias)), 2)
        log(f"今日最终推测结果: {final_val}")
        
        # 5. 发送
        send_data = {
            "date": today.strftime("%Y-%m-%d"),
            "final": final_val,
            "raw": round(today_model['score'], 2),
            "bias": round(bias, 2),
            "reason": reason
        }
        send_feishu(send_data)
    
    log("=== 流程结束 ===")

if __name__ == "__main__":
    main()