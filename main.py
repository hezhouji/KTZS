import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import requests
import json
import os
from datetime import datetime, timedelta

# --- 配置 ---
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK")

# --- 辅助函数：计算百分位得分 ---
def calculate_percentile_score(current_value, history_series, reverse=False):
    """
    计算当前值在历史数据中的百分位 (0-100)
    reverse=True: 值越大，分数越低 (例如股债利差越大，代表越值得买，恐贪指数应越低/越恐惧)
    reverse=False: 值越大，分数越高 (例如价格越高，越贪婪)
    """
    # 移除空值
    history_series = history_series.dropna()
    
    # 计算百分位 (0.0 - 1.0)
    percentile = stats.percentileofscore(history_series, current_value)
    
    if reverse:
        return 100 - percentile
    else:
        return percentile

def get_label(score):
    if score <= 10: return "🥶 极度恐惧 (钻石底)"
    elif score <= 30: return "😨 恐惧 (黄金坑)"
    elif score <= 60: return "😐 中立/震荡"
    elif score <= 80: return "🤩 贪婪 (风险积聚)"
    else: return "🔥 极度贪婪 (赶顶中)"

# --- A股核心模型：股债利差 (FED Model) ---
def analyze_ashare_fundamental():
    print(">>> 正在计算 A股 (股债利差模型)...")
    try:
        # 1. 获取沪深300市盈率 (PE-TTM) 历史数据 (近10年)
        # akshare 接口: 沪深300指数估值
        df_val = ak.stock_zh_index_value_csindex(symbol="000300")
        df_val['日期'] = pd.to_datetime(df_val['日期'])
        df_val.set_index('日期', inplace=True)
        # 只要最近5-8年的数据，太久远的宏观环境不同，参考意义下降
        start_date = datetime.now() - timedelta(days=365*8) 
        df_val = df_val[df_val.index > start_date]

        # 2. 获取中国10年期国债收益率
        df_bond = ak.bond_zh_us_rate()
        df_bond['日期'] = pd.to_datetime(df_bond['日期'])
        df_bond.set_index('日期', inplace=True)
        df_bond = df_bond['中国国债收益率10年']

        # 3. 数据合并 (按日期对齐)
        # 股债利差 = (1 / PE) - 国债收益率/100
        # 注意：akshare返回的国债收益率单位通常是百分比(如 2.3)，计算时需 /100
        
        merged = pd.DataFrame()
        merged['pe'] = df_val['市盈率TTM']
        merged = merged.join(df_bond, how='inner') # 只保留两边都有数据的日期
        
        if merged.empty:
            print("数据合并为空，接口可能变动")
            return None

        # 计算核心指标：股债利差 (ERP)
        # 1/PE 得到股票收益率。 减去 无风险收益率(国债)。
        merged['earnings_yield'] = 1 / merged['pe']
        merged['bond_yield'] = merged['中国国债收益率10年'] / 100
        merged['fed_spread'] = merged['earnings_yield'] - merged['bond_yield']

        # 4. 计算当前状态
        current_spread = merged['fed_spread'].iloc[-1]
        current_pe = merged['pe'].iloc[-1]
        
        # 5. 核心打分：计算当前利差在历史中的排位
        # 利差越大 -> 股票性价比越高 -> 应该对应“恐惧/低分”
        # 所以 reverse=True
        score = calculate_percentile_score(current_spread, merged['fed_spread'], reverse=True)
        
        return {
            "market": "🇨🇳 A股 (沪深300)",
            "score": int(score),
            "label": get_label(score),
            "detail": f"PE: {current_pe:.2f} | 股债利差: {current_spread*100:.2f}%",
            "note": "基于8年股债利差(FED模型)分位"
        }

    except Exception as e:
        print(f"A股计算出错: {e}")
        return None

# --- 美股模型：VIX + 动量混合 ---
def analyze_us_fundamental():
    print(">>> 正在计算 美股 (VIX + 动量模型)...")
    try:
        # 获取 S&P500 和 VIX
        tickers = yf.Tickers("^GSPC ^VIX")
        
        # 获取5年历史，用于计算分位数
        hist_sp = tickers.tickers["^GSPC"].history(period="5y")
        hist_vix = tickers.tickers["^VIX"].history(period="5y")
        
        if len(hist_sp) < 200: return None

        # 因子1: 乖离率 (Bias) - 价格偏离200日均线的程度
        ma200 = hist_sp['Close'].rolling(window=200).mean()
        bias = (hist_sp['Close'] - ma200) / ma200
        current_bias = bias.iloc[-1]
        # Bias越大，越贪婪 (reverse=False)
        score_bias = calculate_percentile_score(current_bias, bias, reverse=False)

        # 因子2: VIX 恐慌指数
        # VIX 越高，市场越恐慌 (Score 越低)，所以 VIX 越高 -> Score 0
        # 也就是 VIX 越高 -> Reverse=True
        current_vix = hist_vix['Close'].iloc[-1]
        score_vix = calculate_percentile_score(current_vix, hist_vix['Close'], reverse=True)
        
        # 因子3: RSI (动量)
        delta = hist_sp['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        # RSI 越高越贪婪，直接用数值即可 (0-100)
        
        # 综合打分权重: VIX(40%) + Bias(40%) + RSI(20%)
        final_score = (score_vix * 0.4) + (score_bias * 0.4) + (current_rsi * 0.2)
        
        return {
            "market": "🇺🇸 美股 (S&P 500)",
            "score": int(final_score),
            "label": get_label(final_score),
            "detail": f"VIX: {current_vix:.2f} | RSI: {current_rsi:.1f}",
            "note": "基于VIX波动率与均线偏离度"
        }

    except Exception as e:
        print(f"美股计算出错: {e}")
        return None

# --- 发送飞书 ---
def send_feishu(data_list):
    if not FEISHU_WEBHOOK:
        print("无 Webhook，跳过发送")
        return

    # 构造卡片内容
    elements = []
    
    # 顶部状态栏颜色
    header_color = "blue"
    
    for item in data_list:
        # 动态颜色图标
        score = item['score']
        state_icon = "🟢" 
        if score > 80: state_icon = "🔴" # 极度风险
        elif score > 60: state_icon = "🟠"
        elif score < 20: state_icon = "💎" # 钻石底
        elif score < 40: state_icon = "🔵"
        
        # 进度条模拟
        bar_len = 10
        filled = int(score / 10)
        progress_bar = "🟥" * filled + "⬜" * (bar_len - filled)

        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**{item['market']}**\n"
                    f"{state_icon} **{item['score']}** {progress_bar}\n"
                    f"🏷️ 状态：{item['label']}\n"
                    f"📊 数据：{item['detail']}\n"
                    f"🧠 逻辑：<font color='grey'>{item['note']}</font>"
                )
            }
        })
        elements.append({"tag": "hr"})

    card_body = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"📅 市场恐贪指数 ({datetime.now().strftime('%m-%d')})"},
                "template": header_color
            },
            "elements": elements
        }
    }
    
    requests.post(FEISHU_WEBHOOK, json=card_body)
    print("飞书消息已发送")

if __name__ == "__main__":
    results = []
    
    res_cn = analyze_ashare_fundamental()
    if res_cn: results.append(res_cn)
    
    res_us = analyze_us_fundamental()
    if res_us: results.append(res_us)
    
    if results:
        send_feishu(results)
    if not FEISHU_WEBHOOK:
    print("错误：未检测到环境变量 FEISHU_WEBHOOK")
else:
    print(f"检测到 Webhook，长度为: {len(FEISHU_WEBHOOK)}")