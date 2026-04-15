import streamlit as st
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, time as dt_time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. 환경 설정
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 1. 인증 및 초고속 캐싱 데이터 수집 함수
# ----------------------------------------------------
@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data, timeout=5)
    return response.json().get('token') if response.status_code == 200 else None

@st.cache_data(ttl=86400)
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={}, timeout=5)
    data = res.json()
    return {f"{item['name']}({item['code']})": item["code"] for item in data.get("list", [])}

@st.cache_data(ttl=36000)
def get_daily_program_avg_cached(token, stock_code, target_date):
    url = f"{host_url}/api/dostk/mrkcond"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90013", "authorization": f"Bearer {token}"}
    req_data = {"stk_cd": stock_code, "date": target_date, "amt_qty_tp": "2"}
    res = requests.post(url, headers=headers, json=req_data, timeout=5)
    if res.status_code == 200:
        data_list = res.json().get('stk_daly_prm_trde_trnsn', [])
        vols = []
        for item in data_list:
            if item.get('dt', '') < target_date:
                buy = abs(int(str(item.get("prm_buy_qty", "0")).replace("-", "").replace(",", "") or 0))
                sell = abs(int(str(item.get("prm_sell_qty", "0")).replace("-", "").replace(",", "") or 0))
                vols.append(buy + sell)
                if len(vols) == 10: break
        return sum(vols) / len(vols) if vols else 0
    return 0

def get_historical_data_generic(token, api_id, url_path, req_data, max_pages=1):
    url = f"{host_url}/api/dostk/{url_path}"
    all_data = []
    next_key = ""
    for _ in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": api_id, "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        response = requests.post(url, headers=headers, json=req_data, timeout=5)
        if response.status_code != 200: break
        res_json = response.json()
        key = 'stk_min_pole_chart_qry' if api_id == 'ka10080' else ('stk_tm_prm_trde_trnsn' if api_id == 'ka90008' else 'trde_ori_mont_trde_qty')
        chunk = res_json.get(key, [])
        if not chunk: break
        all_data.extend(chunk)
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.05)
    return all_data

# ----------------------------------------------------
# 2. 메인 화면 및 데이터 처리
# ----------------------------------------------------
st.set_page_config(page_title="초고속 수급 레이더 v3.1", layout="wide")
st.title("🦅 초고속 수급 복기 대시보드 (v3.1)")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}

auth_token = get_access_token()

with st.sidebar:
    st.header("📅 설정")
    stock_number = st.text_input("종목코드", value="417200")
    selected_date = st.date_input("날짜", datetime.now())
    target_date_str = selected_date.strftime('%Y%m%d')
    
    if auth_token:
        broker_dict = get_broker_list(auth_token)
        broker_names = sorted(list(broker_dict.keys()))
        selected_broker_name1 = st.selectbox("🔎 창구1", broker_names, index=next((i for i, n in enumerate(broker_names) if "키움" in n), 0))
        target_broker_code1 = broker_dict[selected_broker_name1]
        selected_broker_name2 = st.selectbox("🔎 창구2", broker_names, index=next((i for i, n in enumerate(broker_names) if "신한" in n), 0))
        target_broker_code2 = broker_dict[selected_broker_name2]
        
    lag_seconds = st.slider("⏱️ 시간 보정(초)", 0, 180, 60)
    auto_refresh = st.checkbox("🔄 자동 갱신 (1분)", value=False)
    if st.button("🧹 전체 캐시 삭제"):
        st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}
        st.rerun()

if auto_refresh: st_autorefresh(interval=60000, key="auto_refresh")

if auth_token and len(stock_number) == 6:
    current_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
    if 'last_key' not in st.session_state or st.session_state['last_key'] != current_key:
        st.session_state['last_key'] = current_key
        st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}
        fetch_pages = 500
    else:
        fetch_pages = 3

    with st.spinner("데이터 동기화 중..."):
        new_pg = get_historical_data_generic(auth_token, 'ka90008', 'mrkcond', {"amt_qty_tp": "2", "stk_cd": stock_number, "date": target_date_str}, fetch_pages)
        new_brk1 = get_historical_data_generic(auth_token, 'ka10052', 'stkinfo', {"mmcm_cd": target_broker_code1, "stk_cd": stock_number, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}, fetch_pages)
        new_brk2 = get_historical_data_generic(auth_token, 'ka10052', 'stkinfo', {"mmcm_cd": target_broker_code2, "stk_cd": stock_number, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}, fetch_pages)
        new_chart = get_historical_data_generic(auth_token, 'ka10080', 'chart', {"stk_cd": stock_number, "tic_scope": "1", "upd_stkpc_tp": "1"}, 5)
        
        avg_10d_pg_vol = get_daily_program_avg_cached(auth_token, stock_number, target_date_str)

        def sync(old, new):
            df_m = pd.DataFrame(old + new)
            return df_m.drop_duplicates().to_dict('records') if not df_m.empty else []
        
        st.session_state['data_cache']['pg'] = sync(st.session_state['data_cache']['pg'], new_pg)
        st.session_state['data_cache']['brk1'] = sync(st.session_state['data_cache']['brk1'], new_brk1)
        st.session_state['data_cache']['brk2'] = sync(st.session_state['data_cache']['brk2'], new_brk2)
        st.session_state['data_cache']['chart'] = sync(st.session_state['data_cache']['chart'], new_chart)

    # ----------------------------------------------------
    # 3. 데이터 가공 로직
    # ----------------------------------------------------
    chart_raw = st.session_state['data_cache']['chart']
    if chart_raw:
        df = pd.DataFrame(chart_raw)
        time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
        df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
        df.set_index('Datetime', inplace=True)
        df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

        if not df.empty:
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0).astype(int)

            # PG 데이터 처리
            pg_raw = st.session_state['data_cache']['pg']
            if pg_raw:
                df_pg = pd.DataFrame(pg_raw)
                df_pg['Full_Time'] = pd.to_datetime(target_date_str + df_pg['tm'], format='%Y%m%d%H%M%S')
                df_pg['Datetime'] = df_pg['Full_Time'].dt.floor('min')
                for c in ['prm_buy_qty', 'prm_sell_qty']: df_pg[c] = pd.to_numeric(df_pg[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
                df_pg = df_pg.sort_values('Full_Time')
                df_pg_min = df_pg.groupby('Datetime').agg({'prm_buy_qty': 'last', 'prm_sell_qty': 'last'})
                all_minutes = pd.date_range(start=f"{target_date_str} 0900", end=f"{target_date_str} 1530", freq='min')
                df_pg_min = df_pg_min.reindex(all_minutes).ffill().fillna(0)
                df_pg_min['Buy_1m'] = df_pg_min['prm_buy_qty'].diff().fillna(0).clip(lower=0)
                df_pg_min['Sell_1m'] = df_pg_min['prm_sell_qty'].diff().fillna(0).clip(lower=0)
                df_pg_min['Cum_Net'] = df_pg_min['prm_buy_qty'] - df_pg_min['prm_sell_qty']
                df = df.join(df_pg_min[['Buy_1m', 'Sell_1m', 'Cum_Net']], how='left')
                
                # 💡 신기록 타점 로직
                if avg_10d_pg_vol > 0:
                    df['PG_1m_Total'] = df['Buy_1m'].fillna(0) + df['Sell_1m'].fillna(0)
                    df['PG_Anomaly_Pct'] = (df['PG_1m_Total'] / avg_10d_pg_vol) * 100
                    dots, texts, max_v = [], [], 0.0
                    for v in df['PG_Anomaly_Pct']:
                        if pd.notna(v) and v >= 2.0 and v > max_v:
                            max_v = v; dots.append(v); texts.append(f"{v:.1f}")
                        else:
                            dots.append(pd.NA); texts.append("")
                    df['Anomaly_Dot'], df['Anomaly_Text'] = dots, texts

            # 창구 데이터 처리 함수
            def process_brk(raw, suffix, lag):
                if not raw: return pd.DataFrame()
                df_b = pd.DataFrame(raw)
                t_col = 'tm' if 'tm' in df_b.columns else 'stck_cntg_hour'
                df_b['Datetime'] = (pd.to_datetime(target_date_str + df_b[t_col], format='%Y%m%d%H%M%S') - timedelta(seconds=lag)).dt.floor('min')
                def parse(row):
                    qty = int(str(row['mont_trde_qty']).replace(',', '').replace('-', '').replace('+', ''))
                    return (0, qty) if '-' in str(row['mont_trde_qty']) or '매도' in str(row['tp']) else (qty, 0)
                df_b[['B', 'S']] = df_b.apply(parse, axis=1, result_type='expand')
                net_col = 'acc_netprps' if 'acc_netprps' in df_b.columns else 'net_trde_qty'
                df_b['N'] = pd.to_numeric(df_b[net_col].astype(str).str.replace(r'[^\d\-]', '', regex=True), errors='coerce').fillna(0)
                return df_b.groupby('Datetime').agg({'B': 'sum', 'S': 'sum', 'N': 'last'}).rename(columns={'B':f'Buy_{suffix}', 'S':f'Sell_{suffix}', 'N':f'Cum_{suffix}'})

            df = df.join(process_brk(st.session_state['data_cache']['brk1'], 'brk1', lag_seconds), how='left')
            df = df.join(process_brk(st.session_state['data_cache']['brk2'], 'brk2', lag_seconds), how='left')
            df.fillna(0, inplace=True)

            # ----------------------------------------------------
            # 4. 차트 그리기
            # ----------------------------------------------------
            fig = make_subplots(rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                                row_heights=[0.25, 0.1, 0.15, 0.1, 0.2, 0.2],
                                subplot_titles=("가격", "거래량", "프로그램 수급", "🚨 PG 폭발 (2.0↑ 신기록)", f"{selected_broker_name1}", f"{selected_broker_name2}"),
                                specs=[[{"secondary_y":False}],[{"secondary_y":False}],[{"secondary_y":True}],[{"secondary_y":False}],[{"secondary_y":True}],[{"secondary_y":True}]])

            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color='gray'), row=2, col=1)
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m'], marker_color='#ff4d4d', name="PG매수"), row=3, col=1)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m'], marker_color='#0066ff', name="PG매도"), row=3, col=1)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net'], line=dict(color='black', width=2), name="PG누적"), row=3, col=1, secondary_y=True)

            if 'Anomaly_Dot' in df.columns:
                fig.add_trace(go.Scatter(x=df.index, y=df['Anomaly_Dot'], mode='markers+text', text=df['Anomaly_Text'], textposition='top center', marker=dict(color='red', size=8), textfont=dict(color='red', size=12, weight='bold')), row=4, col=1)
                fig.add_hline(y=2.0, line_dash="dot", line_color="orange", row=4, col=1)

            for i, brk in enumerate(['brk1', 'brk2'], 5):
                fig.add_trace(go.Bar(x=df.index, y=df[f'Buy_{brk}'], marker_color='#ff4d4d', opacity=0.4), row=i, col=1)
                fig.add_trace(go.Bar(x=df.index, y=-df[f'Sell_{brk}'], marker_color='#0066ff', opacity=0.4), row=i, col=1)
                fig.add_trace(go.Scatter(x=df.index, y=df[f'Cum_{brk}'], line=dict(color='black', width=1.5)), row=i, col=1, secondary_y=True)

            fig.update_layout(height=1400, template='plotly_white', showlegend=False, xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)
            st.info(f"⚡ 데이터 최적화 완료: {len(df)}분 분량 캐싱됨")
