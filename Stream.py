import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ----------------------------------------------------
# 1. 설정 및 인증 함수
# ----------------------------------------------------
host_url = "https://mockapi.kiwoom.com"  # 실전투자 시 https://openapi.kiwoom.com으로 변경

@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    app_key = st.secrets["APP_KEY"]
    app_secret = st.secrets["APP_SECRET"]
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        return None
    return response.json().get('token')

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={})
    data = res.json()
    broker_dict = {}
    if "list" in data:
        for item in data["list"]: 
            display_name = f"{item['name']}({item['code']})"
            broker_dict[display_name] = item["code"]
    return broker_dict

# ----------------------------------------------------
# 2. 데이터 수집 함수 (가격, 프로그램, 거래원)
# ----------------------------------------------------
def get_historical_minute_chart(token, stock_code):
    url = f"{host_url}/api/dostk/chart"
    all_chart_data = []
    next_key = ""
    for i in range(5): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
        response = requests.post(url, headers=headers, json=data)
        res_json = response.json()
        chunk = res_json.get('stk_min_pole_chart_qry', [])
        if not chunk: break
        all_chart_data.extend(chunk)
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.2) 
    return all_chart_data

def get_historical_program_data(token, stock_code, target_date, max_pages=500):
    url = f"{host_url}/api/dostk/mrkcond"
    all_data = []
    next_key = ""
    for i in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"amt_qty_tp": "2", "stk_cd": stock_code, "date": target_date}
        response = requests.post(url, headers=headers, json=req_data)
        if response.status_code != 200: continue
        chunk = response.json().get('stk_tm_prm_trde_trnsn', [])
        if not chunk: break
        all_data.extend(chunk)
        if chunk[-1].get('tm', '') <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1) 
    return all_data

def get_historical_broker_data(token, stock_code, brk_code, max_pages=500):
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    for i in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        response = requests.post(url, headers=headers, json=req_data)
        if response.status_code != 200: continue
        chunk = response.json().get('trde_ori_mont_trde_qty', [])
        if not chunk: break
        all_data.extend(chunk)
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1)
    return all_data

def merge_api_data(old_data, new_data):
    seen = set()
    merged = []
    for item in old_data + new_data:
        sig = str(item)
        if sig not in seen:
            seen.add(sig)
            merged.append(item)
    return merged

# ----------------------------------------------------
# 3. 메인 UI 및 설정
# ----------------------------------------------------
st.set_page_config(page_title="실시간 수급 복기 v3.0", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 대시보드")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

auth_token = get_access_token()

st.sidebar.header("📅 복기 설정")
stock_number = st.sidebar.text_input("종목코드", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

if auth_token:
    broker_dict = get_broker_list(auth_token)
    broker_names = sorted(list(broker_dict.keys()))
    
    idx1 = next((i for i, name in enumerate(broker_names) if "키움증권" in name), 0)
    selected_broker_name1 = st.sidebar.selectbox("🔎 첫 번째 창구", broker_names, index=idx1)
    target_broker_code1 = broker_dict[selected_broker_name1]

    idx2 = next((i for i, name in enumerate(broker_names) if "신한투자" in name), 0)
    selected_broker_name2 = st.sidebar.selectbox("🔎 두 번째 창구", broker_names, index=idx2)
    target_broker_code2 = broker_dict[selected_broker_name2]

lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)
auto_refresh = st.sidebar.checkbox("🔄 자동 갱신 (당일 실시간)", value=False)

if st.sidebar.button("🧹 캐시 삭제"):
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
    if 'last_search_key' in st.session_state: del st.session_state['last_search_key']
    st.rerun()

# ----------------------------------------------------
# 4. 데이터 처리 및 시각화 로직
# ----------------------------------------------------
if auth_token and len(stock_number) == 6:
    with st.spinner("수급 데이터를 수집 중..."):
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        if st.session_state.get('last_search_key') != current_search_key:
            fetch_pages = 500
            st.session_state['last_search_key'] = current_search_key
            st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
        else:
            fetch_pages = 3

        chart_raw = get_historical_minute_chart(auth_token, stock_number)
        new_pg = get_historical_program_data(auth_token, stock_number, target_date_str, fetch_pages)
        new_brk1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1, fetch_pages)
        new_brk2 = get_historical_broker_data(auth_token, stock_number, target_broker_code2, fetch_pages)

        st.session_state['data_cache']['pg'] = merge_api_data(st.session_state['data_cache']['pg'], new_pg)
        st.session_state['data_cache']['brk1'] = merge_api_data(st.session_state['data_cache']['brk1'], new_brk1)
        st.session_state['data_cache']['brk2'] = merge_api_data(st.session_state['data_cache']['brk2'], new_brk2)

        if chart_raw:
            df = pd.DataFrame(chart_raw)
            time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()
            
            if df.empty:
                st.info("데이터 대기 중...")
                st.stop()

            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[+,-]', '', regex=True), errors='coerce').fillna(0).astype(int)

            # --- 프로그램 & 거래원 전처리 ---
            def process_brk(raw, suffix, lag):
                if not raw: return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                df_b = pd.DataFrame(raw)
                df_b['Datetime'] = pd.to_datetime(target_date_str + df_b['tm'], format='%Y%m%d%H%M%S', errors='coerce') - pd.Timedelta(seconds=lag)
                df_b['Datetime'] = df_b['Datetime'].dt.floor('min')
                def parse_v(row):
                    qty = int(str(row['mont_trde_qty']).replace(',', '').replace('+', '').replace('-', ''))
                    return (0, qty) if '-' in str(row['mont_trde_qty']) or '매도' in str(row['tp']) else (qty, 0)
                df_b[['B','S']] = df_b.apply(parse_v, axis=1, result_type='expand')
                df_res = df_b.groupby('Datetime').agg({'B':'sum', 'S':'sum', 'acc_netprps':'last'})
                df_res['acc_netprps'] = pd.to_numeric(df_res['acc_netprps'].astype(str).str.replace(r'[+,]', '', regex=True)).fillna(0)
                return df_res.rename(columns={'B':f'Buy_1m_{suffix}', 'S':f'Sell_1m_{suffix}', 'acc_netprps':f'Cum_Net_{suffix}'})

            df = df.join(process_brk(st.session_state['data_cache']['brk1'], 'brk1', lag_seconds), how='left').fillna(0)
            df = df.join(process_brk(st.session_state['data_cache']['brk2'], 'brk2', lag_seconds), how='left').fillna(0)

            # --- [핵심] Signal Value & 가변 임계치 T 로직 ---
            df['Max1'] = df['Cum_Net_brk1'].expanding().max(); df['Min1'] = df['Cum_Net_brk1'].expanding().min()
            df['Max2'] = df['Cum_Net_brk2'].expanding().max(); df['Min2'] = df['Cum_Net_brk2'].expanding().min()
            df['Pos1'] = (df['Max1'] - df['Cum_Net_brk1']) - (df['Cum_Net_brk1'] - df['Min1'])
            df['Pos2'] = (df['Max2'] - df['Cum_Net_brk2']) - (df['Cum_Net_brk2'] - df['Min2'])
            
            df['Signal_Value'] = 0.0
            m = df['Pos1'] > df['Pos2']
            df.loc[m, 'Signal_Value'] = (df['Max1'] - df['Cum_Net_brk1']) + (df['Cum_Net_brk2'] - df['Min2'])
            df.loc[~m, 'Signal_Value'] = (df['Max2'] - df['Cum_Net_brk2']) + (df['Cum_Net_brk1'] - df['Min1'])

            last_rp = 0.0; lowest_after_rp = 0.0; T = 0.0
            reds, blues = [pd.NA]*len(df), [pd.NA]*len(df)
            vals = df['Signal_Value'].values
            for i in range(len(df)):
                v = vals[i]
                if v > last_rp:
                    if last_rp > 0 and lowest_after_rp < last_rp: T = last_rp - lowest_after_rp
                    last_rp = v; lowest_after_rp = v; reds[i] = v
                elif v < last_rp:
                    is_b = (v < (last_rp - T)) if T > 0 else True
                    if is_b:
                        blues[i] = v
                        if v < lowest_after_rp: lowest_after_rp = v
            df['Signal_Point_Red'] = reds; df['Signal_Point_Blue'] = blues

            # --- 차트 그리기 ---
            fig = make_subplots(rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                                row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2],
                                specs=[[{"secondary_y":False}],[{"secondary_y":False}],[{"secondary_y":True}],
                                       [{"secondary_y":True}],[{"secondary_y":True}],[{"secondary_y":True}]])

            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량"), row=2, col=1)
            
            # 4층/5층 창구 (검정선 + 빨간점/파란점)
            for r, suffix, name in [(4, 'brk1', selected_broker_name1), (5, 'brk2', selected_broker_name2)]:
                fig.add_trace(go.Bar(x=df.index, y=df[f'Buy_1m_{suffix}'], marker_color='#ff4d4d', opacity=0.3), row=r, col=1, secondary_y=False)
                fig.add_trace(go.Bar(x=df.index, y=-df[f'Sell_1m_{suffix}'], marker_color='#0066ff', opacity=0.3), row=r, col=1, secondary_y=False)
                fig.add_trace(go.Scatter(x=df.index, y=df[f'Cum_Net_{suffix}'], line=dict(color='black', width=2), name=f"{name} 누적"), row=r, col=1, secondary_y=True)
                # 신호 점 찍기
                fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda row: row[f'Cum_Net_{suffix}'] if not pd.isna(row['Signal_Point_Red']) else pd.NA, axis=1), mode='markers', marker=dict(color='red', size=6)), row=r, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda row: row[f'Cum_Net_{suffix}'] if not pd.isna(row['Signal_Point_Blue']) else pd.NA, axis=1), mode='markers', marker=dict(color='blue', size=6)), row=r, col=1, secondary_y=True)

            fig.update_layout(height=1500, template='plotly_white', showlegend=False, xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)

            if auto_refresh:
                time.sleep(60)
                st.rerun()
