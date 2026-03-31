import streamlit as st
import requests
import pandas as pd
import numpy as np  # 속도 최적화를 위해 필수
import time
import concurrent.futures  # 병렬 수집을 위해 필수
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. 환경 설정 (Secrets에서 키 불러오기)
host_url = "https://mockapi.kiwoom.com" 
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 2. 인증 및 데이터 수집 함수들
# ----------------------------------------------------
@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data)
    return response.json().get('token') if response.status_code == 200 else None

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

def get_historical_minute_chart(token, stock_code):
    url = f"{host_url}/api/dostk/chart"
    all_data = []
    next_key = ""
    for i in range(5): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
        response = requests.post(url, headers=headers, json=data)
        chunk = response.json().get('stk_min_pole_chart_qry', [])
        if not chunk: break
        all_data.extend(chunk)
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
    return all_data

def get_historical_program_data(token, stock_code, target_date, max_pages=1500):
    url = f"{host_url}/api/dostk/mrkcond"
    all_data = []
    next_key = ""
    for i in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"amt_qty_tp": "2", "stk_cd": stock_code, "date": target_date}
        response = requests.post(url, headers=headers, json=req_data)
        chunk = response.json().get('stk_tm_prm_trde_trnsn', [])
        if not chunk: break
        all_data.extend(chunk)
        if chunk[-1].get('tm', '') <= "090000" or not response.headers.get('next-key'): break
    return all_data

def get_historical_broker_data(token, stock_code, brk_code, max_pages=1500):
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    for i in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        response = requests.post(url, headers=headers, json=req_data)
        chunk = response.json().get('trde_ori_mont_trde_qty', [])
        if not chunk: break
        all_data.extend(chunk)
        if chunk[-1].get('tm', '') <= "090000" or not response.headers.get('next-key'): break
    return all_data

def merge_api_data(old_data, new_data):
    if not old_data and not new_data: return []
    df_m = pd.DataFrame(old_data + new_data)
    return df_m.drop_duplicates(keep='first').to_dict('records') if not df_m.empty else []

# 🚀 최적화된 거래원 데이터 처리 함수 (벡터화 버전)
def process_broker_data_fast(raw_data, lag_sec, target_date, suffix):
    if not raw_data: return pd.DataFrame()
    db = pd.DataFrame(raw_data)
    tm_col = 'tm' if 'tm' in db.columns else 'stck_cntg_hour'
    db['Datetime'] = (pd.to_datetime(target_date + db[tm_col], format='%Y%m%d%H%M%S', errors='coerce') 
                      - pd.Timedelta(seconds=lag_sec)).dt.floor('min')
    
    qty_str = db['mont_trde_qty'].astype(str).str.replace(',', '', regex=False)
    qty_val = pd.to_numeric(qty_str.str.replace(r'[+-]', '', regex=True), errors='coerce').fillna(0)
    is_sell = (qty_str.str.contains('-', regex=False)) | (db['tp'].astype(str).str.contains('매도', regex=False))
    
    db['Buy_Vol'] = np.where(~is_sell, qty_val, 0)
    db['Sell_Vol'] = np.where(is_sell, qty_val, 0)
    net_val = pd.to_numeric(db['acc_netprps'].astype(str).str.replace(r'[+,,]', '', regex=True), errors='coerce').fillna(0) if 'acc_netprps' in db.columns else 0
    db['Net_Raw'] = net_val
    
    return db.groupby('Datetime').agg({'Buy_Vol': 'sum', 'Sell_Vol': 'sum', 'Net_Raw': 'last'}).rename(
        columns={'Buy_Vol': f'Buy_1m_{suffix}', 'Sell_Vol': f'Sell_1m_{suffix}', 'Net_Raw': f'Cum_Net_{suffix}'})

# ----------------------------------------------------
# 3. 메인 화면 구성
# ----------------------------------------------------
st.set_page_config(page_title="실시간 수급 복기 v2.6", layout="wide")
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
    default_idx1 = next((i for i, n in enumerate(broker_names) if n.startswith("키움")), 0)
    default_idx2 = next((i for i, n in enumerate(broker_names) if n.startswith("신한")), 0)
    
    selected_broker_name1 = st.sidebar.selectbox("🔎 창구 1", broker_names, index=default_idx1)
    target_broker_code1 = broker_dict[selected_broker_name1]
    selected_broker_name2 = st.sidebar.selectbox("🔎 창구 2", broker_names, index=default_idx2)
    target_broker_code2 = broker_dict[selected_broker_name2]
    
lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)
auto_refresh = st.sidebar.checkbox("🔄 1분 자동 갱신", value=False)

if st.sidebar.button("🧹 캐시 삭제"):
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
    if 'last_search_key' in st.session_state: del st.session_state['last_search_key']
    st.rerun()

# ----------------------------------------------------
# 4. 데이터 수집 및 시각화 로직
# ----------------------------------------------------
if auth_token and len(stock_number) == 6: 
    with st.spinner("병렬 수집 및 초고속 분석 중..."):
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        is_first = 'last_search_key' not in st.session_state or st.session_state['last_search_key'] != current_search_key
        
        if is_first:
            fetch_p = 500
            st.session_state['last_search_key'] = current_search_key
            st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
        else:
            fetch_p = 3

        # 🚀 병렬 수집
        with concurrent.futures.ThreadPoolExecutor() as executor:
            f_pg = executor.submit(get_historical_program_data, auth_token, stock_number, target_date_str, fetch_p)
            f_b1 = executor.submit(get_historical_broker_data, auth_token, stock_number, target_broker_code1, fetch_p)
            f_b2 = f_b1 if target_broker_code1 == target_broker_code2 else executor.submit(get_historical_broker_data, auth_token, stock_number, target_broker_code2, fetch_p)
            
            new_pg = f_pg.result(); new_brk1 = f_b1.result(); new_brk2 = f_b2.result() if target_broker_code1 != target_broker_code2 else new_brk1

        chart_raw = get_historical_minute_chart(auth_token, stock_number)
        pg_raw = merge_api_data(st.session_state['data_cache']['pg'], new_pg)
        brk_raw1 = merge_api_data(st.session_state['data_cache']['brk1'], new_brk1)
        brk_raw2 = merge_api_data(st.session_state['data_cache']['brk2'], new_brk2)

        st.session_state['data_cache'].update({'pg': pg_raw, 'brk1': brk_raw1, 'brk2': brk_raw2})

        if chart_raw:
            df = pd.DataFrame(chart_raw)
            df['Datetime'] = pd.to_datetime(df['stk_cntr_tm'] if 'stk_cntr_tm' in df.columns else df['cntr_tm'], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()
            
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[+,-,]', '', regex=True)).fillna(0).astype(int)

            # 수급 결합
            df = df.join(process_broker_data_fast(brk_raw1, lag_seconds, target_date_str, 'brk1'), how='left')
            df = df.join(process_broker_data_fast(brk_raw2, lag_seconds, target_date_str, 'brk2'), how='left')
            
            fill_list = ['Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']
            df[fill_list] = df[fill_list].fillna(0)
            df['Cum_Net_brk1'] = df['Cum_Net_brk1'].ffill().fillna(0)
            df['Cum_Net_brk2'] = df['Cum_Net_brk2'].ffill().fillna(0)
            df.loc[df.index.strftime('%H%M').isin(['0900', '1530']), ['trde_qty'] + fill_list] = 0

            # 🚀 신호 연산 (벡터화)
            df['Max1'] = df['Cum_Net_brk1'].expanding().max(); df['Min1'] = df['Cum_Net_brk1'].expanding().min()
            df['Max2'] = df['Cum_Net_brk2'].expanding().max(); df['Min2'] = df['Cum_Net_brk2'].expanding().min()
            p1 = (df['Max1'] - df['Cum_Net_brk1']) - (df['Cum_Net_brk1'] - df['Min1'])
            p2 = (df['Max2'] - df['Cum_Net_brk2']) - (df['Cum_Net_brk2'] - df['Min2'])
            
            df['Signal_Value'] = np.where(p1 > p2, (df['Max1'] - df['Cum_Net_brk1']) + (df['Cum_Net_brk2'] - df['Min2']),
                                          (df['Max2'] - df['Cum_Net_brk2']) + (df['Cum_Net_brk1'] - df['Min1']))
            df['Value_Max'] = df['Signal_Value'].expanding().max()
            df['Signal_Point'] = np.where((df['Signal_Value'] == df['Value_Max']) & (df['Signal_Value'] > 0), df['Signal_Value'], np.nan)

            # 📊 차트 생성
            fig = make_subplots(rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2],
                                specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]])

            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color='gray', opacity=0.5), row=2, col=1)

            for r_idx, s, b_name in [(4, 'brk1', selected_broker_name1), (5, 'brk2', selected_broker_name2)]:
                fig.add_trace(go.Bar(x=df.index, y=df[f'Buy_1m_{s}'], marker_color='#ff4d4d', opacity=0.4), row=r_idx, col=1, secondary_y=False)
                fig.add_trace(go.Bar(x=df.index, y=-df[f'Sell_1m_{s}'], marker_color='#0066ff', opacity=0.4), row=r_idx, col=1, secondary_y=False)
                fig.add_trace(go.Scatter(x=df.index, y=df[f'Cum_Net_{s}'], line=dict(color='black', width=2)), row=r_idx, col=1, secondary_y=True)
                sig_y = df[f'Cum_Net_{s}'].where(~np.isnan(df['Signal_Point']))
                fig.add_trace(go.Scatter(x=df.index, y=sig_y, mode='markers', marker=dict(color='red', size=7)), row=r_idx, col=1, secondary_y=True)

            fig.update_layout(height=1500, template='plotly_white', hovermode='x unified', showlegend=False, xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)

            if auto_refresh:
                time.sleep(60)
                st.rerun()

        else:
            st.warning("데이터가 없거나 장 시작 전입니다.")
