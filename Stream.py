import streamlit as st
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ----------------------------------------------------
# 1. 설정 및 인증 함수
# ----------------------------------------------------
host_url = "https://mockapi.kiwoom.com" 

@st.cache_data(ttl=3600)
def get_access_token():
    try:
        url = f"{host_url}/oauth2/token"
        app_key = st.secrets["APP_KEY"]
        app_secret = st.secrets["APP_SECRET"]
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
        data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return response.json().get('token')
    except:
        return None
    return None

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
# 2. 데이터 수집 함수
# ----------------------------------------------------
def get_historical_minute_chart(token, stock_code):
    url = f"{host_url}/api/dostk/chart"
    all_chart_data = []
    next_key = ""
    for _ in range(5): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
        response = requests.post(url, headers=headers, json=data)
        chunk = response.json().get('stk_min_pole_chart_qry', [])
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
    for _ in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"amt_qty_tp": "2", "stk_cd": stock_code, "date": target_date}
        response = requests.post(url, headers=headers, json=req_data)
        chunk = response.json().get('stk_tm_prm_trde_trnsn', [])
        if not chunk: break
        all_data.extend(chunk)
        if chunk[-1].get('tm', '999999') <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1)
    return all_data

def get_historical_broker_data(token, stock_code, brk_code, max_pages=500):
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    for _ in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        response = requests.post(url, headers=headers, json=req_data)
        chunk = response.json().get('trde_ori_mont_trde_qty', [])
        if not chunk: break
        all_data.extend(chunk)
        tm = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', '999999'))
        if tm <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1)
    return all_data

def merge_api_data(old_data, new_data):
    seen = set()
    merged = []
    for item in (old_data + new_data):
        sig = str(item)
        if sig not in seen:
            seen.add(sig); merged.append(item)
    return merged

# ----------------------------------------------------
# 3. 메인 화면
# ----------------------------------------------------
st.set_page_config(page_title="수급 복기 v3.1", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

auth_token = get_access_token()
if not auth_token: st.error("API 토큰 발급 실패! Secrets를 확인하세요."); st.stop()

st.sidebar.header("📅 설정")
stock_number = st.sidebar.text_input("종목코드", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

broker_dict = get_broker_list(auth_token)
broker_names = sorted(list(broker_dict.keys()))
idx1 = next((i for i, n in enumerate(broker_names) if "키움" in n), 0)
idx2 = next((i for i, n in enumerate(broker_names) if "신한" in n), 0)
name1 = st.sidebar.selectbox("창구1", broker_names, index=idx1)
name2 = st.sidebar.selectbox("창구2", broker_names, index=idx2)
lag_sec = st.sidebar.slider("보정(초)", 0, 180, 60)

if auth_token and len(stock_number) == 6:
    with st.spinner("데이터 수집 중..."):
        # 캐시 관리
        cur_key = f"{stock_number}_{target_date_str}_{broker_dict[name1]}_{broker_dict[name2]}"
        if st.session_state.get('last_key') != cur_key:
            st.session_state['data_cache'] = {'pg':[], 'brk1':[], 'brk2':[]}
            st.session_state['last_key'] = cur_key
            fetch_p = 500
        else: fetch_p = 3

        c_raw = get_historical_minute_chart(auth_token, stock_number)
        p_raw = get_historical_program_data(auth_token, stock_number, target_date_str, fetch_p)
        b1_raw = get_historical_broker_data(auth_token, stock_number, broker_dict[name1], fetch_p)
        b2_raw = get_historical_broker_data(auth_token, stock_number, broker_dict[name2], fetch_p)

        st.session_state['data_cache']['pg'] = merge_api_data(st.session_state['data_cache']['pg'], p_raw)
        st.session_state['data_cache']['brk1'] = merge_api_data(st.session_state['data_cache']['brk1'], b1_raw)
        st.session_state['data_cache']['brk2'] = merge_api_data(st.session_state['data_cache']['brk2'], b2_raw)

        if c_raw:
            df = pd.DataFrame(c_raw)
            if 'cntr_tm' in df.columns:
    df['Datetime'] = pd.to_datetime(df['cntr_tm'], format='%Y%m%d%H%M%S')
elif 'stk_cntr_tm' in df.columns:
    df['Datetime'] = pd.to_datetime(df['stk_cntr_tm'], format='%Y%m%d%H%M%S')
else:
    # 어떤 열 이름도 찾을 수 없을 때를 위한 방어 로직
    st.error(f"시간 정보(열)를 찾을 수 없습니다. 현재 열 이름: {list(df.columns)}")
    st.stop()
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()
            if df.empty: st.info("장 시작 전입니다."); st.stop()

            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[+,-]', '', regex=True), errors='coerce').fillna(0).astype(int)

            def process_brk(raw, lag):
                if not raw: return pd.DataFrame()
                db = pd.DataFrame(raw)
                db['Datetime'] = pd.to_datetime(target_date_str + db['tm'], format='%Y%m%d%H%M%S', errors='coerce') - pd.Timedelta(seconds=lag)
                db['Datetime'] = db['Datetime'].dt.floor('min')
                def parse_v(row):
                    v_str = str(row['mont_trde_qty']).replace(',','')
                    qty = int(v_str.replace('+','').replace('-',''))
                    return (0, qty) if '-' in v_str or '매도' in str(row['tp']) else (qty, 0)
                db[['B','S']] = db.apply(parse_v, axis=1, result_type='expand')
                res = db.groupby('Datetime').agg({'B':'sum', 'S':'sum', 'acc_netprps':'last'})
                res['acc_netprps'] = pd.to_numeric(res['acc_netprps'].astype(str).str.replace(r'[+,]', '', regex=True), errors='coerce').fillna(0)
                return res

            # 거래원 데이터 병합
            res1 = process_brk(st.session_state['data_cache']['brk1'], lag_sec)
            res2 = process_brk(st.session_state['data_cache']['brk2'], lag_sec)
            df = df.join(res1.rename(columns={'B':'B1','S':'S1','acc_netprps':'C1'}), how='left').fillna(0)
            df = df.join(res2.rename(columns={'B':'B2','S':'S2','acc_netprps':'C2'}), how='left').fillna(0)

            # --- [핵심] Signal Value & T 로직 ---
            df['M1'] = df['C1'].expanding().max(); df['m1'] = df['C1'].expanding().min()
            df['M2'] = df['C2'].expanding().max(); df['m2'] = df['C2'].expanding().min()
            df['P1'] = (df['M1'] - df['C1']) - (df['C1'] - df['m1'])
            df['P2'] = (df['M2'] - df['C2']) - (df['C2'] - df['m2'])
            
            df['SV'] = 0.0
            mask = df['P1'] > df['P2']
            df.loc[mask, 'SV'] = (df['M1'] - df['C1']) + (df['C2'] - df['m2'])
            df.loc[~mask, 'SV'] = (df['M2'] - df['C2']) + (df['C1'] - df['m1'])

            last_rp, low_after_rp, T = 0.0, 0.0, 0.0
            reds, blues = [None]*len(df), [None]*len(df)
            sv_vals = df['SV'].values
            for i in range(len(df)):
                v = sv_vals[i]
                if v > last_rp:
                    if last_rp > 0 and low_after_rp < last_rp: T = last_rp - low_after_rp
                    last_rp = v; low_after_rp = v; reds[i] = v
                elif v < last_rp:
                    is_b = (v < (last_rp - T)) if T > 0 else True
                    if is_b:
                        blues[i] = v
                        if v < low_after_rp: low_after_rp = v
            df['Sig_R'] = reds; df['Sig_B'] = blues

            # --- 차트 그리기 ---
            fig = make_subplots(rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                                row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2],
                                specs=[[{"secondary_y":False}],[{"secondary_y":False}],[{"secondary_y":True}],
                                       [{"secondary_y":True}],[{"secondary_y":True}],[{"secondary_y":True}]])

            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color='gray'), row=2, col=1)
            
            # 창구 1, 2 그리기
            for row_idx, pref, c_name in [(4, '1', name1), (5, '2', name2)]:
                fig.add_trace(go.Bar(x=df.index, y=df[f'B{pref}'], marker_color='#ff4d4d', opacity=0.3), row=row_idx, col=1, secondary_y=False)
                fig.add_trace(go.Bar(x=df.index, y=-df[f'S{pref}'], marker_color='#0066ff', opacity=0.3), row=row_idx, col=1, secondary_y=False)
                fig.add_trace(go.Scatter(x=df.index, y=df[f'C{pref}'], line=dict(color='black', width=2), name=f"{c_name} 누적"), row=row_idx, col=1, secondary_y=True)
                # 신호 점 (None이 아닌 경우만 표시됨)
                r_pts = [df[f'C{pref}'].iloc[i] if df['Sig_R'].iloc[i] is not None else None for i in range(len(df))]
                b_pts = [df[f'C{pref}'].iloc[i] if df['Sig_B'].iloc[i] is not None else None for i in range(len(df))]
                fig.add_trace(go.Scatter(x=df.index, y=r_pts, mode='markers', marker=dict(color='red', size=7), name="강세"), row=row_idx, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=df.index, y=b_pts, mode='markers', marker=dict(color='blue', size=7), name="약세"), row=row_idx, col=1, secondary_y=True)

            fig.update_layout(height=1400, template='plotly_white', showlegend=False, xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)
