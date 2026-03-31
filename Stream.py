import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. 기본 설정 (REST API 접속 정보)
host_url = "https://mockapi.kiwoom.com" 
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 2. 데이터 수집 함수 (이 부분들이 코드 상단에 있어야 합니다)
# ----------------------------------------------------

@st.cache_data(ttl=3000)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    try:
        response = requests.post(url, headers=headers, json=data, timeout=5)
        return response.json().get('token')
    except:
        return None

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    """증권사(거래원) 리스트를 가져오는 함수 (에러 해결 핵심)"""
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={}, timeout=5)
    data = res.json()
    broker_dict = {f"{item['name']}({item['code']})": item["code"] for item in data.get("list", [])}
    return broker_dict

def get_today_minute_chart(token, stock_code):
    """오늘 하루치 분봉만 가져오기"""
    url = f"{host_url}/api/dostk/chart"
    all_data = []
    today_str = datetime.now().strftime('%Y%m%d')
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
    data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
    
    # 당일 데이터는 보통 1~2페이지만 해도 충분합니다
    res = requests.post(url, headers=headers, json=data, timeout=5).json()
    all_data = res.get('stk_min_pole_chart_qry', [])
    return all_data

def get_today_program_data(token, stock_code):
    """오늘의 프로그램 수급 데이터 가져오기"""
    url = f"{host_url}/api/dostk/mrkcond"
    today_str = datetime.now().strftime('%Y%m%d')
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
    req_data = {"amt_qty_tp": "2", "stk_cd": stock_code, "date": today_str}
    res = requests.post(url, headers=headers, json=req_data, timeout=5).json()
    return res.get('stk_tm_prm_trde_trnsn', [])

def get_today_broker_data(token, stock_code, brk_code):
    """오늘의 특정 거래원 수급 데이터 가져오기"""
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
    req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
    res = requests.post(url, headers=headers, json=req_data, timeout=5).json()
    return res.get('trde_ori_mont_trde_qty', [])

def merge_api_data(old_data, new_data):
    if not old_data and not new_data: return []
    df_merged = pd.DataFrame(old_data + new_data)
    return df_merged.drop_duplicates(keep='first').to_dict('records')

# ----------------------------------------------------
# 3. 메인 앱 화면 구성
# ----------------------------------------------------
st.set_page_config(page_title="당일 실시간 수급", layout="wide")
st.title("☀️ 당일 실시간 주도주 수급 분석")

# 데이터 캐시 저장소
if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

token = get_access_token()

with st.sidebar:
    st.header("⚙️ 설정")
    stock_code = st.text_input("종목코드", value="417200")
    
    if token:
        broker_dict = get_broker_list(token)
        b_names = sorted(list(broker_dict.keys()))
        # 기본값으로 키움(039), 신한(002) 등을 찾음
        def_idx1 = next((i for i, n in enumerate(b_names) if "키움" in n), 0)
        def_idx2 = next((i for i, n in enumerate(b_names) if "신한" in n), 0)
        
        sel_brk1 = st.selectbox("🔎 창구 1", b_names, index=def_idx1)
        sel_brk2 = st.selectbox("🔎 창구 2", b_names, index=def_idx2)
        brk_cd1, brk_cd2 = broker_dict[sel_brk1], broker_dict[sel_brk2]
    
    auto_refresh = st.checkbox("🔄 1분 자동 갱신", value=True)

if auto_refresh:
    st_autorefresh(interval=60000, key="today_refresh")

# 데이터 처리 시작
if token and len(stock_code) == 6:
    with st.spinner("데이터 수신 중..."):
        # 1. 데이터 가져오기
        raw_chart = get_today_minute_chart(token, stock_code)
        raw_pg = get_today_program_data(token, stock_code)
        raw_br1 = get_today_broker_data(token, stock_code, brk_cd1)
        raw_br2 = get_today_broker_data(token, stock_code, brk_cd2)

        if not raw_chart:
            st.info("오늘의 데이터가 아직 없거나 장 시작 전입니다.")
            st.stop()

        # 2. 데이터프레임 가공 (당일 필터링)
        today_str = datetime.now().strftime('%Y%m%d')
        df = pd.DataFrame(raw_chart)
        df['dt'] = pd.to_datetime(df['stk_cntr_tm'], format='%Y%m%d%H%M%S')
        df = df[df['dt'].dt.strftime('%Y%m%d') == today_str].sort_values('dt')
        
        # 숫자 변환
        for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
            df[col] = df[col].astype(str).str.replace(r'[+,-]', '', regex=True).astype(int)

        # 3. 수급 데이터 처리 (프로그램/거래원)
        def get_net_series(raw_data, time_key='tm', val_key='acc_netprps'):
            if not raw_data: return pd.Series(0, index=df.index)
            temp_df = pd.DataFrame(raw_data)
            temp_df['dt'] = pd.to_datetime(today_str + temp_df[time_key], format='%Y%m%d%H%M%S').dt.floor('min')
            # 누적 순매수량 숫자 변환
            if val_key == 'prm_net_buy_qty': # 프로그램용
                temp_df['net'] = pd.to_numeric(temp_df['prm_buy_qty']) - pd.to_numeric(temp_df['prm_sell_qty'])
            else: # 거래원용
                temp_df['net'] = pd.to_numeric(temp_df[val_key].astype(str).str.replace(',', ''))
            
            return temp_df.sort_values('dt').groupby('dt')['net'].last().reindex(df.index).ffill().fillna(0)

        df['Net_PG'] = get_net_series(raw_pg, val_key='prm_net_buy_qty')
        df['Net_Brk1'] = get_net_series(raw_br1)
        df['Net_Brk2'] = get_net_series(raw_br2)

        # ----------------------------------------------------
        # 4. 차트 그리기 (당일 전용 4단 차트)
        # ----------------------------------------------------
        fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                           row_heights=[0.4, 0.2, 0.2, 0.2],
                           subplot_titles=("가격", "프로그램 누적", f"{sel_brk1} 누적", f"{sel_brk2} 누적"))

        # 1층: 주가
        fig.add_trace(go.Candlestick(x=df['dt'], open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
        # 2층~4층: 수급선
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_PG'], name="프로그램", line=dict(color='purple')), row=2, col=1)
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_Brk1'], name=sel_brk1, line=dict(color='blue')), row=3, col=1)
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_Brk2'], name=sel_brk2, line=dict(color='red')), row=4, col=1)

        fig.update_layout(height=900, template='plotly_white', xaxis_rangeslider_visible=False, showlegend=False)
        fig.update_yaxes(tickformat=",")
        st.plotly_chart(fig, use_container_width=True)
