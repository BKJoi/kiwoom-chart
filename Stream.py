import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ----------------------------------------------------
# 1. 기본 설정 및 데이터 수집 함수 (기존과 동일)
# ----------------------------------------------------
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

@st.cache_data(ttl=3000)
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

# (중략: get_historical_... 함수 및 merge_api_data 함수는 기존과 동일하게 유지)

# ----------------------------------------------------
# 2. 메인 화면 설정 (사이드바)
# ----------------------------------------------------
st.set_page_config(page_title="실시간 수급 복기 v3.1", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 대시보드 (v3.1)")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

auth_token = get_access_token()

# 사이드바 설정값들
st.sidebar.header("📅 복기 설정")
stock_number = st.sidebar.text_input("종목코드", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

if auth_token:
    broker_dict = get_broker_list(auth_token)
    broker_names = sorted(list(broker_dict.keys()))
    
    selected_broker_name1 = st.sidebar.selectbox("🔎 첫 번째 창구", broker_names, index=0)
    target_broker_code1 = broker_dict[selected_broker_name1]
    
    selected_broker_name2 = st.sidebar.selectbox("🔎 두 번째 창구", broker_names, index=1)
    target_broker_code2 = broker_dict[selected_broker_name2]

lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)
corr_window = st.sidebar.slider("🔄 상관계수 기간 (분)", 3, 60, 30)

if st.sidebar.button("🧹 캐시 삭제 및 새로고침"):
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
    st.rerun()

# ----------------------------------------------------
# 3. [핵심] 차트 조각(Fragment) 정의 - 30초마다 자동 실행
# ----------------------------------------------------
@st.fragment(run_every="30s")
def draw_realtime_dashboard():
    # 이 안의 내용은 기존의 '데이터 수집 + 차트 그리기' 로직을 그대로 가져옵니다.
    with st.spinner(f"데이터 갱신 중... ({datetime.now().strftime('%H:%M:%S')})"):
        
        # [1] 데이터 수집 (기존 로직 수행)
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        is_first_load = 'last_search_key' not in st.session_state or st.session_state['last_search_key'] != current_search_key
        
        fetch_p = 500 if is_first_load else 3
        st.session_state['last_search_key'] = current_search_key
        
        # ... (이하 기존의 get_historical_... 호출 및 데이터 가공 로직 동일) ...
        # (df 생성, 상관계수 계산, make_subplots 과정 수행)
        
        # [2] 최종 차트 출력
        # (기존의 fig 설정 로직들...)
        st.plotly_chart(fig, use_container_width=True)
        st.info(f"💡 30초마다 자동으로 차트만 업데이트됩니다. (마지막 갱신: {datetime.now().strftime('%H:%M:%S')})")

# ----------------------------------------------------
# 4. 실행
# ----------------------------------------------------
if auth_token and len(stock_number) == 6:
    draw_realtime_dashboard()
