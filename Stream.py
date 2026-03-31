import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. 설정 (URL 및 키)
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 2. 필수 함수 (인증/수집)
# ----------------------------------------------------

@st.cache_data(ttl=3000)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    res = requests.post(url, headers=headers, json=data, timeout=5)
    return res.json().get('token') if res.status_code == 200 else None

def get_today_minute_chart(token, stock_code):
    """오늘 하루치 분봉만 가져오기 (5페이지면 충분)"""
    url = f"{host_url}/api/dostk/chart"
    all_data = []
    next_key = ""
    today_str = datetime.now().strftime('%Y%m%d')
    
    for i in range(5): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"next-key": next_key, "tr-cont-key": next_key})
        data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
        res = requests.post(url, headers=headers, json=data, timeout=5).json()
        chunk = res.get('stk_min_pole_chart_qry', [])
        if not chunk: break
        
        all_data.extend(chunk)
        # 데이터가 어제 날짜로 넘어가면 중단
        if chunk[-1].get('stk_cntr_tm', '')[:8] < today_str: break
        next_key = "" # 실제로는 헤더에서 받아야 함
        time.sleep(0.1)
    return all_data

# ... (중량: 프로그램/거래원 데이터 수집 함수는 기존과 동일하게 유지하되 '당일'만 처리)

def merge_api_data(old_data, new_data):
    """데이터가 증발하지 않게 구 데이터와 신 데이터를 합치는 마법의 함수"""
    if not old_data and not new_data: return []
    df = pd.DataFrame(old_data + new_data)
    return df.drop_duplicates(keep='first').to_dict('records')

# ----------------------------------------------------
# 3. 메인 화면 구동
# ----------------------------------------------------
st.set_page_config(page_title="당일 수급 대시보드", layout="wide")
st.title("☀️ 당일 주도주 실시간 수급 차트")

# 오늘 날짜 고정
today_date = datetime.now().strftime('%Y%m%d')

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

# 사이드바
with st.sidebar:
    stock_code = st.text_input("종목코드", value="417200")
    # 거래원 선택 (API를 통해 리스트 자동 생성)
    token = get_access_token()
    broker_list = get_broker_list(token) if token else {}
    target_brk1 = st.selectbox("창구 1", list(broker_list.keys()), index=0)
    target_brk2 = st.selectbox("창구 2", list(broker_list.keys()), index=1)
    auto_refresh = st.checkbox("🔄 1분 자동 갱신", value=True)

if auto_refresh:
    st_autorefresh(interval=60000, key="today_refresh")

# 데이터 처리 및 차트 그리기
if token and len(stock_code) == 6:
    with st.spinner("오늘의 수급 데이터를 분석 중..."):
        # 1. API 호출
        new_chart = get_today_minute_chart(token, stock_code)
        new_pg = get_historical_program_data(token, stock_code, today_date, 5) # 당일이라 5페이지만
        
        # 2. 캐시 업데이트 (Merge)
        st.session_state['data_cache']['pg'] = merge_api_data(st.session_state['data_cache']['pg'], new_pg)
        
        # 3. 데이터프레임 변환
        df = pd.DataFrame(new_chart)
        # ... (이후 시간 컬럼 처리 및 당일 필터링)
        df['dt'] = pd.to_datetime(df['stk_cntr_tm'], format='%Y%m%d%H%M%S')
        df = df[df['dt'].dt.strftime('%Y%m%d') == today_date].sort_values('dt')

        # 📊 차트 레이아웃 (요청하신대로 하나씩 쌓기)
        fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                           subplot_titles=("가격/거래량", "프로그램 누적", "창구1 누적", "창구2 누적"),
                           vertical_spacing=0.05, row_heights=[0.4, 0.2, 0.2, 0.2])

        # [1층] 캔들/거래량
        fig.add_trace(go.Candlestick(x=df['dt'], open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="주가"), row=1, col=1)
        
        # [2층] 프로그램 (캐시 데이터 사용)
        df_pg = pd.DataFrame(st.session_state['data_cache']['pg'])
        if not df_pg.empty:
            df_pg['dt'] = pd.to_datetime(today_date + df_pg['tm'], format='%Y%m%d%H%M%S')
            fig.add_trace(go.Scatter(x=df_pg['dt'], y=pd.to_numeric(df_pg['prm_buy_qty'])-pd.to_numeric(df_pg['prm_sell_qty']), name="프로그램"), row=2, col=1)

        # 차트 출력
        fig.update_layout(height=1000, template='plotly_white', xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)
