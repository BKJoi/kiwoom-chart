import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. 환경 설정
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 2. 데이터 수집 함수 (에러 방어형)
# ----------------------------------------------------

@st.cache_data(ttl=3000)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    try:
        res = requests.post(url, headers=headers, json=data, timeout=5)
        return res.json().get('token')
    except: return None

@st.cache_data(ttl=86400)
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    try:
        res = requests.post(url, headers=headers, json={}, timeout=5).json()
        return {f"{i['name']}({i['code']})": i['code'] for i in res.get('list', [])}
    except: return {}

def get_api_data(url, api_id, token, params):
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": api_id, "authorization": f"Bearer {token}"}
    try:
        res = requests.post(url, headers=headers, json=params, timeout=5).json()
        for key in res.keys():
            if isinstance(res[key], list): return res[key]
        return []
    except: return []

# ----------------------------------------------------
# 3. 메인 앱 로직
# ----------------------------------------------------
st.set_page_config(page_title="당일 수급 분석", layout="wide")
st.title("☀️ 당일 실시간 수급 차트 (에러 프리 버전)")

token = get_access_token()

with st.sidebar:
    st.header("⚙️ 설정")
    stock_code = st.text_input("종목코드", value="417200")
    
    if token:
        broker_dict = get_broker_list(token)
        b_names = sorted(list(broker_dict.keys()))
        idx1 = next((i for i, n in enumerate(b_names) if "키움" in n), 0)
        idx2 = next((i for i, n in enumerate(b_names) if "신한" in n), 0)
        sel_brk1 = st.selectbox("🔎 창구 1", b_names, index=idx1)
        sel_brk2 = st.selectbox("🔎 창구 2", b_names, index=idx2)
        brk_cd1, brk_cd2 = broker_dict[sel_brk1], broker_dict[sel_brk2]
    
    auto_refresh = st.checkbox("🔄 1분 자동 갱신", value=True)

if auto_refresh:
    st_autorefresh(interval=60000, key="today_refresh")

if token and len(stock_code) == 6:
    with st.spinner("데이터 동기화 중..."):
        today = datetime.now().strftime('%Y%m%d')
        
        # 데이터 수집
        raw_chart = get_api_data(f"{host_url}/api/dostk/chart", "ka10080", token, {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"})
        raw_pg = get_api_data(f"{host_url}/api/dostk/mrkcond", "ka90008", token, {"amt_qty_tp": "2", "stk_cd": stock_code, "date": today})
        raw_br1 = get_api_data(f"{host_url}/api/dostk/stkinfo", "ka10052", token, {"mmcm_cd": brk_cd1, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"})
        raw_br2 = get_api_data(f"{host_url}/api/dostk/stkinfo", "ka10052", token, {"mmcm_cd": brk_cd2, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"})

        if not raw_chart:
            st.info("데이터가 없습니다. 장 시작 전이거나 코드를 확인하세요.")
            st.stop()

        # ⭐️ 안전한 데이터프레임 생성 및 컬럼 자동 매핑
        df = pd.DataFrame(raw_chart)
        
        # 시간 컬럼 찾기 (KeyError 방어)
        time_col = next((c for c in ['stk_cntr_tm', 'cntr_tm', 'tm', 'stck_cntg_hour'] if c in df.columns), None)
        if not time_col: 
            st.warning("시간 컬럼을 찾을 수 없습니다."); st.stop()

        df['dt'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S', errors='coerce')
        df = df[df['dt'].dt.strftime('%Y%m%d') == today].sort_values('dt')

        # 숫자 컬럼 변환
        num_cols = {'cur_prc':'현재가', 'open_pric':'시가', 'high_pric':'고가', 'low_pric':'저가', 'trde_qty':'거래량'}
        for eng, kor in num_cols.items():
            if eng in df.columns:
                df[eng] = pd.to_numeric(df[eng].astype(str).str.replace(r'[+,-]', '', regex=True), errors='coerce').fillna(0).astype(int)

        # ⭐️ 수급 계산 함수 (안전 모드)
        def calc_net(raw, is_pg=False):
            if not raw: return pd.Series(0, index=df.index)
            tdf = pd.DataFrame(raw)
            t_col = next((c for c in ['tm', 'stck_cntg_hour'] if c in tdf.columns), 'tm')
            tdf['dt'] = pd.to_datetime(today + tdf[t_col], format='%Y%m%d%H%M%S', errors='coerce').dt.floor('min')
            if is_pg:
                tdf['net'] = pd.to_numeric(tdf['prm_buy_qty']) - pd.to_numeric(tdf['prm_sell_qty'])
            else:
                tdf['net'] = pd.to_numeric(tdf['acc_netprps'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            return tdf.sort_values('dt').groupby('dt')['net'].last().reindex(df.index).ffill().fillna(0)

        df['Net_PG'] = calc_net(raw_pg, True)
        df['Net_Brk1'] = calc_net(raw_br1)
        df['Net_Brk2'] = calc_net(raw_br2)

        # ----------------------------------------------------
        # 4. 4단 차트 시각화
        # ----------------------------------------------------
        fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                           row_heights=[0.4, 0.2, 0.2, 0.2],
                           subplot_titles=("가격", "프로그램 누적", f"{sel_brk1} 누적", f"{sel_brk2} 누적"))

        fig.add_trace(go.Candlestick(x=df['dt'], open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격"), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_PG'], name="프로그램", line=dict(color='purple', width=2)), row=2, col=1)
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_Brk1'], name=sel_brk1, line=dict(color='blue', width=2)), row=3, col=1)
        fig.add_trace(go.Scatter(x=df['dt'], y=df['Net_Brk2'], name=sel_brk2, line=dict(color='red', width=2)), row=4, col=1)

        fig.update_layout(height=900, template='plotly_white', xaxis_rangeslider_visible=False, showlegend=False)
        fig.update_yaxes(tickformat=",")
        st.plotly_chart(fig, use_container_width=True)
