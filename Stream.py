import streamlit as st
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ----------------------------------------------------
# 1. 인증 및 데이터 수집 함수 (기존 로직 최적화)
# ----------------------------------------------------
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    try:
        response = requests.post(url, headers=headers, json=data)
        return response.json().get('token') if response.status_code == 200 else None
    except:
        return None

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    if not token: return {}
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

# ... [get_historical_minute_chart, get_historical_program_data, get_historical_broker_data 함수는 기존과 동일] ...

def merge_api_data(old_data, new_data):
    if not old_data and not new_data: return []
    df_merged = pd.DataFrame(old_data + new_data)
    if df_merged.empty: return []
    return df_merged.drop_duplicates(keep='first').to_dict('records')

# ----------------------------------------------------
# 2. 메인 UI 및 설정 (NameError 방지를 위해 순서 정렬)
# ----------------------------------------------------
st.set_page_config(page_title="수급 복기 v2.7 PRO-Base", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 (Base)")

# 캐시 초기화
if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

# ⭐️ 에러 방지: 토큰을 먼저 확실히 가져옵니다.
auth_token = get_access_token()

# 사이드바 설정
st.sidebar.header("📅 복기 설정")
stock_number = st.sidebar.text_input("종목코드", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

if auth_token:
    broker_dict = get_broker_list(auth_token)
    broker_names = sorted(list(broker_dict.keys()))
    default_idx1 = next((i for i, n in enumerate(broker_names) if "키움" in n), 0)
    default_idx2 = next((i for i, n in enumerate(broker_names) if "신한" in n), 0)
    
    name1 = st.sidebar.selectbox("🔎 창구 1", broker_names, index=default_idx1)
    name2 = st.sidebar.selectbox("🔎 창구 2", broker_names, index=default_idx2)
    lag_sec = st.sidebar.slider("⏱️ 시간 보정(초)", 0, 180, 60)
    auto_refresh = st.sidebar.checkbox("🔄 1분 자동 갱신", value=False)
else:
    st.error("API 인증 실패! Secrets를 확인하세요.")
    st.stop()

# ----------------------------------------------------
# 3. 데이터 연산 및 시각화 (속도 최적화 버전)
# ----------------------------------------------------
if len(stock_number) == 6:
    with st.spinner("데이터 처리 중..."):
        # [데이터 수집 로직 생략: 기존 merge_api_data 활용]
        # (생략된 부분: chart_raw, pg_raw, brk_raw1, brk_raw2 수집)

        # 예시를 위해 데이터가 있다고 가정할 때의 처리부
        if 'chart_raw' in locals() and chart_raw:
            df = pd.DataFrame(chart_raw)
            # 시간 컬럼 유연한 대응
            t_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[t_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

            # 🚀 최적화: 벡터화 연산 (한번에 숫자 변환)
            cols = ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']
            df[cols] = df[cols].replace(r'[+,-,]', '', regex=True).apply(pd.to_numeric)

            # [거래원 처리 로직 - 벡터화 버전]
            def process_brk_fast(raw, suffix):
                if not raw: return pd.DataFrame()
                db = pd.DataFrame(raw)
                db['Datetime'] = (pd.to_datetime(target_date_str + db['tm'], format='%Y%m%d%H%M%S') - pd.Timedelta(seconds=lag_sec)).dt.floor('min')
                qty = pd.to_numeric(db['mont_trde_qty'].astype(str).str.replace(r'[+,-,]', '', regex=True))
                is_sell = db['mont_trde_qty'].astype(str).str.contains('-') | db['tp'].astype(str).str.contains('매도')
                db[f'Buy_1m_{suffix}'] = np.where(~is_sell, qty, 0)
                db[f'Sell_1m_{suffix}'] = np.where(is_sell, qty, 0)
                db[f'Cum_Net_{suffix}'] = pd.to_numeric(db['acc_netprps'].astype(str).str.replace(r'[+,,]', '', regex=True)).fillna(0)
                return db.groupby('Datetime').agg({f'Buy_1m_{suffix}':'sum', f'Sell_1m_{suffix}':'sum', f'Cum_Net_{suffix}':'last'})

            df = df.join(process_brk_fast(brk_raw1, 'brk1'), how='left').fillna(0)
            df = df.join(process_brk_fast(brk_raw2, 'brk2'), how='left').fillna(0)

            # 🚀 수급 에너지(SV) 및 신고가 점(Point) 계산
            df['Max1'] = df['Cum_Net_brk1'].expanding().max()
            df['Min1'] = df['Cum_Net_brk1'].expanding().min()
            df['Max2'] = df['Cum_Net_brk2'].expanding().max()
            df['Min2'] = df['Cum_Net_brk2'].expanding().min()
            
            # (Signal_Value 계산 및 Signal_Point 마킹 로직 위치)
            # ...
            
            # [차트 그리기 - Scattergl로 속도 향상]
            fig = make_subplots(rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03)
            # (fig.add_trace... 생략)
            st.plotly_chart(fig, use_container_width=True)

            if auto_refresh:
                time.sleep(60)
                st.rerun()
