import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. URL은 숨길 필요가 없으므로 직접 입력 (모의투자 또는 실투자 URL)
host_url = "https://mockapi.kiwoom.com" # 또는 모의투자 URL

# 2. 내 진짜 키값은 Streamlit의 안전한 금고(secrets)에서 불러오기!
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 1. 인증 및 데이터 수집 함수
# ----------------------------------------------------
@st.cache_data(ttl=3000)
def get_access_token():
    # 토큰 발급 경로는 /oauth2/token 입니다.
    url = f"{host_url}/oauth2/token"
    
    # 🚨 중요: 키움 API는 api-id를 헤더에 넣어야 할 때가 있습니다.
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "au10001" 
    }
    
    data = {
        "grant_type": "client_credentials", 
        "appkey": app_key, 
        "secretkey": app_secret
    }
    
    response = requests.post(url, headers=headers, json=data, timeout=5)
    
    # 404가 계속 뜬다면, 혹시 host_url 끝에 /가 붙어있지는 않은지 확인해보세요.
    if response.status_code != 200:
        st.error(f"토큰 발급 실패! 상태 코드: {response.status_code}")
        # 만약 여전히 404라면, host_url을 "https://api.kiwoom.com"으로 바꿔서 시도해보세요.
        # (인증 서버는 실전/모의 공용일 수 있습니다.)
        return None
        
    return response.json().get('token')

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={}, timeout=5)
    data = res.json()
    broker_dict = {}
    if "list" in data:
        for item in data["list"]: 
            # ⭐️ 기존: broker_dict[item["name"]] = item["code"]
            # ⭐️ 수정: 화면에 보여줄 글자를 "신한투자증권(002)" 형태로 만듭니다.
            display_name = f"{item['name']}({item['code']})"
            broker_dict[display_name] = item["code"]
    return broker_dict

def get_historical_minute_chart(token, stock_code):
    url = f"{host_url}/api/dostk/chart"
    all_chart_data = []
    next_key = ""
    for i in range(5): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10080", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        data = {"stk_cd": stock_code, "tic_scope": "1", "upd_stkpc_tp": "1"}
        response = requests.post(url, headers=headers, json=data, timeout=5)
        res_json = response.json()
        chunk = res_json.get('stk_min_pole_chart_qry', [])
        if not chunk: break
        all_chart_data.extend(chunk)
        cont_yn = response.headers.get('cont-yn', response.headers.get('tr-cont', 'N'))
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if str(cont_yn).upper() not in ['Y', 'M'] or not next_key: break
        time.sleep(0.5) 
    return all_chart_data

# ----------------------------------------------------
# 강화된 데이터 수집 함수 (9시까지 끝까지 추적)
# ----------------------------------------------------

def get_historical_program_data(token, stock_code, target_date, max_pages=1500): # ⭐️ 1500페이지로 상향
    url = f"{host_url}/api/dostk/mrkcond"
    all_data = []
    next_key = ""
    retry_count = 0 
    
    for i in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        
        req_data = {"amt_qty_tp": "2", "stk_cd": stock_code, "date": target_date}
        response = requests.post(url, headers=headers, json=req_data, timeout=5)
        
        if response.status_code != 200:
            time.sleep(2) # ⭐️ 차단 회피를 위해 조금 더 쉽니다
            continue
            
        res_json = response.json()
        chunk = res_json.get('stk_tm_prm_trde_trnsn', [])
        
        if not chunk:
            retry_count += 1
            if retry_count > 3: break # 3번 연속 없으면 진짜 끝
            time.sleep(0.5)
            continue
        
        retry_count = 0
        all_data.extend(chunk)
        
        # 9시 도달 체크 (데이터가 09:00:00 이하로 내려가면 탈출)
        last_time = chunk[-1].get('tm', '')
        if last_time and last_time <= "090000":
            break
            
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1) 
    return all_data

def get_historical_broker_data(token, stock_code, brk_code, max_pages=1500): # ⭐️ 1500페이지로 상향
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    retry_count = 0
    
    for i in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        response = requests.post(url, headers=headers, json=req_data, timeout=5)
        
        if response.status_code != 200:
            time.sleep(2)
            continue
            
        res_json = response.json()
        chunk = res_json.get('trde_ori_mont_trde_qty', [])
        
        if not chunk:
            retry_count += 1
            if retry_count > 3: break
            time.sleep(0.5)
            continue
            
        retry_count = 0
        all_data.extend(chunk)
        
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time and last_time <= "090000":
            break
            
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.1)
    return all_data

# ==============================================================================
# ⭐️ 핵심 1. 증발 버그 해결: Pandas를 활용한 초고속 중복 방지 로직 (수정됨)
# ==============================================================================
def merge_api_data(old_data, new_data):
    # 과거 데이터와 새 데이터가 모두 비어있다면 빈 리스트를 반환합니다.
    if not old_data and not new_data:
        return []
    
    # 1. 두 데이터를 합쳐서 똑똑한 표(DataFrame) 형태로 만듭니다.
    df_merged = pd.DataFrame(old_data + new_data)
    
    if df_merged.empty:
        return []
        
    # 2. Pandas의 강력한 기능! 모든 내용이 똑같은 행을 단숨에 제거합니다.
    df_merged = df_merged.drop_duplicates(keep='first')
    
    # 3. 차트 그리는 곳에서 쓰기 좋게 다시 원래 형태(딕셔너리 리스트)로 되돌려줍니다.
    return df_merged.to_dict('records')

# ----------------------------------------------------
# 2. 메인 화면 및 차트
# ----------------------------------------------------
st.set_page_config(page_title="실시간 수급 복기 v2.5", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 대시보드")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

auth_token = get_access_token()

st.sidebar.header("📅 복기 설정")
stock_number = st.sidebar.text_input("종목코드 (예: 417200)", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

st.sidebar.markdown("---")
if auth_token:
    broker_dict = get_broker_list(auth_token)
    broker_names = list(broker_dict.keys())
    broker_names.sort() 
    
    # ⭐️ "키움증권(" 으로 시작하는 항목의 위치(인덱스)를 자동으로 찾습니다.
    default_idx1 = next((i for i, name in enumerate(broker_names) if name.startswith("키움증권(")), 0)
    selected_broker_name1 = st.sidebar.selectbox("🔎 첫 번째 창구", broker_names, index=default_idx1)
    target_broker_code1 = broker_dict[selected_broker_name1]

    # ⭐️ "신한투자증권(" 으로 시작하는 항목의 위치를 찾습니다.
    default_idx2 = next((i for i, name in enumerate(broker_names) if name.startswith("신한투자증권(")), 0)
    selected_broker_name2 = st.sidebar.selectbox("🔎 두 번째 창구", broker_names, index=default_idx2)
    target_broker_code2 = broker_dict[selected_broker_name2]
    
lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("🔄 1분 자동 갱신 (당일 실시간 모드)", value=False)
if auto_refresh and target_date_str != datetime.now().strftime('%Y%m%d'):
    st.sidebar.warning("⚠️ 과거 날짜를 볼 때는 자동 갱신을 끄는 것이 좋습니다.")

# ⭐️ 새로 추가된 부드러운 타이머 로직 ⭐️
if auto_refresh:
    # 60000 밀리초(60초)마다 화면을 알아서 새로고침하는 백그라운드 타이머 작동!
    st_autorefresh(interval=60000, limit=None, key="auto_refresh_timer")
    st.sidebar.success("✅ 실시간 자동 갱신 중... (화면 멈춤 없음)")

st.sidebar.markdown("---")
if st.sidebar.button("🧹 오전 데이터 누락 시 클릭 (캐시 삭제)"):
    # 캐시를 완전히 비워서 다음 실행 때 무조건 처음부터(500~1500페이지) 긁게 만듭니다.
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
    # 검색 키까지 초기화해서 완전히 새 종목처럼 인식하게 합니다.
    if 'last_search_key' in st.session_state:
        del st.session_state['last_search_key']
    st.rerun()

import concurrent.futures  # 파일 최상단에 추가되어 있는지 확인하세요

# ... (중략: 데이터 수집 시작 부분) ...

if auth_token and len(stock_number) == 6:
    with st.spinner(f"[{stock_number}] 데이터를 병렬로 초고속 수집 중..."):
        
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        
        # 1. 페이지 결정 (첫 로딩이면 500, 자동 갱신이면 3)
        is_first_load = 'last_search_key' not in st.session_state or st.session_state['last_search_key'] != current_search_key
        
        if is_first_load:
            fetch_p = 500  
            st.session_state['last_search_key'] = current_search_key
            st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
        else:
            fetch_p = 3    

# 🚀 2. 안전한 순차 수집 엔진 실행 (키움 API 1700 에러 차단 방지)
        
        # (1) 프로그램 수급 데이터 수집
        new_pg = get_historical_program_data(auth_token, stock_number, target_date_str, fetch_p)
        time.sleep(0.3) # ⭐️ 서버가 놀라지 않게 0.3초 쉬어줍니다.

        # (2) 첫 번째 창구 데이터 수집
        new_brk1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1, fetch_p)
        time.sleep(0.3) # ⭐️ 다시 0.3초 휴식

        # (3) 두 번째 창구 데이터 수집 (첫 번째 창구와 설정이 다를 때만 요청)
        if target_broker_code1 == target_broker_code2:
            new_brk2 = new_brk1 # 같으면 굳이 서버에 또 물어보지 않고 그대로 복사해서 씁니다.
        else:
            new_brk2 = get_historical_broker_data(auth_token, stock_number, target_broker_code2, fetch_p)
            time.sleep(0.3)
            

        # 3. 차트 데이터 수집
        chart_raw = get_historical_minute_chart(auth_token, stock_number)

        # 🚀 4. 변수 정의 및 캐시 업데이트 (에러 방지 핵심 구간)
        # 이 변수들이 아래쪽 if pg_raw: 등에서 사용됩니다.
        pg_raw = merge_api_data(st.session_state['data_cache']['pg'], new_pg)
        brk_raw1 = merge_api_data(st.session_state['data_cache']['brk1'], new_brk1)
        brk_raw2 = merge_api_data(st.session_state['data_cache']['brk2'], new_brk2)

        st.session_state['data_cache']['pg'] = pg_raw
        st.session_state['data_cache']['brk1'] = brk_raw1
        st.session_state['data_cache']['brk2'] = brk_raw2

        if chart_raw:
            df = pd.DataFrame(chart_raw)
            time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            
            # 지정한 날짜의 데이터만 남깁니다.
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

            if df.empty:
                st.info("⏳ 선택하신 날짜의 데이터가 아직 없거나 장 시작 대기 중입니다.")
                st.stop() # 🚨 더 이상 밑으로 내려가지 않고 여기서 안전하게 코드를 멈춥니다.
            # ==============================================================================
            
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = df[col].astype(str).str.replace('+', '', regex=False).str.replace('-', '', regex=False).str.replace(',', '', regex=False).astype(int)

            # --- 이 아래로 프로그램 데이터 처리(if pg_raw:) 로직 그대로 이어짐 ---

            if pg_raw:
                df_pg = pd.DataFrame(pg_raw)
                
                # ⭐️ [추가된 부분] 'tm' (시간) 컬럼이 제대로 들어왔는지 검사!
                if 'tm' in df_pg.columns and not df_pg.empty:
                    df_pg['Datetime'] = pd.to_datetime(target_date_str + df_pg['tm'], format='%Y%m%d%H%M%S').dt.floor('min')
                    def clean_num(s): return pd.to_numeric(s.astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
                    df_pg['Cum_Buy'] = clean_num(df_pg['prm_buy_qty'])
                    df_pg['Cum_Sell'] = clean_num(df_pg['prm_sell_qty'])
                    
                    df_pg = df_pg.sort_values('Datetime')
                    df_pg_min = df_pg.groupby('Datetime').agg({'Cum_Buy': 'last', 'Cum_Sell': 'last'})
                    df_pg_min['Buy_1m'] = df_pg_min['Cum_Buy'].diff().fillna(df_pg_min['Cum_Buy']).clip(lower=0)
                    df_pg_min['Sell_1m'] = df_pg_min['Cum_Sell'].diff().fillna(df_pg_min['Cum_Sell']).clip(lower=0)
                    df_pg_min['Cum_Net'] = df_pg_min['Cum_Buy'] - df_pg_min['Cum_Sell']
                    
                    df = df.join(df_pg_min[['Buy_1m', 'Sell_1m', 'Cum_Net']], how='left')
                    df['Cum_Net'] = df['Cum_Net'].ffill().fillna(0) 
                    df['Buy_1m'] = df['Buy_1m'].fillna(0)          
                    df['Sell_1m'] = df['Sell_1m'].fillna(0)
                else:
                    # ⭐️ 데이터가 이상하면 안전하게 0으로 채움
                    df['Buy_1m'] = 0; df['Sell_1m'] = 0; df['Cum_Net'] = 0
            else:
                df['Buy_1m'] = 0; df['Sell_1m'] = 0; df['Cum_Net'] = 0

            # ==============================================================================
            # ⭐️ 핵심 2. 키움증권 원본 데이터("매수", "매도") 완벽 판별기
            # ==============================================================================
            # ==============================================================================
            # ⭐️ 거래원 데이터 완벽 파싱 (기호 + 글자 이중 체크 방어선)
            # ==============================================================================
            def process_broker_data(raw_data, lag_sec, suffix):
                if not raw_data:
                    return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                    
                df_b = pd.DataFrame(raw_data)
                time_col_b = 'tm' if 'tm' in df_b.columns else 'stck_cntg_hour'
                if time_col_b not in df_b.columns:
                    return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                    
                df_b['Datetime_Raw'] = pd.to_datetime(target_date_str + df_b[time_col_b], format='%Y%m%d%H%M%S', errors='coerce')
                df_b['Datetime'] = df_b['Datetime_Raw'] - pd.Timedelta(seconds=lag_sec) 
                df_b['Datetime'] = df_b['Datetime'].dt.floor('min')
                
                if 'tp' in df_b.columns and 'mont_trde_qty' in df_b.columns:
                    # 1. 수량 기호(-)와 tp 글자('매도')를 동시에 체크하여 절대 누락 없게 만들기
                    def parse_volume(row):
                        tp_str = str(row['tp'])
                        qty_str = str(row['mont_trde_qty']).replace(',', '')
                        
                        # 수량에 '-'가 붙어있거나, tp에 '매도'라는 글자가 있으면 무조건 매도!
                        if '-' in qty_str or '매도' in tp_str:
                            sell = int(qty_str.replace('-', '').replace('+', '')) if qty_str else 0
                            return 0, sell
                        # 그 외에는 매수!
                        else:
                            buy = int(qty_str.replace('+', '').replace('-', '')) if qty_str else 0
                            return buy, 0

                    # 2. 이중 방어 로직 적용
                    df_b[['Buy_Vol', 'Sell_Vol']] = df_b.apply(parse_volume, axis=1, result_type='expand')
                    
                    if 'acc_netprps' in df_b.columns:
                        df_b['Net_Raw'] = pd.to_numeric(df_b['acc_netprps'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False), errors='coerce').fillna(0).astype(int)
                    else:
                        df_b['Net_Raw'] = 0
                        
                    # 1분 단위 합산
                    df_b_min = df_b.groupby('Datetime').agg({'Buy_Vol': 'sum', 'Sell_Vol': 'sum', 'Net_Raw': 'last'})
                    df_b_min.rename(columns={'Buy_Vol': f'Buy_1m_{suffix}', 'Sell_Vol': f'Sell_1m_{suffix}', 'Net_Raw': f'Cum_Net_{suffix}'}, inplace=True)
                    return df_b_min[[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}']]
                    
                else:
                    return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])

            df_brk1 = process_broker_data(brk_raw1, lag_seconds, 'brk1')
            df = df.join(df_brk1, how='left')
            df['Buy_1m_brk1'] = df['Buy_1m_brk1'].fillna(0)
            df['Sell_1m_brk1'] = df['Sell_1m_brk1'].fillna(0)
            df['Cum_Net_brk1'] = df['Cum_Net_brk1'].ffill().fillna(0)

            df_brk2 = process_broker_data(brk_raw2, lag_seconds, 'brk2')
            df = df.join(df_brk2, how='left')
            df['Buy_1m_brk2'] = df['Buy_1m_brk2'].fillna(0)
            df['Sell_1m_brk2'] = df['Sell_1m_brk2'].fillna(0)
            df['Cum_Net_brk2'] = df['Cum_Net_brk2'].ffill().fillna(0)

            # 동시호가 제거 (이 위쪽은 그대로 둡니다)
            mask_outliers = df.index.strftime('%H%M').isin(['0900', '1530'])
            df.loc[mask_outliers, ['trde_qty', 'Buy_1m', 'Sell_1m', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']] = 0

            # ==============================================================================
            # ⭐️ 1+3번 아이디어 결합 + 60이평 추가: 거래량 가중 평균 관여율 계산
            # ==============================================================================
            # 1. 프로그램 1분 총합 (매수 + 매도)
            df['PG_Total_1m'] = df['Buy_1m'] + df['Sell_1m']
            
            # 2. 1분봉 프로그램 관여율 (%)
            df['PG_Ratio_1m'] = (df['PG_Total_1m'] / df['trde_qty'].replace(0, pd.NA)).fillna(0) * 100
            
            # 3. 최근 20분 및 60분간의 전체 거래량 합산 & 프로그램 거래량 합산
            df['Vol_20m_Sum'] = df['trde_qty'].rolling(window=20, min_periods=1).sum()
            df['PG_20m_Sum'] = df['PG_Total_1m'].rolling(window=20, min_periods=1).sum()
            
            df['Vol_60m_Sum'] = df['trde_qty'].rolling(window=60, min_periods=1).sum() # ⭐️ 60분 거래량 추가
            df['PG_60m_Sum'] = df['PG_Total_1m'].rolling(window=60, min_periods=1).sum() # ⭐️ 60분 PG 추가
            
            # 4. 진짜 20분 & 60분 평균 관여율 (%)
            df['PG_Ratio_20m_True'] = (df['PG_20m_Sum'] / df['Vol_20m_Sum'].replace(0, pd.NA)).fillna(0) * 100
            df['PG_Ratio_60m_True'] = (df['PG_60m_Sum'] / df['Vol_60m_Sum'].replace(0, pd.NA)).fillna(0) * 100 # ⭐️ 60분 관여율 추가

# ==============================================================================
            # ⭐️ [신규 로직] 창구 교차 에너지 지표 계산
            # ==============================================================================
            # 1. 창구 1, 2의 실시간 Max/Min 계산
            df['Max1'] = df['Cum_Net_brk1'].expanding().max()
            df['Min1'] = df['Cum_Net_brk1'].expanding().min()
            df['Max2'] = df['Cum_Net_brk2'].expanding().max()
            df['Min2'] = df['Cum_Net_brk2'].expanding().min()

            # 2. 위치값(Pos) 계산
            df['Pos1'] = (df['Max1'] - df['Cum_Net_brk1']) - (df['Cum_Net_brk1'] - df['Min1'])
            df['Pos2'] = (df['Max2'] - df['Cum_Net_brk2']) - (df['Cum_Net_brk2'] - df['Min2'])

            # 3. 조건에 따른 Value 계산
            df['Signal_Value'] = 0.0
            mask1 = df['Pos1'] > df['Pos2']
            df.loc[mask1, 'Signal_Value'] = (df['Max1'] - df['Cum_Net_brk1']) + (df['Cum_Net_brk2'] - df['Min2'])
            df.loc[~mask1, 'Signal_Value'] = (df['Max2'] - df['Cum_Net_brk2']) + (df['Cum_Net_brk1'] - df['Min1'])

            # 4. 당일 최고치 경신 여부 확인 (신고가 갱신 시점에만 값 남기기)
            df['Value_Max'] = df['Signal_Value'].expanding().max()
            # 신고가를 경신하는 그 '순간'만 추출 (나머지는 NaN 처리해서 차트에 안 보이게 함)
            df['Signal_Point'] = df.apply(lambda x: x['Signal_Value'] if x['Signal_Value'] == x['Value_Max'] and x['Signal_Value'] > 0 else pd.NA, axis=1)
            # ==============================================================================
            # 📊 차트 그리기 (6단)
            # ==============================================================================
            fig = make_subplots(
                rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2], 
                subplot_titles=(
                    "가격 (한국식 컬러)", 
                    "거래량", 
                    "프로그램 수급", 
                    f"{selected_broker_name1} 수급", 
                    f"{selected_broker_name2} 수급",
                    "프로그램 관여율 (막대:1분, 선:20/60 가중평균)" # ⭐️ 제목 수정
                ),
                specs=[
                    [{"secondary_y": False}], 
                    [{"secondary_y": False}], 
                    [{"secondary_y": True}], 
                    [{"secondary_y": True}], 
                    [{"secondary_y": True}],
                    [{"secondary_y": True}]
                ] 
            )

            # 1층: 가격
            fig.add_trace(go.Candlestick(
                x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'],
                name="가격", increasing_line_color='#ff4d4d', increasing_fillcolor='#ff4d4d', decreasing_line_color='#0066ff', decreasing_fillcolor='#0066ff' 
            ), row=1, col=1)

            # 2층: 일반 거래량
            vol_colors = ['#ff4d4d' if c >= o else '#0066ff' for c, o in zip(df['cur_prc'], df['open_pric'])]
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color=vol_colors), row=2, col=1)
            
            # 3층: PG
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m'], name="PG 매수", marker_color='#ff4d4d', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m'], name="PG 매도", marker_color='#0066ff', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net'], mode='lines', name="PG 누적(우측)", line=dict(color='black', width=2.5)), row=3, col=1, secondary_y=True)

            # 4층: 창구 1
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk1'], name=f"{selected_broker_name1} 매수", marker_color='#ff4d4d', opacity=0.4), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk1'], name=f"{selected_broker_name1} 매도", marker_color='#0066ff', opacity=0.4), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], mode='lines', name=f"{selected_broker_name1} 누적", line=dict(color='black', width=2)), row=4, col=1, secondary_y=True)
            # ⭐️ 빨간선(점) 표시: 신호가 발생한 지점의 검정선 위에 빨간 점 찍기
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk1'] if not pd.isna(r['Signal_Point']) else pd.NA, axis=1), 
                                     mode='markers', name="신호(창구1)", marker=dict(color='red', size=6)), row=4, col=1, secondary_y=True)

            # 5층: 창구 2
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk2'], name=f"{selected_broker_name2} 매수", marker_color='#ff4d4d', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk2'], name=f"{selected_broker_name2} 매도", marker_color='#0066ff', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], mode='lines', name=f"{selected_broker_name2} 누적", line=dict(color='black', width=2)), row=5, col=1, secondary_y=True)
            # ⭐️ 빨간선(점) 표시: 신호가 발생한 지점의 검정선 위에 빨간 점 찍기
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk2'] if not pd.isna(r['Signal_Point']) else pd.NA, axis=1), 
                                     mode='markers', name="신호(창구2)", marker=dict(color='red', size=6)), row=5, col=1, secondary_y=True)

# ==============================================================================
            # ⭐️ [수정] 6층: 창구1 & 창구2 누적순매수 상관성 지표 계산 (20분 이동 상관계수)
            # ==============================================================================
            # 두 창구가 얼마나 비슷하게 움직이는지(커플링)를 나타냅니다.
            # 1에 가까우면 똑같이 사고 파는 것, -1에 가까우면 반대로 움직이는 것입니다.
            df['Brk_Correlation'] = df['Cum_Net_brk1'].rolling(window=20).corr(df['Cum_Net_brk2'])

            # ==============================================================================
            # 📊 차트 그리기 (6단) - 수정본
            # ==============================================================================
            fig = make_subplots(
                rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2], 
                subplot_titles=(
                    "가격 (한국식 컬러)", 
                    "거래량", 
                    "프로그램 수급", 
                    f"{selected_broker_name1} 수급", 
                    f"{selected_broker_name2} 수급",
                    "창구1 & 창구2 누적순매수 상관도 (20분 이동)" # ⭐️ 6층 제목 변경
                ),
                specs=[
                    [{"secondary_y": False}], 
                    [{"secondary_y": False}], 
                    [{"secondary_y": True}], 
                    [{"secondary_y": True}], 
                    [{"secondary_y": True}],
                    [{"secondary_y": False}] # ⭐️ 6층은 상관계수만 보므로 단일 축으로 설정
                ] 
            )

            # ... (1층 ~ 5층 코드는 기존과 동일하므로 생략) ...
            # 1층: 가격
            fig.add_trace(go.Candlestick(
                x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'],
                name="가격", increasing_line_color='#ff4d4d', increasing_fillcolor='#ff4d4d', decreasing_line_color='#0066ff', decreasing_fillcolor='#0066ff' 
            ), row=1, col=1)

            # 2층: 거래량
            vol_colors = ['#ff4d4d' if c >= o else '#0066ff' for c, o in zip(df['cur_prc'], df['open_pric'])]
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color=vol_colors), row=2, col=1)
            
            # 3층: PG
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m'], name="PG 매수", marker_color='#ff4d4d', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m'], name="PG 매도", marker_color='#0066ff', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net'], mode='lines', name="PG 누적(우측)", line=dict(color='black', width=2.5)), row=3, col=1, secondary_y=True)

            # 4층: 창구 1
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk1'], name=f"{selected_broker_name1} 매수", marker_color='#ff4d4d', opacity=0.4), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk1'], name=f"{selected_broker_name1} 매도", marker_color='#0066ff', opacity=0.4), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], mode='lines', name=f"{selected_broker_name1} 누적", line=dict(color='black', width=2)), row=4, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk1'] if not pd.isna(r['Signal_Point']) else pd.NA, axis=1), 
                                     mode='markers', name="신호(창구1)", marker=dict(color='red', size=6)), row=4, col=1, secondary_y=True)

            # 5층: 창구 2
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk2'], name=f"{selected_broker_name2} 매수", marker_color='#ff4d4d', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk2'], name=f"{selected_broker_name2} 매도", marker_color='#0066ff', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], mode='lines', name=f"{selected_broker_name2} 누적", line=dict(color='black', width=2)), row=5, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk2'] if not pd.isna(r['Signal_Point']) else pd.NA, axis=1), 
                                     mode='markers', name="신호(창구2)", marker=dict(color='red', size=6)), row=5, col=1, secondary_y=True)

            # ==============================================================================
            # ⭐️ [수정] 6층: 창구 상관성 시각화 (선 그래프)
            # ==============================================================================
            # 상관계수 선
            fig.add_trace(go.Scatter(
                x=df.index, y=df['Brk_Correlation'], 
                mode='lines', name="상관도(20분)", 
                line=dict(color='darkmagenta', width=2),
                fill='tozeroy' # 0을 기준으로 색을 채워 변화를 보기 쉽게 함
            ), row=6, col=1)

            # 0 기준선 (중립)
            fig.add_hline(y=0, line_dash="dash", line_color="gray", row=6, col=1)

            # 차트 레이아웃 업데이트
            fig.update_layout(height=1500, template='plotly_white', barmode='relative', hovermode='x unified', showlegend=False)
            fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor", spikecolor="gray", spikethickness=1, spikedash="dot")
            fig.update_layout(xaxis_rangeslider_visible=False)

            fig.update_yaxes(tickformat=",")
            # 6층 Y축 범위 고정 (-1 ~ 1)
            fig.update_yaxes(range=[-1.1, 1.1], row=6, col=1)

            st.plotly_chart(fig, use_container_width=True)
