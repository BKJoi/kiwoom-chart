import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. URL은 숨길 필요가 없으므로 직접 입력 (모의투자 또는 실투자 URL)
host_url = "https://openapi.kiwoom.com" # 또는 모의투자 URL

# 2. 내 진짜 키값은 Streamlit의 안전한 금고(secrets)에서 불러오기!
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 1. 인증 및 데이터 수집 함수
# ----------------------------------------------------
@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data)
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
        response = requests.post(url, headers=headers, json=data)
        res_json = response.json()
        chunk = res_json.get('stk_min_pole_chart_qry', [])
        if not chunk: break
        all_chart_data.extend(chunk)
        cont_yn = response.headers.get('cont-yn', response.headers.get('tr-cont', 'N'))
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if str(cont_yn).upper() not in ['Y', 'M'] or not next_key: break
        time.sleep(0.5) 
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
        
        # ⭐️ 핵심 방어 로직: 호출 제한(1700 에러 등)에 걸리면 종료하지 않고 3초 대기 후 재시도!
        if response.status_code != 200:
            time.sleep(3)
            continue
            
        res_json = response.json()
        chunk = res_json.get('stk_tm_prm_trde_trnsn', [])
        
        # 정상 응답인데 데이터가 비어있다면 진짜 끝난 것
        if not chunk: break
        
        all_data.extend(chunk)
        
        last_time = chunk[-1].get('tm', '')
        if last_time and last_time <= "090000":
            break
            
        cont_yn = response.headers.get('cont-yn', response.headers.get('tr-cont', 'N'))
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if str(cont_yn).upper() not in ['Y', 'M'] or not next_key: break
        
        time.sleep(0.5) 
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
        
        # ⭐️ 핵심 방어 로직: 차단 시 3초 쉬고 재시도
        if response.status_code != 200:
            time.sleep(3)
            continue
            
        res_json = response.json()
        chunk = res_json.get('trde_ori_mont_trde_qty', [])
        
        if not chunk: break
        
        all_data.extend(chunk)
        
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time and last_time <= "090000":
            break
            
        cont_yn = response.headers.get('cont-yn', response.headers.get('tr-cont', 'N'))
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if str(cont_yn).upper() not in ['Y', 'M'] or not next_key: break
        
        time.sleep(0.5) 
    return all_data
# ==============================================================================
# ⭐️ 핵심 1. 증발 버그 해결: 동시간대 중복 방지 로직 (데이터의 고유 지문 활용)
# ==============================================================================
def merge_api_data(old_data, new_data):
    seen = set()
    merged = []
    # 과거 데이터와 새 데이터를 더한 뒤, 텍스트 자체를 지문으로 써서 완벽한 녀석만 걸러냄
    for item in old_data + new_data:
        sig = str(item)
        if sig not in seen:
            seen.add(sig)
            merged.append(item)
    return merged

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
    
lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 130)

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("🔄 1분 자동 갱신 (당일 실시간 모드)", value=False)
if auto_refresh and target_date_str != datetime.now().strftime('%Y%m%d'):
    st.sidebar.warning("⚠️ 과거 날짜를 볼 때는 자동 갱신을 끄는 것이 좋습니다.")

if auth_token and len(stock_number) == 6: 
    with st.spinner(f"[{stock_number}] 수급 데이터를 수집 중입니다... (약 10~20초 소요)"):
        
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        if 'last_search_key' not in st.session_state or st.session_state['last_search_key'] != current_search_key:
            fetch_pages = 500 
            st.session_state['last_search_key'] = current_search_key
            st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
        else:
            fetch_pages = 3 
            
        chart_raw = get_historical_minute_chart(auth_token, stock_number) 
        new_pg = get_historical_program_data(auth_token, stock_number, target_date_str, max_pages=fetch_pages)
        
        new_brk1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1, max_pages=fetch_pages)
        if target_broker_code1 == target_broker_code2:
            new_brk2 = new_brk1 
        else:
            new_brk2 = get_historical_broker_data(auth_token, stock_number, target_broker_code2, max_pages=fetch_pages)
            
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
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()
            
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = df[col].astype(str).str.replace('+', '', regex=False).str.replace('-', '', regex=False).str.replace(',', '', regex=False).astype(int)

            if pg_raw:
                df_pg = pd.DataFrame(pg_raw)
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

            # 동시호가 제거
            mask_outliers = df.index.strftime('%H%M').isin(['0900', '1530'])
            df.loc[mask_outliers, ['trde_qty', 'Buy_1m', 'Sell_1m', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']] = 0

            # 📊 차트 그리기 (5단)
            fig = make_subplots(
                rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.3, 0.1, 0.2, 0.2, 0.2], 
                subplot_titles=(
                    "가격 (한국식 컬러)", 
                    "거래량", 
                    "프로그램 수급", 
                    f"{selected_broker_name1} 수급", 
                    f"{selected_broker_name2} 수급"
                ),
                specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]] 
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
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk1'], name=f"{selected_broker_name1} 매수", marker_color='#ff4d4d', opacity=0.7), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk1'], name=f"{selected_broker_name1} 매도", marker_color='#0066ff', opacity=0.7), row=4, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], mode='lines', name=f"{selected_broker_name1} 누적(우측)", line=dict(color='black', width=2.5)), row=4, col=1, secondary_y=True)

            # 5층: 창구 2
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk2'], name=f"{selected_broker_name2} 매수", marker_color='#ff4d4d', opacity=0.7), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk2'], name=f"{selected_broker_name2} 매도", marker_color='#0066ff', opacity=0.7), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], mode='lines', name=f"{selected_broker_name2} 누적(우측)", line=dict(color='black', width=2.5)), row=5, col=1, secondary_y=True)

            fig.update_layout(height=1300, template='plotly_white', barmode='relative', hovermode='x unified', showlegend=False)
            fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor", spikecolor="gray", spikethickness=1, spikedash="dot")
            fig.update_layout(xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)
            
            if auto_refresh:
                st.toast("⏳ 1분 뒤에 최신 수급을 다시 스캔합니다...")
                time.sleep(60)
                st.rerun()

        else:
            st.warning("데이터가 없거나 장 시작 전입니다.")