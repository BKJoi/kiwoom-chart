import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# 1. URL 및 키 설정 (Streamlit Secrets 사용)
host_url = "https://mockapi.kiwoom.com"
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 1. 인증 및 데이터 수집 함수
# ----------------------------------------------------
@st.cache_data(ttl=3000)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "au10001" 
    }
    data = {
        "grant_type": "client_credentials", 
        "appkey": app_key, 
        "secretkey": app_secret
    }
    try:
        response = requests.post(url, headers=headers, json=data, timeout=5)
        if response.status_code == 200:
            return response.json().get('token')
        else:
            st.error(f"토큰 발급 실패! 상태 코드: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"인증 서버 연결 오류: {e}")
        return None

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={}, timeout=5)
    data = res.json()
    broker_dict = {}
    if "list" in data:
        for item in data["list"]: 
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
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.2) 
    return all_chart_data

def get_historical_program_data(token, stock_code, target_date, max_pages=1500):
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
            time.sleep(1)
            continue
        res_json = response.json()
        chunk = res_json.get('stk_tm_prm_trde_trnsn', [])
        if not chunk:
            retry_count += 1
            if retry_count > 2: break
            continue
        retry_count = 0
        all_data.extend(chunk)
        if chunk[-1].get('tm', '') <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.05) 
    return all_data

def get_historical_broker_data(token, stock_code, brk_code, max_pages=1500):
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
            time.sleep(1)
            continue
        res_json = response.json()
        chunk = res_json.get('trde_ori_mont_trde_qty', [])
        if not chunk:
            retry_count += 1
            if retry_count > 2: break
            continue
        retry_count = 0
        all_data.extend(chunk)
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time and last_time <= "090000": break
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.05)
    return all_data

def merge_api_data(old_data, new_data):
    if not old_data and not new_data: return []
    df_merged = pd.DataFrame(old_data + new_data)
    if df_merged.empty: return []
    df_merged = df_merged.drop_duplicates(keep='first')
    return df_merged.to_dict('records')

# ----------------------------------------------------
# 2. 메인 화면 및 사이드바 설정
# ----------------------------------------------------
st.set_page_config(page_title="실시간 수급 복기 v2.6", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 대시보드")

if 'data_cache' not in st.session_state:
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}

auth_token = get_access_token()

st.sidebar.header("📅 복기 설정")
stock_number = st.sidebar.text_input("종목코드 (예: 417200)", value="417200")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

if auth_token:
    broker_dict = get_broker_list(auth_token)
    broker_names = sorted(list(broker_dict.keys()))
    
    default_idx1 = next((i for i, name in enumerate(broker_names) if name.startswith("키움증권(")), 0)
    selected_broker_name1 = st.sidebar.selectbox("🔎 첫 번째 창구", broker_names, index=default_idx1)
    target_broker_code1 = broker_dict[selected_broker_name1]

    default_idx2 = next((i for i, name in enumerate(broker_names) if name.startswith("신한투자증권(")), 0)
    selected_broker_name2 = st.sidebar.selectbox("🔎 두 번째 창구", broker_names, index=default_idx2)
    target_broker_code2 = broker_dict[selected_broker_name2]

lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)
auto_refresh = st.sidebar.checkbox("🔄 1분 자동 갱신 (당일 실시간 모드)", value=False)

if auto_refresh:
    st_autorefresh(interval=60000, key="auto_refresh_timer")

if st.sidebar.button("🧹 캐시 삭제 및 새로고침"):
    st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
    if 'last_search_key' in st.session_state: del st.session_state['last_search_key']
    st.rerun()

# ----------------------------------------------------
# 3. 데이터 로드 및 가공
# ----------------------------------------------------
if auth_token and len(stock_number) == 6:
    with st.spinner(f"[{stock_number}] 데이터 분석 중..."):
        current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
        is_first_load = 'last_search_key' not in st.session_state or st.session_state['last_search_key'] != current_search_key
        
        if is_first_load:
            fetch_p = 500
            st.session_state['last_search_key'] = current_search_key
            st.session_state['data_cache'] = {'pg': [], 'brk1': [], 'brk2': []}
        else:
            fetch_p = 5

        # 데이터 수집 (순차적)
        new_pg = get_historical_program_data(auth_token, stock_number, target_date_str, fetch_p)
        time.sleep(0.2)
        new_brk1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1, fetch_p)
        time.sleep(0.2)
        new_brk2 = new_brk1 if target_broker_code1 == target_broker_code2 else get_historical_broker_data(auth_token, stock_number, target_broker_code2, fetch_p)
        chart_raw = get_historical_minute_chart(auth_token, stock_number)

        # 캐시 머지
        pg_raw = merge_api_data(st.session_state['data_cache']['pg'], new_pg)
        brk_raw1 = merge_api_data(st.session_state['data_cache']['brk1'], new_brk1)
        brk_raw2 = merge_api_data(st.session_state['data_cache']['brk2'], new_brk2)
        st.session_state['data_cache'].update({'pg': pg_raw, 'brk1': brk_raw1, 'brk2': brk_raw2})

        if chart_raw:
            df = pd.DataFrame(chart_raw)
            time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

            if df.empty:
                st.info("해당 날짜의 장 중 데이터가 없습니다.")
                st.stop()

            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = df[col].astype(str).str.replace(r'[+,-]', '', regex=True).replace(',', '', regex=True).astype(int)

            # --- 프로그램 수급 가공 ---
            if pg_raw:
                df_pg = pd.DataFrame(pg_raw)
                df_pg['Datetime'] = pd.to_datetime(target_date_str + df_pg['tm'], format='%Y%m%d%H%M%S').dt.floor('min')
                def clean_num(s): return pd.to_numeric(s.astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
                df_pg['Cum_Buy'] = clean_num(df_pg['prm_buy_qty'])
                df_pg['Cum_Sell'] = clean_num(df_pg['prm_sell_qty'])
                df_pg_min = df_pg.sort_values('Datetime').groupby('Datetime').agg({'Cum_Buy': 'last', 'Cum_Sell': 'last'})
                df_pg_min['Buy_1m'] = df_pg_min['Cum_Buy'].diff().fillna(df_pg_min['Cum_Buy']).clip(lower=0)
                df_pg_min['Sell_1m'] = df_pg_min['Cum_Sell'].diff().fillna(df_pg_min['Cum_Sell']).clip(lower=0)
                df_pg_min['Cum_Net'] = df_pg_min['Cum_Buy'] - df_pg_min['Cum_Sell']
                df = df.join(df_pg_min[['Buy_1m', 'Sell_1m', 'Cum_Net']], how='left')
            
            # --- 창구 데이터 가공 함수 ---
            def process_broker_df(raw_data, lag_sec, suffix):
                if not raw_data: return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                db = pd.DataFrame(raw_data)
                t_col = 'tm' if 'tm' in db.columns else 'stck_cntg_hour'
                db['Datetime'] = (pd.to_datetime(target_date_str + db[t_col], format='%Y%m%d%H%M%S', errors='coerce') - pd.Timedelta(seconds=lag_sec)).dt.floor('min')
                def parse_v(row):
                    tp, qty = str(row['tp']), str(row['mont_trde_qty']).replace(',', '')
                    val = int(qty.replace('-', '').replace('+', '')) if qty else 0
                    return (0, val) if ('-' in qty or '매도' in tp) else (val, 0)
                db[['B_V', 'S_V']] = db.apply(parse_v, axis=1, result_type='expand')
                db['Net_R'] = pd.to_numeric(db['acc_netprps'].astype(str).str.replace(r'[+,]', '', regex=True), errors='coerce').fillna(0).astype(int)
                return db.groupby('Datetime').agg({'B_V': 'sum', 'S_V': 'sum', 'Net_R': 'last'}).rename(columns={'B_V': f'Buy_1m_{suffix}', 'S_V': f'Sell_1m_{suffix}', 'Net_R': f'Cum_Net_{suffix}'})

            df = df.join(process_broker_df(brk_raw1, lag_seconds, 'brk1'), how='left')
            df = df.join(process_broker_df(brk_raw2, lag_seconds, 'brk2'), how='left')
            
            # 결측치 및 동시호가 처리
            for c in ['Buy_1m', 'Sell_1m', 'Cum_Net', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Cum_Net_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2', 'Cum_Net_brk2']:
                if c not in df.columns: df[c] = 0
                df[c] = df[c].ffill().fillna(0)
            
            mask_o = df.index.strftime('%H%M').isin(['0900', '1530'])
            df.loc[mask_o, ['trde_qty', 'Buy_1m', 'Sell_1m', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']] = 0

            # ⭐️ [핵심] 6층: 창구 간 순매수 격차 계산
            df['Brk_Net_Gap'] = df['Cum_Net_brk1'] - df['Cum_Net_brk2']

            # 신규 로직: 창구 교차 에너지 (신호 포인트)
            df['Max1'], df['Min1'] = df['Cum_Net_brk1'].expanding().max(), df['Cum_Net_brk1'].expanding().min()
            df['Max2'], df['Min2'] = df['Cum_Net_brk2'].expanding().max(), df['Cum_Net_brk2'].expanding().min()
            df['Pos1'] = (df['Max1'] - df['Cum_Net_brk1']) - (df['Cum_Net_brk1'] - df['Min1'])
            df['Pos2'] = (df['Max2'] - df['Cum_Net_brk2']) - (df['Cum_Net_brk2'] - df['Min2'])
            df['Sig_V'] = 0.0
            m1 = df['Pos1'] > df['Pos2']
            df.loc[m1, 'Sig_V'] = (df['Max1'] - df['Cum_Net_brk1']) + (df['Cum_Net_brk2'] - df['Min2'])
            df.loc[~m1, 'Sig_V'] = (df['Max2'] - df['Cum_Net_brk2']) + (df['Cum_Net_brk1'] - df['Min1'])
            df['Sig_P'] = df.apply(lambda x: x['Sig_V'] if x['Sig_V'] == x['Sig_V'].expanding().max() and x['Sig_V'] > 0 else pd.NA, axis=1)

            # ----------------------------------------------------
            # 4. 차트 그리기 (6단)
            # ----------------------------------------------------
            fig = make_subplots(
                rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.25, 0.1, 0.15, 0.15, 0.15, 0.2], 
                subplot_titles=("가격", "거래량", "프로그램 수급", f"{selected_broker_name1}", f"{selected_broker_name2}", f"수급 격차 ({selected_broker_name1}-{selected_broker_name2})"),
                specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": False}]]
            )

            # 1층: 가격 (캔들)
            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격", increasing_line_color='#ff4d4d', decreasing_line_color='#0066ff'), row=1, col=1)

            # 2층: 거래량
            v_cols = ['#ff4d4d' if c >= o else '#0066ff' for c, o in zip(df['cur_prc'], df['open_pric'])]
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color=v_cols), row=2, col=1)

            # 3층: PG
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m'], name="PG매수", marker_color='#ff4d4d', opacity=0.6), row=3, col=1)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m'], name="PG매도", marker_color='#0066ff', opacity=0.6), row=3, col=1)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net'], name="PG누적", line=dict(color='black', width=2)), row=3, col=1, secondary_y=True)

            # 4층: 창구1
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk1'], name="매수", marker_color='#ff4d4d', opacity=0.4), row=4, col=1)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk1'], name="매도", marker_color='#0066ff', opacity=0.4), row=4, col=1)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], name="누적", line=dict(color='black', width=1.5)), row=4, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk1'] if not pd.isna(r['Sig_P']) else pd.NA, axis=1), mode='markers', marker=dict(color='red', size=6), name="신호"), row=4, col=1, secondary_y=True)

            # 5층: 창구2
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk2'], name="매수", marker_color='#ff4d4d', opacity=0.4), row=5, col=1)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk2'], name="매도", marker_color='#0066ff', opacity=0.4), row=5, col=1)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], name="누적", line=dict(color='black', width=1.5)), row=5, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df.apply(lambda r: r['Cum_Net_brk2'] if not pd.isna(r['Sig_P']) else pd.NA, axis=1), mode='markers', marker=dict(color='red', size=6), name="신호"), row=5, col=1, secondary_y=True)

            # 6층: 수급 격차 (영역형)
            fig.add_trace(go.Scatter(
                x=df.index, y=df['Brk_Net_Gap'], mode='lines', name="격차", 
                line=dict(color='darkslategray', width=2), fill='tozeroy', fillcolor='rgba(0, 128, 128, 0.2)'
            ), row=6, col=1)
            fig.add_hline(y=0, line_dash="solid", line_color="black", row=6, col=1)

            fig.update_layout(height=1400, template='plotly_white', hovermode='x unified', showlegend=False, xaxis_rangeslider_visible=False)
            fig.update_yaxes(tickformat=",")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("데이터 수집에 실패했습니다. 종목코드나 API 상태를 확인하세요.")
