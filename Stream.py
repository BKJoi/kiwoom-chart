import streamlit as st
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh
import concurrent.futures 

# 1. URL은 숨길 필요가 없으므로 직접 입력
host_url = "https://mockapi.kiwoom.com"

# 2. 내 진짜 키값은 Streamlit의 안전한 금고(secrets)에서 불러오기!
app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]

# ----------------------------------------------------
# 1. 인증 및 통합 초고속 수집 함수
# ----------------------------------------------------
@st.cache_data(ttl=3000)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data, timeout=5)
    if response.status_code != 200:
        st.error(f"토큰 발급 실패! 상태 코드: {response.status_code}")
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
            display_name = f"{item['name']}({item['code']})"
            broker_dict[display_name] = item["code"]
    return broker_dict

@st.cache_data(ttl=86400)
def get_daily_program_avg(token, stock_code, target_date):
    url = f"{host_url}/api/dostk/mrkcond"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90013", "authorization": f"Bearer {token}"}
    req_data = {"stk_cd": stock_code, "date": target_date, "amt_qty_tp": "2"}
    res = requests.post(url, headers=headers, json=req_data, timeout=5)
    if res.status_code == 200:
        data_list = res.json().get('stk_daly_prm_trde_trnsn', [])
        vols = []
        for item in data_list:
            if item.get('dt', '') < target_date:
                buy = abs(int(str(item.get("prm_buy_qty", "0")).replace("-", "").replace("+", "").replace(",", "") or 0))
                sell = abs(int(str(item.get("prm_sell_qty", "0")).replace("-", "").replace("+", "").replace(",", "") or 0))
                vols.append(buy + sell)
                if len(vols) == 10: break
        if vols: return sum(vols) / len(vols)
    return 0

# 💡 모든 데이터를 이 함수 하나로 초고속 처리합니다.
def get_historical_data_generic(token, api_id, url_path, req_data, max_pages):
    url = f"{host_url}/api/dostk/{url_path}"
    all_data = []
    next_key = ""
    for _ in range(max_pages):
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": api_id, "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        response = requests.post(url, headers=headers, json=req_data, timeout=5)
        if response.status_code != 200: break
        
        res_json = response.json()
        key = 'stk_min_pole_chart_qry' if api_id == 'ka10080' else ('stk_tm_prm_trde_trnsn' if api_id == 'ka90008' else 'trde_ori_mont_trde_qty')
        chunk = res_json.get(key, [])
        if not chunk: break
        
        all_data.extend(chunk)
        
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        time.sleep(0.05) # 딜레이 최소화
    return all_data

# 💡 [핵심 패치] 시간에 기반한 완벽한 덮어쓰기 병합 (데이터 꼬임 원천 차단)
def sync_data(old_data, new_data):
    if not old_data and not new_data: return []
    # 최신 데이터(new)를 먼저 배치하여 중복 시 최신본이 살아남게 함
    df = pd.DataFrame(new_data + old_data) 
    if df.empty: return []
    
    # API마다 시간 컬럼 이름이 다르므로 자동 탐지
    t_col = next((c for c in ['stk_cntr_tm', 'tm', 'stck_cntg_hour'] if c in df.columns), None)
    if t_col:
        # 시간 단위로 겹치면 과거 데이터 버리고 덮어쓰기! (방탄 로직)
        df = df.drop_duplicates(subset=[t_col], keep='first')
    else:
        df = df.drop_duplicates(keep='first')
        
    return df.to_dict('records')

# ----------------------------------------------------
# 2. 메인 화면 및 설정
# ----------------------------------------------------
st.set_page_config(page_title="초고속 수급 복기 v3.2", layout="wide")
st.title("🚀 실시간 주도주 & 거래원 수급 복기 대시보드 (v3.2 - 방탄 캐시)")

# 💡 [안전] 다른 앱과 절대 충돌하지 않도록 고유한 캐시(v3_data_cache) 이름 사용
if 'v3_data_cache' not in st.session_state:
    st.session_state['v3_data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}

auth_token = get_access_token()

with st.sidebar:
    st.header("📅 복기 설정")
    stock_number = st.text_input("종목코드 (예: 417200)", value="417200")
    selected_date = st.date_input("날짜 선택", datetime.now())
    target_date_str = selected_date.strftime('%Y%m%d')

    st.markdown("---")
    if auth_token:
        broker_dict = get_broker_list(auth_token)
        broker_names = sorted(list(broker_dict.keys())) 
        
        default_idx1 = next((i for i, name in enumerate(broker_names) if name.startswith("키움증권(")), 0)
        selected_broker_name1 = st.selectbox("🔎 첫 번째 창구", broker_names, index=default_idx1)
        target_broker_code1 = broker_dict[selected_broker_name1]

        default_idx2 = next((i for i, name in enumerate(broker_names) if name.startswith("신한투자증권(")), 0)
        selected_broker_name2 = st.selectbox("🔎 두 번째 창구", broker_names, index=default_idx2)
        target_broker_code2 = broker_dict[selected_broker_name2]
        
    lag_seconds = st.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)

    st.markdown("---")
    auto_refresh = st.checkbox("🔄 1분 자동 갱신 (당일 실시간 모드)", value=False)
    if auto_refresh and target_date_str != datetime.now().strftime('%Y%m%d'):
        st.warning("⚠️ 과거 날짜를 볼 때는 자동 갱신을 끄는 것이 좋습니다.")

    if st.button("🧹 전체 데이터 강제 초기화 (캐시 삭제)"):
        st.session_state['v3_data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}
        if 'v3_last_key' in st.session_state: del st.session_state['v3_last_key']
        st.rerun()

if auto_refresh:
    st_autorefresh(interval=60000, limit=None, key="auto_refresh_timer")
    st.sidebar.success("✅ 실시간 방탄 캐시 갱신 중... (속도와 안정성 100%)")

if auth_token and len(stock_number) == 6:
    current_search_key = f"{stock_number}_{target_date_str}_{target_broker_code1}_{target_broker_code2}"
    is_first_load = 'v3_last_key' not in st.session_state or st.session_state['v3_last_key'] != current_search_key
    
    if is_first_load:
        fetch_p = 500  
        st.session_state['v3_last_key'] = current_search_key
        st.session_state['v3_data_cache'] = {'pg': [], 'brk1': [], 'brk2': [], 'chart': []}
    else:
        fetch_p = 2 # 갱신 시에는 2페이지만 초고속으로!

    with st.spinner(f"[{stock_number}] 데이터 증분 동기화 중..."):
        new_pg = get_historical_data_generic(auth_token, 'ka90008', 'mrkcond', {"amt_qty_tp": "2", "stk_cd": stock_number, "date": target_date_str}, fetch_p)
        new_brk1 = get_historical_data_generic(auth_token, 'ka10052', 'stkinfo', {"mmcm_cd": target_broker_code1, "stk_cd": stock_number, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}, fetch_p)
        new_brk2 = new_brk1 if target_broker_code1 == target_broker_code2 else get_historical_data_generic(auth_token, 'ka10052', 'stkinfo', {"mmcm_cd": target_broker_code2, "stk_cd": stock_number, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}, fetch_p)
        new_chart = get_historical_data_generic(auth_token, 'ka10080', 'chart', {"stk_cd": stock_number, "tic_scope": "1", "upd_stkpc_tp": "1"}, fetch_p if is_first_load else 2)

        avg_10d_pg_vol = get_daily_program_avg(auth_token, stock_number, target_date_str)

        # 방탄 로직으로 캐시 업데이트
        st.session_state['v3_data_cache']['pg'] = sync_data(st.session_state['v3_data_cache']['pg'], new_pg)
        st.session_state['v3_data_cache']['brk1'] = sync_data(st.session_state['v3_data_cache']['brk1'], new_brk1)
        st.session_state['v3_data_cache']['brk2'] = sync_data(st.session_state['v3_data_cache']['brk2'], new_brk2)
        st.session_state['v3_data_cache']['chart'] = sync_data(st.session_state['v3_data_cache']['chart'], new_chart)

        # ----------------------------------------------------
        # 3. 데이터 가공 및 차트 렌더링
        # ----------------------------------------------------
        chart_raw = st.session_state['v3_data_cache']['chart']
        if chart_raw:
            df = pd.DataFrame(chart_raw)
            time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

            if df.empty:
                st.info("⏳ 선택하신 날짜의 데이터가 아직 없거나 장 시작 대기 중입니다.")
                st.stop() 
            
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = df[col].astype(str).str.replace('+', '', regex=False).str.replace('-', '', regex=False).str.replace(',', '', regex=False).astype(int)

            pg_raw = st.session_state['v3_data_cache']['pg']
            if pg_raw:
                df_pg = pd.DataFrame(pg_raw)
                if 'tm' in df_pg.columns and not df_pg.empty:
                    df_pg['Full_Time'] = pd.to_datetime(target_date_str + df_pg['tm'], format='%Y%m%d%H%M%S')
                    df_pg['Datetime'] = df_pg['Full_Time'].dt.floor('min')
                    
                    def clean_num(s): return pd.to_numeric(s.astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
                    df_pg['Cum_Buy'] = clean_num(df_pg['prm_buy_qty'])
                    df_pg['Cum_Sell'] = clean_num(df_pg['prm_sell_qty'])
                    df_pg = df_pg.sort_values('Full_Time')
                    
                    df_pg_min = df_pg.groupby('Datetime').agg({'Cum_Buy': 'last', 'Cum_Sell': 'last'})
                    all_minutes = pd.date_range(start=f"{target_date_str} 0900", end=f"{target_date_str} 1530", freq='min')
                    df_pg_min = df_pg_min.reindex(all_minutes).ffill().fillna(0)
                    
                    df_pg_min['Buy_1m'] = df_pg_min['Cum_Buy'].diff().fillna(0).clip(lower=0)
                    df_pg_min['Sell_1m'] = df_pg_min['Cum_Sell'].diff().fillna(0).clip(lower=0)
                    df_pg_min['Cum_Net'] = df_pg_min['Cum_Buy'] - df_pg_min['Cum_Sell']
                    
                    df = df.join(df_pg_min[['Buy_1m', 'Sell_1m', 'Cum_Net']], how='left')
                    df['Cum_Net'] = df['Cum_Net'].ffill().fillna(0)
                    df['Buy_1m'] = df['Buy_1m'].fillna(0)
                    df['Sell_1m'] = df['Sell_1m'].fillna(0)

                    # 💡 4층: PG 2.0 이상 신기록 타점
                    if avg_10d_pg_vol > 0:
                        df['PG_1m_Total'] = df['Buy_1m'] + df['Sell_1m']
                        df['PG_Anomaly_Pct'] = (df['PG_1m_Total'] / avg_10d_pg_vol) * 100
                        
                        anomaly_dots, anomaly_texts = [], []
                        max_pct = 0.0
                        for val in df['PG_Anomaly_Pct']:
                            if pd.notna(val) and val >= 2.0 and val > max_pct:
                                max_pct = val
                                anomaly_dots.append(val); anomaly_texts.append(f"{val:.1f}")
                            else:
                                anomaly_dots.append(pd.NA); anomaly_texts.append("")
                        df['Anomaly_Dot'], df['Anomaly_Text'] = anomaly_dots, anomaly_texts
                    else:
                        df['Anomaly_Dot'], df['Anomaly_Text'] = pd.NA, ""
                else:
                    df['Buy_1m'] = 0; df['Sell_1m'] = 0; df['Cum_Net'] = 0
                    df['Anomaly_Dot'], df['Anomaly_Text'] = pd.NA, ""
            else:
                df['Buy_1m'] = 0; df['Sell_1m'] = 0; df['Cum_Net'] = 0
                df['Anomaly_Dot'], df['Anomaly_Text'] = pd.NA, ""

            def process_broker_data(raw_data, lag_sec, suffix):
                if not raw_data: return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                df_b = pd.DataFrame(raw_data)
                time_col_b = 'tm' if 'tm' in df_b.columns else 'stck_cntg_hour'
                if time_col_b not in df_b.columns: return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])
                
                df_b['Datetime_Raw'] = pd.to_datetime(target_date_str + df_b[time_col_b], format='%Y%m%d%H%M%S', errors='coerce')
                df_b['Datetime'] = df_b['Datetime_Raw'] - pd.Timedelta(seconds=lag_sec) 
                df_b['Datetime'] = df_b['Datetime'].dt.floor('min')
                
                if 'tp' in df_b.columns and 'mont_trde_qty' in df_b.columns:
                    def parse_volume(row):
                        tp_str = str(row['tp'])
                        qty_str = str(row['mont_trde_qty']).replace(',', '')
                        if '-' in qty_str or '매도' in tp_str:
                            sell = int(qty_str.replace('-', '').replace('+', '')) if qty_str else 0
                            return 0, sell
                        else:
                            buy = int(qty_str.replace('+', '').replace('-', '')) if qty_str else 0
                            return buy, 0
                    df_b[['Buy_Vol', 'Sell_Vol']] = df_b.apply(parse_volume, axis=1, result_type='expand')
                    if 'acc_netprps' in df_b.columns:
                        df_b['Net_Raw'] = pd.to_numeric(df_b['acc_netprps'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False), errors='coerce').fillna(0).astype(int)
                    else: df_b['Net_Raw'] = 0
                        
                    df_b_min = df_b.groupby('Datetime').agg({'Buy_Vol': 'sum', 'Sell_Vol': 'sum', 'Net_Raw': 'last'})
                    df_b_min.rename(columns={'Buy_Vol': f'Buy_1m_{suffix}', 'Sell_Vol': f'Sell_1m_{suffix}', 'Net_Raw': f'Cum_Net_{suffix}'}, inplace=True)
                    return df_b_min[[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}']]
                else: return pd.DataFrame(columns=[f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}'])

            df_brk1 = process_broker_data(st.session_state['v3_data_cache']['brk1'], lag_seconds, 'brk1')
            df = df.join(df_brk1, how='left')
            for c in ['Buy_1m_brk1', 'Sell_1m_brk1']: df[c] = df[c].fillna(0).astype(float)
            df['Cum_Net_brk1'] = df['Cum_Net_brk1'].ffill().fillna(0).astype(float)

            df_brk2 = process_broker_data(st.session_state['v3_data_cache']['brk2'], lag_seconds, 'brk2')
            df = df.join(df_brk2, how='left')
            for c in ['Buy_1m_brk2', 'Sell_1m_brk2']: df[c] = df[c].fillna(0).astype(float)
            df['Cum_Net_brk2'] = df['Cum_Net_brk2'].ffill().fillna(0).astype(float)

            mask_outliers = df.index.strftime('%H%M').isin(['0900', '1530'])
            df.loc[mask_outliers, ['trde_qty', 'Buy_1m', 'Sell_1m', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']] = 0

            diff_brk1, diff_brk2 = df['Cum_Net_brk1'].diff(), df['Cum_Net_brk2'].diff()
            df['Red_Dot_1'] = df['Cum_Net_brk1'].where((diff_brk1 < 0) & (diff_brk2 > 0), pd.NA)
            df['Red_Dot_2'] = df['Cum_Net_brk2'].where((diff_brk1 < 0) & (diff_brk2 > 0), pd.NA)
            df['Blue_Dot_1'] = df['Cum_Net_brk1'].where((diff_brk1 > 0) & (diff_brk2 < 0), pd.NA)
            df['Blue_Dot_2'] = df['Cum_Net_brk2'].where((diff_brk1 > 0) & (diff_brk2 < 0), pd.NA)

            # ==============================================================================
            # 📊 차트 그리기 (6단 레이아웃)
            # ==============================================================================
            fig = make_subplots(
                rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.25, 0.1, 0.15, 0.1, 0.2, 0.2], 
                subplot_titles=("가격", "거래량", "프로그램 수급", "🚨 프로그램 1분 폭발 (평균치 2.0 이상 & 신기록)", f"{selected_broker_name1} 수급", f"{selected_broker_name2} 수급"),
                specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": True}]] 
            )

            fig.add_trace(go.Candlestick(x=df.index, open=df['open_pric'], high=df['high_pric'], low=df['low_pric'], close=df['cur_prc'], name="가격", increasing_line_color='#ff4d4d', increasing_fillcolor='#ff4d4d', decreasing_line_color='#0066ff', decreasing_fillcolor='#0066ff'), row=1, col=1)
            vol_colors = ['#ff4d4d' if c >= o else '#0066ff' for c, o in zip(df['cur_prc'], df['open_pric'])]
            fig.add_trace(go.Bar(x=df.index, y=df['trde_qty'], name="거래량", marker_color=vol_colors), row=2, col=1)
            
            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m'], name="PG 매수", marker_color='#ff4d4d', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m'], name="PG 매도", marker_color='#0066ff', opacity=0.7), row=3, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net'], mode='lines', name="PG 누적", line=dict(color='black', width=2.5)), row=3, col=1, secondary_y=True)

            if 'Anomaly_Dot' in df.columns:
                fig.add_trace(go.Scatter(x=df.index, y=df['Anomaly_Dot'], mode='markers+text', text=df['Anomaly_Text'], textposition='top center', name="신기록 폭발타점", marker=dict(color='red', size=8, symbol='circle'), textfont=dict(color='red', size=12, weight='bold')), row=4, col=1)
                fig.add_hline(y=2.0, line_dash="dot", line_color="orange", annotation_text="2.0 컷오프", row=4, col=1)

            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk1'], marker_color='#ff4d4d', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk1'], marker_color='#0066ff', opacity=0.4), row=5, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], line=dict(color='black', width=2)), row=5, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df['Red_Dot_1'], mode='markers', marker=dict(color='red', size=8)), row=5, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df['Blue_Dot_1'], mode='markers', marker=dict(color='blue', size=8)), row=5, col=1, secondary_y=True)

            fig.add_trace(go.Bar(x=df.index, y=df['Buy_1m_brk2'], marker_color='#ff4d4d', opacity=0.4), row=6, col=1, secondary_y=False)
            fig.add_trace(go.Bar(x=df.index, y=-df['Sell_1m_brk2'], marker_color='#0066ff', opacity=0.4), row=6, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], line=dict(color='black', width=2)), row=6, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df['Red_Dot_2'], mode='markers', marker=dict(color='red', size=8)), row=6, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=df.index, y=df['Blue_Dot_2'], mode='markers', marker=dict(color='blue', size=8)), row=6, col=1, secondary_y=True)

            fig.update_layout(height=1400, template='plotly_white', barmode='relative', hovermode='x unified', showlegend=False) 
            fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor", spikecolor="gray", spikethickness=1, spikedash="dot")
            fig.update_layout(xaxis_rangeslider_visible=False)
            fig.update_yaxes(tickformat=",")

            st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("데이터가 없거나 장 시작 전입니다.")
