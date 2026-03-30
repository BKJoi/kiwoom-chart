import streamlit as st
import requests
import pandas as pd
import numpy as np # 속도 향상을 위해 추가
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ... [인증 및 수집 함수는 기존과 동일하므로 생략, 데이터 처리 부분부터 최적화] ...

# [기존 함수 유지]
@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    response = requests.post(url, headers=headers, json=data)
    return response.json().get('token') if response.status_code == 200 else None

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

# ... [차트/수급 수집 함수 생략 - 기존 코드 그대로 사용] ...

# (위 함수들은 기존 코드를 그대로 쓰시되, 아래 메인 로직의 처리 속도를 바꿉니다)

if auth_token and len(stock_number) == 6: 
    with st.spinner("수급 데이터를 최적화하여 분석 중..."):
        # [데이터 수집 로직 - 기존과 동일]
        # (중략: chart_raw, new_pg, new_brk1, new_brk2 수집 및 merge_api_data 호출)
        
        if chart_raw:
            df = pd.DataFrame(chart_raw)
            time_col = 'stk_cntr_tm' if 'stk_cntr_tm' in df.columns else 'cntr_tm'
            df['Datetime'] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S')
            df.set_index('Datetime', inplace=True)
            df = df[df.index.strftime('%Y%m%d') == target_date_str].sort_index()

            if df.empty:
                st.info("⏳ 데이터 대기 중...")
                st.stop()

            # 🚀 최적화 1: 숫자 변환 (벡터화)
            for col in ['open_pric', 'high_pric', 'low_pric', 'cur_prc', 'trde_qty']:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[+,-,]', '', regex=True)).fillna(0).astype(int)

            # 🚀 최적화 2: 거래원 데이터 처리 (apply 제거 버전)
            def process_broker_data_fast(raw_data, lag_sec, suffix):
                if not raw_data: return pd.DataFrame()
                db = pd.DataFrame(raw_data)
                tm_col = 'tm' if 'tm' in db.columns else 'stck_cntg_hour'
                
                # 시간 벡터 연산
                db['Datetime'] = pd.to_datetime(target_date_str + db[tm_col], format='%Y%m%d%H%M%S') - pd.Timedelta(seconds=lag_sec)
                db['Datetime'] = db['Datetime'].dt.floor('min')
                
                # 수량/타입 벡터 연산 (apply 대신 mask 사용)
                qty_str = db['mont_trde_qty'].astype(str).str.replace(',', '')
                qty_val = pd.to_numeric(qty_str.str.replace(r'[+-]', '', regex=True)).fillna(0)
                
                is_sell = (qty_str.str.contains('-')) | (db['tp'].astype(str).str.contains('매도'))
                db['Buy_Vol'] = np.where(~is_sell, qty_val, 0)
                db['Sell_Vol'] = np.where(is_sell, qty_val, 0)
                
                if 'acc_netprps' in db.columns:
                    db['Net_Raw'] = pd.to_numeric(db['acc_netprps'].astype(str).str.replace(r'[+,,]', '', regex=True)).fillna(0)
                else: db['Net_Raw'] = 0
                
                return db.groupby('Datetime').agg({'Buy_Vol':'sum', 'Sell_Vol':'sum', 'Net_Raw':'last'}).rename(
                    columns={'Buy_Vol':f'Buy_1m_{suffix}', 'Sell_Vol':f'Sell_1m_{suffix}', 'Net_Raw':f'Cum_Net_{suffix}'})

            # 최적화된 함수 호출 및 결합
            df = df.join(process_broker_data_fast(brk_raw1, lag_seconds, 'brk1'), how='left')
            df = df.join(process_broker_data_fast(brk_raw2, lag_seconds, 'brk2'), how='left')
            
            # 결측치 채우기
            for s in ['brk1', 'brk2']:
                df[f'Buy_1m_{s}'] = df[f'Buy_1m_{s}'].fillna(0)
                df[f'Sell_1m_{s}'] = df[f'Sell_1m_{s}'].fillna(0)
                df[f'Cum_Net_{s}'] = df[f'Cum_Net_{s}'].ffill().fillna(0)

            # 동시호가 튀는 값 제거
            df.loc[df.index.strftime('%H%M').isin(['0900', '1530']), 
                   ['trde_qty', 'Buy_1m_brk1', 'Sell_1m_brk1', 'Buy_1m_brk2', 'Sell_1m_brk2']] = 0

            # 🚀 최적화 3: Signal_Value 및 Point 계산 (벡터화)
            df['Max1'] = df['Cum_Net_brk1'].expanding().max()
            df['Min1'] = df['Cum_Net_brk1'].expanding().min()
            df['Max2'] = df['Cum_Net_brk2'].expanding().max()
            df['Min2'] = df['Cum_Net_brk2'].expanding().min()

            pos1 = (df['Max1'] - df['Cum_Net_brk1']) - (df['Cum_Net_brk1'] - df['Min1'])
            pos2 = (df['Max2'] - df['Cum_Net_brk2']) - (df['Cum_Net_brk2'] - df['Min2'])
            
            # np.where로 조건문 속도 업그레이드
            df['Signal_Value'] = np.where(pos1 > pos2, 
                                          (df['Max1'] - df['Cum_Net_brk1']) + (df['Cum_Net_brk2'] - df['Min2']),
                                          (df['Max2'] - df['Cum_Net_brk2']) + (df['Cum_Net_brk1'] - df['Min1']))

            df['Value_Max'] = df['Signal_Value'].expanding().max()
            # ⭐️ 최적화 포인트: apply 대신 마스킹 사용
            df['Signal_Point'] = np.nan
            high_mask = (df['Signal_Value'] == df['Value_Max']) & (df['Signal_Value'] > 0)
            df.loc[high_mask, 'Signal_Point'] = df.loc[high_mask, 'Signal_Value']

            # ==============================================================================
            # 📊 차트 시각화 (Scattergl 사용하여 렌더링 속도 개선)
            # ==============================================================================
            fig = make_subplots(...) # [기존 specs 유지]
            
            # [1~3층 생략]
            
            # 4층/5층: 신호 포인트 시각화 최적화
            for r, s, name in [(4, 'brk1', selected_broker_name1), (5, 'brk2', selected_broker_name2)]:
                fig.add_trace(go.Scatter(x=df.index, y=df[f'Cum_Net_{s}'], mode='lines', line=dict(color='black', width=2)), row=r, col=1, secondary_y=True)
                
                # 🚀 최적화: apply 대신 mask된 시리즈를 직접 전달
                sig_y = df[f'Cum_Net_{s}'].where(df['Signal_Point'].notna())
                fig.add_trace(go.Scatter(x=df.index, y=sig_y, mode='markers', marker=dict(color='red', size=6)), row=r, col=1, secondary_y=True)

            # [이후 레이아웃 및 6층 기존 동일]
            st.plotly_chart(fig, use_container_width=True)
