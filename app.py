import streamlit as st
import pandas as pd
import re
import io
import warnings
from datetime import datetime
from rapidfuzz import process, fuzz

warnings.filterwarnings("ignore")

# --- [설정 1] 구글 드라이브 파일 ID (고정) ---
ID_PRICE_REF = "1yyTzZapuX9qTwwfcOjEtRVBfn5QaKPDz"
ID_CODE_REF = "1IIYU0JtaBed7ELoB6ASj3bcoewbNRhk8"
ID_TEMPLATE = "1ckbQu1TTQ8F_SdNutgKBUQl3fGo9yM75"

def get_drive_url(file_id):
    return f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"

# --- [설정 2] 유통점 정보 (기본값: 르위켄) ---
DEFAULT_INFO = {"code": "103-86-00394", "name": "포지티브라이프컴퍼니(주)", "manager": "최현석", "prefix": "르위켄_"}

STRICT_MAPPING = {
    "CANYON": "H_F212AK101", "UMBER": "H_F212ATL21A", "GRAPHITE": "H_F212ATL22C",
    "LOTUS": "H_F212GK281", "로투스": "H_F212GK281", "CHARCOAL": "H_F212ATL22C",
    "DARK BROWN": "H_F212ATL21A", "WORLD ONE": "H_ISOR40780WC", "WORLD CHAIR": "H_ISOR40780WC",
    "FLOS 정품 전구": "FS_RF36608", "FLOS 정품전구": "FS_RF36608",
    "LUMINATOR_WHITE": "A_0344020A_1", "LUMINATOR BIANCO": "A_0344020A_1"
}

def clean_text(text):
    if not text or pd.isna(text): return ""
    return str(text).upper().replace(" ", "").strip()

def transform_engine(order_file, code_ref, price_ref, temp_cols):
    results = []
    TODAY = datetime.now().strftime('%Y-%m-%d')
    
    master_by_code = {str(r['품목코드']).strip(): {"name": str(r['품목명']), "price": int(r['소비자가']) if pd.notna(r['소비자가']) else 0} for _, r in price_ref.iterrows()}
    set_standard = [str(n).upper() for n in code_ref['상품명'].dropna().unique()]

    xls = pd.ExcelFile(order_file)
    order_cnt = 1

    # 1. 파일 안의 모든 시트를 순회 (필터링 없음)
    for sheet in xls.sheet_names:
        df_raw = pd.read_excel(order_file, sheet_name=sheet, header=None)
        if df_raw.empty: continue

        # 2. 헤더(컬럼명) 위치 자동 찾기
        header_idx = 0
        for i, row in df_raw.iterrows():
            if any(k in str(s) for s in row.values for k in ["구매", "상품", "ITEM", "제품"]):
                header_idx = i
                break
        
        df = pd.read_excel(order_file, sheet_name=sheet, skiprows=header_idx)
        df.columns = [str(c).replace(" ", "").upper() for c in df.columns]

        # 3. 필수 컬럼 매핑
        col_item = next((c for c in df.columns if any(k in c for k in ["상품", "구매제품", "모델", "ITEM"])), None)
        col_cust = next((c for c in df.columns if any(k in c for k in ["고객", "수령", "성함", "주문자"])), None)
        col_addr = next((c for c in df.columns if any(k in c for k in ["주소", "배송지"])), None)
        col_phone = next((c for c in df.columns if any(k in c for k in ["전화", "연락처", "휴대폰"])), None)
        col_qty = next((c for c in df.columns if any(k in c for k in ["수량", "QTY"])), None)

        if not col_item: continue

        # 4. 행 데이터 처리
        for _, row in df.iterrows():
            # 변수 초기화 (UnboundLocalError 방지)
            customer_name = "미기재"
            val_item = str(row.get(col_item, ""))
            
            if val_item == "nan" or not val_item.strip() or "취소함" in val_item:
                continue

            # 고객명 추출
            raw_cust = str(row.get(col_cust, '')).strip()
            if raw_cust != "nan" and raw_cust != "":
                customer_name = f"{DEFAULT_INFO['prefix']}{re.sub(r'^(르위켄_|피쏘_|옐로우라이트_|까사디자인_)', '', raw_cust)}"

            clean_name = clean_text(val_item)
            if any(x in clean_name for x in ["시공비", "발송건", "배송비", "배송료"]): 
                continue

            box_codes = []
            final_n = ""

            # 5. 매칭 로직
            for kw, f_code in STRICT_MAPPING.items():
                if kw.replace(" ","") in clean_name:
                    box_codes = [f_code]
                    final_n = master_by_code.get(f_code, {}).get('name', val_item)
                    break
            
            if not box_codes:
                match = process.extractOne(val_item.upper(), set_standard, scorer=fuzz.token_set_ratio)
                if match and match[1] > 60:
                    final_n = match[0]
                    set_rows = code_ref[code_ref['상품명'] == final_n]
                    if not set_rows.empty:
                        box_codes = [str(v).strip() for v in set_rows.iloc[0, 1:] if pd.notna(v) and str(v).strip() != ""]

            # 6. 결과 리스트 적재
            if box_codes:
                for code in box_codes:
                    it = master_by_code.get(code)
                    p_name, p_price = (it['name'], it['price']) if it else (final_n, 0)
                    mult = 0.8 if "PARENTESI" in p_name.upper() else 0.7
                    u_price = int(round(p_price * mult))
                    try:
                        qty_val = re.sub(r'[^0-9.]', '', str(row.get(col_qty, 1)))
                        qty = int(float(qty_val)) if qty_val and qty_val != '.' else 1
                    except: qty = 1
                    
                    res = {c: "" for c in temp_cols}
                    res.update({
                        "입력일자": TODAY, "순번": order_cnt, "유통구분": "3", "거래처코드": DEFAULT_INFO["code"],
                        "거래처명": DEFAULT_INFO["name"], "담당자": DEFAULT_INFO["manager"], "출하창고": "100",
                        "배송주소": str(row.get(col_addr, '')), "고객명": customer_name, "연락처": str(row.get(col_phone, '')),
                        "품목코드": code, "품목명": p_name, "수량": qty, "권장소비자가": p_price,
                        "단가(vat포함)": u_price, "합계액": qty * u_price
                    })
                    results.append(res)
                order_cnt += 1
            else:
                res = {c: "" for c in temp_cols}
                res.update({"입력일자": TODAY, "순번": order_cnt, "고객명": customer_name, "품목명": val_item, "적요": "미매칭"})
                results.append(res); order_cnt += 1

    return pd.DataFrame(results)

# --- UI ---
st.set_page_config(page_title="atempo 유통점 발주 ERP 변환 시스템", layout="wide")
st.title("🤖 atempo 유통점 발주 ERP 변환 시스템")

if 'masters' not in st.session_state:
    try:
        with st.spinner("마스터 데이터 동기화 중..."):
            code_ref = pd.read_excel(get_drive_url(ID_CODE_REF))
            price_ref = pd.read_excel(get_drive_url(ID_PRICE_REF), skiprows=1)
            temp_df = pd.read_excel(get_drive_url(ID_TEMPLATE))
            st.session_state.masters = (code_ref, price_ref, temp_df)
            st.sidebar.success("✅ 기준 데이터 연결 완료")
    except Exception as e:
        st.sidebar.error(f"❌ 드라이브 연결 실패")

uploaded_file = st.file_uploader("📥 엑셀 파일을 업로드하세요 (모든 시트를 읽습니다)", type="xlsx")

if uploaded_file and st.button("🪄 ERP 양식으로 변환하기"):
    if 'masters' in st.session_state:
        m_code, m_price, m_temp = st.session_state.masters
        final_df = transform_engine(uploaded_file, m_code, m_price, m_temp.columns.tolist())
        st.success(f"변환 완료! (총 {len(final_df)}행 추출)")
        st.dataframe(final_df)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False)
        st.download_button("📥 ERP 파일 다운로드", data=output.getvalue(), file_name=f"ERP_FINAL_{datetime.now().strftime('%m%d')}.xlsx")
