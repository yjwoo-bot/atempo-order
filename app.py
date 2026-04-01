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

# --- [설정 2] 유통점 정보 ---
SHOP_DB = {
    "르위켄": {"code": "103-86-00394", "name": "포지티브라이프컴퍼니(주)", "manager": "최현석", "prefix": "르위켄_"},
    "피쏘": {"code": "857-81-02121", "name": "주식회사 피쏘", "manager": "송진영", "prefix": "피쏘_"},
    "옐로우라이트": {"code": "845-86-01861", "name": "(주)옐로우라이트", "manager": "송진영", "prefix": "옐로우라이트_"},
    "까사디자인": {"code": "117-12-31221", "name": "까사디자인", "manager": "송진영", "prefix": "까사디자인_"}
}

# [강제 매핑 규칙]
STRICT_MAPPING = {
    "CANYON": "H_F212AK101",
    "UMBER": "H_F212ATL21A",
    "GRAPHITE": "H_F212ATL22C",
    "LOTUS": "H_F212GK281",
    "로투스": "H_F212GK281",
    "CHARCOAL": "H_F212ATL22C",
    "DARK BROWN": "H_F212ATL21A",
    "WORLD ONE": "H_ISOR40780WC",
    "WORLD CHAIR": "H_ISOR40780WC",
    "FLOS 정품 전구": "FS_RF36608",
    "FLOS 정품전구": "FS_RF36608",
    "LUMINATOR_WHITE": "A_0344020A_1",
    "LUMINATOR BIANCO": "A_0344020A_1"
}

def clean_text(text):
    if not text or pd.isna(text): return ""
    text = str(text).upper()
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'\[.*?\]', '', text)
    return text.strip()

def clean_address(addr):
    if not addr or pd.isna(addr): return ""
    addr = str(addr)
    addr = re.sub(r'\[우편번호\]', '', addr)
    addr = re.sub(r'\d+월\s?\d+일\s?요청', '', addr)
    return addr.strip()

def transform_engine(order_file, code_ref, price_ref, temp_cols):
    results = []
    TODAY = datetime.now().strftime('%Y-%m-%d')
    
    master_by_code = {str(r['품목코드']).strip(): {"name": str(r['품목명']), "price": int(r['소비자가']) if pd.notna(r['소비자가']) else 0} for _, r in price_ref.iterrows()}
    set_standard = [str(n).upper() for n in code_ref['상품명'].dropna().unique()]

    xls = pd.ExcelFile(order_file)
    order_cnt = 1

    for sheet in xls.sheet_names:
        df = pd.read_excel(order_file, sheet_name=sheet)
        df.columns = [str(c).strip() for c in df.columns]
        
        clean_sheet = sheet.replace(" ", "").upper()
        target_key = "르위켄"
        for key in SHOP_DB.keys():
            if key in clean_sheet:
                target_key = key; break
        info = SHOP_DB[target_key]
        
        col_item = next((c for c in df.columns if any(k in str(c).upper() for k in ["상품", "구매 제품", "모델", "ITEM"])), None)
        col_cust = next((c for c in df.columns if any(k in str(c).upper() for k in ["고객", "수령", "성함"])), None)
        col_addr = next((c for c in df.columns if any(k in str(c).upper() for k in ["주소", "배송지"])), None)
        col_phone = next((c for c in df.columns if any(k in str(c).upper() for k in ["전화", "연락처"])), None)
        col_qty = next((c for c in df.columns if any(k in str(c).upper() for k in ["수량", "QTY"])), None)

        if not col_item: continue

        for _, row in df[df[col_item].notna()].iterrows():
            raw_full_name = str(row[col_item])
            if "취소함" in raw_full_name: continue
            
            clean_name = clean_text(raw_full_name)
            if any(x in clean_name for x in ["시공비", "발송건", "배송비"]): continue

            address = clean_address(row.get(col_addr, ''))
            customer_name = f"{info['prefix']}{re.sub(r'^(르위켄_|피쏘_|옐로우라이트_|까사디자인_)', '', str(row.get(col_cust, '')).strip())}"
            
            box_codes = []
            final_n = ""
            
            # 1. 톨로메오 메가 세트 우선 매칭
            if "MEGA" in clean_name and "42" in clean_name:
                final_n = "TOLOMEO MEGA FLOOR PARCHMENT 420"
                set_rows = code_ref[code_ref['상품명'].str.contains("MEGA") & code_ref['상품명'].str.contains("420")]
                if not set_rows.empty:
                    box_codes = [str(v).strip() for v in set_rows.iloc[0, 1:] if pd.notna(v) and str(v).strip() != ""]
            elif "MEGA" in clean_name and "36" in clean_name:
                final_n = "TOLOMEO MEGA FLOOR PARCHMENT 360"
                set_rows = code_ref[code_ref['상품명'].str.contains("MEGA") & code_ref['상품명'].str.contains("360")]
                if not set_rows.empty:
                    box_codes = [str(v).strip() for v in set_rows.iloc[0, 1:] if pd.notna(v) and str(v).strip() != ""]
            
            # 2. 강제 매핑 (Humanscale 등)
            if not box_codes:
                for kw, f_code in STRICT_MAPPING.items():
                    if kw in clean_name:
                        box_codes = [f_code]
                        final_n = master_by_code.get(f_code, {}).get('name', clean_name)
                        break
            
            # 3. 일반 매칭 (IndexError 방지 로직 추가)
            if not box_codes:
                match = process.extractOne(clean_name, set_standard, scorer=fuzz.token_set_ratio)
                if match and match[1] > 70:
                    final_n = match[0]
                    set_rows = code_ref[code_ref['상품명'] == final_n]
                    if not set_rows.empty: # 데이터가 있을 때만 인덱싱
                        box_codes = [str(v).strip() for v in set_rows.iloc[0, 1:] if pd.notna(v) and str(v).strip() != ""]

            # --- 데이터 적재 ---
            if box_codes:
                for code in box_codes:
                    it = master_by_code.get(code)
                    p_name, p_price = (it['name'], it['price']) if it else (final_n, 0)
                    mult = 0.8 if "LUMINATORPARENTESI" in p_name.upper() else 0.7
                    u_price = int(round(p_price * mult))
                    try:
                        qty = int(float(re.sub(r'[^0-9.]', '', str(row.get(col_qty, 1)))))
                    except: qty = 1
                    
                    res = {c: "" for c in temp_cols}
                    res.update({
                        "입력일자": TODAY, "순번": order_cnt, "유통구분": "3", "거래처코드": info["code"],
                        "거래처명": info["name"], "담당자": info["manager"], "출하창고": "100",
                        "배송주소": address, "고객명": customer_name, "연락처": str(row.get(col_phone, '')),
                        "품목코드": code, "품목명": p_name, "수량": qty, "권장소비자가": p_price,
                        "단가(vat포함)": u_price, "합계액": qty * u_price
                    })
                    results.append(res)
                order_cnt += 1
            else:
                res = {c: "" for c in temp_cols}
                res.update({"입력일자": TODAY, "순번": order_cnt, "고객명": customer_name, "품목명": clean_name, "적요": "ERROR"})
                results.append(res); order_cnt += 1

    return pd.DataFrame(results)

# --- [UI] ---
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
    except:
        st.sidebar.error("❌ 드라이브 연결 실패")

uploaded_file = st.file_uploader("📥 통합 발주리스트 파일을 업로드하세요", type="xlsx")

if uploaded_file and st.button("🪄 ERP 양식으로 변환하기"):
    if 'masters' in st.session_state:
        m_code, m_price, m_temp = st.session_state.masters
        final_df = transform_engine(uploaded_file, m_code, m_price, m_temp.columns.tolist())
        st.success("변환이 완료되었습니다!")
        st.dataframe(final_df)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False)
        st.download_button("📥 ERP 파일 다운로드", data=output.getvalue(), file_name=f"ERP_FIXED_{datetime.now().strftime('%m%d')}.xlsx")