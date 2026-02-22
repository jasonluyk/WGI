import streamlit as st
import pandas as pd
import pymongo
import time
from datetime import datetime
from wgi_final import get_manifest_events, pull_dual_event_data

# --- 1. Page Configuration ---
st.set_page_config(
    page_title="WGI 2026 Live Analytics", 
    layout="wide", 
    initial_sidebar_state="collapsed",
    page_icon="🚩"
)

# --- 2. Database Connection Logic ---
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["MONGO_URI"])

client = init_connection()
db = client["rankings_2026"]
collection = db["wgi_analytics"]

# --- 3. Persistent State Recovery ---
# Recover Login Latch
if "admin_logged_in" not in st.session_state:
    latch = db["system_state"].find_one({"type": "admin_session"})
    st.session_state.admin_logged_in = True if latch else False

# Recover Live Event Data Latch
if "active_event_data" not in st.session_state:
    saved = db["live_state"].find_one({"type": "current_session"})
    if saved:
        st.session_state.active_event_data = pd.DataFrame(saved['data'])
        st.session_state.finals_slots = saved['slots']
        st.session_state.active_event_name = saved['name']
        st.session_state.active_urls = saved.get('urls', {})
    else:
        st.session_state.active_event_data = pd.DataFrame()
        st.session_state.finals_slots = {}
        st.session_state.active_event_name = "No Active Event"

# --- 4. Functional Logic ---
@st.cache_data(ttl=300)
def load_national_data():
    items = list(collection.find())
    if not items: return pd.DataFrame()
    df_raw = pd.DataFrame(items)
    df_raw.columns = [c.title() for c in df_raw.columns]
    return df_raw

def sync_to_cloud(df):
    if not df.empty:
        df.columns = [c.lower() for c in df.columns]
        collection.delete_many({}) 
        collection.insert_many(df.to_dict("records"))
        return True
    return False

# --- 5. Main UI ---
st.title("🏆 WGI 2026 Color Guard Analytics")
national_df = load_national_data()

tab1, tab2, tab3, tab4 = st.tabs(["Analytics", "BSI Calculator", "Live Hub", "Admin"])

# --- TAB 1: National Overview ---
with tab1:
    if national_df.empty:
        st.warning("National rankings empty. Sync data in Admin.")
    else:
        sel_class = st.selectbox("Division:", sorted(national_df['Class'].unique()))
        c_df = national_df[national_df['Class'] == sel_class].copy()
        c_df = c_df.sort_values(by='Average_Score', ascending=False)
        c_df['Rank'] = range(1, len(c_df) + 1)
        st.dataframe(c_df[['Rank', 'Guard', 'Average_Score', 'Season_High']], width='stretch', hide_index=True)

# --- TAB 2: BSI Calculator ---
with tab2:
    if national_df.empty:
        st.info("Sync national data in Admin to enable BSI comparisons.")
    else:
        d1, d2 = st.columns(2)
        with d1:
            comp_class = st.selectbox("Division", sorted(national_df['Class'].unique()), key="bsi_c")
        comp_df = national_df[national_df['Class'] == comp_class].copy().sort_values(by='Average_Score', ascending=False)
        comp_df['Rank'] = range(1, len(comp_df) + 1)
        with d2:
            comp_guard = st.selectbox("Guard", sorted(comp_df['Guard'].unique()), key="bsi_g")
        
        if comp_guard:
            g_row = comp_df[comp_df['Guard'] == comp_guard].iloc[0]
            m1, m2, m3 = st.columns(3)
            m1.metric("National Rank", f"#{int(g_row['Rank'])}")
            m2.metric("Average Score", f"{float(g_row['Average_Score']):.2f}")
            
            first = float(comp_df.iloc[0]['Average_Score'])
            gap = float(g_row['Average_Score']) - first
            m3.metric("Gap to #1", f"{gap:.2f}", delta=f"{gap:.2f}")

# --- TAB 3: LIVE HUB (Bakersfield Logic) ---
with tab3:
    if not st.session_state.active_event_data.empty:
        # AUTO-REFRESH LOGIC
        auto_on = st.sidebar.checkbox("🔄 Auto-Poll Scores (60s)")
        if auto_on and "active_urls" in st.session_state:
            st.toast("Polling Live Circuit...")
            urls = st.session_state.active_urls
            df_new, slots_new = pull_dual_event_data(urls.get('prelims'), urls.get('finals'))
            if not df_new.empty:
                st.session_state.active_event_data = df_new
                db["live_state"].update_one({"type": "current_session"}, {"$set": {"data": df_new.to_dict("records")}}, upsert=True)
            time.sleep(60)
            st.rerun()

        live_df = st.session_state.active_event_data.copy()
        slots = st.session_state.get('finals_slots', {})
        
        st.subheader(f"📊 {st.session_state.get('active_event_name', 'Live Event')}")
        
        # Class Filter & Ranking
        all_c = sorted(live_df['Class'].unique())
        f_class = st.selectbox("🎯 Filter Class:", ["All"] + all_c)
        
        if f_class != "All":
            live_df = live_df[live_df['Class'] == f_class]
            live_df['Score'] = pd.to_numeric(live_df['Score'], errors='coerce').fillna(0.0)
            if any(live_df['Score'] > 0):
                live_df = live_df.sort_values(by="Score", ascending=False)
                live_df['Rank'] = range(1, len(live_df) + 1)
                n_slots = slots.get(f_class, 10)
                live_df['Status'] = ["✅ IN" if i < n_slots else "❌ OUT" for i in range(len(live_df))]
        
        st.dataframe(live_df, width='stretch', hide_index=True)
    else:
        st.info("System Idle: Select an event in the Admin tab.")

# --- TAB 4: ADMIN (Persistent Authentication) ---
with tab4:
    if not st.session_state.admin_logged_in:
        st.header("🔐 Admin Access")
        pwd = st.text_input("Password", type="password")
        if st.button("Authorize"):
            if pwd == st.secrets["ADMIN_PASS"]:
                db["system_state"].update_one({"type": "admin_session"}, {"$set": {"active": True}}, upsert=True)
                st.session_state.admin_logged_in = True
                st.rerun()
    else:
        st.success("🛰️ System Link Established")
        if st.button("🛑 Logout / Clear Latch"):
            db["system_state"].delete_one({"type": "admin_session"})
            st.session_state.admin_logged_in = False
            st.rerun()

        st.divider()
        # Ensure manifest is always ready for the dropdown
        if 'found_events' not in st.session_state:
            st.session_state.found_events = get_manifest_events()
        
        event_names = [e['name'] for e in st.session_state.found_events]
        selected_show = st.selectbox("Select Competition:", event_names)
        
        if st.button("🚀 Sync Full Show Map"):
            try:
                target = next(e for e in st.session_state.found_events if e['name'] == selected_show)
                with st.spinner(f"Probing {selected_show}..."):
                    p_url = target['url']['prelims'] if isinstance(target['url'], dict) else target['url']
                    f_url = target['url']['finals'] if isinstance(target['url'], dict) else ""
                    
                    df_live, f_slots = pull_dual_event_data(p_url, f_url)
                    
                    if not df_live.empty:
                        st.session_state.active_event_data = df_live
                        st.session_state.finals_slots = f_slots
                        st.session_state.active_event_name = selected_show
                        st.session_state.active_urls = {"prelims": p_url, "finals": f_url}
                        
                        db["live_state"].update_one(
                            {"type": "current_session"},
                            {"$set": {
                                "name": selected_show, "slots": f_slots, 
                                "data": df_live.to_dict("records"),
                                "urls": st.session_state.active_urls
                            }}, upsert=True
                        )
                        st.success("✅ Bakersfield Logic Synchronized")
                        st.rerun()
            except Exception as e:
                st.error(f"Sync Fault: {e}")