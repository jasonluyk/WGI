import streamlit as st
import pandas as pd
import pymongo
import time
from datetime import datetime
from wgi_final import get_manifest_events, pull_dual_event_data

# --- 1. Page Configuration ---
st.set_page_config(page_title="WGI 2026 Analytics", layout="wide", page_icon="🚩")

# --- 2. Database Connection ---
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["MONGO_URI"])

client = init_connection()
db = client["rankings_2026"]
collection = db["wgi_analytics"]

# --- 3. Persistent State Recovery ---
if "admin_logged_in" not in st.session_state:
    latch = db["system_state"].find_one({"type": "admin_session"})
    st.session_state.admin_logged_in = True if latch else False

# --- 1. SYSTEM INITIALIZATION: Stable Boot ---
# Initialize session variables as empty containers to prevent UI crashes
if "active_event_data" not in st.session_state:
    st.session_state.active_event_data = pd.DataFrame()
if "finals_slots" not in st.session_state:
    st.session_state.finals_slots = {}
if "active_event_name" not in st.session_state:
    st.session_state.active_event_name = "No Active Event"

# Ensure manifest is always loaded so the Admin dropdown doesn't disappear
if 'found_events' not in st.session_state:
    st.session_state.found_events = get_manifest_events()

# --- 4. National Data Loader ---
@st.cache_data(ttl=300)
def load_national_data():
    items = list(collection.find())
    if not items: return pd.DataFrame()
    df_raw = pd.DataFrame(items)
    # Normalize to Title Case for UI consistency
    df_raw.columns = [c.title() for c in df_raw.columns]
    if 'Average_Score' not in df_raw.columns and 'Average_Score' in df_raw.columns:
        df_raw.rename(columns={'Average_Score': 'Average_Score'}, inplace=True)
    return df_raw

st.title("🏆 WGI 2026 Color Guard Analytics")
df = load_national_data()
tab1, tab2, tab3, tab4 = st.tabs(["Analytics", "BSI Calculator", "Live Hub", "Admin"])

# --- TAB 1: National Rankings ---
with tab1:
    if df.empty: st.warning("No data found. Sync in Admin.")
    else:
        sel_class = st.selectbox("Division:", sorted(df['Class'].unique()), key="nav")
        c_df = df[df['Class'] == sel_class].copy().sort_values(by='Average_Score', ascending=False)
        c_df['Rank'] = range(1, len(c_df) + 1)
        st.dataframe(c_df[['Rank', 'Guard', 'Average_Score', 'Season_High']], use_container_width=True, hide_index=True)

# --- TAB 2: BSI Comparison (RESTORED) ---
with tab2:
    if df.empty:
        st.info("Sync national data in the Admin tab to enable BSI comparisons.")
    else:
        # 1. Selection Header: Choosing the Signal Path
        c1, c2 = st.columns(2)
        with c1:
            sel_class = st.selectbox("1. Select Division", sorted(df['Class'].unique()), key="comp_class")
        
        # Filter and Sort to establish fresh Ranks based on the current National Bus
        comp_df = df[df['Class'] == sel_class].copy()
        comp_df = comp_df.sort_values(by='Average_Score', ascending=False)
        comp_df['Rank'] = range(1, len(comp_df) + 1)
        
        with c2:
            sel_guard = st.selectbox("2. Select Guard", sorted(comp_df['Guard'].unique()), key="comp_guard")

        if sel_guard:
            guard_data = comp_df[comp_df['Guard'] == sel_guard].iloc[0]
            
            # --- SECTION 1: Current Standing ---
            st.subheader(f"Current Standing: {sel_guard}")
            m1, m2, m3 = st.columns(3)
            m1.metric("National Rank", f"#{int(guard_data['Rank'])}")
            m2.metric("Average Score", f"{float(guard_data['Average_Score']):.2f}")
            m3.metric("Season High", f"{float(guard_data['Season_High']):.2f}")
            
            st.divider()

            # --- SECTION 2: Class Benchmarks (The 'Goal' Voltage) ---
            st.subheader(f"{sel_class} Benchmarks")
            
            # Identify Benchmark Scores (1st and 15th Place)
            first_place_score = float(comp_df.iloc[0]['Average_Score'])
            
            # Handle classes with fewer than 15 guards
            if len(comp_df) >= 15:
                fifteenth_place_score = float(comp_df.iloc[14]['Average_Score'])
                bubble_label = "15th Place (Finalist Bubble)"
            else:
                fifteenth_place_score = float(comp_df.iloc[-1]['Average_Score'])
                bubble_label = "Last Place (Current Class Size)"

            # Calculate Point Gaps
            my_avg = float(guard_data['Average_Score'])
            gap_to_first = my_avg - first_place_score
            gap_to_fifteenth = my_avg - fifteenth_place_score

            # Display Benchmark Metrics with Gaps
            b1, b2 = st.columns(2)
            b1.metric("1st Place Score", f"{first_place_score:.2f}")
            b2.metric(bubble_label, f"{fifteenth_place_score:.2f}")

            g1, g2 = st.columns(2)
            # 'Delta' shows how far up/down you are from the target
            g1.metric("Gap to 1st", f"{gap_to_first:.2f}", delta=f"{gap_to_first:.2f}")
            g2.metric("Gap to 15th", f"{gap_to_fifteenth:.2f}", delta=f"{gap_to_fifteenth:.2f}")

           # --- SECTION 3: National Standing (Calibrated Percentile) ---
            st.divider()
            total_guards = len(comp_df)
            current_rank = int(guard_data['Rank'])
            
            # 1. Calculate the 'Bottom' percentage
            # Logic: (Total - Rank) / Total
            # Example: (29 - 21) / 29 = 0.275 -> 28%
            bottom_ratio = (float(total_guards) - float(current_rank)) / float(total_guards)
            bottom_percent = int(bottom_ratio * 100)
            
            # 2. Split Logic: Toggle terminology at the 50% threshold
            if current_rank <= (total_guards / 2):
                # Top Half: Show how many teams are below you as a 'Top' percentile
                # Example: Rank 5/29 -> Top 17%
                top_percent = int((float(current_rank) / float(total_guards)) * 100)
                if top_percent == 0: top_percent = 1
                label_text = f"🏆 Overall Standing: Top {top_percent}%"
            else:
                # Bottom Half: Show the percentage of the field remaining below you
                # Example: Rank 21/29 -> Bottom 28%
                label_text = f"📊 Overall Standing: Bottom {bottom_percent}%"

            st.write(f"### {label_text}")
            
            # 3. Progress Bar (Visualizing the 'Power Level')
            # The bar fills from left to right; higher rank = fuller bar.
            # A Rank 21/29 will show a bar that is ~28% full.
            st.progress(
                max(0.0, min(1.0, bottom_ratio)), 
                text=f"{sel_guard} is #{current_rank} out of {total_guards} teams"
            )

# --- TAB 3: LIVE HUB (RESTORED FINALS LOGIC) ---
with tab3:
    if st.session_state.active_event_data.empty:
        # System is in 'Standby' mode
        st.info("⚪ System Idle: No competition currently latched. Use the Admin tab to sync a show.")
    else:
        # System is 'Live'
        st.header(f"📊 Live Signal: {st.session_state.active_event_name}")
        # ... (rest of your ranking and table logic)
    if not st.session_state.active_event_data.empty:
        # Auto-Refresh Sensor
        if st.sidebar.checkbox("🔄 Auto-Poll Bakersfield (60s)"):
            st.toast("Refreshing Scores...")
            urls = st.session_state.get('active_urls', {})
            df_new, s_new = pull_dual_event_data(urls.get('prelims'), urls.get('finals'))
            if not df_new.empty:
                st.session_state.active_event_data = df_new
                db["live_state"].update_one({"type": "current_session"}, {"$set": {"data": df_new.to_dict("records")}}, upsert=True)
            time.sleep(60); st.rerun()

        l_df = st.session_state.active_event_data.copy()
        slots = st.session_state.get('finals_slots', {})
        f_c = st.selectbox("Filter Class:", ["All"] + sorted(l_df['Class'].unique()))
        
        if f_c != "All":
            l_df = l_df[l_df['Class'] == f_c]
            l_df['Score'] = pd.to_numeric(l_df['Score'], errors='coerce').fillna(0.0)
            if any(l_df['Score'] > 0):
                l_df = l_df.sort_values(by="Score", ascending=False)
                l_df['Rank'] = range(1, len(l_df) + 1)
                num = slots.get(f_c, 10)
                l_df['Status'] = ["✅ IN" if i < num else "❌ OUT" for i in range(len(l_df))]
        st.dataframe(l_df, width='stretch', hide_index=True)
    else: st.info("Sync show in Admin tab.")

# --- TAB 4: ADMIN (RESTORED SYNC OPTION) ---
with tab4:
    # Logic is now strictly Session-Based
    if not st.session_state.admin_logged_in:
        st.header("🔐 Admin Access")
        admin_pwd = st.text_input("Enter Password", type="password")
        if st.button("Authorize"):
            if admin_pwd == st.secrets["ADMIN_PASS"]:
                st.session_state.admin_logged_in = True
                # REMOVED: db["system_state"].update_one(...)
                st.rerun()
    else:
        st.success("🛰️ System Link Established (Current Session Only)")
        if st.button("Logout"):
            st.session_state.admin_logged_in = False
            st.rerun()
        
        # MANIFEST FIX: Ensure manifest is ALWAYS loaded for the dropdown
        if 'found_events' not in st.session_state:
            st.session_state.found_events = get_manifest_events()
        
        event_list = [e['name'] for e in st.session_state.found_events]
        sel_show = st.selectbox("Select Competition to Sync:", event_list)
        
        if st.button("🚀 Sync Full Show Map"):
            # 1. Probe the selected event from the manifest
            target = next(e for e in st.session_state.found_events if e['name'] == sel_show)
            
            with st.spinner(f"Latched to {sel_show}. Probing Live Circuit..."):
                # 2. Extract URLs (Handling both Dict and String paths)
                p_url = target['url']['prelims'] if isinstance(target['url'], dict) else target['url']
                f_url = target['url']['finals'] if isinstance(target['url'], dict) else ""
                
                # 3. Execute Scrape (Playwright)
                df_live, f_slots = pull_dual_event_data(p_url, f_url)
                
                if not df_live.empty:
                    # 4. Update RAM (Session State)
                    st.session_state.active_event_data = df_live
                    st.session_state.finals_slots = f_s
                    st.session_state.active_event_name = sel_show
                    st.session_state.active_urls = {"prelims": p_url, "finals": f_url}
                    
                    # 5. Update EEPROM (MongoDB Persistence)
                    db["live_state"].update_one(
                        {"type": "current_session"}, 
                        {"$set": {
                            "name": sel_show, 
                            "slots": f_s, 
                            "data": df_live.to_dict("records"),
                            "urls": st.session_state.active_urls,
                            "last_updated": datetime.now()
                        }}, 
                        upsert=True
                    )
                    st.success(f"✅ {sel_show} Latched to Live Hub")
                    st.rerun()

                st.divider()
                if not st.session_state.active_event_data.empty:
                    # Display current 'RAM' stats
                    st.success(f"🛰️ ACTIVE: {st.session_state.active_event_name}")
                    
                    c1, c2 = st.columns(2)
                    c1.metric("Guards Loaded", len(st.session_state.active_event_data))
                    
                    # Count the total finalists allowed across all classes
                    total_slots = sum(st.session_state.finals_slots.values())
                    c2.metric("Total Finals Slots", total_slots)

                    if st.button("🗑️ Clear System RAM"):
                        st.session_state.active_event_data = pd.DataFrame()
                        st.session_state.finals_slots = {}
                        st.session_state.active_event_name = "No Active Event"
                        st.rerun()