#import asyncio
import sys

# # MUST BE AT THE ABSOLUTE TOP
# if sys.platform == 'win32':
#     # This prevents the NotImplementedError by forcing the Selector loop
#     # even if one is already running in a background thread.
#     try:
#         asyncio.get_event_loop_policy()
#         asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#     except:
#         pass


import pymongo
import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
# Import your helper functions
from wgi_final import get_manifest_events, get_wgi_events_by_date, pull_full_event_data, pull_dual_event_data

import os
os.system("playwright install chromium")


if "active_event_data" not in st.session_state:
    st.session_state.active_event_data = pd.DataFrame()
if "active_event_name" not in st.session_state:
    st.session_state.active_event_name = "No Active Event"


# Initialize session state for login if it doesn't exist
if 'admin_logged_in' not in st.session_state:
    st.session_state.admin_logged_in = False

# Function to handle the login check
def check_login(password):
    if password == st.secrets["ADMIN_PASS"]:
        st.session_state.admin_logged_in = True
        return True
    return False



# --- 1. Page Configuration ---
st.set_page_config(
    page_title="WGI 2026 Analytics", 
    layout="wide", 
    initial_sidebar_state="collapsed",
    page_icon="🚩"
)

# --- 2. Database Connection Logic ---
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["MONGO_URI"])

client = init_connection()
# SWAPPED: database is rankings_2026, collection is wgi_analytics
db = client["rankings_2026"]
collection = db["wgi_analytics"]

# --- 1. INITIALIZATION: Probe for Persistent Latch ---
if "admin_logged_in" not in st.session_state:
    # Check MongoDB for an active admin session latch
    latch = db["system_state"].find_one({"type": "admin_session"})
    st.session_state.admin_logged_in = True if latch else False

# Auto-load the manifest so buttons don't disappear
if st.session_state.admin_logged_in and 'found_events' not in st.session_state:
    st.session_state.found_events = get_manifest_events()

# --- 3. Functional Logic ---

def sync_to_cloud(df):
    """Clears 'wgi_analytics' and uploads fresh data to BSICluster."""
    if not df.empty:
        # Ensure data is numeric before upload
        df.columns = [c.lower() for c in df.columns] # Force lowercase for DB consistency
        data_dict = df.to_dict("records")
        collection.delete_many({}) 
        collection.insert_many(data_dict)
        return True
    return False

@st.cache_data(ttl=300) # Refresh every 5 minutes
def load_data():
    """Pulls current rankings and normalizes columns."""
    items = list(collection.find())
    if not items:
        return pd.DataFrame()
    df_raw = pd.DataFrame(items)
    # Normalize column names to Title Case for the UI logic below
    df_raw.columns = [c.title() for c in df_raw.columns]
    # Handle the specific 'Average_Score' naming if it comes in as 'Average_Score' or 'Average_Score'
    if 'Average_Score' not in df_raw.columns and 'Average_Score' in df_raw.columns:
        df_raw.rename(columns={'Average_Score': 'Average_Score'}, inplace=True)
    return df_raw

# --- 4. Main Application Logic ---
st.title("🏆 WGI 2026 Color Guard Analytics")
df = load_data()

tab1, tab2, tab3, tab4 = st.tabs(["Analytics", "BSI Calculator", "Live Hub", "Admin"])

# --- TAB 1: National Overview ---
with tab1:
    if df.empty:
        st.warning("No data found in BSICluster. Check your Admin tab settings.")
    else:
        selected_class = st.selectbox("Select Division:", sorted(df['Class'].unique()), key="nav_class")
        class_df = df[df['Class'] == selected_class].copy()
        class_df = class_df.sort_values(by='Average_Score', ascending=False)
        class_df['Rank'] = range(1, len(class_df) + 1)
        
        st.dataframe(class_df[['Rank', 'Guard', 'Average_Score', 'Season_High', 'Events_Count']], 
                     width='stretch', hide_index=True)

# --- TAB 2: Guard Comparison (Rebuilt from Scratch) ---
with tab2:
    if df.empty:
        st.info("No data available. Please run the sync in the Admin tab.")
    else:
        # 1. Selection Header
        c1, c2 = st.columns(2)
        with c1:
            sel_class = st.selectbox("1. Select Division", sorted(df['Class'].unique()), key="comp_class")
        
        # Filter and Sort to establish fresh Ranks
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

            # --- SECTION 2: Class Benchmarks (1st and 15th) ---
            st.subheader(f"{sel_class} Benchmarks")
            
            # Identify Benchmark Scores
            first_place_score = float(comp_df.iloc[0]['Average_Score'])
            
            # Handle classes with fewer than 15 guards
            if len(comp_df) >= 15:
                fifteenth_place_score = float(comp_df.iloc[14]['Average_Score'])
                bubble_label = "15th Place (Finalist Bubble)"
            else:
                fifteenth_place_score = float(comp_df.iloc[-1]['Average_Score'])
                bubble_label = "Last Place (Current Class Size)"

            # Calculate Gaps
            my_avg = float(guard_data['Average_Score'])
            gap_to_first = my_avg - first_place_score
            gap_to_fifteenth = my_avg - fifteenth_place_score

            # Display Benchmark Metrics
            b1, b2 = st.columns(2)
            b1.metric("1st Place Score", f"{first_place_score:.2f}")
            b2.metric(bubble_label, f"{fifteenth_place_score:.2f}")

            g1, g2 = st.columns(2)
            g1.metric("Gap to 1st", f"{gap_to_first:.2f}", delta=f"{gap_to_first:.2f}")
            g2.metric("Gap to 15th", f"{gap_to_fifteenth:.2f}", delta=f"{gap_to_fifteenth:.2f}")


            # --- SECTION 2.5: Competitive Density ---
            #st.write("---")
            class_spread = first_place_score - fifteenth_place_score
            
            st.metric(
                label=f"Championship Spread (1st to 15th)", 
                value=f"{class_spread:.2f} pts",
                help="The point range between the current #1 team and the last qualifying finalist spot."
            )
            
            # Contextual analysis for the user
            if class_spread < 5:
                st.info("🔥 **High Density:** This division is extremely competitive. Every tenth of a point matters.")
            elif class_spread > 12:
                st.warning("📊 **Wide Spread:** There is a significant gap between the elite and the bubble teams.")



            

            # --- SECTION 3: National Percentile (Dynamic Top/Bottom) ---
            st.divider()
            total_guards = len(comp_df)
            current_rank = int(guard_data['Rank'])
            
            # 1. Calculate the base ratio (0.0 to 1.0)
            # Example: 21 / 29 = 0.724
            rank_ratio = float(current_rank) / float(total_guards)
            
            # 2. Split Logic: Top 50% vs Bottom 50%
            if rank_ratio <= 0.5:
                # Top Half Logic: Direct percentage
                display_percent = int(rank_ratio * 100)
                if display_percent == 0: display_percent = 1 # UX fix for Rank #1
                label_text = f"Overall Standing: Top {display_percent}%"
                status_color = "Top"
            else:
                # Bottom Half Logic: Show percentage of teams below you
                # Example: (1.0 - 0.724) * 100 = 27.6 -> 28%
                display_percent = int((1.0 - rank_ratio) * 100)
                label_text = f"Overall Standing: Bottom {display_percent}%"
                status_color = "Bottom"

            st.write(f"### {label_text}")
            
            # 3. Progress Bar Fill
            # We still want the bar to be 'fuller' for better ranks
            bar_fill = (float(total_guards) - float(current_rank) + 1.0) / float(total_guards)
            st.progress(
                max(0.0, min(1.0, bar_fill)), 
                text=f"{sel_guard} is #{current_rank} out of {total_guards} teams"
            )
# --- TAB 3: LIVE HUB ---
with tab3:
    if "active_event_data" in st.session_state and not st.session_state.active_event_data.empty:
        # Signal Conditioning
        live_df = st.session_state.active_event_data.copy()
        slots = st.session_state.get('finals_slots', {})
        
        # 1. UI Selectors
        col1, col2 = st.columns(2)
        with col1:
            all_classes = sorted(live_df['Class'].unique())
            selected_class = st.selectbox("🎯 Filter Class:", ["All Classes"] + all_classes)
        with col2:
            school_scope = live_df if selected_class == "All Classes" else live_df[live_df['Class'] == selected_class]
            selected_school = st.selectbox("🏫 Highlight Guard:", ["None"] + sorted(school_scope['Guard'].tolist()))

        # 2. Dynamic Ranking & Advancement Logic
        if selected_class != "All Classes":
            live_df = live_df[live_df['Class'] == selected_class]
            live_df['Score'] = pd.to_numeric(live_df['Score'], errors='coerce').fillna(0.0)
            num_slots = slots.get(selected_class, 10)
            
            if any(live_df['Score'] > 0):
                live_df = live_df.sort_values(by="Score", ascending=False)
                live_df['Rank'] = range(1, len(live_df) + 1)
                live_df['Status'] = ["✅ IN" if i < num_slots else "❌ OUT" for i in range(len(live_df))]
                # Reorder columns for visibility
                cols = ['Status', 'Rank'] + [c for c in live_df.columns if c not in ['Status', 'Rank']]
                live_df = live_df[cols]
            else:
                live_df['SortTime'] = pd.to_datetime(live_df['Perform Time'], format='%I:%M %p', errors='coerce')
                live_df = live_df.sort_values(by='SortTime').drop(columns=['SortTime'])
        
        # 3. Main Display
        st.subheader(f"📊 {st.session_state.get('active_event_name', 'Live Event')}")
        
        def highlight_row(row):
            return ['background-color: #2e4a3e' if row['Guard'] == selected_school else '' for _ in row]

        st.dataframe(
            live_df.style.apply(highlight_row, axis=1),
            width='stretch', hide_index=True,
            column_config={"Score": st.column_config.NumberColumn("Live Score", format="%.3f"), "Rank": "🏆"}
        )

        # 4. Finals Projections Section
        st.divider()
        with st.expander("🔮 View Finals Projections & Bubble"):
            if not slots:
                st.warning("No Finals structure detected. Pull the 'Full Event Map' in Admin.")
            else:
                for g_class, count in slots.items():
                    class_data = st.session_state.active_event_data[st.session_state.active_event_data['Class'] == g_class].copy()
                    class_data['Score'] = pd.to_numeric(class_data['Score'], errors='coerce').fillna(0.0)
                    
                    st.markdown(f"### {g_class} — :orange[{count} Finals Slots]")
                    if any(class_data['Score'] > 0):
                        top_in = class_data.sort_values(by="Score", ascending=False).head(count)
                        st.dataframe(top_in[['Guard', 'Score']], width='stretch', hide_index=True)
    else:
        st.info("⚪ System Idle: Sync a show in the Admin tab to begin.")

# --- TAB 4: ADMIN ---
with tab4:
    if not st.session_state.admin_logged_in:
        st.header("🔐 Admin Access")
        admin_pwd = st.text_input("Admin Password", type="password")
        if st.button("Authorize"):
            if admin_pwd == st.secrets["ADMIN_PASS"]:
                # Burn the latch into MongoDB so it survives refresh
                db["system_state"].update_one(
                    {"type": "admin_session"},
                    {"$set": {"active": True, "timestamp": datetime.now()}},
                    upsert=True
                )
                st.session_state.admin_logged_in = True
                st.rerun()
    else:
        # --- SECURE AREA: Buttons stay visible here ---
        st.success("🛰️ System Link Established (Persistent)")
        
        # Logout logic to clear the latch
        if st.button("🛑 Logout / Clear Latch"):
            db["system_state"].delete_one({"type": "admin_session"})
            st.session_state.admin_logged_in = False
            st.rerun()


        if 'found_events' not in st.session_state:
        with st.spinner("Initializing Manifest..."):
            st.session_state.found_events = get_manifest_events()
            
            if st.button("🚀 Sync Full Show Map"):
                try:
                    target = next(e for e in st.session_state.found_events if e['name'] == selected_show)
                    with st.spinner(f"Probing {selected_show}..."):
                        # Dual-Link Routing logic
                        if isinstance(target['url'], dict):
                            df, slots = pull_dual_event_data(target['url']['prelims'], target['url']['finals'])
                        else:
                            df, slots = pull_dual_event_data(target['url'], "")
                        
                        if not df.empty:
                            st.session_state.active_event_data = df
                            st.session_state.finals_slots = slots
                            st.session_state.active_event_name = selected_show
                            
                            # LATCH TO DB: Save this session for persistence
                            db["live_state"].update_one(
                                {"type": "current_session"},
                                {"$set": {
                                    "name": selected_show,
                                    "slots": slots,
                                    "data": df.to_dict("records"),
                                    "last_updated": datetime.now()
                                }},
                                upsert=True
                            )
                            st.success("✅ State Latched to Database")
                            st.rerun()
                except Exception as e:
                    st.error(f"Sync Fault: {e}")

        # System Status Indicator
        st.divider()
        if not st.session_state.active_event_data.empty:
            st.success(f"🛰️ ACTIVE: {len(st.session_state.active_event_data)} guards in RAM.")
            if st.button("🗑️ Clear RAM"):
                st.session_state.active_event_data = pd.DataFrame()
                st.rerun()