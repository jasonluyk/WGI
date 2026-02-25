import os
import subprocess
import streamlit as st
import pandas as pd
import pymongo
from streamlit_autorefresh import st_autorefresh

if "admin_auth" not in st.session_state:
    st.session_state.admin_auth = False

st.set_page_config(page_title="WGI 2026 Analytics", layout="wide", page_icon="🚩")

@st.cache_resource
def init_connection():
    # 1. Look in the cloud first...
    mongo_url = os.environ.get("MONGO_URI")

    # 2. If we are on your desktop, just use the Streamlit secrets file!
    if not mongo_url:
        mongo_url = st.secrets["MONGO_URI"]
    client = pymongo.MongoClient(mongo_url)
    return client

client = init_connection()
db = client["rankings_2026"]


# This prevents us from having to scrape WGI's protected index page
EVENT_LUT = {
    "Flint Regional": "a0tUy00000YzB6GIAV",
    "Gulfport Regional+": "a0tUy00000YzDHhIAN",
    "Indianapolis Regional+": "a0tUy00000Yyws4IAB",
    "Philadelphia Regional+": "a0tUy00000YzDxdIAF",
    "San Diego Regional": "a0tUy00000Yysy7IAB",
    "Austin Regional+": "a0tUy00000YzFLAIA3",
    "Charlotte Regional+": "a0tUy00000Yz92qIAB",
    "Chicago Regional": "a0tUy00000YzFLBIA3",
    "Denver Regional": "a0tUy00000Yz439IAB",
    "Nashville Regional": "a0tUy00000YzDklIAF",
    "Phoenix Regional+": "a0tUy00000Yz5FSIAZ",
    "Tampa Regional": "a0tUy00000YynIQIAZ",
    "Knoxville Regional": "a0tUy00000YzFQCIA3",
    "Manhattan Beach Regional+": "a0tUy00000Yz77ZIAR",
    "Memphis Regional": "a0tUy00000YzYSbIAN",
    "Minneapolis Regional": "a0tUy00000Yz2SpIAJ",
    "Seattle Regional": "a0tUy00000YzNDwIAN",
    "Tulsa Regional+": "a0tUy00000YzA2KIAV",
    "Union City Regional": "a0tUy00000YzNCGIA3",
    "Avon Regional+": "a0tUy00000Z064WIAR",
    "Bellevue Regional": "a0tUy00000YzzO6IAJ",
    "Las Vegas Regional": "a0tUy00000Z09giIAB",
    "Mansfield Regional+": "a0tUy00000Z0BddIAF",
    "Richmond Regional": "a0tUy00000YzgwTIAR",
    "Salt Lake City Regional": "a0tUy00000YzyOmIAJ",
    "Stuart Regional+": "a0tUy00000Z0CI0IAN",
    "Bethlehem Regional+": "a0tUy00000Yze6fIAB",
    "Buford Regional+": "a0tUy00000Z0770IAB",
    "Palm Desert Regional+": "a0tUy00000Z0IC8IAN",
    "San Antonio Regional+": "a0tUy00000Z0LbNIAV",
    # Added the specific target from your earlier code:
    "Test Target": "a0uUy000004Hny6IAC"
}

# --- Load National Data ---
def load_national_data():
    items = list(db["wgi_analytics"].find({}, {"_id": 0}))
    if not items: return pd.DataFrame()
    df = pd.DataFrame(items)
    df.columns = [str(c).title() for c in df.columns]
    
    # Safety net: If 'Score' exists but 'Show' doesn't, it means seed_db hasn't run yet
    if 'Show' not in df.columns:
        df['Show'] = "Legacy Database Format"
    return df

# --- Helper to aggregate the flat data ---
def get_aggregated_national_data(raw_df):
    if raw_df.empty: return raw_df
    # Group by Guard and Class to mathematically find their high and average
    agg_df = raw_df.groupby(['Guard', 'Class']).agg(
        Season_High=('Score', 'max'),
        Average_Score=('Score', 'mean'),
        Shows_Attended=('Show', 'count')
    ).reset_index()
    return agg_df

df = load_national_data()


def load_live_data():
    live_doc = db["live_state"].find_one({"type": "current_session"})
    if live_doc and "data" in live_doc:
        df = pd.DataFrame(live_doc["data"])
        spots_dict = live_doc.get("spots", {}) # Grab the spot counts
        return df, spots_dict
    return pd.DataFrame(), {}

# Ensure you unpack both variables where you call it:
live_df, live_spots_dict = load_live_data()

df = load_national_data()


def calculate_advancement(df, event_name, class_spots):
    """Dynamically calculates Finals advancement based on WGI Regional rules."""
    
    # Check if this is a Regional+ event
    is_plus_event = "+" in event_name
    
    # Create the new Status column and default everyone to waiting
    df['Status'] = "⏳ Pending Score"
    
    # Extract the "Base Class" (e.g., "Scholastic A" from "Scholastic A - Round 1")
    df['Base Class'] = df['Class'].apply(lambda x: x.split(' - ')[0] if ' - ' in x else x)
    
    for base_class in df['Base Class'].unique():
        # How many spots are available for this entire class?
        total_spots = class_spots.get(base_class, 0)
        if total_spots == 0: continue
            
        class_mask = df['Base Class'] == base_class
        
        # --- REGIONAL+ SCHOLASTIC A LOGIC ---
        if is_plus_event and base_class == "Scholastic A":
            # Pod 1: Rounds 1 & 2
            pod1_mask = class_mask & df['Class'].str.contains("Round 1|Round 2", na=False)
            # Pod 2: Rounds 3 & 4
            pod2_mask = class_mask & df['Class'].str.contains("Round 3|Round 4", na=False)
            
            scored_pod1 = df[pod1_mask & (df['Prelims Score'] > 0.0)]
            scored_pod2 = df[pod2_mask & (df['Prelims Score'] > 0.0)]
            
            # 1. Top 5 from Pod 1
            pod1_adv = scored_pod1.nlargest(5, 'Prelims Score')
            df.loc[pod1_adv.index, 'Status'] = "✅ Pod 1 Adv"
            
            # 2. Top 5 from Pod 2
            pod2_adv = scored_pod2.nlargest(5, 'Prelims Score')
            df.loc[pod2_adv.index, 'Status'] = "✅ Pod 2 Adv"
            
            # 3. The 5 Wildcards (Next highest scores overall)
            remaining_mask = class_mask & (df['Prelims Score'] > 0.0) & (~df.index.isin(pod1_adv.index)) & (~df.index.isin(pod2_adv.index))
            wildcards = df[remaining_mask].nlargest(5, 'Prelims Score')
            df.loc[wildcards.index, 'Status'] = "🌟 Wildcard"
            
            # 4. Mark the rest as cut
            below_mask = class_mask & (df['Prelims Score'] > 0.0) & (df['Status'] == "⏳ Pending Score")
            df.loc[below_mask, 'Status'] = "❌ Below Cutline"
            
        # --- STANDARD ADVANCEMENT LOGIC ---
        else:
            scored = df[class_mask & (df['Prelims Score'] > 0.0)]
            advanced = scored.nlargest(total_spots, 'Prelims Score')
            
            df.loc[advanced.index, 'Status'] = "✅ Advanced"
            
            below_mask = class_mask & (df['Prelims Score'] > 0.0) & (~df.index.isin(advanced.index))
            df.loc[below_mask, 'Status'] = "❌ Below Cutline"
            
    # Clean up the dataframe for display
    return df

st.title("🏆 WGI 2026 Color Guard Analytics")
tab1, tab2, tab3, tab5, tab4 = st.tabs(["Overview", "National Stats", "Live Hub", "Past Events", "Admin"])

# --- TAB 1: National Rankings ---
with tab1:
    st.header("National Class Rankings")
    if df.empty: 
        st.warning("No data found. Run seed_db.py to populate the database.")
    else:
        # 1. Select the Division
        c1, c2 = st.columns(2)
        with c1:
            sel_class = st.selectbox("1. Division:", sorted(df['Class'].unique()), key="nav_class")
        
        # 2. Select the Show (Filters based on the chosen division)
        with c2:
            available_shows = sorted(df[df['Class'] == sel_class]['Show'].unique())
            sel_show = st.selectbox("2. Event:", ["All Shows"] + available_shows, key="nav_show")

        st.divider()

        # 3. Dynamic Display Logic
        if sel_show == "All Shows":
            # Display Aggregated Season Data
            c_df = df[df['Class'] == sel_class].copy()
            agg_df = get_aggregated_national_data(c_df)
            agg_df = agg_df.sort_values(by='Season_High', ascending=False)
            agg_df['Rank'] = range(1, len(agg_df) + 1)
            
            st.subheader(f"Overall National Rankings: {sel_class}")
            st.dataframe(
                agg_df[['Rank', 'Guard', 'Season_High', 'Average_Score', 'Shows_Attended']], 
                width='stretch', 
                hide_index=True
            )
        else:
            # Display Specific Event Results
            c_df = df[(df['Class'] == sel_class) & (df['Show'] == sel_show)].copy()
            c_df = c_df.sort_values(by='Score', ascending=False)
            c_df['Rank'] = range(1, len(c_df) + 1)
            
            st.subheader(f"Results: {sel_show}")
            st.dataframe(
                c_df[['Rank', 'Guard', 'Score']], 
                width='stretch', 
                hide_index=True
            )

# --- TAB 2: Compare Guards (BSI) ---
with tab2:
    st.header("BSI Comparison Calculator")
    if df.empty:
        st.info("Sync national data in the Admin tab first.")
    else:
        # The BSI calculator needs the aggregated data (Averages and Highs)
        agg_national_df = get_aggregated_national_data(df)
        
        c1, c2 = st.columns(2)
        with c1: 
            sel_class_bsi = st.selectbox("1. Select Division", sorted(agg_national_df['Class'].unique()), key="comp_class")
        
        # Filter down to the selected class
        comp_df = agg_national_df[agg_national_df['Class'] == sel_class_bsi].copy().sort_values(by='Average_Score', ascending=False)
        
        # SAFETY NET: Make sure the class isn't empty before calculating ranks
        if not comp_df.empty:
            comp_df['Rank'] = range(1, len(comp_df) + 1)
            
            with c2: 
                sel_guard = st.selectbox("2. Select Guard", sorted(comp_df['Guard'].unique()), key="comp_guard")

            # Everything related to the selected guard MUST be indented under this IF
            if sel_guard:
                guard_data = comp_df[comp_df['Guard'] == sel_guard].iloc[0]
                st.subheader(f"Current Standing: {sel_guard}")
                m1, m2, m3 = st.columns(3)
                m1.metric("National Rank", f"#{int(guard_data['Rank'])}")
                m2.metric("Average Score", f"{float(guard_data['Average_Score']):.2f}")
                m3.metric("Season High", f"{float(guard_data['Season_High']):.2f}")
                
                st.divider()
                st.subheader(f"{sel_class_bsi} Benchmarks")
                
                first_place_score = float(comp_df.iloc[0]['Average_Score'])
                if len(comp_df) >= 15:
                    fifteenth_place_score = float(comp_df.iloc[14]['Average_Score'])
                    bubble_label = "15th Place (Finalist Bubble)"
                else:
                    fifteenth_place_score = float(comp_df.iloc[-1]['Average_Score'])
                    bubble_label = "Last Place (Current Class Size)"

                my_avg = float(guard_data['Average_Score'])
                gap_to_first = my_avg - first_place_score
                gap_to_fifteenth = my_avg - fifteenth_place_score

                b1, b2 = st.columns(2)
                b1.metric("1st Place Score", f"{first_place_score:.2f}")
                b2.metric(bubble_label, f"{fifteenth_place_score:.2f}")

                g1, g2 = st.columns(2)
                g1.metric("Gap to 1st", f"{gap_to_first:.2f}", delta=f"{gap_to_first:.2f}")
                g2.metric("Gap to 15th", f"{gap_to_fifteenth:.2f}", delta=f"{gap_to_fifteenth:.2f}")
                
                st.divider()
                total_guards = len(comp_df)
                current_rank = int(guard_data['Rank'])
                bottom_ratio = (float(total_guards) - float(current_rank)) / float(total_guards)
                bottom_percent = int(bottom_ratio * 100)
                
                if current_rank <= (total_guards / 2):
                    top_percent = int((float(current_rank) / float(total_guards)) * 100)
                    if top_percent == 0: top_percent = 1
                    label_text = f"🏆 Overall Standing: Top {top_percent}%"
                else:
                    label_text = f"📊 Overall Standing: Bottom {bottom_percent}%"

                st.write(f"### {label_text}")
                st.progress(max(0.0, min(1.0, bottom_ratio)), text=f"{sel_guard} is #{current_rank} out of {total_guards} teams")
        else:
            st.warning(f"No performance data found for {sel_class_bsi} yet.")



# --- TAB 3: Live Hub ---
with tab3:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=180000, key="hub_refresh")
    
    st.header("Live Event Signal")
    
    c1, c2 = st.columns([0.8, 0.2])
    with c1:
        active_show = db["system_state"].find_one({"type": "active_show_name"})
        show_name = active_show["name"] if active_show else "Unknown Show"
        st.subheader(f"📊 Live Signal: {show_name}")
    with c2:
        if st.button("🔄 Refresh Now"):
            st.rerun()
            
    if live_df.empty:
        st.info("⚪ System Idle: No live show currently latched. Load one via Admin.")
    else:
        # 1. Create a "Base Class" column so split rounds group together
        live_df['Base Class'] = live_df['Class'].apply(lambda x: x.split(' - ')[0] if ' - ' in str(x) else str(x))
        base_classes = sorted(live_df['Base Class'].unique())
        
# 2. Build the Smart Dropdown Options
        dropdown_options = ["All"]
        for bc in base_classes:
            sub_classes = sorted(live_df[live_df['Base Class'] == bc]['Class'].unique())
            if len(sub_classes) > 1 or sub_classes[0] != bc:
                dropdown_options.append(f"🏆 ALL {bc} (Leaderboard)")
                for sc in sub_classes:
                    dropdown_options.append(sc)
            else:
                dropdown_options.append(bc)
        
        # --- STICKY DROPDOWN LOGIC ---
        if "current_hub_view" not in st.session_state:
            st.session_state.current_hub_view = "All"
            
        try:
            start_index = dropdown_options.index(st.session_state.current_hub_view)
        except ValueError:
            start_index = 0
            
        # THIS IS THE ONLY SELECTBOX!
        f_c = st.selectbox("View Leaderboard for:", dropdown_options, index=start_index, key="live_hub_filter")
        
        st.session_state.current_hub_view = f_c
        # -----------------------------
        
        # 3. Figure out the Base Class vs the Specific Round
        is_leaderboard_view = "🏆 ALL" in f_c
        
        if is_leaderboard_view:
            base_target_class = f_c.replace("🏆 ALL ", "").replace(" (Leaderboard)", "")
            specific_round = None
        else:
            base_target_class = f_c.split(' - ')[0] if ' - ' in f_c else f_c
            specific_round = f_c
            
        engine_spots = live_spots_dict.copy() if isinstance(live_spots_dict, dict) else {}
        
        # 3. Figure out the Base Class vs the Specific Round
        is_leaderboard_view = "🏆 ALL" in f_c
        
        if is_leaderboard_view:
            base_target_class = f_c.replace("🏆 ALL ", "").replace(" (Leaderboard)", "")
            specific_round = None
        else:
            base_target_class = f_c.split(' - ')[0] if ' - ' in f_c else f_c
            specific_round = f_c
            
        engine_spots = live_spots_dict.copy() if isinstance(live_spots_dict, dict) else {}
        
        if f_c != "All":
            guards_in_class = len(live_df[live_df['Base Class'] == base_target_class])
            auto_detected = engine_spots.get(base_target_class, 0)
            default_spots = auto_detected if auto_detected > 0 else min(10, guards_in_class)
            default_spots = max(1, default_spots) 
            
            spots = st.number_input(
                f"Finals Spots for {base_target_class} (Auto-Detected: {auto_detected}):", 
                min_value=1, 
                max_value=max(1, guards_in_class), 
                value=int(default_spots), 
                step=1, 
                key=f"spots_{base_target_class}"
            )
            engine_spots[base_target_class] = spots
            
            processed_df = calculate_advancement(live_df.copy(), show_name, engine_spots)
            
            if is_leaderboard_view:
                display_df = processed_df[processed_df['Base Class'] == base_target_class].copy()
            else:
                display_df = processed_df[processed_df['Class'] == specific_round].copy()
            
            display_df['SortTime'] = pd.to_datetime(display_df['Prelims Time'], format='%I:%M %p', errors='coerce')
            display_df['HasScore'] = display_df['Prelims Score'] > 0.0
            
            display_df = display_df.sort_values(by=["HasScore", "Prelims Score", "SortTime"], ascending=[False, False, True])
            display_df['Prelims Rank'] = range(1, len(display_df) + 1)
            
            cols = ['Status', 'Prelims Time', 'Guard', 'Class', 'Prelims Score', 'Prelims Rank', 'Finals Score']
            valid_cols = [c for c in cols if c in display_df.columns]
            st.dataframe(display_df[valid_cols], width='stretch', hide_index=True)
            
        else:
            # --- The "All" View Logic ---
            processed_df = calculate_advancement(live_df.copy(), show_name, engine_spots)
            display_df = processed_df.copy()
            
            display_df['SortTime'] = pd.to_datetime(display_df['Prelims Time'], format='%I:%M %p', errors='coerce')
            display_df['HasScore'] = display_df['Prelims Score'] > 0.0
            
            display_df = display_df.sort_values(
                by=["Base Class", "HasScore", "Prelims Score", "SortTime"], 
                ascending=[True, False, False, True]
            )
            
            cols = ['Status', 'Prelims Time', 'Guard', 'Class', 'Prelims Score', 'Finals Score']
            valid_cols = [c for c in cols if c in display_df.columns]
            
            st.dataframe(display_df[valid_cols], width='stretch', hide_index=True)


# --- TAB 4: Admin (The Control Deck) ---
with tab4:
    st.header("Admin Control Deck")
    
    # 1. THE LOGIN SCREEN
    if not st.session_state.admin_auth:
        st.info("🔒 Secure area. Please log in to access system controls.")
        pwd = st.text_input("Admin Password", type="password", key="admin_login_pwd")
        
        if st.button("Login"):
            if pwd == st.secrets["ADMIN_PASS"]:
                st.session_state.admin_auth = True
                st.rerun() # Instantly redraws the page to show the controls
            else:
                st.error("❌ Access Denied.")
                
    # 2. THE CONTROL DECK (Only shows if authenticated)
    else:
        c1, c2 = st.columns([0.8, 0.2])
        with c1:
            st.success("🛰️ System Link Established")
        with c2:
            if st.button("Logout"):
                st.session_state.admin_auth = False
                st.rerun()
                
        st.divider()
        
        # --- YOUR EXISTING ADMIN CONTROLS GO HERE ---
        st.subheader("1. System Discovery")
        if st.button("🚀 Auto-Discover WGI Events"):
            db["system_state"].insert_one({"type": "scraper_command", "action": "sync_national"})
            st.toast("Auto-Discovery command sent to Background Worker.")

        st.divider()
        
        st.subheader("🛠️ Database Management")
        
        # We make the button red/primary so you don't click it by accident during a live show!
        if st.button("🌱 Seed Database (Run seed_db.py)", type="primary"):
            with st.spinner("Running seed_db.py in the background..."):
                try:
                    # This tells the server to run the script and capture any print() statements
                    result = subprocess.run(
                        ["python", "seed_db.py"], 
                        capture_output=True, 
                        text=True, 
                        check=True
                    )
                    st.success("Database seeded successfully!")
                    
                    # Show the terminal output in a dropdown so you can verify it worked
                    with st.expander("View Terminal Output"):
                        st.code(result.stdout)
                        
                except subprocess.CalledProcessError as e:
                    st.error("🚨 Error running seed_db.py!")
                    with st.expander("View Error Log"):
                        st.code(e.stderr)

        st.divider()
        st.subheader("2. Live Event Control")
        
        discovered_events = list(db["event_metadata"].find({}, {"_id": 0}))
        
        if not discovered_events:
            st.warning("No events found. Run the Auto-Discover above.")
        else:
            # --- THE SMART FILTER ---
            # Future/Current = No ShowID yet, OR has schedule links
            live_candidates = [
                e for e in discovered_events 
                if not e.get("show_id") or e.get("p_url") or e.get("f_url")
            ]
            
            # Sort alphabetically
            live_candidates = sorted(live_candidates, key=lambda x: x.get("name", ""))
            event_options = {e["name"]: e for e in live_candidates}
            
            c1, c2 = st.columns([0.7, 0.3])
            with c2:
                # A fallback toggle just in case you need to see old events
                st.write("") # Spacing to align with the selectbox
                show_all = st.checkbox("Include Past Events")
                
            if show_all:
                 all_sorted = sorted(discovered_events, key=lambda x: x.get("name", ""))
                 event_options = {e["name"]: e for e in all_sorted}
                 
            with c1:
                selected_show_name = st.selectbox("Select Live Event:", list(event_options.keys()))
                
            # THE FIX: Only try to load data if a valid event is actually selected
            if selected_show_name:
                event_data = event_options[selected_show_name]
                target_show_id = event_data.get("show_id", "")
                
                st.write(f"**Auto-Matched WGI ID:** `{target_show_id if target_show_id else 'Not posted yet'}`")
                
                st.caption("Paste the CompetitionSuite schedules for this specific show.")
                p_url = st.text_input("Prelims Schedule URL", value=event_data.get("p_url", ""))
                f_url = st.text_input("Finals Schedule URL", value=event_data.get("f_url", ""))
                
                if st.button("📡 Latch & Save Event"):
                    db["event_metadata"].update_one(
                        {"name": selected_show_name},
                        {"$set": {"p_url": p_url, "f_url": f_url}}
                    )
                    
                    db["system_state"].insert_one({
                        "type": "scraper_command", 
                        "action": "sync_live", 
                        "show_id": target_show_id,
                        "prelims_url": p_url, 
                        "finals_url": f_url
                    })
                    
                    db["system_state"].update_one(
                        {"type": "active_show_name"}, 
                        {"$set": {
                            "name": selected_show_name, 
                            "show_id": target_show_id, 
                            "p_url": p_url, 
                            "f_url": f_url
                        }}, 
                        upsert=True
                    )
                    st.toast(f"Saved links and latched onto {selected_show_name}!")
            else:
                st.info("⏳ Waiting for Auto-Discovery to find upcoming events. You can check 'Include Past Events' to view older shows.")
                
        if st.button("🗑️ Clear Live Data"):
            db["live_state"].delete_many({})
            db["system_state"].delete_one({"type": "active_show_name"})
            st.rerun()

# --- TAB 5: Past Events Archive ---
with tab5:
    st.header("Past Events Archive")
    st.caption("View finalized leaderboards for completed WGI events.")
    
    all_events = list(db["event_metadata"].find({}, {"_id": 0}))
    
    if not all_events:
        st.info("No events found. Run Auto-Discovery in the Admin tab.")
    else:
        completed_events = [e for e in all_events if e.get("show_id")]
        completed_events = sorted(completed_events, key=lambda x: x.get("name", ""))
        event_dict = {e["name"]: e["show_id"] for e in completed_events}
        
        c1, c2 = st.columns([0.8, 0.2])
        with c1:
            selected_archive = st.selectbox("Select Completed Event:", ["-- Choose an Event --"] + list(event_dict.keys()))
        with c2:
            st.write("") # Spacing
            st.write("")
            if st.button("📥 Request Scores") and selected_archive != "-- Choose an Event --":
                target_id = event_dict[selected_archive]
                
                # 1. Set the database flag to "loading"
                db["archive_state"].update_one(
                    {"type": "current_archive"},
                    {"$set": {"status": "loading", "event_name": selected_archive, "show_id": target_id}},
                    upsert=True
                )
                
                # 2. Send the command to the worker
                db["system_state"].insert_one({
                    "type": "scraper_command", 
                    "action": "sync_archive", 
                    "show_id": target_id,
                    "event_name": selected_archive
                })
                # 3. Instantly rerun the page to trigger the spinner below
                st.rerun() 
        
        st.divider()
        
        # --- THE AUTO-REFRESH LOGIC ---
        archive_doc = db["archive_state"].find_one({"type": "current_archive"})
        
        if archive_doc and archive_doc.get("event_name") == selected_archive:
            status = archive_doc.get("status")
            
            if status == "loading":
                # Streamlit will spin, wait 2 seconds, and refresh itself!
                with st.spinner("Worker is extracting scores from WGI (Waiting for Salesforce)..."):
                    import time
                    time.sleep(2)
                    st.rerun() 
                    
            elif status == "complete":
                c1, c2 = st.columns([0.8, 0.2])
                with c1: st.success(f"✅ Displaying Leaderboard for: {selected_archive}")
                with c2: 
                    # Keep a manual refresh button just in case
                    if st.button("🔄 Refresh View"): st.rerun()
                
                df = pd.DataFrame(archive_doc.get("data", []))
                if not df.empty:
                    # 1. Get unique classes for the dropdown
                    classes_available = sorted(df['Class'].unique())
                    f_class = st.selectbox("Filter Class:", ["All"] + classes_available, key="archive_class_filter")
                    
                    display_df = df.copy()
                    
                    # 2. Filter the dataframe if a specific class is chosen
                    if f_class != "All":
                        display_df = display_df[display_df['Class'] == f_class]
                        
                        # Add a clean Rank column for the specific class
                        display_df['Rank'] = range(1, len(display_df) + 1)
                        
                        # Rearrange columns so Rank is first
                        cols = ['Rank', 'Guard', 'Class', 'Final Score']
                        st.dataframe(display_df[cols], width='stretch', hide_index=True)
                        
                    else:
                        # If "All" is selected, just show everything grouped by class
                        st.dataframe(display_df, width='stretch', hide_index=True)
                else:
                    st.warning("No scores found. (Are you sure this event has finished?)")
                
        elif selected_archive != "-- Choose an Event --":
            st.info("Click 'Request Scores' to command the background worker to fetch the data.")