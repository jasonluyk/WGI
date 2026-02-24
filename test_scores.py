import pymongo
import random
import streamlit as st

# Connect to your local database (Update this if your connection string is different!)
client = pymongo.MongoClient(st.secrets["MONGO_URI"])
db = client["rankings_2026"] # Make sure this matches your DB name!

def inject_round_scores(target_round):
    # Grab the active show dynamically (no hardcoded IDs!)
    live_state_doc = db["live_state"].find_one()
    
    if not live_state_doc or "data" not in live_state_doc:
        print("❌ No active show data found! Make sure you latched an event in Admin.")
        return
        
    combined_data = live_state_doc["data"]
    updated_count = 0
    
    # Loop through the guards and score the specific round
    for guard_info in combined_data:
        guard_name = guard_info.get("Guard", "Unknown")
        
        if target_round.lower() in guard_info.get("Class", "").lower():
            # Generate a random plausible color guard score
            fake_score = round(random.uniform(65.0, 85.0), 2)
            guard_info["Prelims Score"] = fake_score
            guard_info["Status"] = "✅ Scored" 
            print(f"   🎺 {guard_name}: {fake_score}")
            updated_count += 1
            
    # Save the batch back to the exact same document
    if updated_count > 0:
        db["live_state"].update_one(
            {"_id": live_state_doc["_id"]},
            {"$set": {"data": combined_data}}
        )
        print(f"\n✅ Successfully published {updated_count} scores to CompetitionSuite (Simulation)!")
    else:
        print(f"\n⚠️ Could not find any guards in {target_round}. Check your spelling!")

if __name__ == "__main__":
    print("🎓 WGI TABULATOR SIMULATOR")
    print("-" * 30)
    
    while True:
        print("\nAvailable Rounds: 'Round 1', 'Round 2', 'Round 3', 'Round 4'")
        target = input("Which round just finished? (or type 'q' to quit): ").strip()
        
        if target.lower() == 'q':
            break
        else:
            inject_round_scores(target)