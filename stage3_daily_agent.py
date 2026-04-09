#!/usr/bin/env python3
import os, sys, json, requests, math
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Percentage targets (must sum to 1.0)
STRATEGIC_WEIGHTS = {
    "Warm / Hot": 0.30,
    "Sphere / Repeat": 0.20,
    "Recently Active": 0.10,
    "8-11 Yr Owners": 0.15,
    "10+ Yr Owners": 0.10,
    "Untouched": 0.05,
    "Cold 6+ Months": 0.05,
    "Data Quality": 0.05
}

def get_weighted_leads(client_id, cooldown_days, total_target=20):
    cutoff = (datetime.now() - timedelta(days=cooldown_days)).strftime("%Y-%m-%d")
    recent = supabase.table("lead_presentations").select("lead_id").eq("client_id", str(client_id)).gte("presented_date", cutoff).execute()
    excluded = [row['lead_id'] for row in recent.data]

    final_selection = []
    seen_ids = set()

    print(f"--- Allocation for {total_target} total leads ---")
    for segment, weight in STRATEGIC_WEIGHTS.items():
        # Calculate how many leads this segment should contribute
        segment_limit = math.ceil(total_target * weight)
        
        query = supabase.table("leads").select("*").eq("client_id", str(client_id)).eq("segment", segment)
        if excluded:
            query = query.not_.in_("lead_id", excluded)
        
        # Fetch a bit more than needed to account for duplicates/data issues
        res = query.limit(segment_limit + 5).execute()
        
        added = 0
        for l in res.data:
            if len(final_selection) < total_target and added < segment_limit and l['lead_id'] not in seen_ids:
                final_selection.append(l)
                seen_ids.add(l['lead_id'])
                added += 1
        
        if added > 0:
            print(f"  {segment}: {added} leads selected")

    return final_selection

def generate_narrative(lead):
    # (Same logic as before, using the Gemini 2.5 Flash Verified URL)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    # Custom persona logic
    segment = lead.get('segment', 'General')
    prompt = (
        f"You are Brian White, a real estate expert. Write a 2-sentence outreach to {lead.get('name')} "
        f"for their property at {lead.get('address')}. Segment: {segment}. "
        f"Tone: Professional but neighborly. Under 40 words."
    )

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.json()['candidates'][0]['content']['parts'][0]['text'].strip() if r.status_code == 200 else "Checking in on your property."
    except:
        return "Market update for your home."

def main(client_id, total_leads=20):
    print(f"\n{'='*25} STAGE 3: SCALABLE AGENT {'='*25}")
    res = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
    if not res.data: return
    config = res.data[0]
    
    # Step 1: Get balanced leads based on percentages
    top_leads = get_weighted_leads(client_id, config.get("cooldown_days", 30), total_leads)
    
    # Step 2: Generate narratives
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')} ({lead.get('segment')})")

    # Step 3: Save results (omitted for brevity, same as previous logic)
    # ... (Save to daily_briefings and lead_presentations) ...
    print(f"\n✓ Stage 3 Complete. Processed {len(top_leads)} leads.")

if __name__ == "__main__":
    # You can now pass the count as a second argument!
    # e.g., python stage3_daily_agent.py [client_id] 40
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    main(cid, count)
