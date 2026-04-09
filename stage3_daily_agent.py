#!/usr/bin/env python3
import os
import sys
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SEGMENTS = {
    "1_warm_hot": {"name": "Warm/Hot", "priority": 1},
    "2_sphere_repeat": {"name": "Past Clients / Sphere", "priority": 2},
    "3_recently_active": {"name": "Recently Active", "priority": 3},
    "4_untouched": {"name": "Untouched", "priority": 4},
    "5_milestone_8_11yr": {"name": "8-11 Year Owners", "priority": 5},
    "6_milestone_10plus": {"name": "10+ Year Owners", "priority": 6},
    "7_cold_6months": {"name": "Cold 6+ Months", "priority": 7},
    "8_data_quality_issue": {"name": "Data Quality", "priority": 8},
    "unassigned": {"name": "Unassigned", "priority": 9},
}

def load_client_config(client_id):
    res = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
    return res.data[0] if res.data else None

def get_segment_counts(client_id):
    res = supabase.table("leads").select("segment", count="exact").eq("client_id", str(client_id)).execute()
    counts = defaultdict(int)
    for row in res.data:
        counts[row.get("segment", "unassigned")] += 1
    return dict(counts)

def calculate_proportional_allocation(segment_counts, total_target=20):
    total_leads = sum(segment_counts.values())
    if total_leads == 0: return {}
    allocation = {s: round((c/total_leads) * total_target) for s, c in segment_counts.items()}
    diff = total_target - sum(allocation.values())
    if diff != 0 and segment_counts:
        biggest = max(segment_counts, key=segment_counts.get)
        allocation[biggest] += diff
    return allocation

def select_leads_with_cooldown(client_id, segment, limit, cooldown_days):
    if limit <= 0: return []
    cutoff = (datetime.now() - timedelta(days=cooldown_days)).strftime("%Y-%m-%d")
    recent = supabase.table("lead_presentations").select("lead_id").eq("client_id", str(client_id)).gte("presented_date", cutoff).execute()
    excluded = [row['lead_id'] for row in recent.data]
    query = supabase.table("leads").select("*").eq("client_id", str(client_id)).eq("segment", segment)
    if excluded: query = query.not_.in_("lead_id", excluded)
    res = query.order("name", desc=False).limit(limit).execute()
    return res.data or []

def generate_narrative(lead):
    """Direct REST call to Gemini - No SDK involved to avoid 404s."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    segment_name = SEGMENTS.get(lead.get("segment"), {}).get("name", "Unknown")
    
    payload = {
        "contents": [{
            "parts": [{
                "text": f"Write a warm 2-sentence outreach for {lead.get('name')} at {lead.get('address')}. Segment: {segment_name}. No jargon."
            }]
        }]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        # Handle potential API errors directly
        if response.status_code == 200:
            result = response.json()
            return result['candidates'][0]['content']['parts'][0]['text'].strip()
        else:
            print(f"⚠️ API Error: {response.status_code} - {response.text}")
            return f"Hi {lead.get('name')}, thinking of your property at {lead.get('address')}."
    except Exception as e:
        print(f"⚠️ Request Error: {e}")
        return "Checking in on your property value."

def store_results(client_id, leads, client_name):
    today = datetime.now().strftime("%Y-%m-%d")
    briefing = {
        "client_id": str(client_id), "client_name": client_name,
        "briefing_date": today, "lead_count": len(leads),
        "leads_json": json.dumps(leads), "created_at": datetime.now().isoformat()
    }
    supabase.table("daily_briefings").upsert(briefing, on_conflict="client_id,briefing_date").execute()
    
    logs = [{
        "client_id": str(client_id), "lead_id": l.get("lead_id"),
        "presented_date": today, "segment": l.get("segment"),
        "narrative": l.get("narrative"), "status": "sent"
    } for l in leads]
    supabase.table("lead_presentations").upsert(logs, on_conflict="client_id,lead_id,presented_date").execute()

def main(client_id):
    print(f"\n{'='*25} STAGE 3: DAILY AGENT {'='*25}")
    config = load_client_config(client_id)
    if not config: return
    
    cooldown = config.get("cooldown_days", 90)
    print(f"✓ Config Loaded: {config['client_name']} (Cooldown: {cooldown} days)")
    
    counts = get_segment_counts(client_id)
    alloc = calculate_proportional_allocation(counts)
    
    top_leads = []
    print("\n✓ Selecting Leads...")
    for seg, lim in alloc.items():
        batch = select_leads_with_cooldown(client_id, seg, lim, cooldown)
        top_leads.extend(batch)

    print(f"\n✓ Generating Narratives (Direct REST)...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')}")

    store_results(client_id, top_leads, config.get("client_name"))
    print(f"\n{'='*22} STAGE 3 COMPLETE {'='*22}\n")

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    main(cid)
