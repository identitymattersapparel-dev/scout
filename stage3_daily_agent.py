#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime
from collections import defaultdict

from supabase import create_client
from dotenv import load_dotenv
from google import genai # Switched to the new recommended package

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = genai.Client(api_key=GEMINI_API_KEY) # New client initialization

SEGMENTS = {
    "1_warm_hot": {"name": "Warm/Hot", "emoji": "🔥", "priority": 1},
    "2_sphere_repeat": {"name": "Past Clients / Sphere", "emoji": "⭐", "priority": 2},
    "3_recently_active": {"name": "Recently Active", "emoji": "📞", "priority": 3},
    "4_untouched": {"name": "Untouched", "emoji": "🎯", "priority": 4},
    "5_milestone_8_11yr": {"name": "8-11 Year Owners", "emoji": "🏠", "priority": 5},
    "6_milestone_10plus": {"name": "10+ Year Owners", "emoji": "👑", "priority": 6},
    "7_cold_6months": {"name": "Cold 6+ Months", "emoji": "❄️", "priority": 7},
    "8_data_quality_issue": {"name": "Data Quality", "emoji": "⚠️", "priority": 8},
    "unassigned": {"name": "Unassigned", "emoji": "❓", "priority": 9},
}

def load_client_config(client_id):
    try:
        response = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
        if not response.data: return None
        return response.data[0]
    except Exception as e:
        print(f"❌ Config Error: {e}")
        return None

def get_segment_counts(client_id):
    try:
        response = supabase.table("leads").select("segment", count="exact").eq("client_id", str(client_id)).execute()
        counts = defaultdict(int)
        for row in response.data:
            counts[row.get("segment", "unassigned")] += 1
        return dict(counts)
    except Exception as e:
        print(f"❌ Segment Error: {e}")
        return {}

def calculate_proportional_allocation(segment_counts, total_target=20):
    total_leads = sum(segment_counts.values())
    if total_leads == 0: return {}
    allocation = {s: round((c/total_leads) * total_target) for s, c in segment_counts.items()}
    diff = total_target - sum(allocation.values())
    if diff != 0 and segment_counts:
        allocation[max(segment_counts, key=segment_counts.get)] += diff
    return allocation

def select_leads_from_segment(client_id, segment, limit):
    if limit <= 0: return []
    try:
        res = supabase.table("leads").select("*").eq("client_id", str(client_id)).eq("segment", segment).limit(limit).execute()
        return res.data or []
    except Exception as e:
        print(f"⚠️ Query Error: {e}")
        return []

def generate_narrative(lead):
    """Uses Gemini 3 Flash via the new google-genai package."""
    try:
        prompt = f"Write a 2-sentence warm outreach to {lead.get('name')} regarding their property at {lead.get('address')}. Segment: {lead.get('segment')}. No sales jargon."
        
        # Using the standard model string with the new SDK
        response = client.models.generate_content(
            model="gemini-3-flash", 
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Gemini Error: {e}")
        return f"Hi {lead.get('name')}, checking in on your property interest."

def store_daily_briefing(client_id, leads, client_name):
    """Uses upsert to avoid duplicate key errors on the same date."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        data = {
            "client_id": str(client_id),
            "client_name": client_name,
            "briefing_date": today,
            "lead_count": len(leads),
            "leads_json": json.dumps(leads),
            "created_at": datetime.now().isoformat(),
        }
        # .upsert() replaces the record if (client_id, briefing_date) already exists
        supabase.table("daily_briefings").upsert(data, on_conflict="client_id,briefing_date").execute()
        return True
    except Exception as e:
        print(f"⚠️ Briefing Error: {e}")
        return False

def log_presentations(client_id, leads):
    """Uses upsert to prevent unique constraint violations on leads."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        logs = [{
            "client_id": str(client_id),
            "lead_id": l.get("lead_id"),
            "presented_date": today,
            "segment": l.get("segment"),
            "narrative": l.get("narrative"),
            "status": "sent"
        } for l in leads]
        supabase.table("lead_presentations").upsert(logs, on_conflict="client_id,lead_id,presented_date").execute()
        return True
    except Exception as e:
        print(f"⚠️ Log Error: {e}")
        return False

def stage3_daily_agent(client_id):
    print(f"\n{'='*20} DAILY AGENT RUN {'='*20}")
    config = load_client_config(client_id)
    if not config: return False
    
    counts = get_segment_counts(client_id)
    alloc = calculate_proportional_allocation(counts)
    
    top_leads = []
    for seg, lim in alloc.items():
        top_leads.extend(select_leads_from_segment(client_id, seg, lim))
    
    for lead in top_leads:
        lead['narrative'] = generate_narrative(lead)
        print(f"✓ Narrated: {lead.get('name')}")

    store_daily_briefing(client_id, top_leads, config.get("client_name"))
    log_presentations(client_id, top_leads)
    print(f"{'='*20} RUN COMPLETE {'='*20}")
    return True

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    stage3_daily_agent(cid)
