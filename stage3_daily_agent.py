#!/usr/bin/env python3
"""
Scout Engine - Stage 3: Daily Agent
Generates top 20 leads daily with AI-generated outreach narratives.
Includes dynamic cooldown logic and Supabase upsert handling.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict

from supabase import create_client
from dotenv import load_dotenv
from google import genai

load_dotenv()

# Environment Setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_API_KEY)

# ============================================================================
# SEGMENT METADATA
# ============================================================================

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
        print(f"❌ Error loading config: {e}")
        return None

def get_segment_counts(client_id):
    try:
        response = supabase.table("leads").select("segment", count="exact").eq("client_id", str(client_id)).execute()
        counts = defaultdict(int)
        for row in response.data:
            segment = row.get("segment", "unassigned")
            counts[segment] += 1
        return dict(counts)
    except Exception as e:
        print(f"❌ Error loading segment counts: {e}")
        return {}

def calculate_proportional_allocation(segment_counts, total_target=20):
    total_leads = sum(segment_counts.values())
    if total_leads == 0: return {}
    allocation = {s: round((c/total_leads) * total_target) for s, c in segment_counts.items()}
    diff = total_target - sum(allocation.values())
    if diff != 0 and segment_counts:
        biggest_segment = max(segment_counts, key=segment_counts.get)
        allocation[biggest_segment] += diff
    return allocation

def select_leads_with_cooldown(client_id, segment, limit, cooldown_days):
    if limit <= 0: return []
    cutoff_date = (datetime.now() - timedelta(days=cooldown_days)).strftime("%Y-%m-%d")
    try:
        recent_res = supabase.table("lead_presentations") \
            .select("lead_id") \
            .eq("client_id", str(client_id)) \
            .gte("presented_date", cutoff_date) \
            .execute()
        excluded_ids = [row['lead_id'] for row in recent_res.data]
        query = supabase.table("leads").select("*").eq("client_id", str(client_id)).eq("segment", segment)
        if excluded_ids:
            query = query.not_.in_("lead_id", excluded_ids)
        response = query.order("name", desc=False).limit(limit).execute()
        return response.data if response.data else []
    except Exception as e:
        print(f"⚠️ Cooldown Filter Error for {segment}: {e}")
        return []

def generate_narrative(lead):
    """Generate AI narrative using the explicit production model ID."""
    try:
        segment_name = SEGMENTS.get(lead.get("segment"), {}).get("name", "Unknown")
        prompt = f"""Write a warm 2-sentence outreach for {lead.get('name')} regarding their property at {lead.get('address')}. 
        Segment: {segment_name}. No sales jargon. Offer value or market insight."""

        # Using 'gemini-1.5-flash-latest' to ensure we hit the production endpoint
        response = ai_client.models.generate_content(
            model="gemini-1.5-flash-latest", 
            contents=prompt
        )
        return response.text.strip() if response.text else "Checking in on your property value."
    except Exception as e:
        print(f"⚠️ Gemini Error for {lead.get('name')}: {e}")
        return f"Hi {lead.get('name')}, I have a quick update on the local market for you."

def store_daily_briefing(client_id, leads, client_name):
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
        supabase.table("daily_briefings").upsert(data, on_conflict="client_id,briefing_date").execute()
        print(f"✓ Daily briefing saved (Upserted for {today})")
        return True
    except Exception as e:
        print(f"⚠️ Briefing Storage Error: {e}")
        return False

def log_presentations(client_id, leads):
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
        print(f"✓ Logged {len(leads)} presentations")
        return True
    except Exception as e:
        print(f"⚠️ Log Storage Error: {e}")
        return False

def stage3_daily_agent(client_id):
    print(f"\n{'='*25} STAGE 3: DAILY AGENT {'='*25}")
    config = load_client_config(client_id)
    if not config: return False
    
    cooldown = config.get("cooldown_days", 90)
    print(f"✓ Config Loaded: {config['client_name']} (Cooldown: {cooldown} days)")
    
    segment_counts = get_segment_counts(client_id)
    allocation = calculate_proportional_allocation(segment_counts)
    
    top_leads = []
    print("\n✓ Selecting Leads...")
    for segment, limit in allocation.items():
        leads = select_leads_with_cooldown(client_id, segment, limit, cooldown)
        top_leads.extend(leads)
        if leads:
            print(f"  - {segment:25s}: {len(leads)} leads selected")

    print(f"\n✓ Generating Narratives...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')}")

    store_daily_briefing(client_id, top_leads, config.get("client_name"))
    log_presentations(client_id, top_leads)
    
    print(f"\n{'='*22} STAGE 3 COMPLETE {'='*22}\n")
    return True

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    stage3_daily_agent(cid)
