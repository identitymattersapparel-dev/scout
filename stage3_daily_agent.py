#!/usr/bin/env python3
"""
Scout Engine - Stage 3: Daily Agent
Generates top 20 leads daily with AI-generated outreach narratives and stores in database.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict

from supabase import create_client
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

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
    """Load client config from Supabase."""
    try:
        response = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
        if not response.data or len(response.data) == 0:
            print(f"❌ No config found for client_id: {client_id}")
            return None
        config = response.data[0]
        print(f"✓ Loaded config: {config['client_name']}")
        return config
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def get_segment_counts(client_id):
    """Query database to get count of leads per segment."""
    try:
        response = supabase.table("leads").select("segment", count="exact").eq("client_id", str(client_id)).execute()
        counts = defaultdict(int)
        for row in response.data:
            segment = row.get("segment", "unassigned")
            counts[segment] += 1
        
        print(f"✓ Segment counts loaded:")
        for segment, count in sorted(counts.items()):
            segment_name = SEGMENTS.get(segment, {}).get("name", segment)
            print(f"  {segment_name:25s} {count:4d} leads")
        return dict(counts)
    except Exception as e:
        print(f"❌ Error loading segment counts: {e}")
        return {}

def calculate_proportional_allocation(segment_counts, total_target=20):
    """Calculate how many leads to pick from each segment (proportional)."""
    total_leads = sum(segment_counts.values())
    if total_leads == 0:
        return {}
    
    allocation = {}
    allocated_total = 0
    for segment, count in segment_counts.items():
        proportion = count / total_leads
        allocated = round(proportion * total_target)
        allocation[segment] = allocated
        allocated_total += allocated
    
    diff = total_target - allocated_total
    if diff != 0 and segment_counts:
        biggest_segment = max(segment_counts.keys(), key=lambda x: segment_counts[x])
        allocation[biggest_segment] += diff
    
    return allocation

def select_leads_from_segment(client_id, segment, limit):
    """Query top leads from a segment."""
    if limit <= 0: return []
    try:
        response = (
            supabase.table("leads")
            .select("*")
            .eq("client_id", str(client_id))
            .eq("segment", segment)
            .order("name", desc=False)
            .limit(limit)
            .execute()
        )
        return response.data if response.data else []
    except Exception as e:
        print(f"⚠️ Error querying segment {segment}: {e}")
        return []

def generate_narrative(lead):
    """Generate AI narrative using Gemini 3 Flash."""
    try:
        segment_info = SEGMENTS.get(lead.get("segment"), {})
        segment_name = segment_info.get("name", "Unknown")
        
        prompt = f"""Generate a brief (2-3 sentence) personalized outreach narrative for a real estate lead.

Lead Information:
- Name: {lead.get('name', 'Unknown')}
- Address: {lead.get('address', 'Unknown')}
- Segment: {segment_name}

Create a warm, conversational opening. Address them by name. 
Reference their property or segment naturally. 
Offer a clear value like a market insight or property update. 
Keep it professional, no jargon."""

        # UPDATED TO GEMINI 3 FLASH
        model = genai.GenerativeModel("gemini-3-flash")
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.7,
                max_output_tokens=150
            )
        )
        
        return response.text.strip() if response.text else "Following up on your property interest."
    except Exception as e:
        print(f"⚠️ Gemini Error for {lead.get('name')}: {e}")
        return f"Hi {lead.get('name')}, I'd like to provide an update on the real estate market in your area."

def store_daily_briefing(client_id, leads, client_name):
    """Store daily briefing in database."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        briefing_data = {
            "client_id": str(client_id),
            "client_name": client_name,
            "briefing_date": today,
            "lead_count": len(leads),
            "leads_json": json.dumps([{
                "name": lead.get("name"),
                "email": lead.get("email"),
                "phone": lead.get("phone"),
                "address": lead.get("address"),
                "segment": lead.get("segment"),
                "narrative": lead.get("narrative"),
            } for lead in leads]),
            "created_at": datetime.now().isoformat(),
        }
        supabase.table("daily_briefings").insert([briefing_data]).execute()
        return True
    except Exception as e:
        print(f"⚠️ Error storing briefing: {e}")
        return False

def log_presentations(client_id, leads):
    """Log lead presentations."""
    try:
        presented_date = datetime.now().strftime("%Y-%m-%d")
        presentations = [{
            "client_id": str(client_id),
            "lead_id": lead.get("lead_id", ""),
            "presented_date": presented_date,
            "segment": lead.get("segment", "unassigned"),
            "narrative": lead.get("narrative", ""),
            "status": "sent",
        } for lead in leads]
        supabase.table("lead_presentations").insert(presentations).execute()
        return True
    except Exception as e:
        print(f"⚠️ Error logging presentations: {e}")
        return False

def stage3_daily_agent(client_id):
    """Main Stage 3 orchestration."""
    print("\n" + "=" * 30 + " STAGE 3: DAILY AGENT " + "=" * 30)
    
    config = load_client_config(client_id)
    if not config: return False
    
    segment_counts = get_segment_counts(client_id)
    if not segment_counts: return False
    
    allocation = calculate_proportional_allocation(segment_counts, total_target=20)
    
    top_leads = []
    for segment, limit in allocation.items():
        leads = select_leads_from_segment(client_id, segment, limit)
        top_leads.extend(leads)
    
    print(f"\n✓ Generating narratives using Gemini 3 Flash...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name', 'Unknown')}")
    
    store_daily_briefing(client_id, top_leads, config.get("client_name"))
    log_presentations(client_id, top_leads)
    
    print("=" * 30 + " STAGE 3 COMPLETE " + "=" * 30)
    return True

if __name__ == "__main__":
    client_id = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    success = stage3_daily_agent(client_id)
    sys.exit(0 if success else 1)
