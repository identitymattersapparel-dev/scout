#!/usr/bin/env python3
"""
Scout Engine - Stage 3: Daily Agent
Delivers top 20 leads daily via Telegram with AI-generated outreach narratives.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict
import requests

from supabase import create_client
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

# ============================================================================
# SEGMENT METADATA
# ============================================================================

SEGMENTS = {
    "1_warm_hot": {
        "name": "Warm/Hot",
        "emoji": "🔥",
        "priority": 1,
    },
    "2_sphere_repeat": {
        "name": "Past Clients / Sphere",
        "emoji": "⭐",
        "priority": 2,
    },
    "3_recently_active": {
        "name": "Recently Active",
        "emoji": "📞",
        "priority": 3,
    },
    "4_untouched": {
        "name": "Untouched",
        "emoji": "🎯",
        "priority": 4,
    },
    "5_milestone_8_11yr": {
        "name": "8-11 Year Owners",
        "emoji": "🏠",
        "priority": 5,
    },
    "6_milestone_10plus": {
        "name": "10+ Year Owners",
        "emoji": "👑",
        "priority": 6,
    },
    "7_cold_6months": {
        "name": "Cold 6+ Months",
        "emoji": "❄️",
        "priority": 7,
    },
    "8_data_quality_issue": {
        "name": "Data Quality",
        "emoji": "⚠️",
        "priority": 8,
    },
    "unassigned": {
        "name": "Unassigned",
        "emoji": "❓",
        "priority": 9,
    },
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
        print("❌ No leads available")
        return {}
    
    allocation = {}
    allocated_total = 0
    
    # First pass: calculate proportional allocation
    for segment, count in segment_counts.items():
        proportion = count / total_leads
        allocated = round(proportion * total_target)
        allocation[segment] = allocated
        allocated_total += allocated
    
    # Second pass: adjust for rounding errors (ensure sum = total_target)
    diff = total_target - allocated_total
    if diff != 0:
        # Adjust the highest-count segment
        biggest_segment = max(segment_counts.keys(), key=lambda x: segment_counts[x])
        allocation[biggest_segment] += diff
    
    print(f"\n✓ Proportional allocation (target: {total_target} leads):")
    for segment in sorted(allocation.keys(), key=lambda x: SEGMENTS.get(x, {}).get("priority", 99)):
        segment_name = SEGMENTS.get(segment, {}).get("name", segment)
        allocated = allocation[segment]
        if allocated > 0:
            print(f"  {segment_name:25s} {allocated:2d} leads")
    
    return allocation


def select_leads_from_segment(client_id, segment, limit):
    """Query top leads from a segment, ranked by priority."""
    if limit <= 0:
        return []
    
    try:
        # Query: order by days_since_contact DESC (older first), then years_owned DESC, then name ASC
        response = (
            supabase.table("leads")
            .select("*")
            .eq("client_id", str(client_id))
            .eq("segment", segment)
            .order("name", desc=False)  # Alphabetical
            .limit(limit)
            .execute()
        )
        
        leads = response.data if response.data else []
        return leads
    except Exception as e:
        print(f"⚠️  Error querying segment {segment}: {e}")
        return []


def get_top_20_leads(client_id, allocation):
    """Select top 20 leads across all segments (proportionally)."""
    top_leads = []
    
    print(f"\n✓ Selecting leads from each segment...")
    
    for segment in sorted(allocation.keys(), key=lambda x: SEGMENTS.get(x, {}).get("priority", 99)):
        limit = allocation[segment]
        if limit <= 0:
            continue
        
        leads = select_leads_from_segment(client_id, segment, limit)
        top_leads.extend(leads)
        
        segment_name = SEGMENTS.get(segment, {}).get("name", segment)
        print(f"  {segment_name:25s} selected {len(leads):2d}/{limit} leads")
    
    print(f"\n✓ Total leads selected: {len(top_leads)}")
    return top_leads


def generate_narrative(lead):
    """Generate AI narrative using Gemini."""
    try:
        segment_info = SEGMENTS.get(lead.get("segment"), {})
        segment_name = segment_info.get("name", "Unknown")
        
        prompt = f"""Generate a brief (2-3 sentence) personalized outreach narrative for a real estate lead.

Lead Information:
- Name: {lead.get('name', 'Unknown')}
- Address: {lead.get('address', 'Unknown')}
- Email: {lead.get('email', 'N/A')}
- Phone: {lead.get('phone', 'N/A')}
- Segment: {segment_name}
- Last Contact: Unknown days ago
- Property Type: Residential

Create a warm, conversational opening that:
1. Addresses them by name
2. References their property or segment (e.g., "I noticed your beautiful home..." or "As a valued past client...")
3. Offers clear value (market insight, refinance opportunity, property update)
4. Includes a natural call-to-action

Keep it warm, professional, and 2-3 sentences max. No jargon, no sales speak."""

        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt, stream=False)
        
        narrative = response.text.strip() if response.text else "Reach out about your property."
        return narrative
    except Exception as e:
        print(f"⚠️  Error generating narrative for {lead.get('name')}: {e}")
        # Fallback narrative
        segment_name = SEGMENTS.get(lead.get("segment"), {}).get("name", "opportunity")
        return f"I'd like to discuss {segment_name.lower()} with you about your property."


def format_telegram_message(leads, client_name):
    """Format leads as a Telegram message."""
    today = datetime.now().strftime("%B %d, %Y")
    
    message = f"📍 SCOUT DAILY TOP {len(leads)}\n"
    message += f"{client_name}\n"
    message += f"{today}\n"
    message += "=" * 50 + "\n\n"
    
    for i, lead in enumerate(leads, 1):
        segment_info = SEGMENTS.get(lead.get("segment"), {})
        emoji = segment_info.get("emoji", "•")
        segment_name = segment_info.get("name", "Unknown")
        
        message += f"Lead {i}/{len(leads)}\n"
        message += f"{emoji} {segment_name}\n"
        message += "─" * 40 + "\n"
        message += f"📝 {lead.get('name', 'Unknown')}\n"
        
        if lead.get('address'):
            message += f"📍 {lead['address']}\n"
        
        if lead.get('email'):
            message += f"✉️  {lead['email']}\n"
        if lead.get('phone'):
            message += f"☎️  {lead['phone']}\n"
        
        message += f"\n💬 {lead.get('narrative', 'Follow up on property opportunity.')}\n"
        message += "\n" + "=" * 50 + "\n\n"
    
    return message


def send_telegram_message(message):
    """Send message via Telegram Bot API."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("ok"):
                print(f"✓ Telegram message sent (message_id: {result['result']['message_id']})")
                return result['result']['message_id']
            else:
                print(f"❌ Telegram API error: {result.get('description')}")
                return None
        else:
            print(f"❌ Telegram send failed (status {response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error sending Telegram message: {e}")
        return None


def log_presentations(client_id, leads, telegram_message_id=None):
    """Log lead presentations to avoid re-presenting within cooldown period."""
    try:
        presented_date = datetime.now().strftime("%Y-%m-%d")
        
        presentations = []
        for lead in leads:
            presentations.append({
                "client_id": str(client_id),
                "lead_id": lead.get("lead_id", ""),
                "presented_date": presented_date,
                "segment": lead.get("segment", "unassigned"),
                "narrative": lead.get("narrative", ""),
                "status": "sent",
                "telegram_message_id": telegram_message_id,
            })
        
        # Batch insert
        response = supabase.table("lead_presentations").insert(presentations).execute()
        print(f"✓ Logged {len(presentations)} lead presentations")
        return True
    except Exception as e:
        print(f"⚠️  Error logging presentations: {e}")
        # Don't fail the entire job if logging fails
        return False


def stage3_daily_agent(client_id):
    """Main Stage 3 orchestration."""
    print("\n" + "=" * 80)
    print("STAGE 3: DAILY AGENT")
    print("=" * 80)
    
    # Load config
    config = load_client_config(client_id)
    if not config:
        return False
    
    client_name = config.get("client_name", "Unknown")
    
    # Get segment counts
    print("\n✓ Loading segment counts...")
    segment_counts = get_segment_counts(client_id)
    if not segment_counts:
        print("❌ No leads available")
        return False
    
    # Calculate proportional allocation
    allocation = calculate_proportional_allocation(segment_counts, total_target=20)
    if not allocation:
        return False
    
    # Select top 20 leads
    top_leads = get_top_20_leads(client_id, allocation)
    if not top_leads:
        print("❌ No leads selected")
        return False
    
    # Generate narratives for each lead
    print(f"\n✓ Generating narratives (using Gemini 1.5 Flash)...")
    for i, lead in enumerate(top_leads, 1):
        narrative = generate_narrative(lead)
        lead['narrative'] = narrative
        print(f"  [{i}/{len(top_leads)}] {lead.get('name', 'Unknown')}")
    
    # Format Telegram message
    print(f"\n✓ Formatting Telegram message...")
    telegram_message = format_telegram_message(top_leads, client_name)
    
    # Send Telegram
    print(f"\n✓ Sending via Telegram...")
    telegram_message_id = send_telegram_message(telegram_message)
    
    # Log presentations
    print(f"\n✓ Logging presentations...")
    log_presentations(client_id, top_leads, telegram_message_id)
    
    print("\n" + "=" * 80)
    print(f"STAGE 3 COMPLETE: {len(top_leads)} leads sent via Telegram")
    print("=" * 80)
    
    return True


if __name__ == "__main__":
    # Default to Brian's client_id if no argument provided
    client_id = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    success = stage3_daily_agent(client_id)
    sys.exit(0 if success else 1)

