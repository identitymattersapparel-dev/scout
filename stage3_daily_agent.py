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

# ============================================================================
# STRATEGIC CONFIGURATION
# ============================================================================

# Percentage targets (must sum to 1.0)
STRATEGIC_WEIGHTS = {
    "1_warm_hot": 0.30,
    "2_sphere_repeat": 0.20,
    "3_recently_active": 0.10,
    "5_milestone_8_11yr": 0.15,
    "6_milestone_10plus": 0.10,
    "4_untouched": 0.05,
    "7_cold_6months": 0.05,
    "8_data_quality_issue": 0.05
}

# The "Trusted Advisor" Voice matrix
SEGMENT_STRATEGIES = {
    "1_warm_hot": "Professional and observant. Acknowledge their recent interest and offer a specific data point.",
    "2_sphere_repeat": "Relational and low-pressure. Focus on being a long-term resource for their real estate wealth.",
    "3_recently_active": "Analytical tone. Mention you noticed their activity on the site and offer to provide deeper context on those specific listings.",
    "5_milestone_8_11yr": "Equity focused. Explain that owners in their specific purchase window are seeing unique equity positions right now.",
    "6_milestone_10plus": "Historical perspective. Contrast the market shift since their purchase date with current neighborhood trends.",
    "4_untouched": "Introduction of expertise. Establish yourself as a local specialist who monitors property values in their specific area.",
    "7_cold_6months": "Market insight update. Provide a high-level, zero-pressure observation of recent activity near their home.",
    "8_data_quality_issue": "Accuracy focus. Explain you are updating your neighborhood reports and want to ensure their specific property data is correct."
}

def generate_narrative(lead):
    """Uses Gemini 1.5 Flash to write a contextual 2-sentence update."""
    # Production Endpoint
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    segment_key = lead.get('segment', 'General')
    strategy = SEGMENT_STRATEGIES.get(segment_key, "Professional and concise.")
    
    prompt = (
        f"You are Brian White, a highly professional real estate advisor. Write a 2-sentence outreach to {lead.get('name')} "
        f"regarding their property at {lead.get('address')}. "
        f"Context: Their lead segment is '{segment_key}'. "
        f"Strategy: {strategy} "
        f"Constraint: Act as a 'Trusted Advisor'. DO NOT use sales cliches, urgency tactics, or words like 'hot', 'fire', or 'exploding'. "
        f"Focus on explaining the specific reason for this touchpoint. Keep it under 40 words."
    )

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            return r.json()['candidates'][0]['content']['parts'][0]['text'].strip().replace('"', '')
        print(f"⚠️ API Error {r.status_code} for {lead.get('name')}")
        return f"Checking in regarding the property at {lead.get('address')}."
    except Exception as e:
        return "Market update for your property."

def get_weighted_leads(client_id, cooldown_days, total_target=20):
    """Pulls a balanced sample across segments, respecting the cooldown."""
    # Logic Fix: Using GTE ensures anyone added 'today' is caught in the exclusion for second runs.
    cutoff = (datetime.now() - timedelta(days=cooldown_days)).strftime("%Y-%m-%d")
    
    print(f"--- Strategic Allocation ({total_target} leads) ---")
    print(f"[*] Filtering out leads contacted since {cutoff}...")

    # Get IDs of leads presented within the cooldown period (including today)
    recent = supabase.table("lead_presentations").select("lead_id").eq("client_id", str(client_id)).gte("presented_date", cutoff).execute()
    excluded = [row['lead_id'] for row in recent.data]

    final_selection = []
    seen_ids = set()

    for segment, weight in STRATEGIC_WEIGHTS.items():
        segment_limit = math.ceil(total_target * weight)
        
        query = supabase.table("leads").select("*").eq("client_id", str(client_id)).eq("segment", segment)
        if excluded:
            query = query.not_.in_("lead_id", excluded)
        
        res = query.limit(segment_limit + 10).execute()
        
        added = 0
        for l in res.data:
            if len(final_selection) < total_target and added < segment_limit and l['lead_id'] not in seen_ids:
                final_selection.append(l)
                seen_ids.add(l['lead_id'])
                added += 1
        
        if added > 0:
            print(f"  ✓ {segment.ljust(20)}: {added} leads")

    return final_selection

def main(client_id, total_leads=20):
    print(f"\n{'='*25} STAGE 3: SCALABLE AGENT {'='*25}")
    
    # Load Config
    res = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
    if not res.data:
        print("❌ Error: Client config not found.")
        return
    config = res.data[0]
    
    print(f"✓ Config: {config['client_name']} (Target: {total_leads} leads)")

    # 1. Selection
    top_leads = get_weighted_leads(client_id, config.get("cooldown_days", 30), total_leads)
    
    if not top_leads:
        print("⚠️ No leads found matching criteria (check cooldown or segment names).")
        return

    # 2. Narrative Generation
    print(f"✓ Generating {len(top_leads)} Narratives via Gemini 1.5 Flash...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')} ({lead.get('segment')})")

    # 3. Final Save
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Save the briefing summary
    briefing = {
        "client_id": str(client_id), 
        "client_name": config['client_name'], 
        "briefing_date": today, 
        "lead_count": len(top_leads), 
        "leads_json": json.dumps(top_leads)
    }
    supabase.table("daily_briefings").upsert(briefing, on_conflict="client_id,briefing_date").execute()
    
    # Save individual logs for cooldown tracking
    logs = [{
        "client_id": str(client_id), 
        "lead_id": l['lead_id'], 
        "presented_date": today, 
        "segment": l.get('segment'), 
        "narrative": l['narrative'], 
        "status": "sent"
    } for l in top_leads]
    
    supabase.table("lead_presentations").upsert(logs, on_conflict="client_id,lead_id,presented_date").execute()
    
    print(f"\n✓ Stage 3 Complete. Briefing generated for {today}.")

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    main(cid, count)
