#!/usr/bin/env python3
import os, sys, json, requests
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def generate_narrative(lead):
    # UPDATED: Using your specific available model and the v1beta endpoint
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{
            "parts": [{"text": f"Write a 2-sentence property update for {lead.get('name')} at {lead.get('address')}. Professional and concise."}]
        }]
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            return r.json()['candidates'][0]['content']['parts'][0]['text'].strip()
        print(f"⚠️ API Error {r.status_code}: {r.text}")
        return f"Checking in regarding the property at {lead.get('address')}."
    except Exception as e:
        return "Market update for your property."

def main(client_id):
    print(f"\n{'='*25} STAGE 3: DAILY AGENT {'='*25}")
    res = supabase.table("client_configs").select("*").eq("client_id", str(client_id)).execute()
    if not res.data: return
    config = res.data[0]
    cooldown = config.get("cooldown_days", 30)
    print(f"✓ Config: {config['client_name']} (Model: Gemini 2.5 Flash)")

    # Deduplicated Selection
    cutoff = (datetime.now() - timedelta(days=cooldown)).strftime("%Y-%m-%d")
    recent = supabase.table("lead_presentations").select("lead_id").eq("client_id", str(client_id)).gte("presented_date", cutoff).execute()
    excluded = [row['lead_id'] for row in recent.data]
    
    query = supabase.table("leads").select("*").eq("client_id", str(client_id))
    if excluded: query = query.not_.in_("lead_id", excluded)
    leads_res = query.limit(40).execute() # Grab extra to account for duplicates
    
    seen_ids = set()
    top_leads = []
    for l in leads_res.data:
        if l['lead_id'] not in seen_ids and len(top_leads) < 20:
            top_leads.append(l)
            seen_ids.add(l['lead_id'])

    print(f"✓ Generating {len(top_leads)} Narratives...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')}")

    # Final Save
    today = datetime.now().strftime("%Y-%m-%d")
    briefing = {"client_id": str(client_id), "client_name": config['client_name'], "briefing_date": today, "lead_count": len(top_leads), "leads_json": json.dumps(top_leads)}
    supabase.table("daily_briefings").upsert(briefing, on_conflict="client_id,briefing_date").execute()
    
    logs = [{"client_id": str(client_id), "lead_id": l['lead_id'], "presented_date": today, "segment": l.get('segment'), "narrative": l['narrative'], "status": "sent"} for l in top_leads]
    supabase.table("lead_presentations").upsert(logs, on_conflict="client_id,lead_id,presented_date").execute()
    print(f"\n✓ Stage 3 Complete. Leads saved for {today}.")

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    main(cid)
