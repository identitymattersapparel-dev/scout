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

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    # FORCING V1 STABLE
    url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": f"Write a 2-sentence property update for {lead.get('name')} at {lead.get('address')}."}]}]}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            return r.json()['candidates'][0]['content']['parts'][0]['text'].strip()
        print(f"⚠️ Error: {r.status_code} - {r.text}")
        return "Quick update on your home value."
    except:
        return "Checking in."

def store_results(client_id, leads, client_name):
    today = datetime.now().strftime("%Y-%m-%d")
    briefing = {"client_id": str(client_id), "client_name": client_name, "briefing_date": today, "lead_count": len(leads), "leads_json": json.dumps(leads), "created_at": datetime.now().isoformat()}
    supabase.table("daily_briefings").upsert(briefing, on_conflict="client_id,briefing_date").execute()
    logs = [{"client_id": str(client_id), "lead_id": l.get("lead_id"), "presented_date": today, "segment": l.get("segment"), "narrative": l.get("narrative"), "status": "sent"} for l in leads]
    supabase.table("lead_presentations").upsert(logs, on_conflict="client_id,lead_id,presented_date").execute()

def main(client_id):
    print(f"\n{'='*25} STAGE 3: DAILY AGENT {'='*25}")
    config = load_client_config(client_id)
    if not config: return
    cooldown = config.get("cooldown_days", 90)
    print(f"✓ Config Loaded: {config['client_name']} (Cooldown: {cooldown} days)")
    alloc = calculate_proportional_allocation(get_segment_counts(client_id))
    top_leads = []
    for seg, lim in alloc.items():
        top_leads.extend(select_leads_with_cooldown(client_id, seg, lim, cooldown))
    print(f"\n✓ Generating Narratives...")
    for i, lead in enumerate(top_leads, 1):
        lead['narrative'] = generate_narrative(lead)
        print(f"  [{i}/{len(top_leads)}] {lead.get('name')}")
    store_results(client_id, top_leads, config.get("client_name"))
    print(f"\n{'='*22} STAGE 3 COMPLETE {'='*22}\n")

if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "62960ae5-4e6f-4b03-82b0-1c3396271268"
    main(cid)
