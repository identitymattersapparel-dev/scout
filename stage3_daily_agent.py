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
import requests

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

