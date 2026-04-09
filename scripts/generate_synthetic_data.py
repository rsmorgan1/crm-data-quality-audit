"""
Generate Synthetic CRM Dataset for Data Quality Audit Project
=============================================================
Creates a ~500-record PE deal pipeline dataset with intentional data quality
issues to demonstrate profiling, scoring, remediation, and monitoring skills.

Quality Issues Embedded:
- Missing values (~15-25% across critical fields)
- Inconsistent formatting (mixed case, abbreviations, variations)
- Duplicate records (~5% of dataset)
- Invalid/illogical dates (future dates, dates before company founding)
- Inconsistent industry/focus area naming
- Outlier financial values
- Malformed email addresses and phone numbers
- Whitespace issues (leading/trailing spaces)
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

N_RECORDS = 500

# ============================================================
# Reference Data (with intentional variations for quality issues)
# ============================================================

INDUSTRIES_CLEAN = ["Healthcare", "Education", "Technology-Enabled Services"]
INDUSTRIES_DIRTY = [
    "Healthcare", "healthcare", "HEALTHCARE", "Health Care", "HC",
    "Education", "education", "EDUCATION", "Ed", "Edu",
    "Technology-Enabled Services", "TES", "Tech-Enabled Services",
    "Technology Enabled Services", "tech enabled services"
]

FOCUS_AREAS = {
    "Healthcare": [
        "Provider Services", "Pharma Services", "Chronic Care Solutions",
        "Behavioral Health", "Healthy Living & Wellness", "Animal Health",
        "Home Healthcare", "Physician Practice Enablement"
    ],
    "Education": [
        "Higher Education Outsourcing", "Skills Gap Training",
        "Corporate Training", "Tutoring & Test Prep"
    ],
    "Technology-Enabled Services": [
        "Compliance Services", "Logistics Management",
        "Specialty Healthcare Logistics", "Residential Services",
        "Property Services"
    ]
}

# Dirty focus area variations
FOCUS_AREA_DIRTY_MAP = {
    "Provider Services": ["Provider Services", "provider services", "Provider Svcs", "Prov Services", "Provider Srvcs"],
    "Pharma Services": ["Pharma Services", "pharma services", "Pharmaceutical Services", "Pharma Svcs"],
    "Chronic Care Solutions": ["Chronic Care Solutions", "Chronic Care", "chronic care solutions", "CCS"],
    "Property Services": ["Property Services", "property services", "Prop Services", "Property Svcs"],
    "Behavioral Health": ["Behavioral Health", "behavioral health", "BH", "Behav Health"],
}

DEAL_TYPES = ["Platform", "Add-On"]
DEAL_TYPES_DIRTY = ["Platform", "platform", "PLATFORM", "Plat", "Add-On", "add-on", "ADD-ON", "Addon", "Add On"]

STAGES_ORDERED = ["Reviewed", "Indication", "Offer", "Closed - Won", "Closed - Lost", "Passed"]
STAGES_DIRTY = [
    "Reviewed", "reviewed", "REVIEWED", "Rev",
    "Indication", "indication", "IOI", "Indicated",
    "Offer", "offer", "LOI", "Letter of Intent",
    "Closed - Won", "Closed Won", "closed won", "Won", "Acquired",
    "Closed - Lost", "Closed Lost", "closed lost", "Lost",
    "Passed", "passed", "PASSED", "Pass", "Declined"
]

SOURCES = ["Intermediary", "Proprietary", "Conference", "Direct Outreach", "Referral"]
SOURCES_DIRTY = [
    "Intermediary", "intermediary", "Broker", "Investment Banker",
    "Proprietary", "proprietary", "Internal", "Prop",
    "Conference", "conference", "Event", "Trade Show",
    "Direct Outreach", "direct outreach", "Cold Outreach", "Direct",
    "Referral", "referral", "Referred", "Network"
]

DEAL_OWNERS = [
    "James Mitchell", "Sarah Chen", "Michael Torres",
    "Emily Richardson", "David Park", "Rachel Adams"
]

INTERMEDIARY_FIRMS = [
    "Meridian Capital Partners", "Lighthouse Advisory Group",
    "Pinnacle M&A Advisors", "Cascade Investment Banking",
    "Summit Strategic Partners", "Bridgepoint Capital",
    "Horizon Partners LLC", "Atlas Financial Advisory",
    "Northstar Securities", "Ironbridge Capital Group",
    "Keystone Advisors", "Sterling M&A Group",
    None  # Some deals have no intermediary
]


def generate_company_name():
    """Generate realistic-sounding company names."""
    templates = [
        lambda: f"{fake.last_name()} {random.choice(['Health', 'Medical', 'Care', 'Therapy', 'Services', 'Solutions', 'Group', 'Partners'])}",
        lambda: f"{fake.last_name()} & {fake.last_name()} {random.choice(['Associates', 'Group', 'Holdings', 'Partners'])}",
        lambda: f"{random.choice(['Advanced', 'Premier', 'National', 'American', 'United', 'Pacific', 'Atlantic'])} {random.choice(['Health', 'Medical', 'Care', 'Education', 'Tech', 'Services', 'Solutions'])}",
        lambda: f"{fake.city()} {random.choice(['Healthcare', 'Medical Group', 'Education Services', 'Therapy Center', 'Wellness'])}",
        lambda: f"{random.choice(['Nova', 'Apex', 'Vertex', 'Pulse', 'Catalyst', 'Bridge', 'Core'])} {random.choice(['Health', 'Medical', 'Learning', 'Tech', 'Care'])} {random.choice(['Inc', 'LLC', 'Corp', 'Group'])}",
    ]
    return random.choice(templates)()


def generate_record(record_id, date_range_start, date_range_end):
    """Generate a single CRM record with potential quality issues."""

    # Pick industry (weighted toward Healthcare like real data)
    industry_weights = [0.62, 0.12, 0.26]
    industry_clean = np.random.choice(INDUSTRIES_CLEAN, p=industry_weights)

    # Decide if this record gets dirty industry name (~30% chance)
    if random.random() < 0.30:
        industry = random.choice(INDUSTRIES_DIRTY)
    else:
        industry = industry_clean

    # Focus area
    focus_areas_for_industry = FOCUS_AREAS.get(industry_clean, FOCUS_AREAS["Healthcare"])
    focus_area_clean = random.choice(focus_areas_for_industry)

    # Dirty focus area (~25% chance)
    if focus_area_clean in FOCUS_AREA_DIRTY_MAP and random.random() < 0.25:
        focus_area = random.choice(FOCUS_AREA_DIRTY_MAP[focus_area_clean])
    else:
        focus_area = focus_area_clean

    # Deal type (weighted: 70% platform, 30% add-on)
    deal_type_clean = np.random.choice(DEAL_TYPES, p=[0.70, 0.30])
    deal_type = random.choice(DEAL_TYPES_DIRTY) if random.random() < 0.20 else deal_type_clean

    # Source
    source = random.choice(SOURCES_DIRTY) if random.random() < 0.20 else random.choice(SOURCES)

    # Stage (weighted toward early stages)
    stage_weights = [0.35, 0.25, 0.15, 0.05, 0.10, 0.10]
    stage_clean = np.random.choice(STAGES_ORDERED, p=stage_weights)
    stage = random.choice(STAGES_DIRTY) if random.random() < 0.20 else stage_clean

    # Dates
    date_sourced = fake.date_between(start_date=date_range_start, end_date=date_range_end)
    days_to_review = random.randint(1, 30)
    date_reviewed = date_sourced + timedelta(days=days_to_review)

    # Stage date (sometimes invalid)
    if random.random() < 0.05:  # 5% chance of future date
        stage_date = datetime.now().date() + timedelta(days=random.randint(30, 365))
    elif random.random() < 0.05:  # 5% chance date before sourced
        stage_date = date_sourced - timedelta(days=random.randint(30, 365))
    else:
        stage_date = date_reviewed + timedelta(days=random.randint(0, 90))

    # Financial data
    revenue = round(random.uniform(5, 80) * 1_000_000, -3)  # $5M - $80M
    ebitda_margin = random.uniform(0.08, 0.35)
    ebitda = round(revenue * ebitda_margin, -3)

    # Outlier financials (~3%)
    if random.random() < 0.03:
        revenue = round(random.uniform(500, 2000) * 1_000_000, -3)  # Unrealistically large
    if random.random() < 0.03:
        ebitda = -abs(ebitda)  # Negative EBITDA (might be valid but worth flagging)

    deal_size = round(random.uniform(10, 50) * 1_000_000, -3)

    # Company and contact info
    company_name = generate_company_name()
    contact_name = fake.name()
    contact_email = fake.email()
    contact_phone = fake.phone_number()

    # Dirty contact data (~15%)
    if random.random() < 0.08:
        contact_email = contact_email.replace("@", " at ")  # Malformed email
    if random.random() < 0.05:
        contact_email = "N/A"
    if random.random() < 0.10:
        contact_phone = ""  # Missing phone
    if random.random() < 0.05:
        contact_phone = "TBD"

    # Whitespace issues (~10%)
    if random.random() < 0.10:
        company_name = f"  {company_name}  "
    if random.random() < 0.08:
        contact_name = f" {contact_name}"

    # Build record
    record = {
        "deal_id": f"D-{record_id:04d}",
        "company_name": company_name,
        "industry": industry,
        "focus_area": focus_area,
        "deal_type": deal_type,
        "source": source,
        "intermediary": random.choice(INTERMEDIARY_FIRMS),
        "deal_stage": stage,
        "deal_owner": random.choice(DEAL_OWNERS),
        "date_sourced": date_sourced.isoformat(),
        "date_reviewed": date_reviewed.isoformat(),
        "stage_date": stage_date.isoformat(),
        "revenue_usd": revenue,
        "ebitda_usd": ebitda,
        "deal_size_usd": deal_size,
        "primary_contact": contact_name,
        "contact_email": contact_email,
        "contact_phone": contact_phone,
        "city": fake.city(),
        "state": fake.state_abbr(),
        "notes": fake.sentence(nb_words=random.randint(5, 15)) if random.random() > 0.30 else "",
    }

    # Introduce missing values (~15-25% across various fields)
    nullable_fields = [
        ("focus_area", 0.12), ("intermediary", 0.15), ("deal_size_usd", 0.18),
        ("revenue_usd", 0.10), ("ebitda_usd", 0.15), ("date_reviewed", 0.08),
        ("stage_date", 0.10), ("city", 0.05), ("state", 0.05),
        ("contact_email", 0.12), ("contact_phone", 0.15),
    ]
    for field, prob in nullable_fields:
        if random.random() < prob:
            record[field] = None

    return record


def generate_duplicates(records, n_dupes=25):
    """Create duplicate records with slight variations."""
    dupes = []
    source_indices = random.sample(range(len(records)), min(n_dupes, len(records)))

    for idx in source_indices:
        dupe = records[idx].copy()
        # Slight variations
        variation = random.choice(["same", "case", "spacing", "stage"])
        if variation == "case":
            dupe["company_name"] = dupe["company_name"].upper() if dupe["company_name"] else dupe["company_name"]
        elif variation == "spacing":
            if dupe["company_name"]:
                dupe["company_name"] = f"  {dupe['company_name'].strip()}  "
        elif variation == "stage":
            dupe["deal_stage"] = random.choice(STAGES_DIRTY)

        dupe["deal_id"] = f"D-{500 + len(dupes):04d}"
        dupes.append(dupe)

    return dupes


def main():
    print("Generating synthetic CRM dataset...")

    # Generate main records
    date_start = datetime(2023, 1, 1).date()
    date_end = datetime(2025, 12, 31).date()

    records = []
    for i in range(1, N_RECORDS + 1):
        record = generate_record(i, date_start, date_end)
        records.append(record)

    # Add duplicates (~5%)
    duplicates = generate_duplicates(records, n_dupes=25)
    records.extend(duplicates)

    # Shuffle
    random.shuffle(records)

    # Create DataFrame
    df = pd.DataFrame(records)

    # Save
    output_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(output_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    output_path = os.path.join(data_dir, "crm_deals_raw.csv")
    df.to_csv(output_path, index=False)

    # Print summary
    print(f"\nDataset generated: {len(df)} records")
    print(f"Saved to: {output_path}")
    print(f"\nColumn overview:")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Records: {len(df)}")
    print(f"\nMissing value counts:")
    missing = df.isnull().sum()
    for col in missing[missing > 0].index:
        print(f"  {col}: {missing[col]} ({missing[col]/len(df)*100:.1f}%)")

    print(f"\nQuality issues embedded:")
    print(f"  Duplicates: ~{len(duplicates)} records")
    print(f"  Inconsistent industry names: {df['industry'].nunique()} unique values (should be 3)")
    print(f"  Inconsistent deal stages: {df['deal_stage'].nunique()} unique values (should be 6)")
    print(f"  Inconsistent deal types: {df['deal_type'].nunique()} unique values (should be 2)")


if __name__ == "__main__":
    main()
