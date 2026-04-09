"""
Data Remediation Engine
========================
Applies standardization and cleaning rules to the raw CRM dataset.
Documents every transformation for audit trail transparency.

Remediation Steps:
1. Whitespace normalization
2. Industry standardization (fuzzy mapping)
3. Focus area standardization
4. Deal type standardization
5. Deal stage standardization
6. Source standardization
7. Email validation and flagging
8. Duplicate identification and resolution
9. Date validation and flagging
10. Financial outlier flagging
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime


# ============================================================
# Mapping Dictionaries (normalize dirty values to clean)
# ============================================================

INDUSTRY_MAP = {
    "healthcare": "Healthcare", "health care": "Healthcare", "hc": "Healthcare",
    "education": "Education", "ed": "Education", "edu": "Education",
    "technology-enabled services": "Technology-Enabled Services",
    "tes": "Technology-Enabled Services",
    "tech-enabled services": "Technology-Enabled Services",
    "technology enabled services": "Technology-Enabled Services",
    "tech enabled services": "Technology-Enabled Services",
}

DEAL_TYPE_MAP = {
    "platform": "Platform", "plat": "Platform",
    "add-on": "Add-On", "addon": "Add-On", "add on": "Add-On",
}

STAGE_MAP = {
    "reviewed": "Reviewed", "rev": "Reviewed",
    "indication": "Indication", "ioi": "Indication", "indicated": "Indication",
    "offer": "Offer", "loi": "Offer", "letter of intent": "Offer",
    "closed - won": "Closed - Won", "closed won": "Closed - Won", "won": "Closed - Won", "acquired": "Closed - Won",
    "closed - lost": "Closed - Lost", "closed lost": "Closed - Lost", "lost": "Closed - Lost",
    "passed": "Passed", "pass": "Passed", "declined": "Passed",
}

SOURCE_MAP = {
    "intermediary": "Intermediary", "broker": "Intermediary", "investment banker": "Intermediary",
    "proprietary": "Proprietary", "internal": "Proprietary", "prop": "Proprietary",
    "conference": "Conference", "event": "Conference", "trade show": "Conference",
    "direct outreach": "Direct Outreach", "cold outreach": "Direct Outreach", "direct": "Direct Outreach",
    "referral": "Referral", "referred": "Referral", "network": "Referral",
}

FOCUS_AREA_MAP = {
    "provider services": "Provider Services", "provider svcs": "Provider Services",
    "prov services": "Provider Services", "provider srvcs": "Provider Services",
    "pharma services": "Pharma Services", "pharmaceutical services": "Pharma Services",
    "pharma svcs": "Pharma Services",
    "chronic care solutions": "Chronic Care Solutions", "chronic care": "Chronic Care Solutions",
    "ccs": "Chronic Care Solutions",
    "property services": "Property Services", "prop services": "Property Services",
    "property svcs": "Property Services",
    "behavioral health": "Behavioral Health", "bh": "Behavioral Health",
    "behav health": "Behavioral Health",
}


def standardize_field(series, mapping):
    """Apply a mapping dictionary to standardize field values."""
    return series.fillna("").str.strip().str.lower().map(
        lambda x: mapping.get(x, x.title() if x else None)
    ).replace("", None)


def remediate(df):
    """Apply all remediation steps and return cleaned dataframe + change log."""
    df_clean = df.copy()
    changes = []

    # 1. Whitespace normalization on all string columns
    for col in df_clean.select_dtypes(include="object").columns:
        original = df_clean[col].copy()
        df_clean[col] = df_clean[col].fillna("").str.strip().replace("", None)
        n_changed = (original.fillna("") != df_clean[col].fillna("")).sum()
        if n_changed > 0:
            changes.append({"step": "whitespace_trim", "field": col, "records_changed": int(n_changed)})

    # 2. Industry standardization
    original = df_clean["industry"].copy()
    df_clean["industry"] = standardize_field(df_clean["industry"], INDUSTRY_MAP)
    n_changed = (original.fillna("") != df_clean["industry"].fillna("")).sum()
    changes.append({"step": "industry_standardization", "field": "industry", "records_changed": int(n_changed)})

    # 3. Focus area standardization
    original = df_clean["focus_area"].copy()
    df_clean["focus_area"] = standardize_field(df_clean["focus_area"], FOCUS_AREA_MAP)
    n_changed = (original.fillna("") != df_clean["focus_area"].fillna("")).sum()
    changes.append({"step": "focus_area_standardization", "field": "focus_area", "records_changed": int(n_changed)})

    # 4. Deal type standardization
    original = df_clean["deal_type"].copy()
    df_clean["deal_type"] = standardize_field(df_clean["deal_type"], DEAL_TYPE_MAP)
    n_changed = (original.fillna("") != df_clean["deal_type"].fillna("")).sum()
    changes.append({"step": "deal_type_standardization", "field": "deal_type", "records_changed": int(n_changed)})

    # 5. Deal stage standardization
    original = df_clean["deal_stage"].copy()
    df_clean["deal_stage"] = standardize_field(df_clean["deal_stage"], STAGE_MAP)
    n_changed = (original.fillna("") != df_clean["deal_stage"].fillna("")).sum()
    changes.append({"step": "deal_stage_standardization", "field": "deal_stage", "records_changed": int(n_changed)})

    # 6. Source standardization
    original = df_clean["source"].copy()
    df_clean["source"] = standardize_field(df_clean["source"], SOURCE_MAP)
    n_changed = (original.fillna("") != df_clean["source"].fillna("")).sum()
    changes.append({"step": "source_standardization", "field": "source", "records_changed": int(n_changed)})

    # 7. Email validation - flag invalid, don't delete
    df_clean["email_valid"] = df_clean["contact_email"].fillna("").str.match(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    invalid_emails = (~df_clean["email_valid"] & df_clean["contact_email"].notna() & df_clean["contact_email"].ne("")).sum()
    changes.append({"step": "email_validation_flag", "field": "contact_email", "records_flagged": int(invalid_emails)})

    # 8. Duplicate resolution - keep first occurrence, flag others
    df_clean["_company_norm"] = df_clean["company_name"].fillna("").str.strip().str.lower()
    df_clean["is_duplicate"] = df_clean.duplicated(subset=["_company_norm", "date_sourced"], keep="first")
    n_dupes = df_clean["is_duplicate"].sum()
    changes.append({"step": "duplicate_flagging", "field": "company_name+date_sourced", "records_flagged": int(n_dupes)})

    # Remove duplicates for clean dataset
    df_deduped = df_clean[~df_clean["is_duplicate"]].copy()
    df_deduped = df_deduped.drop(columns=["_company_norm", "is_duplicate", "email_valid"])

    # 9. Date validation - flag future dates and illogical sequences
    for date_col in ["date_sourced", "date_reviewed", "stage_date"]:
        df_deduped[date_col] = pd.to_datetime(df_deduped[date_col], errors="coerce")

    today = pd.Timestamp(datetime.now().date())
    df_deduped["date_flag"] = ""

    future_sourced = df_deduped["date_sourced"] > today
    future_reviewed = df_deduped["date_reviewed"] > today
    future_stage = df_deduped["stage_date"] > today
    any_future = future_sourced | future_reviewed | future_stage
    df_deduped.loc[any_future, "date_flag"] = "FUTURE_DATE"

    bad_seq = (df_deduped["date_reviewed"].notna() & df_deduped["date_sourced"].notna() &
               (df_deduped["date_reviewed"] < df_deduped["date_sourced"]))
    df_deduped.loc[bad_seq, "date_flag"] = "INVALID_SEQUENCE"

    n_date_issues = (df_deduped["date_flag"] != "").sum()
    changes.append({"step": "date_validation_flag", "field": "dates", "records_flagged": int(n_date_issues)})

    # 10. Financial outlier flagging
    df_deduped["financial_flag"] = ""
    if "revenue_usd" in df_deduped.columns:
        rev_outlier = df_deduped["revenue_usd"].notna() & (
            (df_deduped["revenue_usd"] > 200_000_000) | (df_deduped["revenue_usd"] < 1_000_000)
        )
        df_deduped.loc[rev_outlier, "financial_flag"] = "REVENUE_OUTLIER"

    if "ebitda_usd" in df_deduped.columns:
        neg_ebitda = df_deduped["ebitda_usd"].notna() & (df_deduped["ebitda_usd"] < 0)
        df_deduped.loc[neg_ebitda, "financial_flag"] = df_deduped.loc[neg_ebitda, "financial_flag"].apply(
            lambda x: f"{x}|NEGATIVE_EBITDA" if x else "NEGATIVE_EBITDA"
        )

    n_fin_issues = (df_deduped["financial_flag"] != "").sum()
    changes.append({"step": "financial_outlier_flag", "field": "financials", "records_flagged": int(n_fin_issues)})

    # Convert dates back to string for CSV
    for date_col in ["date_sourced", "date_reviewed", "stage_date"]:
        df_deduped[date_col] = df_deduped[date_col].dt.strftime("%Y-%m-%d")

    return df_deduped, changes


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_path = os.path.join(project_dir, "data", "crm_deals_raw.csv")

    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} raw records")

    df_clean, changes = remediate(df)

    # Save cleaned data
    output_dir = os.path.join(project_dir, "data")
    df_clean.to_csv(os.path.join(output_dir, "crm_deals_cleaned.csv"), index=False)

    # Save change log
    out_dir = os.path.join(project_dir, "output")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "remediation_log.json"), "w") as f:
        json.dump(changes, f, indent=2)

    print(f"\nCleaned dataset: {len(df_clean)} records (removed {len(df) - len(df_clean)} duplicates)")
    print(f"\nRemediation steps applied:")
    for c in changes:
        action = c.get("records_changed", c.get("records_flagged", 0))
        label = "changed" if "records_changed" in c else "flagged"
        print(f"  {c['step']}: {action} records {label}")

    # Re-profile the cleaned data
    from data_profiler import generate_profile_report
    report, record_scores = generate_profile_report(df_clean)

    with open(os.path.join(out_dir, "profile_report_cleaned.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    df_scored = pd.concat([df_clean.reset_index(drop=True), record_scores.reset_index(drop=True)], axis=1)
    df_scored.to_csv(os.path.join(out_dir, "deals_cleaned_with_scores.csv"), index=False)

    print(f"\n{'='*60}")
    print(f"  CRM DATA QUALITY SCORECARD (Post-Remediation)")
    print(f"{'='*60}")
    sc = report["scorecard"]
    print(f"\n  COMPOSITE SCORE:  {sc['composite_score']}/100\n")
    print(f"  Completeness:     {sc['completeness']}/100")
    print(f"  Consistency:      {sc['consistency']}/100")
    print(f"  Accuracy:         {sc['accuracy']}/100")
    print(f"  Uniqueness:       {sc['uniqueness']}/100")
    print(f"  Records:          {report['summary']['total_records']}")
    print(f"{'='*60}")
