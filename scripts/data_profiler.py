"""
Data Profiling & Quality Scoring Engine
========================================
Profiles a CRM dataset and produces quality scores across 5 dimensions:
  1. Completeness - % of non-null values in critical fields
  2. Consistency  - % of values conforming to standard reference lists
  3. Accuracy     - % of values passing logical validation rules
  4. Uniqueness   - % of records that are not duplicates
  5. Timeliness   - % of records with valid, logical date sequences

Outputs a per-record score (0-100) and an aggregate scorecard.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os


# ============================================================
# Reference Standards (what "clean" looks like)
# ============================================================

VALID_INDUSTRIES = {"Healthcare", "Education", "Technology-Enabled Services"}

VALID_FOCUS_AREAS = {
    "Provider Services", "Pharma Services", "Chronic Care Solutions",
    "Behavioral Health", "Healthy Living & Wellness", "Animal Health",
    "Home Healthcare", "Physician Practice Enablement",
    "Higher Education Outsourcing", "Skills Gap Training",
    "Corporate Training", "Tutoring & Test Prep",
    "Compliance Services", "Logistics Management",
    "Specialty Healthcare Logistics", "Residential Services",
    "Property Services"
}

VALID_DEAL_TYPES = {"Platform", "Add-On"}

VALID_STAGES = {
    "Reviewed", "Indication", "Offer",
    "Closed - Won", "Closed - Lost", "Passed"
}

VALID_SOURCES = {
    "Intermediary", "Proprietary", "Conference",
    "Direct Outreach", "Referral"
}

# Critical fields that should never be empty
CRITICAL_FIELDS = [
    "deal_id", "company_name", "industry", "deal_type",
    "deal_stage", "deal_owner", "date_sourced"
]

# Important fields that should ideally be populated
IMPORTANT_FIELDS = [
    "focus_area", "source", "revenue_usd", "ebitda_usd",
    "deal_size_usd", "primary_contact", "contact_email"
]


def profile_completeness(df):
    """Score completeness: % of non-null values in critical + important fields."""
    all_fields = CRITICAL_FIELDS + IMPORTANT_FIELDS
    results = {}

    for field in all_fields:
        if field in df.columns:
            total = len(df)
            non_null = df[field].notna().sum()
            # Also count empty strings and whitespace-only as missing
            if df[field].dtype == object:
                non_empty = df[field].fillna("").str.strip().ne("").sum()
                results[field] = {
                    "total": total,
                    "populated": int(non_empty),
                    "missing": total - int(non_empty),
                    "completeness_pct": round(non_empty / total * 100, 1)
                }
            else:
                results[field] = {
                    "total": total,
                    "populated": int(non_null),
                    "missing": total - int(non_null),
                    "completeness_pct": round(non_null / total * 100, 1)
                }

    overall = np.mean([v["completeness_pct"] for v in results.values()])
    return results, round(overall, 1)


def profile_consistency(df):
    """Score consistency: % of values that match standard reference lists."""
    results = {}

    # Industry consistency
    if "industry" in df.columns:
        valid_mask = df["industry"].fillna("").str.strip().isin(VALID_INDUSTRIES)
        non_null_mask = df["industry"].notna()
        valid_count = (valid_mask & non_null_mask).sum()
        total_non_null = non_null_mask.sum()
        results["industry"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "unique_values": int(df["industry"].nunique()),
            "expected_unique": len(VALID_INDUSTRIES),
            "consistency_pct": round(valid_count / max(total_non_null, 1) * 100, 1),
            "invalid_examples": df.loc[~valid_mask & non_null_mask, "industry"].unique()[:5].tolist()
        }

    # Focus area consistency
    if "focus_area" in df.columns:
        valid_mask = df["focus_area"].fillna("").str.strip().isin(VALID_FOCUS_AREAS)
        non_null_mask = df["focus_area"].notna()
        valid_count = (valid_mask & non_null_mask).sum()
        total_non_null = non_null_mask.sum()
        results["focus_area"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "unique_values": int(df["focus_area"].nunique()),
            "expected_unique": len(VALID_FOCUS_AREAS),
            "consistency_pct": round(valid_count / max(total_non_null, 1) * 100, 1),
            "invalid_examples": df.loc[~valid_mask & non_null_mask, "focus_area"].unique()[:5].tolist()
        }

    # Deal type consistency
    if "deal_type" in df.columns:
        valid_mask = df["deal_type"].fillna("").str.strip().isin(VALID_DEAL_TYPES)
        non_null_mask = df["deal_type"].notna()
        valid_count = (valid_mask & non_null_mask).sum()
        total_non_null = non_null_mask.sum()
        results["deal_type"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "consistency_pct": round(valid_count / max(total_non_null, 1) * 100, 1),
            "invalid_examples": df.loc[~valid_mask & non_null_mask, "deal_type"].unique()[:5].tolist()
        }

    # Deal stage consistency
    if "deal_stage" in df.columns:
        valid_mask = df["deal_stage"].fillna("").str.strip().isin(VALID_STAGES)
        non_null_mask = df["deal_stage"].notna()
        valid_count = (valid_mask & non_null_mask).sum()
        total_non_null = non_null_mask.sum()
        results["deal_stage"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "consistency_pct": round(valid_count / max(total_non_null, 1) * 100, 1),
            "invalid_examples": df.loc[~valid_mask & non_null_mask, "deal_stage"].unique()[:5].tolist()
        }

    # Source consistency
    if "source" in df.columns:
        valid_mask = df["source"].fillna("").str.strip().isin(VALID_SOURCES)
        non_null_mask = df["source"].notna()
        valid_count = (valid_mask & non_null_mask).sum()
        total_non_null = non_null_mask.sum()
        results["source"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "consistency_pct": round(valid_count / max(total_non_null, 1) * 100, 1),
            "invalid_examples": df.loc[~valid_mask & non_null_mask, "source"].unique()[:5].tolist()
        }

    overall = np.mean([v["consistency_pct"] for v in results.values()])
    return results, round(overall, 1)


def profile_accuracy(df):
    """Score accuracy: logical validation rules."""
    results = {}
    total = len(df)

    # Email format validation
    if "contact_email" in df.columns:
        non_null = df["contact_email"].notna() & df["contact_email"].ne("")
        valid_email = df["contact_email"].fillna("").str.match(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        valid_count = (valid_email & non_null).sum()
        total_non_null = non_null.sum()
        results["email_format"] = {
            "valid": int(valid_count),
            "invalid": int(total_non_null - valid_count),
            "accuracy_pct": round(valid_count / max(total_non_null, 1) * 100, 1)
        }

    # Date sequence: sourced <= reviewed <= stage_date
    if all(c in df.columns for c in ["date_sourced", "date_reviewed", "stage_date"]):
        ds = pd.to_datetime(df["date_sourced"], errors="coerce")
        dr = pd.to_datetime(df["date_reviewed"], errors="coerce")
        sd = pd.to_datetime(df["stage_date"], errors="coerce")

        # Check sourced before reviewed
        has_both = ds.notna() & dr.notna()
        valid_seq = (dr >= ds) | ~has_both
        results["date_sequence_sourced_reviewed"] = {
            "valid": int(valid_seq.sum()),
            "invalid": int((~valid_seq).sum()),
            "accuracy_pct": round(valid_seq.sum() / total * 100, 1)
        }

        # Check no future dates
        today = pd.Timestamp(datetime.now().date())
        future_dates = (ds > today) | (dr > today) | (sd > today)
        results["no_future_dates"] = {
            "valid": int((~future_dates).sum()),
            "invalid": int(future_dates.sum()),
            "accuracy_pct": round((~future_dates).sum() / total * 100, 1)
        }

    # Financial reasonableness: EBITDA < Revenue
    if all(c in df.columns for c in ["revenue_usd", "ebitda_usd"]):
        has_both = df["revenue_usd"].notna() & df["ebitda_usd"].notna()
        valid_fin = (df["ebitda_usd"].abs() <= df["revenue_usd"]) | ~has_both
        results["ebitda_vs_revenue"] = {
            "valid": int(valid_fin.sum()),
            "invalid": int((~valid_fin).sum()),
            "accuracy_pct": round(valid_fin.sum() / total * 100, 1)
        }

    # Revenue range check ($1M - $200M typical for PE targets)
    if "revenue_usd" in df.columns:
        has_rev = df["revenue_usd"].notna()
        in_range = (df["revenue_usd"] >= 1_000_000) & (df["revenue_usd"] <= 200_000_000)
        valid_range = in_range | ~has_rev
        results["revenue_range"] = {
            "valid": int(valid_range.sum()),
            "invalid": int((~valid_range).sum()),
            "accuracy_pct": round(valid_range.sum() / total * 100, 1)
        }

    overall = np.mean([v["accuracy_pct"] for v in results.values()])
    return results, round(overall, 1)


def profile_uniqueness(df):
    """Score uniqueness: identify duplicate records."""
    # Check for exact duplicates on company_name (normalized)
    df_check = df.copy()
    df_check["_company_norm"] = df_check["company_name"].fillna("").str.strip().str.lower()

    dupes = df_check[df_check.duplicated(subset=["_company_norm", "date_sourced"], keep=False)]

    unique_pct = round((1 - len(dupes) / max(len(df), 1)) * 100, 1)

    return {
        "total_records": len(df),
        "duplicate_records": len(dupes),
        "unique_records": len(df) - len(dupes),
        "uniqueness_pct": unique_pct,
        "duplicate_companies": dupes["_company_norm"].unique()[:10].tolist()
    }, unique_pct


def compute_record_scores(df):
    """Compute a per-record quality score (0-100)."""
    scores = pd.DataFrame(index=df.index)

    # Completeness score per record (40% weight)
    all_fields = CRITICAL_FIELDS + IMPORTANT_FIELDS
    existing = [f for f in all_fields if f in df.columns]
    completeness = df[existing].notna().mean(axis=1) * 100
    scores["completeness"] = completeness

    # Consistency score per record (30% weight)
    consistency_checks = []
    if "industry" in df.columns:
        consistency_checks.append(df["industry"].fillna("").str.strip().isin(VALID_INDUSTRIES).astype(int))
    if "deal_type" in df.columns:
        consistency_checks.append(df["deal_type"].fillna("").str.strip().isin(VALID_DEAL_TYPES).astype(int))
    if "deal_stage" in df.columns:
        consistency_checks.append(df["deal_stage"].fillna("").str.strip().isin(VALID_STAGES).astype(int))
    if "source" in df.columns:
        consistency_checks.append(df["source"].fillna("").str.strip().isin(VALID_SOURCES).astype(int))

    if consistency_checks:
        scores["consistency"] = pd.concat(consistency_checks, axis=1).mean(axis=1) * 100
    else:
        scores["consistency"] = 100

    # Accuracy score per record (20% weight)
    accuracy_checks = []
    if "contact_email" in df.columns:
        valid_email = df["contact_email"].fillna("").str.match(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        ) | df["contact_email"].isna() | df["contact_email"].eq("")
        accuracy_checks.append(valid_email.astype(int))

    if all(c in df.columns for c in ["date_sourced", "date_reviewed"]):
        ds = pd.to_datetime(df["date_sourced"], errors="coerce")
        dr = pd.to_datetime(df["date_reviewed"], errors="coerce")
        valid_date = (dr >= ds) | ds.isna() | dr.isna()
        accuracy_checks.append(valid_date.astype(int))

    if accuracy_checks:
        scores["accuracy"] = pd.concat(accuracy_checks, axis=1).mean(axis=1) * 100
    else:
        scores["accuracy"] = 100

    # Weighted composite score
    scores["quality_score"] = (
        scores["completeness"] * 0.40 +
        scores["consistency"] * 0.30 +
        scores["accuracy"] * 0.30
    ).round(1)

    return scores


def generate_profile_report(df):
    """Generate comprehensive data profile report."""
    report = {
        "summary": {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "profiled_at": datetime.now().isoformat()
        }
    }

    # Run all profilers
    completeness_detail, completeness_score = profile_completeness(df)
    consistency_detail, consistency_score = profile_consistency(df)
    accuracy_detail, accuracy_score = profile_accuracy(df)
    uniqueness_detail, uniqueness_score = profile_uniqueness(df)

    # Composite score
    composite = round(
        completeness_score * 0.30 +
        consistency_score * 0.30 +
        accuracy_score * 0.25 +
        uniqueness_score * 0.15,
    1)

    report["scorecard"] = {
        "composite_score": composite,
        "completeness": completeness_score,
        "consistency": consistency_score,
        "accuracy": accuracy_score,
        "uniqueness": uniqueness_score,
    }

    report["completeness"] = completeness_detail
    report["consistency"] = consistency_detail
    report["accuracy"] = accuracy_detail
    report["uniqueness"] = uniqueness_detail

    # Record-level scores
    record_scores = compute_record_scores(df)

    return report, record_scores


if __name__ == "__main__":
    # Load data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_path = os.path.join(project_dir, "data", "crm_deals_raw.csv")

    df = pd.read_csv(data_path)
    report, record_scores = generate_profile_report(df)

    # Save report
    output_dir = os.path.join(project_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, "profile_report_raw.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Save record scores
    df_with_scores = pd.concat([df, record_scores], axis=1)
    df_with_scores.to_csv(os.path.join(output_dir, "deals_with_scores.csv"), index=False)

    # Print scorecard
    print("=" * 60)
    print("  CRM DATA QUALITY SCORECARD (Pre-Remediation)")
    print("=" * 60)
    sc = report["scorecard"]
    print(f"\n  COMPOSITE SCORE:  {sc['composite_score']}/100\n")
    print(f"  Completeness:     {sc['completeness']}/100  (30% weight)")
    print(f"  Consistency:      {sc['consistency']}/100  (30% weight)")
    print(f"  Accuracy:         {sc['accuracy']}/100  (25% weight)")
    print(f"  Uniqueness:       {sc['uniqueness']}/100  (15% weight)")
    print(f"\n  Records:          {report['summary']['total_records']}")
    print(f"  Duplicates:       {report['uniqueness']['duplicate_records']}")

    # Record score distribution
    print(f"\n  Record Score Distribution:")
    print(f"    Mean:   {record_scores['quality_score'].mean():.1f}")
    print(f"    Median: {record_scores['quality_score'].median():.1f}")
    print(f"    Min:    {record_scores['quality_score'].min():.1f}")
    print(f"    Max:    {record_scores['quality_score'].max():.1f}")
    print(f"    < 50:   {(record_scores['quality_score'] < 50).sum()} records")
    print(f"    50-75:  {((record_scores['quality_score'] >= 50) & (record_scores['quality_score'] < 75)).sum()} records")
    print(f"    75-90:  {((record_scores['quality_score'] >= 75) & (record_scores['quality_score'] < 90)).sum()} records")
    print(f"    90+:    {(record_scores['quality_score'] >= 90).sum()} records")
    print("=" * 60)
