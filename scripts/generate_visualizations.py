"""
Data Quality Visualization Suite
==================================
Generates publication-ready charts for the CRM Data Quality Audit portfolio project.

Charts produced:
1. Before/After Scorecard comparison (bar chart)
2. Completeness heatmap by field
3. Record score distribution (histogram)
4. Consistency issues breakdown
5. Quality improvement waterfall
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import json
import os

# Style configuration
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Arial', 'DejaVu Sans'],
    'font.size': 11,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'axes.labelsize': 12,
    'figure.facecolor': 'white',
    'axes.facecolor': '#FAFAFA',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.color': '#CCCCCC',
})

NAVY = '#1B3A5C'
BLUE = '#2E75B6'
GREEN = '#2D8B46'
RED = '#C0392B'
AMBER = '#D4832F'
GRAY = '#888888'
LIGHT_BLUE = '#D5E8F0'


def load_reports(output_dir):
    """Load pre and post remediation reports."""
    with open(os.path.join(output_dir, "profile_report_raw.json")) as f:
        raw_report = json.load(f)
    with open(os.path.join(output_dir, "profile_report_cleaned.json")) as f:
        clean_report = json.load(f)
    return raw_report, clean_report


def chart_scorecard_comparison(raw, clean, output_dir):
    """Chart 1: Before/After scorecard comparison."""
    categories = ["Composite", "Completeness", "Consistency", "Accuracy", "Uniqueness"]
    raw_scores = [
        raw["scorecard"]["composite_score"],
        raw["scorecard"]["completeness"],
        raw["scorecard"]["consistency"],
        raw["scorecard"]["accuracy"],
        raw["scorecard"]["uniqueness"],
    ]
    clean_scores = [
        clean["scorecard"]["composite_score"],
        clean["scorecard"]["completeness"],
        clean["scorecard"]["consistency"],
        clean["scorecard"]["accuracy"],
        clean["scorecard"]["uniqueness"],
    ]

    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.bar(x - width/2, raw_scores, width, label='Before Remediation',
                   color=RED, alpha=0.8, edgecolor='white', linewidth=0.5)
    bars2 = ax.bar(x + width/2, clean_scores, width, label='After Remediation',
                   color=GREEN, alpha=0.8, edgecolor='white', linewidth=0.5)

    ax.set_ylabel('Score (out of 100)')
    ax.set_title('Data Quality Scorecard: Before vs. After Remediation')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend(loc='lower right')
    ax.set_ylim(60, 105)
    ax.axhline(y=95, color=GREEN, linestyle='--', alpha=0.4, label='Target (95)')

    # Add value labels
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points", ha='center', va='bottom',
                    fontsize=9, color=RED, fontweight='bold')
    for bar in bars2:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points", ha='center', va='bottom',
                    fontsize=9, color=GREEN, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "01_scorecard_comparison.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: 01_scorecard_comparison.png")


def chart_completeness_by_field(raw, output_dir):
    """Chart 2: Field-level completeness (pre-remediation)."""
    fields = []
    pcts = []
    for field, data in raw["completeness"].items():
        fields.append(field.replace("_", " ").title())
        pcts.append(data["completeness_pct"])

    # Sort by completeness
    sorted_pairs = sorted(zip(pcts, fields))
    pcts, fields = zip(*sorted_pairs)

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = [GREEN if p >= 90 else AMBER if p >= 75 else RED for p in pcts]
    bars = ax.barh(fields, pcts, color=colors, edgecolor='white', linewidth=0.5, height=0.6)

    ax.set_xlabel('Completeness (%)')
    ax.set_title('Field-Level Completeness (Pre-Remediation)')
    ax.set_xlim(0, 105)
    ax.axvline(x=90, color=GREEN, linestyle='--', alpha=0.4)
    ax.axvline(x=75, color=AMBER, linestyle='--', alpha=0.4)

    for bar, pct in zip(bars, pcts):
        ax.annotate(f'{pct:.1f}%', xy=(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2),
                    va='center', fontsize=9, fontweight='bold', color=NAVY)

    # Legend
    patches = [
        mpatches.Patch(color=GREEN, label='Good (90%+)'),
        mpatches.Patch(color=AMBER, label='Needs Attention (75-90%)'),
        mpatches.Patch(color=RED, label='Critical (<75%)')
    ]
    ax.legend(handles=patches, loc='lower right')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "02_completeness_by_field.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: 02_completeness_by_field.png")


def chart_score_distribution(output_dir):
    """Chart 3: Record-level quality score distribution (before and after)."""
    # Load both scored datasets
    raw_scores = pd.read_csv(os.path.join(output_dir, "deals_with_scores.csv"))
    clean_scores = pd.read_csv(os.path.join(output_dir, "deals_cleaned_with_scores.csv"))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    # Before
    ax1.hist(raw_scores["quality_score"].dropna(), bins=20, color=RED, alpha=0.7,
             edgecolor='white', linewidth=0.5)
    ax1.set_title('Before Remediation')
    ax1.set_xlabel('Quality Score')
    ax1.set_ylabel('Number of Records')
    ax1.axvline(x=raw_scores["quality_score"].mean(), color=NAVY, linestyle='--', linewidth=2,
                label=f'Mean: {raw_scores["quality_score"].mean():.1f}')
    ax1.legend()

    # After
    ax2.hist(clean_scores["quality_score"].dropna(), bins=20, color=GREEN, alpha=0.7,
             edgecolor='white', linewidth=0.5)
    ax2.set_title('After Remediation')
    ax2.set_xlabel('Quality Score')
    ax2.axvline(x=clean_scores["quality_score"].mean(), color=NAVY, linestyle='--', linewidth=2,
                label=f'Mean: {clean_scores["quality_score"].mean():.1f}')
    ax2.legend()

    fig.suptitle('Record-Level Quality Score Distribution', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "03_score_distribution.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: 03_score_distribution.png")


def chart_consistency_breakdown(raw, output_dir):
    """Chart 4: Consistency issues by field."""
    fields = []
    valid_pcts = []
    invalid_counts = []

    for field in ["industry", "focus_area", "deal_type", "deal_stage", "source"]:
        if field in raw["consistency"]:
            data = raw["consistency"][field]
            fields.append(field.replace("_", " ").title())
            valid_pcts.append(data["consistency_pct"])
            invalid_counts.append(data["invalid"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: consistency percentages
    colors = [GREEN if p >= 90 else AMBER if p >= 75 else RED for p in valid_pcts]
    bars = ax1.barh(fields, valid_pcts, color=colors, edgecolor='white', linewidth=0.5)
    ax1.set_xlabel('Consistency (%)')
    ax1.set_title('Field Consistency (Pre-Remediation)')
    ax1.set_xlim(0, 105)
    ax1.axvline(x=95, color=GREEN, linestyle='--', alpha=0.4)

    for bar, pct in zip(bars, valid_pcts):
        ax1.annotate(f'{pct:.1f}%', xy=(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2),
                    va='center', fontsize=9, fontweight='bold')

    # Right: invalid counts
    bars2 = ax2.barh(fields, invalid_counts, color=RED, alpha=0.7, edgecolor='white', linewidth=0.5)
    ax2.set_xlabel('Number of Invalid Records')
    ax2.set_title('Records Needing Standardization')

    for bar, count in zip(bars2, invalid_counts):
        ax2.annotate(f'{count}', xy=(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2),
                    va='center', fontsize=9, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "04_consistency_breakdown.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: 04_consistency_breakdown.png")


def chart_remediation_impact(output_dir):
    """Chart 5: Summary remediation impact infographic."""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 8)
    ax.axis('off')

    # Title
    ax.text(5, 7.5, 'Remediation Impact Summary', ha='center', va='center',
            fontsize=18, fontweight='bold', color=NAVY)

    # Metrics boxes
    metrics = [
        ("Records Processed", "525 \u2192 500", "25 duplicates removed"),
        ("Composite Score", "91.6 \u2192 97.7", "+6.1 points"),
        ("Consistency", "84.2 \u2192 100.0", "+15.8 points"),
        ("Fields Standardized", "5 fields", "~408 values corrected"),
    ]

    for i, (title, value, detail) in enumerate(metrics):
        x = 1.25 + (i % 2) * 4.5
        y = 5.5 - (i // 2) * 3
        rect = plt.Rectangle((x - 1.5, y - 1), 3.5, 2.2, linewidth=1,
                              edgecolor=BLUE, facecolor=LIGHT_BLUE, alpha=0.3, zorder=1)
        ax.add_patch(rect)
        ax.text(x + 0.25, y + 0.7, title, ha='center', va='center',
                fontsize=11, fontweight='bold', color=NAVY, zorder=2)
        ax.text(x + 0.25, y + 0.05, value, ha='center', va='center',
                fontsize=16, fontweight='bold', color=GREEN, zorder=2)
        ax.text(x + 0.25, y - 0.6, detail, ha='center', va='center',
                fontsize=9, color=GRAY, zorder=2)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "05_remediation_impact.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: 05_remediation_impact.png")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    output_dir = os.path.join(project_dir, "output")

    print("Generating visualizations...\n")

    raw_report, clean_report = load_reports(output_dir)

    chart_scorecard_comparison(raw_report, clean_report, output_dir)
    chart_completeness_by_field(raw_report, output_dir)
    chart_score_distribution(output_dir)
    chart_consistency_breakdown(raw_report, output_dir)
    chart_remediation_impact(output_dir)

    print(f"\nAll visualizations saved to: {output_dir}/")


if __name__ == "__main__":
    main()
