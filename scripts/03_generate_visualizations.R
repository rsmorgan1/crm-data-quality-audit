# ============================================================
# CRM Data Quality Visualization Suite (R / ggplot2)
# ============================================================
# Generates publication-ready charts for the CRM Data Quality
# Audit portfolio project using ggplot2 and tidyverse.
#
# Charts produced:
#   1. Before/After Scorecard comparison (grouped bar)
#   2. Field-level completeness (horizontal bar)
#   3. Record score distribution (side-by-side histogram)
#   4. Consistency issues breakdown (dual panel)
#   5. Remediation impact summary (infographic)
# ============================================================

library(tidyverse)
library(jsonlite)
library(patchwork)   # for combining plots


# ============================================================
# Color Palette & Theme
# ============================================================

NAVY      <- "#1B3A5C"
BLUE      <- "#2E75B6"
GREEN     <- "#2D8B46"
RED       <- "#C0392B"
AMBER     <- "#D4832F"
GRAY      <- "#888888"
LIGHT_BLUE <- "#D5E8F0"

theme_quality <- function() {
  theme_minimal(base_size = 11, base_family = "sans") +
    theme(
      plot.title       = element_text(face = "bold", size = 14, color = NAVY),
      plot.subtitle    = element_text(size = 10, color = GRAY),
      axis.title       = element_text(size = 12),
      panel.background = element_rect(fill = "#FAFAFA", color = NA),
      panel.grid.major = element_line(color = "#CCCCCC", linewidth = 0.3),
      panel.grid.minor = element_blank(),
      legend.position  = "bottom"
    )
}


# ============================================================
# Helper: Load Reports
# ============================================================

load_reports <- function(output_dir) {
  raw   <- fromJSON(file.path(output_dir, "profile_report_raw_r.json"))
  clean <- fromJSON(file.path(output_dir, "profile_report_cleaned_r.json"))
  list(raw = raw, clean = clean)
}


# ============================================================
# Chart 1: Before / After Scorecard Comparison
# ============================================================

chart_scorecard_comparison <- function(raw, clean, output_dir) {
  categories <- c("Composite", "Completeness", "Consistency", "Accuracy", "Uniqueness")
  keys       <- c("composite_score", "completeness", "consistency", "accuracy", "uniqueness")

  df <- tibble(
    category = rep(categories, 2),
    phase    = rep(c("Before Remediation", "After Remediation"), each = 5),
    score    = c(
      map_dbl(keys, ~ raw[[.x]]),
      map_dbl(keys, ~ clean[[.x]])
    )
  ) %>%
    mutate(
      category = factor(category, levels = categories),
      phase    = factor(phase, levels = c("Before Remediation", "After Remediation"))
    )

  p <- ggplot(df, aes(x = category, y = score, fill = phase)) +
    geom_col(position = position_dodge(width = 0.7), width = 0.6, alpha = 0.85) +
    geom_text(
      aes(label = sprintf("%.1f", score), color = phase),
      position = position_dodge(width = 0.7),
      vjust = -0.5, size = 3.2, fontface = "bold", show.legend = FALSE
    ) +
    geom_hline(yintercept = 95, linetype = "dashed", color = GREEN, alpha = 0.5) +
    scale_fill_manual(values = c("Before Remediation" = RED, "After Remediation" = GREEN)) +
    scale_color_manual(values = c("Before Remediation" = RED, "After Remediation" = GREEN)) +
    coord_cartesian(ylim = c(60, 105)) +
    labs(
      title = "Data Quality Scorecard: Before vs. After Remediation",
      y     = "Score (out of 100)",
      x     = NULL,
      fill  = NULL
    ) +
    theme_quality()

  ggsave(file.path(output_dir, "01_scorecard_comparison_r.png"),
         plot = p, width = 10, height = 6, dpi = 150)
  cat("  Saved: 01_scorecard_comparison_r.png\n")
}


# ============================================================
# Chart 2: Field-Level Completeness (Pre-Remediation)
# ============================================================

chart_completeness_by_field <- function(output_dir) {
  # Use scored records to compute field-level completeness
  df_raw <- read_csv(file.path(output_dir, "deals_with_scores_r.csv"), show_col_types = FALSE)

  critical  <- c("deal_id", "company_name", "industry", "deal_type",
                  "deal_stage", "deal_owner", "date_sourced")
  important <- c("focus_area", "source", "revenue_usd", "ebitda_usd",
                  "deal_size_usd", "primary_contact", "contact_email")
  all_fields <- intersect(c(critical, important), names(df_raw))

  completeness <- map_dfr(all_fields, function(f) {
    vals <- df_raw[[f]]
    if (is.character(vals)) {
      populated <- sum(!is.na(vals) & str_trim(vals) != "")
    } else {
      populated <- sum(!is.na(vals))
    }
    tibble(
      field = str_to_title(str_replace_all(f, "_", " ")),
      pct   = round(populated / nrow(df_raw) * 100, 1)
    )
  }) %>%
    mutate(
      color_group = case_when(
        pct >= 90 ~ "Good (90%+)",
        pct >= 75 ~ "Needs Attention (75-90%)",
        TRUE      ~ "Critical (<75%)"
      ),
      field = fct_reorder(field, pct)
    )

  p <- ggplot(completeness, aes(x = pct, y = field, fill = color_group)) +
    geom_col(width = 0.6, alpha = 0.85) +
    geom_text(aes(label = sprintf("%.1f%%", pct)),
              hjust = -0.1, size = 3.2, fontface = "bold", color = NAVY) +
    geom_vline(xintercept = 90, linetype = "dashed", color = GREEN, alpha = 0.5) +
    geom_vline(xintercept = 75, linetype = "dashed", color = AMBER, alpha = 0.5) +
    scale_fill_manual(
      values = c("Good (90%+)" = GREEN,
                 "Needs Attention (75-90%)" = AMBER,
                 "Critical (<75%)" = RED)
    ) +
    coord_cartesian(xlim = c(0, 110)) +
    labs(
      title = "Field-Level Completeness (Pre-Remediation)",
      x     = "Completeness (%)",
      y     = NULL,
      fill  = NULL
    ) +
    theme_quality()

  ggsave(file.path(output_dir, "02_completeness_by_field_r.png"),
         plot = p, width = 10, height = 7, dpi = 150)
  cat("  Saved: 02_completeness_by_field_r.png\n")
}


# ============================================================
# Chart 3: Record Score Distribution (Before & After)
# ============================================================

chart_score_distribution <- function(output_dir) {
  raw_scores   <- read_csv(file.path(output_dir, "deals_with_scores_r.csv"), show_col_types = FALSE)
  clean_scores <- read_csv(file.path(output_dir, "deals_cleaned_with_scores_r.csv"), show_col_types = FALSE)

  raw_mean   <- mean(raw_scores$quality_score, na.rm = TRUE)
  clean_mean <- mean(clean_scores$quality_score, na.rm = TRUE)

  p1 <- ggplot(raw_scores, aes(x = quality_score)) +
    geom_histogram(bins = 20, fill = RED, alpha = 0.7, color = "white", linewidth = 0.3) +
    geom_vline(xintercept = raw_mean, linetype = "dashed", color = NAVY, linewidth = 1) +
    annotate("text", x = raw_mean - 2, y = Inf, vjust = 1.5,
             label = sprintf("Mean: %.1f", raw_mean),
             fontface = "bold", size = 3.5, color = NAVY) +
    labs(title = "Before Remediation", x = "Quality Score", y = "Number of Records") +
    theme_quality()

  p2 <- ggplot(clean_scores, aes(x = quality_score)) +
    geom_histogram(bins = 20, fill = GREEN, alpha = 0.7, color = "white", linewidth = 0.3) +
    geom_vline(xintercept = clean_mean, linetype = "dashed", color = NAVY, linewidth = 1) +
    annotate("text", x = clean_mean - 2, y = Inf, vjust = 1.5,
             label = sprintf("Mean: %.1f", clean_mean),
             fontface = "bold", size = 3.5, color = NAVY) +
    labs(title = "After Remediation", x = "Quality Score", y = NULL) +
    theme_quality()

  combined <- p1 + p2 +
    plot_annotation(
      title = "Record-Level Quality Score Distribution",
      theme = theme(plot.title = element_text(face = "bold", size = 14, color = NAVY, hjust = 0.5))
    )

  ggsave(file.path(output_dir, "03_score_distribution_r.png"),
         plot = combined, width = 14, height = 5, dpi = 150)
  cat("  Saved: 03_score_distribution_r.png\n")
}


# ============================================================
# Chart 4: Consistency Breakdown (Pre-Remediation)
# ============================================================

chart_consistency_breakdown <- function(output_dir) {
  # Compute consistency from raw data against reference standards
  df_raw <- read_csv(
    file.path(dirname(output_dir), "data", "crm_deals_raw.csv"),
    show_col_types = FALSE
  )

  valid_lists <- list(
    Industry   = c("Healthcare", "Education", "Technology-Enabled Services"),
    `Focus Area` = c("Provider Services", "Pharma Services", "Chronic Care Solutions",
                     "Behavioral Health", "Healthy Living & Wellness", "Animal Health",
                     "Home Healthcare", "Physician Practice Enablement",
                     "Higher Education Outsourcing", "Skills Gap Training",
                     "Corporate Training", "Tutoring & Test Prep",
                     "Compliance Services", "Logistics Management",
                     "Specialty Healthcare Logistics", "Residential Services",
                     "Property Services"),
    `Deal Type`  = c("Platform", "Add-On"),
    `Deal Stage` = c("Reviewed", "Indication", "Offer", "Closed - Won", "Closed - Lost", "Passed"),
    Source     = c("Intermediary", "Proprietary", "Conference", "Direct Outreach", "Referral")
  )

  col_map <- c(Industry = "industry", `Focus Area` = "focus_area",
               `Deal Type` = "deal_type", `Deal Stage` = "deal_stage",
               Source = "source")

  consistency <- map_dfr(names(valid_lists), function(label) {
    col <- col_map[[label]]
    vals <- str_trim(df_raw[[col]])
    non_null <- !is.na(vals) & vals != ""
    valid <- vals %in% valid_lists[[label]] & non_null
    tibble(
      field           = label,
      consistency_pct = round(sum(valid) / max(sum(non_null), 1) * 100, 1),
      invalid_count   = sum(non_null) - sum(valid)
    )
  }) %>%
    mutate(
      color_group = case_when(
        consistency_pct >= 90 ~ "good",
        consistency_pct >= 75 ~ "warn",
        TRUE                  ~ "bad"
      ),
      field = factor(field, levels = rev(field))
    )

  p1 <- ggplot(consistency, aes(x = consistency_pct, y = field, fill = color_group)) +
    geom_col(width = 0.6, alpha = 0.85) +
    geom_text(aes(label = sprintf("%.1f%%", consistency_pct)),
              hjust = -0.1, size = 3.2, fontface = "bold") +
    geom_vline(xintercept = 95, linetype = "dashed", color = GREEN, alpha = 0.5) +
    scale_fill_manual(values = c(good = GREEN, warn = AMBER, bad = RED), guide = "none") +
    coord_cartesian(xlim = c(0, 110)) +
    labs(title = "Field Consistency (Pre-Remediation)", x = "Consistency (%)", y = NULL) +
    theme_quality() + theme(legend.position = "none")

  p2 <- ggplot(consistency, aes(x = invalid_count, y = field)) +
    geom_col(width = 0.6, fill = RED, alpha = 0.7) +
    geom_text(aes(label = invalid_count), hjust = -0.2, size = 3.2, fontface = "bold") +
    labs(title = "Records Needing Standardization", x = "Number of Invalid Records", y = NULL) +
    theme_quality()

  combined <- p1 + p2

  ggsave(file.path(output_dir, "04_consistency_breakdown_r.png"),
         plot = combined, width = 14, height = 5, dpi = 150)
  cat("  Saved: 04_consistency_breakdown_r.png\n")
}


# ============================================================
# Chart 5: Remediation Impact Summary
# ============================================================

chart_remediation_impact <- function(raw, clean, output_dir) {
  metrics <- tibble(
    title  = c("Records Processed", "Composite Score", "Consistency", "Fields Standardized"),
    value  = c("525 \u2192 500",
               sprintf("%.1f \u2192 %.1f", raw$composite_score, clean$composite_score),
               sprintf("%.1f \u2192 %.1f", raw$consistency, clean$consistency),
               "5 fields"),
    detail = c("25 duplicates removed",
               sprintf("+%.1f points", clean$composite_score - raw$composite_score),
               sprintf("+%.1f points", clean$consistency - raw$consistency),
               "~408 values corrected"),
    x      = c(1, 3, 1, 3),
    y      = c(2, 2, 1, 1)
  )

  p <- ggplot(metrics) +
    # Rounded rectangles via geom_tile
    geom_tile(aes(x = x, y = y), width = 1.8, height = 0.85,
              fill = LIGHT_BLUE, color = BLUE, linewidth = 0.5, alpha = 0.4) +
    geom_text(aes(x = x, y = y + 0.22, label = title),
              fontface = "bold", size = 3.8, color = NAVY) +
    geom_text(aes(x = x, y = y - 0.02, label = value),
              fontface = "bold", size = 5.5, color = GREEN) +
    geom_text(aes(x = x, y = y - 0.25, label = detail),
              size = 3, color = GRAY) +
    # Title
    annotate("text", x = 2, y = 2.8, label = "Remediation Impact Summary",
             fontface = "bold", size = 6, color = NAVY) +
    coord_cartesian(xlim = c(-0.2, 4.2), ylim = c(0.3, 3.1)) +
    theme_void()

  ggsave(file.path(output_dir, "05_remediation_impact_r.png"),
         plot = p, width = 10, height = 6, dpi = 150)
  cat("  Saved: 05_remediation_impact_r.png\n")
}


# ============================================================
# Main
# ============================================================

if (sys.nframe() == 0) {
  project_dir <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
  # project_dir <- "path/to/project-1-crm-data-quality-audit"  # manual override

  output_dir <- file.path(project_dir, "output")

  cat("Generating visualizations (ggplot2)...\n\n")

  reports <- load_reports(output_dir)
  raw_sc  <- reports$raw
  clean_sc <- reports$clean

  chart_scorecard_comparison(raw_sc, clean_sc, output_dir)
  chart_completeness_by_field(output_dir)
  chart_score_distribution(output_dir)
  chart_consistency_breakdown(output_dir)
  chart_remediation_impact(raw_sc, clean_sc, output_dir)

  cat(sprintf("\nAll visualizations saved to: %s/\n", output_dir))
}
