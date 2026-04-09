# ============================================================
# CRM Data Quality Profiler (R)
# ============================================================
# Profiles the raw CRM dataset across 4 quality dimensions:
#   1. Completeness - % of non-null values in critical fields
#   2. Consistency  - % of values matching standard references
#   3. Accuracy     - % passing logical validation rules
#   4. Uniqueness   - % of non-duplicate records
#
# Produces a per-record quality score (0-100) and aggregate scorecard.
# ============================================================

library(tidyverse)
library(jsonlite)

# ============================================================
# Reference Standards
# ============================================================

VALID_INDUSTRIES <- c("Healthcare", "Education", "Technology-Enabled Services")

VALID_FOCUS_AREAS <- c(
  "Provider Services", "Pharma Services", "Chronic Care Solutions",
  "Behavioral Health", "Healthy Living & Wellness", "Animal Health",
  "Home Healthcare", "Physician Practice Enablement",
  "Higher Education Outsourcing", "Skills Gap Training",
  "Corporate Training", "Tutoring & Test Prep",
  "Compliance Services", "Logistics Management",
  "Specialty Healthcare Logistics", "Residential Services",
  "Property Services"
)

VALID_DEAL_TYPES <- c("Platform", "Add-On")

VALID_STAGES <- c(
  "Reviewed", "Indication", "Offer",
  "Closed - Won", "Closed - Lost", "Passed"
)

VALID_SOURCES <- c(
  "Intermediary", "Proprietary", "Conference",
  "Direct Outreach", "Referral"
)

CRITICAL_FIELDS <- c(
  "deal_id", "company_name", "industry", "deal_type",
  "deal_stage", "deal_owner", "date_sourced"
)

IMPORTANT_FIELDS <- c(
  "focus_area", "source", "revenue_usd", "ebitda_usd",
  "deal_size_usd", "primary_contact", "contact_email"
)


# ============================================================
# Profiling Functions
# ============================================================

profile_completeness <- function(df) {
  all_fields <- intersect(c(CRITICAL_FIELDS, IMPORTANT_FIELDS), names(df))

  field_stats <- map_dfr(all_fields, function(field) {
    vals <- df[[field]]
    if (is.character(vals)) {
      populated <- sum(!is.na(vals) & str_trim(vals) != "", na.rm = TRUE)
    } else {
      populated <- sum(!is.na(vals))
    }
    tibble(
      field = field,
      total = nrow(df),
      populated = populated,
      missing = nrow(df) - populated,
      completeness_pct = round(populated / nrow(df) * 100, 1)
    )
  })

  overall <- mean(field_stats$completeness_pct)
  list(details = field_stats, score = round(overall, 1))
}


profile_consistency <- function(df) {
  checks <- list(
    industry = list(valid = VALID_INDUSTRIES, col = "industry"),
    focus_area = list(valid = VALID_FOCUS_AREAS, col = "focus_area"),
    deal_type = list(valid = VALID_DEAL_TYPES, col = "deal_type"),
    deal_stage = list(valid = VALID_STAGES, col = "deal_stage"),
    source = list(valid = VALID_SOURCES, col = "source")
  )

  field_stats <- map_dfr(names(checks), function(name) {
    check <- checks[[name]]
    col <- check$col
    if (!col %in% names(df)) return(NULL)

    vals <- df[[col]]
    non_null <- !is.na(vals)
    trimmed <- str_trim(vals)
    valid <- trimmed %in% check$valid & non_null
    total_non_null <- sum(non_null)

    invalid_examples <- unique(trimmed[non_null & !valid])
    invalid_examples <- head(invalid_examples, 5)

    tibble(
      field = col,
      valid_count = sum(valid),
      invalid_count = total_non_null - sum(valid),
      unique_values = n_distinct(vals, na.rm = TRUE),
      consistency_pct = round(sum(valid) / max(total_non_null, 1) * 100, 1),
      invalid_examples = list(invalid_examples)
    )
  })

  overall <- mean(field_stats$consistency_pct)
  list(details = field_stats, score = round(overall, 1))
}


profile_accuracy <- function(df) {
  results <- tibble()

  # Email format validation
  if ("contact_email" %in% names(df)) {
    non_null <- !is.na(df$contact_email) & df$contact_email != ""
    valid_email <- str_detect(
      replace_na(df$contact_email, ""),
      "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    )
    valid_count <- sum(valid_email & non_null)
    total_non_null <- sum(non_null)
    results <- bind_rows(results, tibble(
      check = "email_format",
      valid = valid_count,
      invalid = total_non_null - valid_count,
      accuracy_pct = round(valid_count / max(total_non_null, 1) * 100, 1)
    ))
  }

  # Date sequence: sourced <= reviewed
  if (all(c("date_sourced", "date_reviewed") %in% names(df))) {
    ds <- as.Date(df$date_sourced, format = "%Y-%m-%d")
    dr <- as.Date(df$date_reviewed, format = "%Y-%m-%d")
    has_both <- !is.na(ds) & !is.na(dr)
    valid_seq <- (dr >= ds) | !has_both
    results <- bind_rows(results, tibble(
      check = "date_sequence",
      valid = sum(valid_seq),
      invalid = sum(!valid_seq),
      accuracy_pct = round(sum(valid_seq) / nrow(df) * 100, 1)
    ))
  }

  # No future dates
  if (any(c("date_sourced", "date_reviewed", "stage_date") %in% names(df))) {
    today <- Sys.Date()
    future <- rep(FALSE, nrow(df))
    for (col in c("date_sourced", "date_reviewed", "stage_date")) {
      if (col %in% names(df)) {
        d <- as.Date(df[[col]], format = "%Y-%m-%d")
        future <- future | (!is.na(d) & d > today)
      }
    }
    results <- bind_rows(results, tibble(
      check = "no_future_dates",
      valid = sum(!future),
      invalid = sum(future),
      accuracy_pct = round(sum(!future) / nrow(df) * 100, 1)
    ))
  }

  # Revenue range ($1M - $200M)
  if ("revenue_usd" %in% names(df)) {
    has_rev <- !is.na(df$revenue_usd)
    in_range <- df$revenue_usd >= 1e6 & df$revenue_usd <= 2e8
    valid_range <- in_range | !has_rev
    results <- bind_rows(results, tibble(
      check = "revenue_range",
      valid = sum(valid_range),
      invalid = sum(!valid_range),
      accuracy_pct = round(sum(valid_range) / nrow(df) * 100, 1)
    ))
  }

  overall <- mean(results$accuracy_pct)
  list(details = results, score = round(overall, 1))
}


profile_uniqueness <- function(df) {
  df_check <- df %>%
    mutate(.company_norm = str_to_lower(str_trim(replace_na(company_name, ""))))

  dupes <- df_check %>%
    group_by(.company_norm, date_sourced) %>%
    filter(n() > 1) %>%
    ungroup()

  uniqueness_pct <- round((1 - nrow(dupes) / max(nrow(df), 1)) * 100, 1)

  list(
    total_records = nrow(df),
    duplicate_records = nrow(dupes),
    unique_records = nrow(df) - nrow(dupes),
    uniqueness_pct = uniqueness_pct,
    score = uniqueness_pct
  )
}


# ============================================================
# Record-Level Scoring
# ============================================================

compute_record_scores <- function(df) {
  all_fields <- intersect(c(CRITICAL_FIELDS, IMPORTANT_FIELDS), names(df))

  # Completeness per record (40% weight)
  completeness <- rowMeans(!is.na(df[, all_fields, drop = FALSE])) * 100

  # Consistency per record (30% weight)
  consistency_checks <- list()
  if ("industry" %in% names(df))
    consistency_checks$industry <- str_trim(replace_na(df$industry, "")) %in% VALID_INDUSTRIES
  if ("deal_type" %in% names(df))
    consistency_checks$deal_type <- str_trim(replace_na(df$deal_type, "")) %in% VALID_DEAL_TYPES
  if ("deal_stage" %in% names(df))
    consistency_checks$deal_stage <- str_trim(replace_na(df$deal_stage, "")) %in% VALID_STAGES
  if ("source" %in% names(df))
    consistency_checks$source <- str_trim(replace_na(df$source, "")) %in% VALID_SOURCES

  consistency <- rowMeans(as.data.frame(consistency_checks)) * 100

  # Accuracy per record (30% weight)
  accuracy_checks <- list()
  if ("contact_email" %in% names(df)) {
    valid_email <- str_detect(
      replace_na(df$contact_email, ""),
      "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    ) | is.na(df$contact_email) | df$contact_email == ""
    accuracy_checks$email <- valid_email
  }
  if (all(c("date_sourced", "date_reviewed") %in% names(df))) {
    ds <- as.Date(df$date_sourced, format = "%Y-%m-%d")
    dr <- as.Date(df$date_reviewed, format = "%Y-%m-%d")
    valid_date <- (dr >= ds) | is.na(ds) | is.na(dr)
    accuracy_checks$date_seq <- valid_date
  }

  accuracy <- rowMeans(as.data.frame(accuracy_checks)) * 100

  # Composite
  quality_score <- round(completeness * 0.40 + consistency * 0.30 + accuracy * 0.30, 1)

  tibble(
    completeness = round(completeness, 1),
    consistency = round(consistency, 1),
    accuracy = round(accuracy, 1),
    quality_score = quality_score
  )
}


# ============================================================
# Main Report Generator
# ============================================================

generate_profile_report <- function(df) {
  comp <- profile_completeness(df)
  cons <- profile_consistency(df)
  acc <- profile_accuracy(df)
  uniq <- profile_uniqueness(df)

  composite <- round(
    comp$score * 0.30 +
    cons$score * 0.30 +
    acc$score * 0.25 +
    uniq$score * 0.15,
    1
  )

  scorecard <- list(
    composite_score = composite,
    completeness = comp$score,
    consistency = cons$score,
    accuracy = acc$score,
    uniqueness = uniq$score
  )

  record_scores <- compute_record_scores(df)

  list(
    scorecard = scorecard,
    completeness = comp,
    consistency = cons,
    accuracy = acc,
    uniqueness = uniq,
    record_scores = record_scores
  )
}


# ============================================================
# Run
# ============================================================

if (sys.nframe() == 0) {
  # Set paths relative to this script
  project_dir <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
  # If not running in RStudio, set manually:
  # project_dir <- "path/to/project-1-crm-data-quality-audit"

  data_path <- file.path(project_dir, "data", "crm_deals_raw.csv")
  output_dir <- file.path(project_dir, "output")
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  cat("Loading raw CRM data...\n")
  df <- read_csv(data_path, show_col_types = FALSE)

  cat("Profiling data quality...\n")
  report <- generate_profile_report(df)

  # Save scorecard
  write_json(report$scorecard, file.path(output_dir, "profile_report_raw_r.json"), pretty = TRUE)

  # Save scored records
  df_scored <- bind_cols(df, report$record_scores)
  write_csv(df_scored, file.path(output_dir, "deals_with_scores_r.csv"))

  # Print scorecard
  sc <- report$scorecard
  cat("\n", strrep("=", 60), "\n")
  cat("  CRM DATA QUALITY SCORECARD (Pre-Remediation)\n")
  cat(strrep("=", 60), "\n\n")
  cat(sprintf("  COMPOSITE SCORE:  %s/100\n\n", sc$composite_score))
  cat(sprintf("  Completeness:     %s/100  (30%% weight)\n", sc$completeness))
  cat(sprintf("  Consistency:      %s/100  (30%% weight)\n", sc$consistency))
  cat(sprintf("  Accuracy:         %s/100  (25%% weight)\n", sc$accuracy))
  cat(sprintf("  Uniqueness:       %s/100  (15%% weight)\n", sc$uniqueness))
  cat(sprintf("\n  Records:          %d\n", nrow(df)))
  cat(sprintf("  Duplicates:       %d\n", report$uniqueness$duplicate_records))

  # Score distribution
  qs <- report$record_scores$quality_score
  cat(sprintf("\n  Record Score Distribution:\n"))
  cat(sprintf("    Mean:   %.1f\n", mean(qs, na.rm = TRUE)))
  cat(sprintf("    Median: %.1f\n", median(qs, na.rm = TRUE)))
  cat(sprintf("    Min:    %.1f\n", min(qs, na.rm = TRUE)))
  cat(sprintf("    Max:    %.1f\n", max(qs, na.rm = TRUE)))
  cat(strrep("=", 60), "\n")
}
