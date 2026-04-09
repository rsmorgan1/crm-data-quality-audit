# ============================================================
# CRM Data Remediation Engine (R)
# ============================================================
# Applies standardization and cleaning rules to the raw CRM dataset.
# Documents every transformation for audit trail transparency.
#
# Remediation Steps:
#   1. Whitespace normalization
#   2. Categorical field standardization (case-insensitive mapping)
#   3. Email validation and flagging
#   4. Duplicate identification and removal
#   5. Date validation and flagging
#   6. Financial outlier flagging
# ============================================================

library(tidyverse)
library(jsonlite)

# Source the profiler for re-scoring after remediation
# source("scripts_r/01_data_profiler.R")  # uncomment if running standalone


# ============================================================
# Standardization Mappings
# ============================================================

industry_map <- tribble(
  ~dirty,                           ~clean,
  "healthcare",                     "Healthcare",
  "health care",                    "Healthcare",
  "hc",                             "Healthcare",
  "education",                      "Education",
  "ed",                             "Education",
  "edu",                            "Education",
  "technology-enabled services",    "Technology-Enabled Services",
  "tes",                            "Technology-Enabled Services",
  "tech-enabled services",          "Technology-Enabled Services",
  "technology enabled services",    "Technology-Enabled Services",
  "tech enabled services",          "Technology-Enabled Services"
)

deal_type_map <- tribble(
  ~dirty,       ~clean,
  "platform",   "Platform",
  "plat",       "Platform",
  "add-on",     "Add-On",
  "addon",      "Add-On",
  "add on",     "Add-On"
)

stage_map <- tribble(
  ~dirty,              ~clean,
  "reviewed",          "Reviewed",
  "rev",               "Reviewed",
  "indication",        "Indication",
  "ioi",               "Indication",
  "indicated",         "Indication",
  "offer",             "Offer",
  "loi",               "Offer",
  "letter of intent",  "Offer",
  "closed - won",      "Closed - Won",
  "closed won",        "Closed - Won",
  "won",               "Closed - Won",
  "acquired",          "Closed - Won",
  "closed - lost",     "Closed - Lost",
  "closed lost",       "Closed - Lost",
  "lost",              "Closed - Lost",
  "passed",            "Passed",
  "pass",              "Passed",
  "declined",          "Passed"
)

source_map <- tribble(
  ~dirty,              ~clean,
  "intermediary",      "Intermediary",
  "broker",            "Intermediary",
  "investment banker", "Intermediary",
  "proprietary",       "Proprietary",
  "internal",          "Proprietary",
  "prop",              "Proprietary",
  "conference",        "Conference",
  "event",             "Conference",
  "trade show",        "Conference",
  "direct outreach",   "Direct Outreach",
  "cold outreach",     "Direct Outreach",
  "direct",            "Direct Outreach",
  "referral",          "Referral",
  "referred",          "Referral",
  "network",           "Referral"
)

focus_area_map <- tribble(
  ~dirty,                        ~clean,
  "provider services",           "Provider Services",
  "provider svcs",               "Provider Services",
  "prov services",               "Provider Services",
  "provider srvcs",              "Provider Services",
  "pharma services",             "Pharma Services",
  "pharmaceutical services",     "Pharma Services",
  "pharma svcs",                 "Pharma Services",
  "chronic care solutions",      "Chronic Care Solutions",
  "chronic care",                "Chronic Care Solutions",
  "ccs",                         "Chronic Care Solutions",
  "property services",           "Property Services",
  "prop services",               "Property Services",
  "property svcs",               "Property Services",
  "behavioral health",           "Behavioral Health",
  "bh",                          "Behavioral Health",
  "behav health",                "Behavioral Health"
)


# ============================================================
# Standardization Helper
# ============================================================

standardize_field <- function(values, mapping) {
  # Trim and lowercase for matching
  normalized <- str_to_lower(str_trim(replace_na(values, "")))

  # Build lookup vector from mapping tibble
  lookup <- setNames(mapping$clean, mapping$dirty)

  # Apply mapping; if no match, use title case of original
  result <- map_chr(normalized, function(v) {
    if (v == "") return(NA_character_)
    if (v %in% names(lookup)) return(lookup[[v]])
    return(str_to_title(v))
  })

  result
}


# ============================================================
# Main Remediation Function
# ============================================================

remediate <- function(df) {
  changes <- list()
  df_clean <- df

  # 1. Whitespace normalization on all character columns
  char_cols <- names(df_clean)[sapply(df_clean, is.character)]
  for (col in char_cols) {
    original <- df_clean[[col]]
    df_clean[[col]] <- str_trim(df_clean[[col]])
    df_clean[[col]] <- if_else(df_clean[[col]] == "", NA_character_, df_clean[[col]])
    n_changed <- sum(replace_na(original, "") != replace_na(df_clean[[col]], ""))
    if (n_changed > 0) {
      changes <- append(changes, list(list(
        step = "whitespace_trim", field = col, records_changed = n_changed
      )))
    }
  }

  # 2. Industry standardization
  original <- df_clean$industry
  df_clean$industry <- standardize_field(df_clean$industry, industry_map)
  n_changed <- sum(replace_na(original, "") != replace_na(df_clean$industry, ""))
  changes <- append(changes, list(list(
    step = "industry_standardization", field = "industry", records_changed = n_changed
  )))

  # 3. Focus area standardization
  original <- df_clean$focus_area
  df_clean$focus_area <- standardize_field(df_clean$focus_area, focus_area_map)
  n_changed <- sum(replace_na(original, "") != replace_na(df_clean$focus_area, ""))
  changes <- append(changes, list(list(
    step = "focus_area_standardization", field = "focus_area", records_changed = n_changed
  )))

  # 4. Deal type standardization
  original <- df_clean$deal_type
  df_clean$deal_type <- standardize_field(df_clean$deal_type, deal_type_map)
  n_changed <- sum(replace_na(original, "") != replace_na(df_clean$deal_type, ""))
  changes <- append(changes, list(list(
    step = "deal_type_standardization", field = "deal_type", records_changed = n_changed
  )))

  # 5. Deal stage standardization
  original <- df_clean$deal_stage
  df_clean$deal_stage <- standardize_field(df_clean$deal_stage, stage_map)
  n_changed <- sum(replace_na(original, "") != replace_na(df_clean$deal_stage, ""))
  changes <- append(changes, list(list(
    step = "deal_stage_standardization", field = "deal_stage", records_changed = n_changed
  )))

  # 6. Source standardization
  original <- df_clean$source
  df_clean$source <- standardize_field(df_clean$source, source_map)
  n_changed <- sum(replace_na(original, "") != replace_na(df_clean$source, ""))
  changes <- append(changes, list(list(
    step = "source_standardization", field = "source", records_changed = n_changed
  )))

  # 7. Email validation (flag, don't delete)
  df_clean <- df_clean %>%
    mutate(email_valid = str_detect(
      replace_na(contact_email, ""),
      "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    ))
  invalid_emails <- sum(!df_clean$email_valid & !is.na(df_clean$contact_email) & df_clean$contact_email != "")
  changes <- append(changes, list(list(
    step = "email_validation_flag", field = "contact_email", records_flagged = invalid_emails
  )))

  # 8. Duplicate identification and removal
  df_clean <- df_clean %>%
    mutate(.company_norm = str_to_lower(str_trim(replace_na(company_name, ""))))

  df_clean <- df_clean %>%
    mutate(is_duplicate = duplicated(
      tibble(.company_norm, date_sourced),
      fromLast = FALSE
    ))

  n_dupes <- sum(df_clean$is_duplicate)
  changes <- append(changes, list(list(
    step = "duplicate_removal", field = "company_name+date_sourced", records_removed = n_dupes
  )))

  df_deduped <- df_clean %>%
    filter(!is_duplicate) %>%
    select(-email_valid, -.company_norm, -is_duplicate)

  # 9. Date validation and flagging
  df_deduped <- df_deduped %>%
    mutate(
      date_sourced_dt = as.Date(date_sourced),
      date_reviewed_dt = as.Date(date_reviewed),
      stage_date_dt = as.Date(stage_date)
    ) %>%
    mutate(date_flag = case_when(
      date_sourced_dt > Sys.Date() | date_reviewed_dt > Sys.Date() | stage_date_dt > Sys.Date()
        ~ "FUTURE_DATE",
      !is.na(date_reviewed_dt) & !is.na(date_sourced_dt) & date_reviewed_dt < date_sourced_dt
        ~ "INVALID_SEQUENCE",
      TRUE ~ ""
    )) %>%
    select(-date_sourced_dt, -date_reviewed_dt, -stage_date_dt)

  n_date_issues <- sum(df_deduped$date_flag != "")
  changes <- append(changes, list(list(
    step = "date_validation_flag", field = "dates", records_flagged = n_date_issues
  )))

  # 10. Financial outlier flagging
  df_deduped <- df_deduped %>%
    mutate(financial_flag = case_when(
      !is.na(revenue_usd) & (revenue_usd > 2e8 | revenue_usd < 1e6) & !is.na(ebitda_usd) & ebitda_usd < 0
        ~ "REVENUE_OUTLIER|NEGATIVE_EBITDA",
      !is.na(revenue_usd) & (revenue_usd > 2e8 | revenue_usd < 1e6)
        ~ "REVENUE_OUTLIER",
      !is.na(ebitda_usd) & ebitda_usd < 0
        ~ "NEGATIVE_EBITDA",
      TRUE ~ ""
    ))

  n_fin_issues <- sum(df_deduped$financial_flag != "")
  changes <- append(changes, list(list(
    step = "financial_outlier_flag", field = "financials", records_flagged = n_fin_issues
  )))

  list(df_clean = df_deduped, changes = changes)
}


# ============================================================
# Run
# ============================================================

if (sys.nframe() == 0) {
  project_dir <- dirname(dirname(rstudioapi::getSourceEditorContext()$path))
  # project_dir <- "path/to/project-1-crm-data-quality-audit"  # manual override

  source(file.path(project_dir, "scripts_r", "01_data_profiler.R"))

  data_path <- file.path(project_dir, "data", "crm_deals_raw.csv")
  output_dir <- file.path(project_dir, "output")

  cat("Loading raw CRM data...\n")
  df <- read_csv(data_path, show_col_types = FALSE)
  cat(sprintf("Loaded %d raw records\n\n", nrow(df)))

  cat("Applying remediation...\n")
  result <- remediate(df)
  df_clean <- result$df_clean
  changes <- result$changes

  # Save cleaned data
  write_csv(df_clean, file.path(project_dir, "data", "crm_deals_cleaned_r.csv"))

  # Save change log
  write_json(changes, file.path(output_dir, "remediation_log_r.json"), pretty = TRUE)

  cat(sprintf("\nCleaned dataset: %d records (removed %d duplicates)\n",
              nrow(df_clean), nrow(df) - nrow(df_clean)))

  cat("\nRemediation steps:\n")
  for (c in changes) {
    action <- c$records_changed %||% c$records_flagged %||% c$records_removed %||% 0
    label <- if (!is.null(c$records_changed)) "changed"
             else if (!is.null(c$records_removed)) "removed"
             else "flagged"
    cat(sprintf("  %s: %d records %s\n", c$step, action, label))
  }

  # Re-profile cleaned data
  cat("\nRe-profiling cleaned data...\n")
  report <- generate_profile_report(df_clean)

  write_json(report$scorecard, file.path(output_dir, "profile_report_cleaned_r.json"), pretty = TRUE)

  df_scored <- bind_cols(df_clean, report$record_scores)
  write_csv(df_scored, file.path(output_dir, "deals_cleaned_with_scores_r.csv"))

  sc <- report$scorecard
  cat("\n", strrep("=", 60), "\n")
  cat("  CRM DATA QUALITY SCORECARD (Post-Remediation)\n")
  cat(strrep("=", 60), "\n\n")
  cat(sprintf("  COMPOSITE SCORE:  %s/100\n\n", sc$composite_score))
  cat(sprintf("  Completeness:     %s/100\n", sc$completeness))
  cat(sprintf("  Consistency:      %s/100\n", sc$consistency))
  cat(sprintf("  Accuracy:         %s/100\n", sc$accuracy))
  cat(sprintf("  Uniqueness:       %s/100\n", sc$uniqueness))
  cat(sprintf("  Records:          %d\n", nrow(df_clean)))
  cat(strrep("=", 60), "\n")
}
