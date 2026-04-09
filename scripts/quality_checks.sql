-- ============================================================
-- CRM Data Quality Checks (SQL)
-- ============================================================
-- These queries can be run against the CRM dataset loaded into
-- any SQL database (SQLite, PostgreSQL, MySQL, etc.)
-- Each query identifies a specific data quality issue.
-- ============================================================


-- ============================================================
-- 1. COMPLETENESS CHECKS
-- ============================================================

-- 1a. Missing values in critical fields
SELECT
    'deal_id' AS field,
    COUNT(*) AS total_records,
    SUM(CASE WHEN deal_id IS NULL OR TRIM(deal_id) = '' THEN 1 ELSE 0 END) AS missing,
    ROUND(100.0 * SUM(CASE WHEN deal_id IS NOT NULL AND TRIM(deal_id) != '' THEN 1 ELSE 0 END) / COUNT(*), 1) AS completeness_pct
FROM crm_deals
UNION ALL
SELECT 'company_name', COUNT(*),
    SUM(CASE WHEN company_name IS NULL OR TRIM(company_name) = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN company_name IS NOT NULL AND TRIM(company_name) != '' THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals
UNION ALL
SELECT 'industry', COUNT(*),
    SUM(CASE WHEN industry IS NULL OR TRIM(industry) = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN industry IS NOT NULL AND TRIM(industry) != '' THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals
UNION ALL
SELECT 'focus_area', COUNT(*),
    SUM(CASE WHEN focus_area IS NULL OR TRIM(focus_area) = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN focus_area IS NOT NULL AND TRIM(focus_area) != '' THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals
UNION ALL
SELECT 'revenue_usd', COUNT(*),
    SUM(CASE WHEN revenue_usd IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN revenue_usd IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals
UNION ALL
SELECT 'ebitda_usd', COUNT(*),
    SUM(CASE WHEN ebitda_usd IS NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN ebitda_usd IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals
UNION ALL
SELECT 'contact_email', COUNT(*),
    SUM(CASE WHEN contact_email IS NULL OR TRIM(contact_email) = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN contact_email IS NOT NULL AND TRIM(contact_email) != '' THEN 1 ELSE 0 END) / COUNT(*), 1)
FROM crm_deals;


-- ============================================================
-- 2. CONSISTENCY CHECKS
-- ============================================================

-- 2a. Non-standard industry values
SELECT industry, COUNT(*) AS record_count
FROM crm_deals
WHERE industry NOT IN ('Healthcare', 'Education', 'Technology-Enabled Services')
  AND industry IS NOT NULL
GROUP BY industry
ORDER BY record_count DESC;

-- 2b. Non-standard deal stage values
SELECT deal_stage, COUNT(*) AS record_count
FROM crm_deals
WHERE deal_stage NOT IN ('Reviewed', 'Indication', 'Offer', 'Closed - Won', 'Closed - Lost', 'Passed')
  AND deal_stage IS NOT NULL
GROUP BY deal_stage
ORDER BY record_count DESC;

-- 2c. Non-standard deal type values
SELECT deal_type, COUNT(*) AS record_count
FROM crm_deals
WHERE deal_type NOT IN ('Platform', 'Add-On')
  AND deal_type IS NOT NULL
GROUP BY deal_type
ORDER BY record_count DESC;

-- 2d. Non-standard source values
SELECT source, COUNT(*) AS record_count
FROM crm_deals
WHERE source NOT IN ('Intermediary', 'Proprietary', 'Conference', 'Direct Outreach', 'Referral')
  AND source IS NOT NULL
GROUP BY source
ORDER BY record_count DESC;


-- ============================================================
-- 3. ACCURACY CHECKS
-- ============================================================

-- 3a. Future dates (should not exist)
SELECT deal_id, company_name, date_sourced, date_reviewed, stage_date
FROM crm_deals
WHERE date_sourced > CURRENT_DATE
   OR date_reviewed > CURRENT_DATE
   OR stage_date > CURRENT_DATE;

-- 3b. Date sequence violations (reviewed before sourced)
SELECT deal_id, company_name, date_sourced, date_reviewed
FROM crm_deals
WHERE date_reviewed < date_sourced
  AND date_reviewed IS NOT NULL
  AND date_sourced IS NOT NULL;

-- 3c. Financial outliers (revenue outside expected range)
SELECT deal_id, company_name, revenue_usd, ebitda_usd
FROM crm_deals
WHERE revenue_usd IS NOT NULL
  AND (revenue_usd > 200000000 OR revenue_usd < 1000000);

-- 3d. Negative EBITDA (flag for review, may be legitimate)
SELECT deal_id, company_name, revenue_usd, ebitda_usd,
    ROUND(ebitda_usd * 1.0 / NULLIF(revenue_usd, 0) * 100, 1) AS ebitda_margin_pct
FROM crm_deals
WHERE ebitda_usd < 0;

-- 3e. Invalid email formats
SELECT deal_id, company_name, contact_email
FROM crm_deals
WHERE contact_email IS NOT NULL
  AND contact_email != ''
  AND contact_email NOT LIKE '%_@_%.__%';


-- ============================================================
-- 4. UNIQUENESS CHECKS
-- ============================================================

-- 4a. Duplicate detection (same company + sourced date)
SELECT
    LOWER(TRIM(company_name)) AS normalized_company,
    date_sourced,
    COUNT(*) AS occurrence_count,
    GROUP_CONCAT(deal_id) AS deal_ids
FROM crm_deals
GROUP BY LOWER(TRIM(company_name)), date_sourced
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;


-- ============================================================
-- 5. AGGREGATE SCORECARD QUERY
-- ============================================================

-- Produces a single-row summary of data quality metrics
SELECT
    COUNT(*) AS total_records,

    -- Completeness (% of critical fields populated)
    ROUND(100.0 * (
        SUM(CASE WHEN company_name IS NOT NULL AND TRIM(company_name) != '' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN industry IS NOT NULL AND TRIM(industry) != '' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN deal_type IS NOT NULL AND TRIM(deal_type) != '' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN deal_stage IS NOT NULL AND TRIM(deal_stage) != '' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN deal_owner IS NOT NULL AND TRIM(deal_owner) != '' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN date_sourced IS NOT NULL THEN 1 ELSE 0 END)
    ) / (COUNT(*) * 6.0), 1) AS critical_field_completeness_pct,

    -- Consistency (% matching valid reference values)
    ROUND(100.0 * SUM(CASE WHEN industry IN ('Healthcare', 'Education', 'Technology-Enabled Services') THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS industry_consistency_pct,

    -- Uniqueness
    ROUND(100.0 * (COUNT(*) - (
        SELECT SUM(cnt - 1) FROM (
            SELECT COUNT(*) AS cnt
            FROM crm_deals
            GROUP BY LOWER(TRIM(company_name)), date_sourced
            HAVING COUNT(*) > 1
        )
    )) / COUNT(*), 1) AS uniqueness_pct

FROM crm_deals;
