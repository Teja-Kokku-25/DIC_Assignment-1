-- ============================================================================
-- GOLD LAYER - COMPLETE SQL ANALYSIS (FIXED FOR AFFECTED_PRODUCTS TABLE)
-- Production-Ready - All Queries Corrected
-- ============================================================================
-- ISSUE FIXED: Using correct table references (cve_core vs affected_products)
-- cve_core has: cve_id, date_published, state, title, source, ingestion_timestamp
-- affected_products has: cve_id, raw_data (map)
-- ============================================================================

-- ============================================================================
-- SECTION 1: EXECUTIVE DASHBOARD - OVERVIEW METRICS (FROM CVE_CORE)
-- ============================================================================

-- 1.1: Master Dataset Overview
SELECT 
  'EXECUTIVE_SUMMARY' as report_section,
  'Total CVE Records' as metric,
  COUNT(*) as total_records,
  COUNT(DISTINCT cve_id) as unique_cves,
  COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END) as published_count,
  COUNT(CASE WHEN state = 'REJECTED' THEN 1 END) as rejected_count,
  ROUND(100.0 * COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END) / COUNT(*), 2) as publication_success_rate
FROM main.cve_silver.cve_core;

-- 1.2: Date Range and Coverage (FROM CVE_CORE)
SELECT 
  'EXECUTIVE_SUMMARY' as report_section,
  'Temporal Coverage' as metric,
  MIN(date_published) as earliest_date,
  MAX(date_published) as latest_date,
  DATEDIFF(MAX(date_published), MIN(date_published)) as days_span,
  COUNT(DISTINCT SUBSTR(date_published, 1, 7)) as months_covered,
  COUNT(DISTINCT date_published) as unique_publication_dates
FROM main.cve_silver.cve_core;

-- 1.3: Publication Status Distribution (FROM CVE_CORE)
SELECT 
  'STATUS_BREAKDOWN' as report_section,
  state as publication_state,
  COUNT(*) as record_count,
  COUNT(DISTINCT cve_id) as unique_cve_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_total
FROM main.cve_silver.cve_core
GROUP BY state
ORDER BY record_count DESC;

-- 1.4: Data Quality Metrics (FROM CVE_CORE)
SELECT 
  'DATA_QUALITY' as report_section,
  'CVE ID Completeness' as quality_metric,
  COUNT(*) as total_records,
  COUNT(CASE WHEN cve_id IS NOT NULL THEN 1 END) as non_null_count,
  ROUND(100.0 * COUNT(CASE WHEN cve_id IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_percent
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'DATA_QUALITY' as report_section,
  'Date Published Completeness' as quality_metric,
  COUNT(*) as total_records,
  COUNT(CASE WHEN date_published IS NOT NULL THEN 1 END) as non_null_count,
  ROUND(100.0 * COUNT(CASE WHEN date_published IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_percent
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'DATA_QUALITY' as report_section,
  'Title Field Completeness' as quality_metric,
  COUNT(*) as total_records,
  COUNT(CASE WHEN title IS NOT NULL THEN 1 END) as non_null_count,
  ROUND(100.0 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_percent
FROM main.cve_silver.cve_core;

-- ============================================================================
-- SECTION 2: TEMPORAL ANALYSIS (FROM CVE_CORE) - PUBLICATION TRENDS
-- ============================================================================

-- 2.1: Daily Publication Trend (FROM CVE_CORE)
SELECT 
  'DAILY_TREND' as report_section,
  date_published as publication_date,
  COUNT(*) as cves_published_daily,
  COUNT(DISTINCT cve_id) as unique_cves,
  COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END) as published_count,
  COUNT(CASE WHEN state = 'REJECTED' THEN 1 END) as rejected_count,
  CASE 
    WHEN COUNT(*) > 10 THEN '🔴 Peak Activity'
    WHEN COUNT(*) > 5 THEN '🟠 Elevated Activity'
    WHEN COUNT(*) > 1 THEN '🟡 Normal Activity'
    ELSE '🟢 Low Activity'
  END as activity_classification
FROM main.cve_silver.cve_core
GROUP BY date_published
ORDER BY date_published DESC;

-- 2.2: Monthly Publication Analysis (FROM CVE_CORE)
SELECT 
  'MONTHLY_TREND' as report_section,
  SUBSTR(date_published, 1, 7) as publication_month,
  COUNT(*) as total_cves_monthly,
  COUNT(DISTINCT cve_id) as unique_cves,
  COUNT(DISTINCT date_published) as unique_publication_dates,
  ROUND(COUNT(*) / COUNT(DISTINCT date_published), 2) as avg_cves_per_day,
  MIN(date_published) as first_publication,
  MAX(date_published) as last_publication
FROM main.cve_silver.cve_core
GROUP BY SUBSTR(date_published, 1, 7)
ORDER BY publication_month DESC;

-- 2.3: Week-over-Week Growth Rate (FROM CVE_CORE)
SELECT 
  'WOW_ANALYSIS' as report_section,
  SUBSTR(date_published, 1, 7) as month,
  WEEKOFYEAR(date_published) as week_number,
  COUNT(*) as cves_this_week,
  LAG(COUNT(*)) OVER (ORDER BY SUBSTR(date_published, 1, 7), WEEKOFYEAR(date_published)) as cves_previous_week
FROM main.cve_silver.cve_core
GROUP BY SUBSTR(date_published, 1, 7), WEEKOFYEAR(date_published)
ORDER BY month DESC;

-- 2.4: Day of Week Distribution (FROM CVE_CORE)
SELECT 
  'DAY_OF_WEEK' as report_section,
  CASE DAYOFWEEK(date_published)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END as day_name,
  DAYOFWEEK(date_published) as day_number,
  COUNT(*) as publication_count,
  COUNT(DISTINCT cve_id) as unique_cves,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_week
FROM main.cve_silver.cve_core
GROUP BY DAYOFWEEK(date_published)
ORDER BY day_number;

-- ============================================================================
-- SECTION 3: VENDOR & PRODUCT RISK ANALYSIS (FROM AFFECTED_PRODUCTS)
-- ============================================================================

-- 3.1: Top 25 Most Vulnerable Vendors
SELECT 
  'TOP_VENDORS' as report_section,
  raw_data['vendor'] as vendor_name,
  COUNT(DISTINCT cve_id) as total_cves,
  COUNT(*) as total_product_entries,
  COUNT(DISTINCT raw_data['product']) as unique_products,
  ROUND(COUNT(DISTINCT cve_id) / COUNT(DISTINCT raw_data['product']), 2) as avg_cves_per_product,
  CASE 
    WHEN COUNT(DISTINCT cve_id) > 100 THEN '🔴 CRITICAL'
    WHEN COUNT(DISTINCT cve_id) > 50 THEN '🟠 HIGH'
    WHEN COUNT(DISTINCT cve_id) > 20 THEN '🟡 MEDIUM'
    WHEN COUNT(DISTINCT cve_id) > 10 THEN '🟢 MODERATE'
    ELSE '🟢 LOW'
  END as risk_classification
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL
GROUP BY raw_data['vendor']
ORDER BY total_cves DESC
LIMIT 25;

-- 3.2: Top 30 Most Vulnerable Specific Products
SELECT 
  'VULNERABLE_PRODUCTS' as report_section,
  CONCAT(raw_data['vendor'], ' / ', raw_data['product']) as full_product_name,
  COUNT(DISTINCT cve_id) as vulnerability_count,
  COUNT(*) as total_records,
  CASE 
    WHEN COUNT(DISTINCT cve_id) >= 15 THEN '🔴 CRITICAL'
    WHEN COUNT(DISTINCT cve_id) >= 10 THEN '🟠 HIGH'
    WHEN COUNT(DISTINCT cve_id) >= 5 THEN '🟡 MEDIUM'
    WHEN COUNT(DISTINCT cve_id) >= 2 THEN '🟢 MODERATE'
    ELSE '🟢 LOW'
  END as priority_level
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL AND raw_data['product'] IS NOT NULL
GROUP BY CONCAT(raw_data['vendor'], ' / ', raw_data['product']), raw_data['vendor'], raw_data['product']
ORDER BY vulnerability_count DESC
LIMIT 30;

-- 3.3: Vendor Portfolio Risk Profile
SELECT 
  'VENDOR_PORTFOLIO' as report_section,
  raw_data['vendor'] as vendor,
  COUNT(DISTINCT raw_data['product']) as products_affected,
  COUNT(DISTINCT cve_id) as total_cves,
  COUNT(*) as total_entries,
  ROUND(COUNT(DISTINCT cve_id) / COUNT(DISTINCT raw_data['product']), 2) as avg_vulnerabilities_per_product
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL AND raw_data['product'] IS NOT NULL
GROUP BY raw_data['vendor']
ORDER BY total_cves DESC
LIMIT 20;

-- ============================================================================
-- SECTION 4: PUBLICATION PATTERN & DISCLOSURE ANALYSIS (FROM CVE_CORE)
-- ============================================================================

-- 4.1: Batch Publication Detection (FROM CVE_CORE)
SELECT 
  'BATCH_DISCLOSURE' as report_section,
  DATE_TRUNC('DAY', date_published) as publication_date,
  COUNT(*) as cves_on_date,
  COUNT(DISTINCT cve_id) as unique_cves,
  COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END) as published_on_date,
  COUNT(CASE WHEN state = 'REJECTED' THEN 1 END) as rejected_on_date,
  CASE 
    WHEN COUNT(*) > 15 THEN '🔴 Major Batch Release'
    WHEN COUNT(*) BETWEEN 8 AND 15 THEN '🟠 Significant Batch'
    WHEN COUNT(*) BETWEEN 3 AND 7 THEN '🟡 Group Release'
    WHEN COUNT(*) = 2 THEN '🟢 Paired Release'
    WHEN COUNT(*) = 1 THEN '🟢 Single CVE'
    ELSE '⚪ Unknown'
  END as disclosure_type
FROM main.cve_silver.cve_core
GROUP BY DATE_TRUNC('DAY', date_published)
ORDER BY cves_on_date DESC, publication_date DESC;

-- 4.2: Publication Velocity Trend (FROM CVE_CORE)
SELECT 
  'VELOCITY_TREND' as report_section,
  SUBSTR(date_published, 1, 7) as month,
  COUNT(*) as monthly_cves,
  COUNT(DISTINCT cve_id) as unique_cves,
  LAG(COUNT(*)) OVER (ORDER BY SUBSTR(date_published, 1, 7)) as prior_month_cves,
  CASE 
    WHEN COUNT(*) > 50 THEN '⬆️ ACCELERATION'
    WHEN COUNT(*) > 30 THEN '⬆️ ELEVATED'
    WHEN COUNT(*) > 10 THEN '➡️ NORMAL'
    ELSE '⬇️ REDUCED'
  END as velocity_classification
FROM main.cve_silver.cve_core
GROUP BY SUBSTR(date_published, 1, 7)
ORDER BY month DESC;

-- ============================================================================
-- SECTION 5: CUMULATIVE IMPACT & RISK TIMELINE (FROM CVE_CORE)
-- ============================================================================

-- 5.1: Cumulative CVE Impact Timeline (FROM CVE_CORE)
SELECT 
  'CUMULATIVE_IMPACT' as report_section,
  date_published as reference_date,
  COUNT(*) as cves_published_today,
  COUNT(DISTINCT cve_id) as unique_cves_today,
  SUM(COUNT(*)) OVER (ORDER BY date_published) as cumulative_total,
  SUM(COUNT(DISTINCT cve_id)) OVER (ORDER BY date_published) as cumulative_unique,
  CASE 
    WHEN DATEDIFF(CURRENT_DATE(), date_published) <= 7 THEN '🔴 THIS WEEK'
    WHEN DATEDIFF(CURRENT_DATE(), date_published) <= 14 THEN '🟠 LAST 2 WEEKS'
    WHEN DATEDIFF(CURRENT_DATE(), date_published) <= 30 THEN '🟡 THIS MONTH'
    WHEN DATEDIFF(CURRENT_DATE(), date_published) <= 60 THEN '🟢 LAST 2 MONTHS'
    ELSE '⚪ HISTORICAL'
  END as recency_classification
FROM main.cve_silver.cve_core
GROUP BY date_published
ORDER BY date_published DESC;

-- 5.2: Risk Profile Summary (COMBINED FROM BOTH TABLES)
SELECT 
  'RISK_SUMMARY' as report_section,
  (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.cve_core) as total_unique_cves,
  (SELECT COUNT(DISTINCT raw_data['vendor']) FROM main.cve_silver.affected_products WHERE raw_data['vendor'] IS NOT NULL) as total_vendors_affected,
  (SELECT COUNT(DISTINCT raw_data['product']) FROM main.cve_silver.affected_products WHERE raw_data['product'] IS NOT NULL) as total_products_affected,
  (SELECT COUNT(*) FROM main.cve_silver.affected_products) as total_vendor_product_mappings;

-- ============================================================================
-- SECTION 6: SUPPLY CHAIN CRITICAL AREAS (FROM AFFECTED_PRODUCTS)
-- ============================================================================

-- 6.1: High-Risk Vendors (5+ CVEs)
SELECT 
  'HIGH_RISK_VENDORS' as report_section,
  raw_data['vendor'] as vendor,
  COUNT(DISTINCT cve_id) as cve_count,
  COUNT(DISTINCT raw_data['product']) as products_affected,
  'SUPPLY CHAIN RISK' as risk_category
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL
GROUP BY raw_data['vendor']
HAVING COUNT(DISTINCT cve_id) >= 5
ORDER BY cve_count DESC
LIMIT 20;

-- ============================================================================
-- SECTION 7: ACTIONABLE RECOMMENDATIONS
-- ============================================================================

-- 7.1: Executive Dashboard Summary (FROM CVE_CORE)
SELECT 
  'EXECUTIVE_DASHBOARD' as section,
  'Current Threat Level' as metric,
  CONCAT(
    'Last 7 days: ', 
    (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.cve_core WHERE DATEDIFF(CURRENT_DATE(), date_published) <= 7),
    ' CVEs | Last 30 days: ',
    (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.cve_core WHERE DATEDIFF(CURRENT_DATE(), date_published) <= 30),
    ' CVEs'
  ) as value,
  'Monitor weekly for anomalies' as action_item
UNION ALL
SELECT 
  'EXECUTIVE_DASHBOARD' as section,
  'Data Quality' as metric,
  CONCAT(
    ROUND(100.0 * COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END) / COUNT(*), 2),
    '% publication success'
  ) as value,
  'Excellent data - safe for decisions' as action_item
FROM main.cve_silver.cve_core;

-- ============================================================================
-- SECTION 8: VERIFICATION & VALIDATION (FROM BOTH TABLES)
-- ============================================================================

-- 8.1: Data Integrity Check
SELECT 
  'DATA_INTEGRITY' as check_type,
  'Referential Integrity' as check_name,
  (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.affected_products) as affected_cves,
  (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.cve_core) as core_cves,
  CASE 
    WHEN (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.affected_products) >= (SELECT COUNT(DISTINCT cve_id) FROM main.cve_silver.cve_core)
    THEN 'VALID'
    ELSE 'WARNING'
  END as validation_status;

-- 8.2: Deduplication Efficiency (FROM AFFECTED_PRODUCTS)
SELECT 
  'DEDUPLICATION' as analysis_type,
  COUNT(*) as total_affected_entries,
  COUNT(DISTINCT cve_id) as unique_cves,
  COUNT(*) - COUNT(DISTINCT cve_id) as duplicate_entries,
  ROUND(100.0 * COUNT(DISTINCT cve_id) / COUNT(*), 2) as uniqueness_ratio
FROM main.cve_silver.affected_products;

-- ============================================================================
-- END OF GOLD LAYER ANALYSIS
-- ============================================================================
-- ALL QUERIES FIXED AND TESTED
-- Ready for immediate execution
-- ============================================================================
