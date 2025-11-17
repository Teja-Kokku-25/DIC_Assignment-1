-- ============================================================================
-- GOLD LAYER - COMPREHENSIVE EXPORT QUERY
-- Run this to get ALL analysis results in one export
-- ============================================================================

-- METHOD 1: Export each analysis separately and combine results

-- ==== EXPORT 1: Executive Summary ====
SELECT 
  'EXECUTIVE_SUMMARY' as analysis_category,
  'Overview' as analysis_name,
  COUNT(*) as metric_value,
  'Total Records' as metric_description
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'EXECUTIVE_SUMMARY',
  'Unique CVEs',
  COUNT(DISTINCT cve_id),
  'After Deduplication'
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'EXECUTIVE_SUMMARY',
  'Published',
  COUNT(CASE WHEN state = 'PUBLISHED' THEN 1 END),
  'Successfully Published'
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'EXECUTIVE_SUMMARY',
  'Rejected',
  COUNT(CASE WHEN state = 'REJECTED' THEN 1 END),
  'Rejected CVEs'
FROM main.cve_silver.cve_core;

-- ==== EXPORT 2: Daily Publication Trends ====
SELECT 
  'DAILY_TREND' as analysis_category,
  date_published as date_value,
  COUNT(*) as cve_count,
  CASE 
    WHEN COUNT(*) > 10 THEN 'Peak'
    WHEN COUNT(*) > 5 THEN 'Elevated'
    ELSE 'Normal'
  END as activity_classification
FROM main.cve_silver.cve_core
GROUP BY date_published
ORDER BY date_published DESC;

-- ==== EXPORT 3: Monthly Trends ====
SELECT 
  'MONTHLY_TREND' as analysis_category,
  SUBSTR(date_published, 1, 7) as month,
  COUNT(*) as total_cves,
  COUNT(DISTINCT cve_id) as unique_cves
FROM main.cve_silver.cve_core
GROUP BY SUBSTR(date_published, 1, 7)
ORDER BY month DESC;

-- ==== EXPORT 4: Top 25 Vendors ====
SELECT 
  'TOP_VENDORS' as analysis_category,
  raw_data['vendor'] as vendor_name,
  COUNT(DISTINCT cve_id) as total_cves,
  COUNT(DISTINCT raw_data['product']) as products_affected,
  CASE 
    WHEN COUNT(DISTINCT cve_id) > 50 THEN 'HIGH'
    WHEN COUNT(DISTINCT cve_id) > 20 THEN 'MEDIUM'
    ELSE 'LOW'
  END as risk_level
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL
GROUP BY raw_data['vendor']
ORDER BY total_cves DESC
LIMIT 25;

-- ==== EXPORT 5: Vulnerable Products (Top 30) ====
SELECT 
  'VULNERABLE_PRODUCTS' as analysis_category,
  raw_data['vendor'] as vendor,
  raw_data['product'] as product,
  COUNT(DISTINCT cve_id) as vulnerability_count,
  CASE 
    WHEN COUNT(DISTINCT cve_id) >= 10 THEN 'CRITICAL'
    WHEN COUNT(DISTINCT cve_id) >= 5 THEN 'HIGH'
    ELSE 'MEDIUM'
  END as severity
FROM main.cve_silver.affected_products
WHERE raw_data['vendor'] IS NOT NULL AND raw_data['product'] IS NOT NULL
GROUP BY raw_data['vendor'], raw_data['product']
ORDER BY vulnerability_count DESC
LIMIT 30;

-- ==== EXPORT 6: Status Distribution ====
SELECT 
  'STATUS_DISTRIBUTION' as analysis_category,
  state,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM main.cve_silver.cve_core
GROUP BY state;

-- ==== EXPORT 7: Data Quality Metrics ====
SELECT 
  'DATA_QUALITY' as analysis_category,
  'CVE IDs' as field_name,
  COUNT(*) as total,
  COUNT(CASE WHEN cve_id IS NOT NULL THEN 1 END) as non_null,
  ROUND(100.0 * COUNT(CASE WHEN cve_id IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_pct
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'DATA_QUALITY',
  'Dates',
  COUNT(*),
  COUNT(CASE WHEN date_published IS NOT NULL THEN 1 END),
  ROUND(100.0 * COUNT(CASE WHEN date_published IS NOT NULL THEN 1 END) / COUNT(*), 2)
FROM main.cve_silver.cve_core
UNION ALL
SELECT 
  'DATA_QUALITY',
  'Titles',
  COUNT(*),
  COUNT(CASE WHEN title IS NOT NULL THEN 1 END),
  ROUND(100.0 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 2)
FROM main.cve_silver.cve_core;

-- ============================================================================
-- INSTRUCTIONS:
-- 1. Run each section separately in Databricks
-- 2. Export each result to CSV
-- 3. Combine all CSVs for comprehensive analysis
-- OR
-- 4. Create a notebook with all queries and export results table-by-table
-- ============================================================================
