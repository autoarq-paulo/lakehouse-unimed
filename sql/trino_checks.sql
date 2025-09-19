-- Trino Health Checks and Smoke Tests
-- Run these commands to verify the Lakehouse setup

-- Basic connectivity and catalog checks
SELECT 'Trino connectivity test' as test_name, CURRENT_TIMESTAMP as timestamp;

-- Show available catalogs
SHOW CATALOGS;

-- Show schemas in lake catalog
SHOW SCHEMAS FROM lake;

-- Show tables in silver schema
SHOW TABLES FROM lake.silver;

-- Show tables in gold schema (views)
SHOW TABLES FROM lake.gold;

-- Describe silver claims table structure
DESCRIBE lake.silver.claims;

-- Count records in silver claims table
SELECT 
    'Silver Claims Count' as metric,
    COUNT(*) as value,
    CURRENT_TIMESTAMP as checked_at
FROM lake.silver.claims;

-- Sample data from silver claims
SELECT 
    'Silver Claims Sample' as test_name,
    claim_id,
    member_id,
    provider_name,
    claim_amount,
    service_date,
    claim_amount_category,
    data_quality_score,
    processing_timestamp
FROM lake.silver.claims 
ORDER BY processing_timestamp DESC
LIMIT 5;

-- Test Gold views
SELECT 
    'Gold Claims Summary Sample' as test_name,
    service_year,
    service_month,
    provider_name,
    total_claims,
    total_amount,
    avg_claim_amount
FROM lake.gold.claims_summary 
ORDER BY total_amount DESC
LIMIT 5;

-- Monthly trend data
SELECT 
    'Monthly Trend Sample' as test_name,
    month_date,
    claims_count,
    unique_members,
    total_amount,
    avg_quality_score
FROM lake.gold.monthly_claims_trend 
ORDER BY month_date DESC
LIMIT 5;

-- Provider performance top 5
SELECT 
    'Top Providers by Volume' as test_name,
    provider_name,
    total_claims,
    total_billed,
    avg_claim_amount,
    high_value_percentage
FROM lake.gold.provider_performance 
ORDER BY total_claims DESC
LIMIT 5;

-- Data quality metrics
SELECT 
    'Data Quality Summary' as test_name,
    service_year,
    service_month,
    total_records,
    high_quality_percentage,
    avg_quality_score,
    last_processed
FROM lake.gold.data_quality_dashboard 
ORDER BY service_year DESC, service_month DESC
LIMIT 3;

-- Recent activity summary
SELECT 
    'Recent Activity' as test_name,
    period,
    claims_processed,
    unique_members,
    total_amount,
    avg_quality,
    last_update
FROM lake.gold.recent_activity
ORDER BY 
    CASE 
        WHEN period = 'Last 24 Hours' THEN 1
        WHEN period = 'Last 7 Days' THEN 2
        WHEN period = 'Last 30 Days' THEN 3
        ELSE 4
    END;

-- Table statistics
SELECT 
    'Table Statistics' as test_name,
    'silver.claims' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_name) as unique_providers,
    MIN(service_date) as earliest_service,
    MAX(service_date) as latest_service,
    MIN(processing_timestamp) as first_processed,
    MAX(processing_timestamp) as last_processed
FROM lake.silver.claims;

-- Data distribution by amount category
SELECT 
    'Amount Category Distribution' as test_name,
    claim_amount_category,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    SUM(claim_amount) as total_amount
FROM lake.silver.claims
GROUP BY claim_amount_category
ORDER BY count DESC;

-- Quality score distribution
SELECT 
    'Quality Score Distribution' as test_name,
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN data_quality_score >= 0.7 THEN 'Good (0.7-0.89)'
        WHEN data_quality_score >= 0.5 THEN 'Fair (0.5-0.69)'
        ELSE 'Poor (<0.5)'
    END as quality_range,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM lake.silver.claims
GROUP BY 
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN data_quality_score >= 0.7 THEN 'Good (0.7-0.89)'
        WHEN data_quality_score >= 0.5 THEN 'Fair (0.5-0.69)'
        ELSE 'Poor (<0.5)'
    END
ORDER BY count DESC;

-- Check for any NULL or problematic data
SELECT 
    'Data Issues Check' as test_name,
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) as null_claim_ids,
    SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) as null_member_ids,
    SUM(CASE WHEN service_date IS NULL THEN 1 ELSE 0 END) as null_service_dates,
    SUM(CASE WHEN claim_amount IS NULL OR claim_amount < 0 THEN 1 ELSE 0 END) as invalid_amounts,
    SUM(CASE WHEN provider_name = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown_providers,
    COUNT(*) as total_records
FROM lake.silver.claims;

-- Final status check
SELECT 
    'Health Check Complete' as status,
    CURRENT_TIMESTAMP as completed_at,
    'All checks executed successfully' as message;