-- Gold Layer Views for Lakehouse Unimed
-- These views provide business-ready data for analytics and reporting

-- Create Gold schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS lake.gold;

-- Claims Summary View
CREATE OR REPLACE VIEW lake.gold.claims_summary AS
SELECT 
    service_year,
    service_month,
    provider_name,
    claim_amount_category,
    COUNT(*) as total_claims,
    COUNT(DISTINCT member_id) as unique_members,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_claim_amount,
    MIN(claim_amount) as min_claim_amount,
    MAX(claim_amount) as max_claim_amount,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(CASE WHEN data_quality_score >= 0.8 THEN 1 END) as high_quality_claims,
    MAX(processing_timestamp) as last_processed
FROM lake.silver.claims
WHERE service_date >= DATE '2023-01-01'
GROUP BY 
    service_year,
    service_month,
    provider_name,
    claim_amount_category;

-- Monthly Claims Trend View
CREATE OR REPLACE VIEW lake.gold.monthly_claims_trend AS
SELECT 
    service_year,
    service_month,
    DATE(service_year || '-' || LPAD(CAST(service_month AS VARCHAR), 2, '0') || '-01') as month_date,
    COUNT(*) as claims_count,
    COUNT(DISTINCT member_id) as unique_members,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    COUNT(CASE WHEN claim_amount_category = 'HIGH' THEN 1 END) as high_value_claims,
    COUNT(CASE WHEN claim_amount_category = 'VERY_HIGH' THEN 1 END) as very_high_value_claims,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score
FROM lake.silver.claims
WHERE service_date >= DATE '2023-01-01'
GROUP BY 
    service_year,
    service_month
ORDER BY 
    service_year DESC,
    service_month DESC;

-- Provider Performance View
CREATE OR REPLACE VIEW lake.gold.provider_performance AS
SELECT 
    provider_name,
    COUNT(*) as total_claims,
    COUNT(DISTINCT member_id) as unique_members,
    SUM(claim_amount) as total_billed,
    AVG(claim_amount) as avg_claim_amount,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY claim_amount) as median_claim_amount,
    COUNT(CASE WHEN claim_amount_category IN ('HIGH', 'VERY_HIGH') THEN 1 END) as high_value_claims,
    ROUND(COUNT(CASE WHEN claim_amount_category IN ('HIGH', 'VERY_HIGH') THEN 1 END) * 100.0 / COUNT(*), 2) as high_value_percentage,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score,
    MIN(service_date) as first_service_date,
    MAX(service_date) as last_service_date,
    COUNT(DISTINCT DATE(service_year || '-' || service_month || '-01')) as active_months
FROM lake.silver.claims
WHERE provider_name != 'UNKNOWN'
GROUP BY provider_name
HAVING COUNT(*) >= 10  -- Only providers with at least 10 claims
ORDER BY total_billed DESC;

-- Data Quality Dashboard View
CREATE OR REPLACE VIEW lake.gold.data_quality_dashboard AS
SELECT 
    service_year,
    service_month,
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_quality_score >= 0.9 THEN 1 END) as excellent_quality,
    COUNT(CASE WHEN data_quality_score >= 0.7 AND data_quality_score < 0.9 THEN 1 END) as good_quality,
    COUNT(CASE WHEN data_quality_score >= 0.5 AND data_quality_score < 0.7 THEN 1 END) as fair_quality,
    COUNT(CASE WHEN data_quality_score < 0.5 THEN 1 END) as poor_quality,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score,
    ROUND(COUNT(CASE WHEN data_quality_score >= 0.8 THEN 1 END) * 100.0 / COUNT(*), 2) as high_quality_percentage,
    COUNT(CASE WHEN claim_id IS NULL THEN 1 END) as missing_claim_id,
    COUNT(CASE WHEN member_id IS NULL THEN 1 END) as missing_member_id,
    COUNT(CASE WHEN service_date IS NULL THEN 1 END) as missing_service_date,
    COUNT(CASE WHEN claim_amount IS NULL OR claim_amount <= 0 THEN 1 END) as invalid_amount,
    MAX(processing_timestamp) as last_processed
FROM lake.silver.claims
GROUP BY 
    service_year,
    service_month
ORDER BY 
    service_year DESC,
    service_month DESC;

-- Member Activity View
CREATE OR REPLACE VIEW lake.gold.member_activity AS
SELECT 
    member_id,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_claim_amount,
    COUNT(DISTINCT provider_name) as providers_used,
    COUNT(CASE WHEN claim_amount_category IN ('HIGH', 'VERY_HIGH') THEN 1 END) as high_value_claims,
    MIN(service_date) as first_service_date,
    MAX(service_date) as last_service_date,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score,
    DATE_DIFF('day', MIN(service_date), MAX(service_date)) as activity_span_days,
    COUNT(DISTINCT DATE(service_year || '-' || service_month || '-01')) as active_months
FROM lake.silver.claims
WHERE member_id IS NOT NULL
GROUP BY member_id
HAVING COUNT(*) >= 2  -- Members with at least 2 claims
ORDER BY total_amount DESC;

-- Recent Activity Summary
CREATE OR REPLACE VIEW lake.gold.recent_activity AS
SELECT 
    'Last 30 Days' as period,
    COUNT(*) as claims_processed,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_name) as unique_providers,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    ROUND(AVG(data_quality_score), 3) as avg_quality,
    MAX(processing_timestamp) as last_update
FROM lake.silver.claims
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY

UNION ALL

SELECT 
    'Last 7 Days' as period,
    COUNT(*) as claims_processed,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_name) as unique_providers,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    ROUND(AVG(data_quality_score), 3) as avg_quality,
    MAX(processing_timestamp) as last_update
FROM lake.silver.claims
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY

UNION ALL

SELECT 
    'Last 24 Hours' as period,
    COUNT(*) as claims_processed,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_name) as unique_providers,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    ROUND(AVG(data_quality_score), 3) as avg_quality,
    MAX(processing_timestamp) as last_update
FROM lake.silver.claims
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' DAY;

-- Log the view creation
SELECT 'Gold views created successfully at ' || CAST(CURRENT_TIMESTAMP AS VARCHAR) as status;