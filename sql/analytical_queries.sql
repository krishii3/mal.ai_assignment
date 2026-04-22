-- =====================================================================
-- Analytical queries against the unified canonical model
-- Run with:  duckdb -c "...query..."  or from Python: duckdb.sql(...)
-- Data source: data/output/unified_payments_v2.parquet
-- =====================================================================


-- Q1. Daily payment volume + AED total, per payment type
--     Downstream use: finance team's daily P&L dashboard
SELECT
    CAST(timestamp AS DATE)        AS txn_date,
    payment_type,
    COUNT(*)                        AS txn_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_amount_aed
FROM 'data/output/unified_payments_v2.parquet'
WHERE currency = 'AED'
GROUP BY txn_date, payment_type
ORDER BY txn_date DESC, payment_type;


-- Q2. Failure rate by source system (data quality / ops health)
SELECT
    source_system,
    COUNT(*)                                                  AS total,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)        AS failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)
                  / COUNT(*), 2)                              AS failure_pct
FROM 'data/output/unified_payments_v2.parquet'
GROUP BY source_system
ORDER BY failure_pct DESC;


-- Q3. Top 10 bill categories by total AED (biller concentration risk)
--     Uses the v2 payment_metadata JSON column, demonstrating how type-specific
--     attributes remain queryable without schema inheritance joins.
SELECT
    payment_metadata->>'biller_category' AS biller_category,
    COUNT(*)                              AS payments,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_aed
FROM 'data/output/unified_payments_v2.parquet'
WHERE payment_type = 'BILL_PAYMENT'
GROUP BY biller_category
ORDER BY total_aed DESC
LIMIT 10;


-- Q4. Per-customer payment frequency + rank (window function)
SELECT
    customer_id,
    COUNT(*)                                          AS payment_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2)      AS total_aed,
    RANK() OVER (ORDER BY COUNT(*) DESC)              AS frequency_rank
FROM 'data/output/unified_payments_v2.parquet'
WHERE status = 'COMPLETED' AND currency = 'AED'
GROUP BY customer_id
ORDER BY frequency_rank
LIMIT 10;


-- Q5. Schema version distribution (contract rollout monitoring)
SELECT
    schema_version,
    COUNT(*) AS events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM 'data/output/unified_payments_v2.parquet'
GROUP BY schema_version;


-- Q6. Currency mix — FX exposure for risk team
SELECT
    currency,
    COUNT(*) AS txn_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_in_native_ccy
FROM 'data/output/unified_payments_v2.parquet'
GROUP BY currency
ORDER BY txn_count DESC;
