-- ==============================================================================
-- FinStream Analytics & Monitoring Queries (MySQL)
-- ==============================================================================
USE finstream;

-- 1. Total Transactions and Total Transaction Value (Gross Volume)
SELECT 
    COUNT(*) AS total_transactions,
    COALESCE(SUM(amount), 0.00) AS total_volume_inr,
    COALESCE(AVG(amount), 0.00) AS average_transaction_inr,
    MIN(amount) AS min_amount_inr,
    MAX(amount) AS max_amount_inr
FROM transactions;

-- 2. Transactions by Risk Status
SELECT 
    status,
    COUNT(*) AS transaction_count,
    COALESCE(SUM(amount), 0.00) AS total_amount_inr,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions), 2) AS percentage_of_total
FROM transactions
GROUP BY status
ORDER BY transaction_count DESC;

-- 3. High-Value Transactions Above INR 50,000
SELECT 
    transaction_id,
    account_id,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    status
FROM transactions
WHERE amount > 50000.00
ORDER BY amount DESC;

-- 4. Top Accounts by Total Transaction Value
SELECT 
    account_id,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_spent_inr,
    MAX(amount) AS highest_single_transaction
FROM transactions
GROUP BY account_id
ORDER BY total_spent_inr DESC
LIMIT 10;

-- 5. Transaction Counts by Time Interval (Hourly Aggregate)
SELECT 
    DATE_FORMAT(transaction_timestamp, '%Y-%m-%d %H:00:00') AS hour_window,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_volume_inr
FROM transactions
GROUP BY hour_window
ORDER BY hour_window DESC;

-- 6. Accounts that Triggered Rapid-Spending Alerts
SELECT 
    a.account_id,
    COUNT(*) AS rapid_alert_count,
    GROUP_CONCAT(DISTINCT a.transaction_id ORDER BY a.created_at SEPARATOR ', ') AS flagged_transaction_ids,
    MAX(a.created_at) AS latest_alert_time
FROM transaction_alerts a
WHERE a.alert_type = 'RAPID_SPENDING'
GROUP BY a.account_id
ORDER BY rapid_alert_count DESC;

-- 7. Duplicate / Collision and Rejected Event Counts
SELECT 
    rejection_type,
    COUNT(*) AS count,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen
FROM rejected_events
GROUP BY rejection_type
ORDER BY count DESC;

-- 8. Latest 10 Transactions for a Given Account (e.g. ACC101 or parameterized)
SELECT 
    transaction_id,
    account_id,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    status,
    created_at
FROM transactions
WHERE account_id = 'ACC101'
ORDER BY transaction_timestamp DESC
LIMIT 10;
