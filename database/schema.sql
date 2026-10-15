-- ==============================================================================
-- FinStream Database Schema (MySQL 8+)
-- Real-Time Financial Transaction Monitoring System
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS finstream
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE finstream;

-- 1. Main Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_timestamp DATETIME(6) NOT NULL,
    merchant VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT uk_transactions_transaction_id UNIQUE (transaction_id),
    INDEX idx_transactions_account_timestamp (account_id, transaction_timestamp),
    INDEX idx_transactions_timestamp (transaction_timestamp),
    INDEX idx_transactions_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Transaction Alerts Table
-- Preserves multiple alerts (e.g. HIGH_VALUE and RAPID_SPENDING) for a single transaction
CREATE TABLE IF NOT EXISTS transaction_alerts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    alert_type VARCHAR(32) NOT NULL,
    description VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_alerts_transaction_id (transaction_id),
    INDEX idx_alerts_account_id (account_id),
    INDEX idx_alerts_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Rejected & Collision Events Table
-- Captures validation errors, unparseable JSON payloads, and ID collisions
CREATE TABLE IF NOT EXISTS rejected_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(64),
    payload TEXT NOT NULL,
    reason VARCHAR(255) NOT NULL,
    rejection_type VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_rejected_transaction_id (transaction_id),
    INDEX idx_rejected_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
