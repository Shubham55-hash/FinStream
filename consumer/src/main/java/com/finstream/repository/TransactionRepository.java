package com.finstream.repository;

import com.finstream.model.TransactionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Repository interface for MySQL persistence using JdbcTemplate.
 * Provides transactional queries for persistence, monitoring, and live dashboard stats.
 */
@Repository
public class TransactionRepository {

    private static final Logger logger = LoggerFactory.getLogger(TransactionRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public TransactionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static class TransactionRowMapper implements RowMapper<TransactionEntity> {
        @Override
        public TransactionEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            TransactionEntity entity = new TransactionEntity();
            entity.setId(rs.getLong("id"));
            entity.setTransactionId(rs.getString("transaction_id"));
            entity.setAccountId(rs.getString("account_id"));
            entity.setAmount(rs.getBigDecimal("amount"));
            entity.setCurrency(rs.getString("currency"));

            Timestamp ts = rs.getTimestamp("transaction_timestamp");
            if (ts != null) {
                entity.setTransactionTimestamp(ts.toInstant().atOffset(ZoneOffset.UTC));
            }

            entity.setMerchant(rs.getString("merchant"));
            entity.setStatus(rs.getString("status"));

            Timestamp created = rs.getTimestamp("created_at");
            if (created != null) {
                entity.setCreatedAt(created.toInstant());
            }
            return entity;
        }
    }

    /**
     * Find an existing transaction by its unique transaction ID.
     */
    public Optional<TransactionEntity> findByTransactionId(String transactionId) {
        String sql = "SELECT id, transaction_id, account_id, amount, currency, transaction_timestamp, " +
                "merchant, status, created_at FROM transactions WHERE transaction_id = ?";
        try {
            TransactionEntity entity = jdbcTemplate.queryForObject(sql, new TransactionRowMapper(), transactionId);
            return Optional.ofNullable(entity);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Insert a new transaction into MySQL.
     * Throws DuplicateKeyException if a concurrent worker inserted the same transaction_id.
     */
    public void saveTransaction(TransactionEntity entity) {
        String sql = "INSERT INTO transactions (transaction_id, account_id, amount, currency, " +
                "transaction_timestamp, merchant, status) VALUES (?, ?, ?, ?, ?, ?, ?)";

        Timestamp ts = Timestamp.from(entity.getTransactionTimestamp().toInstant());
        jdbcTemplate.update(
                sql,
                entity.getTransactionId(),
                entity.getAccountId(),
                entity.getAmount(),
                entity.getCurrency(),
                ts,
                entity.getMerchant(),
                entity.getStatus()
        );
    }

    /**
     * Persist an alert record into the transaction_alerts table.
     */
    public void saveAlert(String transactionId, String accountId, String alertType, String description) {
        String sql = "INSERT INTO transaction_alerts (transaction_id, account_id, alert_type, description) " +
                "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE description = VALUES(description)";
        jdbcTemplate.update(sql, transactionId, accountId, alertType, description);
        logger.info("[DB ALERT] Recorded alert {} for txId='{}', accountId='{}'", alertType, transactionId, accountId);
    }

    /**
     * Persist rejected event details for audit and analysis.
     */
    public void saveRejectedEvent(String transactionId, String payload, String reason, String rejectionType) {
        String safeReason = (reason != null && reason.length() > 255) ? reason.substring(0, 252) + "..." : reason;
        String sql = "INSERT INTO rejected_events (transaction_id, payload, reason, rejection_type) " +
                "VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(sql, transactionId, payload, safeReason, rejectionType);
    }

    /**
     * Count distinct transactions for an account within a closed rolling window [start, end].
     * Uses inclusive boundaries (>= start AND <= end).
     */
    public int countTransactionsInWindow(String accountId, OffsetDateTime windowStart, OffsetDateTime windowEnd) {
        String sql = "SELECT COUNT(DISTINCT transaction_id) FROM transactions " +
                "WHERE account_id = ? AND transaction_timestamp >= ? AND transaction_timestamp <= ?";

        Timestamp startTs = Timestamp.from(windowStart.toInstant());
        Timestamp endTs = Timestamp.from(windowEnd.toInstant());

        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, accountId, startTs, endTs);
        return count != null ? count : 0;
    }

    /**
     * Fetch recent transactions for live dashboard monitoring.
     */
    public List<Map<String, Object>> getRecentTransactions(int limit) {
        String sql = "SELECT transaction_id, account_id, amount, currency, transaction_timestamp, merchant, status, created_at " +
                "FROM transactions ORDER BY id DESC LIMIT ?";
        return jdbcTemplate.queryForList(sql, limit);
    }

    /**
     * Fetch recent alerts for live dashboard monitoring.
     */
    public List<Map<String, Object>> getRecentAlerts(int limit) {
        String sql = "SELECT id, transaction_id, account_id, alert_type, description, created_at " +
                "FROM transaction_alerts ORDER BY id DESC LIMIT ?";
        return jdbcTemplate.queryForList(sql, limit);
    }

    /**
     * Fetch high-level statistics for the dashboard header cards.
     */
    public Map<String, Object> getDashboardStats() {
        String txSql = "SELECT " +
                "COUNT(*) AS total_transactions, " +
                "COALESCE(SUM(amount), 0) AS total_volume_inr, " +
                "COALESCE(SUM(CASE WHEN status LIKE '%HIGH_VALUE%' THEN 1 ELSE 0 END), 0) AS high_value_count, " +
                "COALESCE(SUM(CASE WHEN status LIKE '%RAPID_SPENDING%' THEN 1 ELSE 0 END), 0) AS rapid_spending_count " +
                "FROM transactions";

        Map<String, Object> stats = jdbcTemplate.queryForMap(txSql);

        Integer totalAlerts = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM transaction_alerts", Integer.class);
        Integer totalRejected = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM rejected_events", Integer.class);

        stats.put("total_alerts", totalAlerts != null ? totalAlerts : 0);
        stats.put("total_rejected", totalRejected != null ? totalRejected : 0);
        return stats;
    }

    /**
     * Clear all records from transactions, alerts, and rejected events tables.
     * Useful for manual demo resetting from the web dashboard.
     */
    public void clearAllData() {
        jdbcTemplate.update("DELETE FROM transaction_alerts");
        jdbcTemplate.update("DELETE FROM rejected_events");
        jdbcTemplate.update("DELETE FROM transactions");
        logger.info("[DB RESET] Cleared all data from transaction_alerts, rejected_events, and transactions");
    }
}
