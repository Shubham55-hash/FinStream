package com.finstream.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Entity representing a persisted financial transaction record in MySQL.
 */
public class TransactionEntity {

    private Long id;
    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private OffsetDateTime transactionTimestamp;
    private String merchant;
    private String status;
    private Instant createdAt;

    public TransactionEntity() {
    }

    public TransactionEntity(String transactionId, String accountId, BigDecimal amount,
                             String currency, OffsetDateTime transactionTimestamp,
                             String merchant, String status) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.transactionTimestamp = transactionTimestamp;
        this.merchant = merchant;
        this.status = status;
        this.createdAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public OffsetDateTime getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void setTransactionTimestamp(OffsetDateTime transactionTimestamp) {
        this.transactionTimestamp = transactionTimestamp;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Compare this persisted entity against an incoming event.
     * Returns true if all business attributes match (idempotent duplicate),
     * or false if any field differs (ID collision).
     */
    public boolean matchesEvent(TransactionEvent event) {
        if (event == null) return false;
        if (!Objects.equals(this.accountId, event.getAccountId())) return false;
        if (this.amount == null || event.getAmount() == null || this.amount.compareTo(event.getAmount()) != 0) return false;
        if (!Objects.equals(this.currency, event.getCurrency())) return false;
        if (!Objects.equals(this.merchant, event.getMerchant())) return false;
        if (this.transactionTimestamp != null && event.getTimestamp() != null) {
            // Compare timestamps normalized to epoch milliseconds to avoid timezone offset discrepancies
            if (this.transactionTimestamp.toInstant().toEpochMilli() != event.getTimestamp().toInstant().toEpochMilli()) {
                return false;
            }
        }
        return true;
    }
}
