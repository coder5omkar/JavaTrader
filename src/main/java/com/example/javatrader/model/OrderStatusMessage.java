package com.example.javatrader.model;

import jakarta.persistence.*;

@Entity
@Table(name = "order_status")
public class OrderStatusMessage {
    @Id
    private String orderId;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private long timestamp;
    private String securityId;

    @Enumerated(EnumType.STRING)
    private TransactionType transactionType;

    private float ltp;
    private float executedPrice;
    private int executedQuantity;
    private String rejectionReason;

    // Enums for better type safety
    public enum OrderStatus {
        PENDING, TRADED, REJECTED, CANCELLED, PARTIALLY_FILLED
    }

    public enum TransactionType {
        BUY, SELL
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        this.securityId = securityId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public float getLtp() {
        return ltp;
    }

    public void setLtp(float ltp) {
        this.ltp = ltp;
    }

    public float getExecutedPrice() {
        return executedPrice;
    }

    public void setExecutedPrice(float executedPrice) {
        this.executedPrice = executedPrice;
    }

    public int getExecutedQuantity() {
        return executedQuantity;
    }

    public void setExecutedQuantity(int executedQuantity) {
        this.executedQuantity = executedQuantity;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }

    public void setRejectionReason(String rejectionReason) {
        this.rejectionReason = rejectionReason;
    }

    public OrderStatusMessage(String orderId, OrderStatus status, long timestamp, String securityId, TransactionType transactionType, float ltp, float executedPrice, int executedQuantity, String rejectionReason) {
        this.orderId = orderId;
        this.status = status;
        this.timestamp = timestamp;
        this.securityId = securityId;
        this.transactionType = transactionType;
        this.ltp = ltp;
        this.executedPrice = executedPrice;
        this.executedQuantity = executedQuantity;
        this.rejectionReason = rejectionReason;
    }

    public OrderStatusMessage() {
    }
}

